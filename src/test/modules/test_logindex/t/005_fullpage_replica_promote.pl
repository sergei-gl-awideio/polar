# 005_fullpage_replica_promote.pl
#	  The two cases a single node cannot show, both about resuming the
#	  fullpage_no counter without aliasing snapshots somebody else still reads.
#
#	  A replica shares the primary's storage and reads fullpage snapshots out of
#	  it by number, so a counter that resumes too low overwrites pages under a
#	  reader rather than merely wasting space.  Two ways in:
#
#	  A crash restart with the replica still attached.  The primary rebuilds
#	  shared memory, replays, and resumes the counter while the replica holds a
#	  slot and keeps reading.
#
#	  A promotion.  The replica has tracked max_fullpage_no through replay, so
#	  promotion must resume beyond existing segments as well as its replay
#	  counter, since older retained records need not be replayed.
#
# IDENTIFICATION
#	  src/test/modules/test_logindex/t/005_fullpage_replica_promote.pl

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# FULLPAGE_SEGMENT_SIZE / BLCKSZ, from access/polar_fullpage.h.
use constant FULLPAGE_NUM_PER_FILE => 256 * 1024 * 1024 / 8192;

my $db = 'postgres';

my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->polar_init_primary;

my $node_replica = PostgreSQL::Test::Cluster->new('replica');
$node_replica->polar_init_replica($node_primary);

# restart_after_crash is off by default in PostgreSQL::Test::Cluster::init, and
# the primary reinitialising under an attached replica is half of what this
# file is for.
foreach my $node ($node_primary, $node_replica)
{
	$node->append_conf(
		'postgresql.conf', q[
    shared_preload_libraries = 'test_logindex'
    logging_collector=off
    restart_after_crash=on
]);
}

$node_primary->start;
$node_primary->polar_create_slot($node_replica->name);
$node_replica->start;

$node_primary->safe_psql($db, 'CREATE EXTENSION test_logindex;');

# Real PWT_FPSI records, which the replica replays and counts.
$node_primary->safe_psql($db, 'select test_fullpage();');
$node_primary->wait_for_catchup($node_replica);

my $written = $node_primary->safe_psql($db, 'select test_fullpage_max_no();');
note("primary counter after writing = $written");

# Primary and replica share polar storage, so this one listing describes the
# segments both of them can reach.
my $fpdir = $node_primary->polar_get_datadir . '/polar_fullpage';
opendir(my $dh, $fpdir) or die "could not open \"$fpdir\": $!";
my %existing = map { $_ => 1 } grep { /\.fp$/ } readdir($dh);
closedir($dh);

my $segfile = sub { sprintf('%08X.fp', int($_[0] / FULLPAGE_NUM_PER_FILE)) };

ok($existing{ $segfile->($written - 1) },
	'the segment holding the last fullpage_no written is on shared storage');

# The replica replayed those records, so its own counter moved.  This is what
# makes the promotion below land on the replay-tracked branch.
my $replayed = $node_replica->safe_psql($db, 'select test_fullpage_max_no();');
note("replica counter from replay = $replayed");
cmp_ok($replayed, '>', 0, 'the replica tracked max_fullpage_no through replay');

### 1. crash restart with the replica still attached

my $bg = $node_primary->background_psql($db);
my $pid = $bg->query_safe('select pg_backend_pid()');
$bg->query_safe('select test_fullpage_publish_inflight(4242)');

# A backend, not the node: stopping the primary would restart it as a new
# process with fresh shared memory, and the replica would see it go away rather
# than reinitialise underneath.  See 004 for the longer version.
my $offset = -s $node_primary->logfile;
kill 'KILL', $pid;

$node_primary->poll_query_until($db, 'select true;')
  or die 'the primary did not come back after the crash restart';

my $log = slurp_file($node_primary->logfile, $offset);
like($log, qr/all server processes terminated; reinitializing/,
	'the primary crash restarted under an attached replica');

is($node_primary->safe_psql($db, 'select test_fullpage_inflight_occupied();'),
	'0', 'the crash restart left no reservation pinning the sweep');

my ($resumed) = $log =~ /redo done at max_fullpage_no = (\d+)/;
note("primary resumed at max_fullpage_no = "
	  . (defined $resumed ? $resumed : 'no line'));

ok(defined $resumed && !$existing{ $segfile->($resumed) },
	'the primary resumed outside every segment the replica can still read');

# The replica is meant to ride the restart out and keep answering.  Poll rather
# than ask once: it shares storage with a postmaster that just reinitialised.
ok($node_replica->poll_query_until($db, 'select true;'),
	'the replica survived the primary crash restart');

### 2. promotion

$node_primary->stop('i');

$offset = -s $node_replica->logfile;
$node_replica->promote;
$node_replica->polar_wait_for_startup(60);

$log = slurp_file($node_replica->logfile, $offset);

like($log, qr/redo done at max_fullpage_no =/,
	'the promoted node established its allocation frontier');

my ($promoted) = $log =~ /redo done at max_fullpage_no = (\d+)/;
note("promoted node resumed at max_fullpage_no = "
	  . (defined $promoted ? $promoted : 'no line'));

ok(defined $promoted, 'the promoted node reported a resumed fullpage_no');

SKIP:
{
	skip 'no resumed fullpage_no to check', 2 unless defined $promoted;

	cmp_ok($promoted, '>=', $replayed,
		'the promoted node did not resume below what it had replayed');
	ok(!$existing{ $segfile->($promoted) },
		'the promoted node resumed outside every segment already written');
}

$node_replica->stop;
done_testing();
