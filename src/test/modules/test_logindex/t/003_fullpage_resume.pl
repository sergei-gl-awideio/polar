# Recovery must not reuse snapshots, even with holes or no usable logindex.
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

use constant PAGES_PER_SEGMENT => 256 * 1024 * 1024 / 8192;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->polar_init_primary;
$node->append_conf('postgresql.conf', q[
shared_preload_libraries = 'test_logindex'
logging_collector = off
]);
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION test_logindex');

is($node->safe_psql('postgres', 'SELECT test_fullpage_replay_counter()'),
	'0', 'replay keeps the maximum plus one');

# A private directory lets us test errors without disturbing live snapshots.
my $dir = $node->polar_get_datadir . '/test_fullpage_resume';
mkdir $dir or die "mkdir $dir: $!";
is($node->safe_psql('postgres', 'SELECT test_fullpage_resume(0)'),
	'0', 'an empty directory starts at zero');
is($node->safe_psql('postgres', 'SELECT test_fullpage_resume(123)'),
	'123', 'an empty directory preserves the replay counter');

for my $name ('00000002.fp', '00000009.fp', 'fullpagetemp.999',
	'FFFFFFFF.fp.backup', 'not-a-segment')
{
	open(my $fh, '>', "$dir/$name") or die "create $name: $!";
	close $fh or die "close $name: $!";
}
is($node->safe_psql('postgres', 'SELECT test_fullpage_resume(0)'),
	10 * PAGES_PER_SEGMENT, 'scan crosses holes and ignores unrelated files');
my $floor = 20 * PAGES_PER_SEGMENT + 123;
is($node->safe_psql('postgres', "SELECT test_fullpage_resume($floor)"),
	$floor, 'a replay counter in a hole remains a lower bound');

chmod 0000, $dir or die "chmod $dir: $!";
my ($stdout, $stderr);
my $rc = $node->psql('postgres', 'SELECT test_fullpage_resume(0)',
	stdout => \$stdout, stderr => \$stderr);
chmod 0700, $dir or die "restore permissions on $dir: $!";
isnt($rc, 0, 'directory access failure rejects recovery');
like($stderr, qr/could not open directory/, 'directory failure is reported');

# Leave real FPSI records after the redo point, then crash and replay them.
$node->safe_psql('postgres', 'SELECT test_fullpage()');
my $before = $node->safe_psql('postgres', 'SELECT test_fullpage_max_no()');
my $fpdir = $node->polar_get_datadir . '/polar_fullpage';
opendir(my $dh, $fpdir) or die "opendir $fpdir: $!";
my @segments = map { hex(substr($_, 0, -3)) }
	grep { /^[0-9A-F]{8,16}\.fp$/ } readdir($dh);
closedir $dh;
my $highest = (sort { $b <=> $a } @segments)[0];
$node->stop('immediate');
$node->start;
my $resumed = $node->safe_psql('postgres', 'SELECT test_fullpage_max_no()');
cmp_ok($resumed, '>=', $before, 'crash recovery never moves allocation backwards');
cmp_ok($resumed, '>=', ($highest + 1) * PAGES_PER_SEGMENT,
	'crash recovery resumes beyond every pre-crash segment');

$node->stop;
done_testing();
