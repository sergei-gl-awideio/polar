# Promotion after the storage owner's logindex numbering has been reset.
# Inject the reset explicitly: this tests the mismatch, not a presumed natural
# trigger involving timeline changes. Keep the replica's memory ring alive.
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(time);

local $ENV{PGCTLTIMEOUT} = 30;
my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->polar_init_primary;
my $replica = PostgreSQL::Test::Cluster->new('replica');
$replica->polar_init_replica($primary);
for my $node ($primary, $replica)
{
	$node->append_conf('postgresql.conf', q[
shared_preload_libraries = 'polar_vfs,polar_io_stat,polar_worker,test_logindex'
logging_collector = off
restart_after_crash = off
autovacuum = off
checkpoint_timeout = '1h'
max_wal_size = '8GB'
]);
}
$replica->append_conf('postgresql.conf', 'polar_logindex_mem_size = 8MB');
$primary->start;
$primary->polar_create_slot($replica->name);
$replica->start;
$primary->safe_psql('postgres', q[
CREATE EXTENSION test_logindex;
CREATE TABLE promote_reset AS SELECT generate_series(1,400000) AS i;
]);
for (1 .. 4)
{
	$primary->safe_psql('postgres', 'UPDATE promote_reset SET i = i + 1');
}
# A memory-only repair must retain everything needed after checkpoint.redo.
# Incremental checkpoints can initially lag the just-completed workload.
my $workload_end = $primary->lsn('insert');
my $covered = 0;
for (1 .. 30)
{
	$primary->safe_psql('postgres', 'CHECKPOINT');
	$covered = $primary->safe_psql('postgres',
		"SELECT redo_lsn >= '$workload_end'::pg_lsn FROM pg_control_checkpoint()") eq 't';
	last if $covered;
	select undef, undef, undef, 0.1;
}
ok($covered, 'checkpoint has advanced past the workload') or BAIL_OUT('checkpoint did not advance');
$primary->wait_for_catchup($replica);
my $expected = $primary->safe_psql('postgres', 'SELECT count(*), sum(i) FROM promote_reset');
$primary->stop;

my $dir = $primary->polar_get_datadir . '/pg_logindex';
my $meta_hex = $replica->safe_psql('postgres', "SELECT encode(test_logindex_reset_meta(), 'hex')");
ok(length($meta_hex) > 0, 'replica has wrapped its ring and retains higher table IDs');
open my $meta, '>:raw', "$dir/log_index_meta.reset" or die "open metadata: $!";
print {$meta} pack('H*', $meta_hex);
close $meta or die "close metadata: $!";
rename "$dir/log_index_meta.reset", "$dir/log_index_meta" or die "replace metadata: $!";
my @files = (glob("$dir/*.tbl"), glob("$dir/[0-9A-F][0-9A-F][0-9A-F][0-9A-F]"));
ok(@files > 0, 'old logindex files exist before the simulated reset');
for my $file (@files)
{
	unlink $file or die "unlink $file: $!";
}

$replica->promote;
ok($replica->poll_query_until('postgres', 'SELECT NOT pg_is_in_recovery()', 't', 30),
	'promotion completes after the numbering reset');
my ($stdout, $stderr) = ('', '');
my $checkpoint = $replica->psql('postgres', 'CHECKPOINT',
	stdout => \$stdout, stderr => \$stderr, timeout => 30);
is($checkpoint, 0, 'checkpoint succeeds after promotion') or diag($stderr);
is($replica->safe_psql('postgres', 'SELECT count(*), sum(i) FROM promote_reset'),
	$expected, 'all data remains readable after promotion');

cmp_ok($replica->safe_psql('postgres', 'SELECT test_logindex_promoted_storage()'),
	'>', 0, 'repaired table segments and blooms can be read from disk');

my $start = time();
my $stopped = $replica->stop('fast', fail_ok => 1);
ok($stopped, 'fast shutdown completes');
cmp_ok(time() - $start, '<', 30, 'shutdown completes before the deadline');
my $log = slurp_file($replica->logfile);
unlike($log, qr/abnormal database system shutdown|could not complete checkpoint|PANIC:/,
	'shutdown did not conceal a failed checkpoint or crash');
like($log, qr/database system is shut down/, 'server reports a clean shutdown');
if (!$stopped)
{
	$replica->stop('immediate', fail_ok => 1);
	BAIL_OUT('fast shutdown failed');
}
$replica->start;
is($replica->safe_psql('postgres', 'SELECT count(*), sum(i) FROM promote_reset'),
	$expected, 'restart reads the same data');
$replica->safe_psql('postgres', 'UPDATE promote_reset SET i = i + 1; CHECKPOINT');
$replica->stop;
done_testing();
