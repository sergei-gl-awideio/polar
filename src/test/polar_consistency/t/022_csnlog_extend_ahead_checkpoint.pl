# 022_csnlog_extend_ahead_checkpoint.pl
#	  Test that a checkpoint cannot complete while a CSNLOG batch is
#	  half-zeroed by polar_csnlog_extend_ahead().
#
#	  The batch's zero-page record goes to WAL before its pages are zeroed.
#	  If a checkpoint took its redo point after the record but wrote out only
#	  the pages zeroed so far, hot standby recovery from it would replay
#	  commits into the remaining pages and fail with a short read of the
#	  CSNLOG segment.
#
# IDENTIFICATION
#	  src/test/polar_consistency/t/022_csnlog_extend_ahead_checkpoint.pl

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_fault_injector} eq 'no')
{
	plan skip_all => 'Fault injector not supported by this build';
}

# CSNLOG geometry: 1024 XIDs per page, batches of 128 pages, 32-page segments.
my $xids_per_page = 1024;
my $batch_pages = 128;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init(allows_streaming => 1);
$node->append_conf(
	'postgresql.conf', qq(
polar_csn_enable = on
polar_csnlog_slot_size = 4096
autovacuum = off
checkpoint_timeout = '1h'
max_wal_size = '8GB'
));
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION IF NOT EXISTS faultinjector');

sub next_xid
{
	return $node->safe_psql('postgres',
		'SELECT pg_snapshot_xmax(pg_current_snapshot())');
}

sub consume_xids_sql
{
	my ($n) = @_;
	return
	  "DO \$\$ BEGIN FOR i IN 1..$n LOOP PERFORM pg_current_xact_id(); COMMIT; END LOOP; END \$\$;";
}

# Get just below the middle of batch 1 (pages 128..255).  Crossing into it
# zeroes it synchronously, which gives polar_csnlog_extend_ahead() its base.
my $batch1_middle = ($batch_pages + $batch_pages / 2) * $xids_per_page;
$node->safe_psql('postgres',
	consume_xids_sql($batch1_middle - 2000 - next_xid()));
note "next XID before the ahead zeroing: " . next_xid();

# Suspend the ahead zeroing of batch 2 (pages 256..383) before page 326,
# i.e. partway into the segment holding pages 320..351.
my $suspend_page = 2 * $batch_pages + 70;
$node->safe_psql('postgres',
	"SELECT inject_fault('polar_csnlog_zero_batch_page', 'suspend', '', '', 71, 71, 0)"
);

# This session is the only one allocating XIDs, so it does the ahead zeroing,
# then goes on to commit XIDs on the pages that were not zeroed yet.
my $xact = $node->background_psql('postgres');
$xact->query_until(qr/started/,
	"\\echo started\n" . consume_xids_sql(150000) . "\n");

$node->safe_psql('postgres',
	"SELECT wait_until_triggered_fault('polar_csnlog_zero_batch_page', 1)");

my $ckpt_lsn = $node->safe_psql('postgres',
	'SELECT checkpoint_lsn FROM pg_control_checkpoint()');
my $ckpt = $node->background_psql('postgres');
$ckpt->query_until(qr/checkpointing/, "\\echo checkpointing\nCHECKPOINT;\n");

# Wait for the checkpoint to either block or (wrongly) complete.
my $waiting_sql =
  "SELECT count(*) = 1 FROM pg_stat_activity WHERE backend_type = 'checkpointer' AND wait_event = 'CheckpointDelayStart'";
$node->poll_query_until('postgres',
	"SELECT ($waiting_sql) OR checkpoint_lsn <> '$ckpt_lsn' FROM pg_control_checkpoint()"
) or die "checkpoint neither waited nor completed";
is($node->safe_psql('postgres', $waiting_sql),
	't', 'checkpoint waits for the CSNLOG batch to be zeroed');

$node->safe_psql('postgres',
	"SELECT inject_fault('polar_csnlog_zero_batch_page', 'resume')");
$node->safe_psql('postgres',
	"SELECT inject_fault('polar_csnlog_zero_batch_page', 'reset')");

$ckpt->quit;
$xact->quit;

note "next XID after commits: " . next_xid();
cmp_ok(next_xid(), '>', ($suspend_page + 1) * $xids_per_page,
	'committed XIDs past the suspended page');

# The commits were synchronous, so their WAL is flushed up to here.
my $commits_lsn =
  $node->safe_psql('postgres', 'SELECT pg_current_wal_flush_lsn()');

# Hot standby recovery from that checkpoint replays the commits into CSNLOG.
$node->stop('immediate');
$node->set_standby_mode;
my $log_offset = -s $node->logfile;
my $started = $node->start(fail_ok => 1);
ok($started, 'hot standby recovery starts');
is($node->safe_psql('postgres', 'SELECT pg_is_in_recovery()'),
	't', 'node is in hot standby')
  if $started;
ok( $started
	  && $node->poll_query_until('postgres',
		"SELECT pg_last_wal_replay_lsn() >= '$commits_lsn'::pg_lsn"),
	'replay reaches the commits');
unlike(
	slurp_file($node->logfile, $log_offset),
	qr/could not access status of transaction/,
	'no CSNLOG read failure during replay');

$node->stop if $started;
done_testing();
