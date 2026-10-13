# 002_fullpage_wal_timeline.pl
#	  The fullpage code finds the wal file holding an lsn by building its name
#	  from ThisTimeLineID.  That global is declared in xlog.c and assigned
#	  nowhere: the only writes to it are the side effect of two
#	  GetXLogReplayRecPtr(&ThisTimeLineID) calls, both on replica only paths.
#	  So in a primary's startup process and checkpointer it is still 0, every
#	  such name comes out on timeline 0, and no file by that name exists.
#
#	  polar_logindex_calc_max_fullpage_no() reacts to a missing wal file by
#	  resetting max_fullpage_no to 0, which is destructive: the next future
#	  page write re-issues fullpage_no 0 and overwrites the fullpage space from
#	  the bottom.
#
#	  Read the timeline straight out of the wal file name it reports.
#
# IDENTIFICATION
#	  src/test/modules/test_logindex/t/002_fullpage_wal_timeline.pl

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->polar_init_primary;
# Only two settings, and the second one is the whole reason this file can read
# anything: polar_init_primary turns logging_collector on, which would send the
# startup process's output to its own log directory rather than to
# $node->logfile, where the checks below look.  Everything else this test wants
# (polar_logindex_mem_size, max_worker_processes) is already in that same
# default block in PostgreSQL::Test::Cluster.
$node->append_conf(
	'postgresql.conf', q[
    shared_preload_libraries = 'test_logindex'
    logging_collector=off
]);
$node->start;

my $db = 'postgres';
$node->safe_psql($db, 'CREATE EXTENSION test_logindex;');

# Leaves the fullpage logindex holding real records, and puts the table that
# holds them on storage: the meta file only learns a max lsn from a table that
# has gone inactive, and without one a restart brings the snapshot back with
# nothing for the function under test to look up.
$node->safe_psql($db, 'select test_fullpage();');
$node->safe_psql($db, 'select test_fullpage_roll_table();');
$node->safe_psql($db, 'CHECKPOINT;');

my $offset = -s $node->logfile;
$node->restart;

# polar_logindex_calc_max_fullpage_no() runs in the startup process at the end
# of StartupXLOG().  Read what it did from its own log lines.
my $log = slurp_file($node->logfile, $offset);
my ($wal) = $log =~ /fullpage_no reset to 0: no wal file "([^"]+)"/;
my ($resumed) = $log =~ /redo done at max_fullpage_no = (\d+)/;

# The function logs one line or the other on every run, so requiring one of
# them is what stops the rest of this file reporting success against a log it
# never actually read.
ok(defined $wal || defined $resumed,
	'the startup process reported what it did with max_fullpage_no');

if (defined $wal)
{
	# The name is <tli><log><seg>, so the timeline is its first field.  A reset
	# on timeline 0 is the bug; a reset on a real timeline is the promotion
	# case the fix does not cover.
	my ($name) = $wal =~ m{([0-9A-F]{24})$};
	my $tli = defined $name ? substr($name, 0, 8) : 'unparsed';

	note("startup process reset the counter, wal file looked for: $wal");
	isnt($tli, '00000000',
		'the wal file name was built on a real timeline, not timeline 0');
}

note("startup process resumed at max_fullpage_no = "
	  . (defined $resumed ? $resumed : 'no line'));

ok(defined $resumed && $resumed ne '0',
	'the startup process resumed a non-zero max_fullpage_no');

$node->stop;
done_testing();
