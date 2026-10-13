# 004_fullpage_inflight_crash.pl
#	  A writer publishes its reservation into a shared in-flight slot before it
#	  takes a fullpage_no, and remove_old_fullpage_files() refuses to sweep
#	  below the oldest slot still occupied.  A slot that outlived the process
#	  that published it would therefore pin the sweep floor forever: nothing
#	  under that number would ever be reclaimed or recycled again.
#
#	  polar_fullpage_inflight_shmem_exit() clears the slot on every ordinary
#	  exit, but a backend killed outright never runs it - quickdie() skips
#	  proc_exit(), and SIGKILL skips everything.  What covers that is one level
#	  up: the postmaster answers an abnormal child exit with a crash restart,
#	  and reset_shared() builds shared memory again from nothing, so the slots
#	  come back as they were initialised.  This test holds that claim down.
#
# IDENTIFICATION
#	  src/test/modules/test_logindex/t/004_fullpage_inflight_crash.pl

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('primary');
$node->polar_init_primary;
# PostgreSQL::Test::Cluster::init turns restart_after_crash off, which would
# make the postmaster shut down instead of reinitialising.  The crash restart
# is the whole subject here.
$node->append_conf(
	'postgresql.conf', q[
    shared_preload_libraries = 'test_logindex'
    logging_collector=off
    restart_after_crash=on
]);
$node->start;

my $db = 'postgres';
$node->safe_psql($db, 'CREATE EXTENSION test_logindex;');

# Nothing here writes future pages, which need a replica to set
# polar_oldest_apply_lsn, so every slot should be idle to start with.
is($node->safe_psql($db, 'select test_fullpage_inflight_occupied();'),
	'0', 'no in-flight reservations on a quiet node');

# A session that publishes a reservation and then stays alive holding it.
my $bg = $node->background_psql($db);
my $pid = $bg->query_safe('select pg_backend_pid()');
$bg->query_safe('select test_fullpage_publish_inflight(4242)');

is($node->safe_psql($db, 'select test_fullpage_inflight_occupied();'),
	'1', 'the published reservation is visible to other backends');

# Kill a backend rather than stopping the node.  stop('immediate') takes the
# postmaster down with it, and the start after it is a new process that gets
# fresh shared memory for free, so the check below could not fail: it would
# assert nothing.  An abnormal child exit instead makes the running postmaster
# reinitialise in place, which is the only path on which a slot could have
# survived at all.  SIGKILL rather than SIGQUIT because it is the stronger
# case - quickdie() at least runs, and neither of them puts the slot back.
my $offset = -s $node->logfile;
kill 'KILL', $pid;

$node->poll_query_until($db, 'select true;')
  or die 'node did not come back after the crash restart';

my $log = slurp_file($node->logfile, $offset);
like($log, qr/all server processes terminated; reinitializing/,
	'the postmaster crash restarted rather than reaping one backend');

is($node->safe_psql($db, 'select test_fullpage_inflight_occupied();'),
	'0', 'the crash restart left no reservation behind to pin the sweep');

# The killed session's psql is gone; do not try to shut it down politely.
$bg->{stdin} = '' if defined $bg->{stdin};

$node->stop;
done_testing();
