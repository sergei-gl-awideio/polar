# 019_vacuum_stream_replica_promote.pl
#	  Streaming VACUUM with an attached replica, and right after promoting it.
#
# VACUUM reads the heap through read streams, so a single read covers up to
# io_combine_limit blocks and WaitReadBuffers() replays each of them from the
# logindex when needed.  Check both sides of that:
#
# 1. VACUUM on the primary, with several index cycles, while a replica is
#    attached; the replica must see the same, consistent data.
# 2. VACUUM on the replica right after it was promoted with the primary's
#    latest changes not yet flushed to shared storage, so the pages VACUUM
#    reads must be replayed as they are read.
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $db = 'postgres';

# Every check returns a count of problems found, so all must be 0.
sub check_integrity
{
	my ($node, $table, $what) = @_;

	foreach my $check (
		"SELECT count(*) FROM pg_check_visible('$table')",
		"SELECT count(*) FROM pg_check_frozen('$table')",
		"SELECT count(*) FROM verify_heapam('$table')",
		"SELECT count(*) FROM (SELECT bt_index_check(indexrelid, true) FROM pg_index WHERE indrelid = '$table'::regclass) s WHERE false")
	{
		is($node->safe_psql($db, $check), '0', "$what: $check");
	}
}

foreach my $io_combine_limit (16, 32)
{
	my $node_primary = PostgreSQL::Test::Cluster->new("primary_$io_combine_limit");
	$node_primary->polar_init_primary;

	my $node_replica = PostgreSQL::Test::Cluster->new("replica_$io_combine_limit");
	$node_replica->polar_init_replica($node_primary);

	foreach my $node ($node_primary, $node_replica)
	{
		# A large checkpoint distance keeps the primary's changes in its
		# buffers, so the promoted replica must replay them on read.
		$node->append_conf(
			'postgresql.conf', qq[
autovacuum = off
shared_buffers = 256MB
checkpoint_timeout = 1h
max_wal_size = 4GB
io_combine_limit = $io_combine_limit
]);
	}

	$node_primary->start;
	$node_primary->polar_create_slot($node_replica->name);
	$node_replica->start;

	$node_primary->safe_psql($db,
		'CREATE EXTENSION amcheck; CREATE EXTENSION pg_visibility; CREATE EXTENSION pg_buffercache;');

	foreach my $table ('t_cycles', 't_promote')
	{
		$node_primary->safe_psql(
			$db, qq[
CREATE TABLE $table (id int, k int, pad text) WITH (autovacuum_enabled = off);
INSERT INTO $table SELECT g, g % 1000, repeat('p', 60) FROM generate_series(1, 300000) g;
CREATE INDEX ${table}_id ON $table (id);
CREATE INDEX ${table}_k ON $table (k);
]);
	}
	$node_primary->safe_psql($db, 'CHECKPOINT');

	### 1. VACUUM on the primary with the replica attached

	$node_primary->safe_psql($db, 'DELETE FROM t_cycles WHERE id % 3 <> 0');

	# The replica's feedback holds back the primary's horizon through its
	# slot; wait until the deleted tuples are removable.
	my $delete_xid = $node_primary->safe_psql($db, 'SELECT txid_current()');
	$node_primary->wait_for_catchup($node_replica);
	$node_primary->poll_query_until($db,
		"SELECT bool_and(xmin IS NULL OR xmin::text::bigint > $delete_xid) FROM pg_replication_slots"
	) or die "the replica's slot kept holding back the horizon";
	my $verbose;
	$node_primary->psql(
		$db,
		"SET maintenance_work_mem = '256kB'; VACUUM (VERBOSE, PARALLEL 0) t_cycles;",
		stderr => \$verbose,
		on_error_die => 1);
	my ($index_scans) = $verbose =~ /index scans: (\d+)/;
	cmp_ok($index_scans // 0, '>', 1,
		"io_combine_limit=$io_combine_limit: primary VACUUM ran several index cycles");
	check_integrity($node_primary, 't_cycles',
		"io_combine_limit=$io_combine_limit primary t_cycles");

	$node_primary->wait_for_catchup($node_replica);
	my $count_sql = 'SELECT count(*), sum(id) FROM t_cycles';
	is( $node_replica->safe_psql($db, $count_sql),
		$node_primary->safe_psql($db, $count_sql),
		"io_combine_limit=$io_combine_limit: replica sees the vacuumed rows");
	is( $node_replica->safe_psql($db,
			"SELECT count(*) FROM verify_heapam('t_cycles')"),
		'0',
		"io_combine_limit=$io_combine_limit: verify_heapam on the replica");
	is( $node_replica->safe_psql($db,
			"SELECT count(*) FROM (SELECT bt_index_check(indexrelid, true) FROM pg_index WHERE indrelid = 't_cycles'::regclass) s WHERE false"
		),
		'0',
		"io_combine_limit=$io_combine_limit: bt_index_check on the replica");

	### 2. VACUUM right after promotion, before pages are flushed

	$node_primary->safe_psql($db,
		'DELETE FROM t_promote WHERE id % 4 = 0; UPDATE t_promote SET k = k + 1 WHERE id % 7 = 0;');
	my $expected = $node_primary->safe_psql($db,
		'SELECT count(*), sum(id), sum(k) FROM t_promote');
	$node_primary->wait_for_catchup($node_replica);

	# Changes still only in the primary's buffers are what the promoted node
	# has to replay as VACUUM reads the pages.
	cmp_ok(
		$node_primary->safe_psql($db,
			"SELECT count(*) FROM pg_buffercache WHERE isdirty AND relfilenode = pg_relation_filenode('t_promote')"
		),
		'>', 0,
		"io_combine_limit=$io_combine_limit: t_promote has unflushed pages when the primary dies");

	$node_primary->kill9;
	$node_replica->promote;
	$node_replica->polar_wait_for_startup(60);
	$node_replica->polar_drop_all_slots;

	$node_replica->psql(
		$db,
		"SET maintenance_work_mem = '256kB'; VACUUM (VERBOSE, PARALLEL 0) t_promote;",
		stderr => \$verbose,
		on_error_die => 1);
	like($verbose, qr/finished vacuuming "postgres.public.t_promote"/,
		"io_combine_limit=$io_combine_limit: VACUUM finished on the promoted node");
	is($node_replica->safe_psql($db,
			'SELECT count(*), sum(id), sum(k) FROM t_promote'),
		$expected,
		"io_combine_limit=$io_combine_limit: promoted node has the primary's rows");
	check_integrity($node_replica, 't_promote',
		"io_combine_limit=$io_combine_limit promoted t_promote");

	# A second pass over the now-clean table must find nothing to do.
	$node_replica->psql(
		$db, "VACUUM (VERBOSE) t_promote;",
		stderr => \$verbose,
		on_error_die => 1);
	like($verbose, qr/tuples: 0 removed/,
		"io_combine_limit=$io_combine_limit: second VACUUM on the promoted node removed nothing");

	$node_replica->stop;
}

done_testing();
