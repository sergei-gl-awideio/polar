/*-------------------------------------------------------------------------
 *
 * test_fullpage_sweep.c
 *	  Tests for the floor remove_old_fullpage_files() sweeps by.
 *
 * fullpage_no is reserved with a bare atomic and the record carrying it is
 * inserted later, so a record with a higher lsn can carry a lower fullpage_no.
 * The sweep survives that by keeping a margin below the number it reads at the
 * logindex min lsn: max_fullpage_no_gap for the reservations that already
 * reached WAL, and the oldest reservation still in flight for the ones that
 * have not.  Neither bound can be produced by a single backend, which is why
 * this runs writers as background workers.
 *
 * The writers snapshot a different block every time.  The sweep does nothing
 * until the fullpage logindex has rolled a memory table, and a table holds
 * LOG_INDEX_MEM_TBL_SEG_NUM segments of LOG_INDEX_ITEM_SEG_LSN_NUM lsns each,
 * so hammering one page needs 40960 snapshots to fill one.  A distinct page tag
 * claims a segment of its own, so spreading the writes over more blocks than a
 * table has segments rolls it in about a fifth of that, which is the difference
 * between twenty seconds and three.
 *
 * IDENTIFICATION
 *	  src/test/modules/test_logindex/test_fullpage_sweep.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <sys/stat.h>

#include "access/polar_fullpage.h"
#include "access/polar_logindex.h"
#include "access/polar_logindex_internal.h"
#include "access/polar_logindex_redo.h"
#include "access/relation.h"
#include "access/table.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/pg_control.h"
#include "fmgr.h"
#include "libpq/pqsignal.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "postmaster/bgworker.h"
#include "storage/bufmgr.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/polar_fd.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "storage/spin.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "test_module_init.h"

/*
 * Two writers already make max_fullpage_no run ahead of an individual
 * reservation while its record is being inserted, which is all the concurrency
 * this needs; the run asserts that it happened.  More of them only multiplies
 * the 256MB segment fills, since every writer that races to cross a segment
 * boundary zero fills a file and all but one are thrown away.
 */
#define TEST_SWEEP_WRITERS			2

/* How long the writers may run before we give up waiting for a rolled table */
#define TEST_SWEEP_MIN_MSECS		(1 * 1000)
#define TEST_SWEEP_MAX_MSECS		(120 * 1000)

/*
 * Segments the deterministic floor test lays down below the counter.  The
 * floor lands a segment or two below it, depending on how much the writers got
 * through, so this only has to be comfortably more than that.
 */
#define TEST_SWEEP_PLACEHOLDERS		12

typedef enum test_sweep_status
{
	TEST_SWEEP_INIT = 0,
	TEST_SWEEP_RUN,
	TEST_SWEEP_STOP
} test_sweep_status;

typedef struct test_fullpage_sweep_meta
{
	slock_t		lock;
	int			status;
	uint64		writes;
	Oid			relid;
	BlockNumber nblocks;
} test_fullpage_sweep_meta;

static test_fullpage_sweep_meta *sweep_meta = NULL;
static volatile sig_atomic_t got_sigterm = false;

PGDLLEXPORT void test_fullpage_sweep_write_worker(Datum main_arg);

/* Built by t/000_test_logindex.pl; see test_sweep_set_relation() */
#define TEST_SWEEP_RELNAME			"test_fullpage_sweep_rel"

Size
test_fullpage_sweep_request_shmem_size(void)
{
	return MAXALIGN(sizeof(test_fullpage_sweep_meta));
}

void
test_fullpage_sweep_shmem_startup(void)
{
	bool		found = false;

	sweep_meta = (test_fullpage_sweep_meta *)
		ShmemInitStruct("test_fullpage_sweep_meta",
						sizeof(test_fullpage_sweep_meta), &found);

	if (!found)
	{
		SpinLockInit(&sweep_meta->lock);
		sweep_meta->status = TEST_SWEEP_INIT;
		sweep_meta->writes = 0;
		sweep_meta->relid = InvalidOid;
		sweep_meta->nblocks = 0;
	}
}

static void
test_sweep_attach_shmem(void)
{
	bool		found = false;

	sweep_meta = (test_fullpage_sweep_meta *)
		ShmemInitStruct("test_fullpage_sweep_meta",
						sizeof(test_fullpage_sweep_meta), &found);
	Assert(found);
}

static int
test_sweep_get_status(void)
{
	int			status;

	SpinLockAcquire(&sweep_meta->lock);
	status = sweep_meta->status;
	SpinLockRelease(&sweep_meta->lock);

	return status;
}

static void
test_sweep_set_status(int status)
{
	SpinLockAcquire(&sweep_meta->lock);
	sweep_meta->status = status;
	SpinLockRelease(&sweep_meta->lock);
}

static void
test_sweep_sigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);
	errno = save_errno;
}

static bool
test_sweep_segment_exists(polar_fullpage_ctl_t ctl, uint64 seg_no)
{
	char		path[MAXPGPATH] = {0};
	struct stat statbuf;

	FULLPAGE_SEG_FILE_NAME(ctl, path, seg_no);

	return polar_lstat(path, &statbuf) == 0;
}

/*
 * An empty file at a segment's name.  The sweep only stats these and unlinks
 * them, so there is no reason to pay the 256MB zero fill a real segment costs.
 */
static void
test_sweep_make_placeholder(polar_fullpage_ctl_t ctl, uint64 seg_no)
{
	char		path[MAXPGPATH] = {0};
	int			fd;

	if (test_sweep_segment_exists(ctl, seg_no))
		return;

	FULLPAGE_SEG_FILE_NAME(ctl, path, seg_no);

	fd = BasicOpenFile(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);

	if (fd < 0 || polar_close(fd) != 0)
		elog(PANIC, "could not create fullpage placeholder \"%s\": %m", path);
}

static void
test_sweep_remove_placeholder(polar_fullpage_ctl_t ctl, uint64 seg_no)
{
	char		path[MAXPGPATH] = {0};

	FULLPAGE_SEG_FILE_NAME(ctl, path, seg_no);

	if (polar_unlink(path) != 0)
		elog(PANIC, "could not remove fullpage placeholder \"%s\": %m", path);
}

/*
 * Write one fullpage snapshot and read it back.
 *
 * Two things are asserted beyond the round trip.  The reservation must be
 * released by the time polar_log_fullpage_snapshot_image() returns, because a
 * slot left set would pin the sweep floor for the life of the instance.  And
 * the segment must still be on disk afterwards: the record is at the top of
 * the counter and nowhere near the sweep floor, so a sweep that removed it
 * would be removing a snapshot a replica could still ask for.  That is the
 * failure the in-flight floor exists to prevent, and with a sweep running
 * concurrently in the foreground it is the one this catches.
 */
static void
test_sweep_write_one(polar_fullpage_ctl_t ctl, Relation rel, BlockNumber blkno,
					 XLogReaderState *state, Page verify, Page expect)
{
	Buffer		buf;
	XLogRecPtr	start_lsn;
	XLogRecPtr	end_lsn;
	uint64		fullpage_no;

	buf = ReadBufferExtended(rel, MAIN_FORKNUM, blkno, RBM_NORMAL, NULL);

	end_lsn = polar_log_fullpage_snapshot_image(ctl, buf, InvalidXLogRecPtr);
	start_lsn = ProcLastRecPtr;

	if (!(pg_atomic_read_u64(&ctl->inflight_fullpage_no[MyProcNumber]) ==
		  POLAR_FULLPAGE_NO_INFLIGHT))
		elog(ERROR, "fullpage reservation was not released");

	XLogFlush(end_lsn);

	polar_logindex_read_xlog(state, start_lsn);
	Assert(state->record->header.xl_rmid == RM_XLOG_ID);
	Assert((state->record->header.xl_info & ~XLR_INFO_MASK) == POLAR_WAL);
	Assert(*((PolarWalType *) XLogRecGetData(state)) == PWT_FPSI);

	memcpy(&fullpage_no, XLogRecGetData(state) + sizeof(PolarWalType),
		   sizeof(uint64));

	if (!test_sweep_segment_exists(ctl, FULLPAGE_FILE_SEG_NO(fullpage_no)))
		elog(ERROR, "fullpage segment removed under a writer");

	polar_read_fullpage(ctl, verify, fullpage_no);

	/*
	 * Compare against storage, not against the buffer.
	 * polar_log_fullpage_snapshot_image() snapshots what smgrread() returns,
	 * and that differs from the shared buffer in pd_checksum: the checksum is
	 * computed into a copy at write time, so a block built in memory and
	 * checkpointed has it set on storage and still zero in the buffer that
	 * built it.  Reading storage again is also the stronger assertion, since
	 * it is exactly what the snapshot was supposed to preserve.
	 */
	smgrread(RelationGetSmgr(rel), MAIN_FORKNUM, blkno, (char *) expect);
	if (memcmp((char *) expect, (char *) verify, BLCKSZ) != 0)
		elog(ERROR, "fullpage snapshot does not match storage");

	ReleaseBuffer(buf);
}

/*
 * Writer worker: snapshot successive pages until told to stop.
 *
 * Every writer runs in its own process, so each one has its own pgprocno and
 * its own slot in ctl->inflight_fullpage_no, and its own cached
 * open_fullpage_file descriptor.  Both matter: the sweep scans all the slots,
 * and the cached descriptor is what makes a removed segment silent rather than
 * an error.
 */
void
test_fullpage_sweep_write_worker(Datum main_arg)
{
	polar_fullpage_ctl_t ctl;
	XLogReaderState *state;
	MemoryContext oldcontext;
	Relation	rel;
	Page		verify;
	Page		expect;
	BlockNumber nblocks;
	BlockNumber blkno;
	int			worker_id = DatumGetInt32(main_arg);
	uint64		writes = 0;

	pqsignal(SIGTERM, test_sweep_sigterm);
	BackgroundWorkerUnblockSignals();
	BackgroundWorkerInitializeConnection("postgres", NULL, 0);

	test_sweep_attach_shmem();

	ctl = polar_logindex_redo_instance->fullpage_ctl;
	Assert(ctl != NULL);

	verify = MemoryContextAllocAligned(TopMemoryContext, BLCKSZ,
									   PG_IO_ALIGN_SIZE, 0);
	expect = MemoryContextAllocAligned(TopMemoryContext, BLCKSZ,
									   PG_IO_ALIGN_SIZE, 0);

	while (!got_sigterm && test_sweep_get_status() == TEST_SWEEP_INIT)
		pg_usleep(1000);

	/*
	 * One transaction for the whole run.  The xlog reader has to outlive the
	 * transaction it first decodes in, or its decode buffer is freed under it
	 * and the next read walks a dangling list; keeping the reader in
	 * TopMemoryContext is not enough on its own.  Nothing in the loop grows
	 * memory: the reader, the page buffer and the relation are all reused.
	 */
	StartTransactionCommand();

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);
	state = XLogReaderAllocate(wal_segment_size, NULL,
							   XL_ROUTINE(.page_read = &read_local_xlog_page,
										  .segment_open = &wal_segment_open,
										  .segment_close = &wal_segment_close),
							   NULL);
	Assert(state != NULL);
	MemoryContextSwitchTo(oldcontext);

	rel = relation_open(sweep_meta->relid, AccessShareLock);
	nblocks = sweep_meta->nblocks;
	Assert(nblocks > 0);

	/* Each writer starts in a different part of the relation */
	blkno = (BlockNumber) (((uint64) worker_id * nblocks) / TEST_SWEEP_WRITERS);

	while (!got_sigterm && test_sweep_get_status() == TEST_SWEEP_RUN)
	{
		test_sweep_write_one(ctl, rel, blkno, state, verify, expect);
		blkno = (blkno + 1) % nblocks;
		writes++;
	}

	relation_close(rel, AccessShareLock);
	CommitTransactionCommand();

	SpinLockAcquire(&sweep_meta->lock);
	sweep_meta->writes += writes;
	SpinLockRelease(&sweep_meta->lock);

	/* Nothing may be left published, or the sweep floor never rises again */
	if (!(pg_atomic_read_u64(&ctl->inflight_fullpage_no[MyProcNumber]) ==
		  POLAR_FULLPAGE_NO_INFLIGHT))
		elog(ERROR, "fullpage reservation was not released");

	proc_exit(0);
}

static BackgroundWorkerHandle *
test_sweep_launch_worker(BackgroundWorker *worker, int32 worker_id)
{
	BackgroundWorkerHandle *handle = NULL;

	memset(worker, 0, sizeof(BackgroundWorker));
	worker->bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker->bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker->bgw_restart_time = BGW_NEVER_RESTART;
	sprintf(worker->bgw_library_name, "test_logindex");
	sprintf(worker->bgw_function_name, "test_fullpage_sweep_write_worker");
	worker->bgw_main_arg = Int32GetDatum(worker_id);
	snprintf(worker->bgw_name, BGW_MAXLEN, "test_fullpage_sweep_writer");
	snprintf(worker->bgw_type, BGW_MAXLEN, "test_fullpage_sweep");
	worker->bgw_notify_pid = MyProcPid;

	if (!RegisterDynamicBackgroundWorker(worker, &handle))
		elog(ERROR, "could not register fullpage sweep writer");

	return handle;
}

/*
 * Publish the relation the writers snapshot from.
 *
 * It has to have more blocks than a logindex memory table has segments, or the
 * writers cannot roll a table in a reasonable number of snapshots and the sweep
 * never gets a min lsn to work from.  t/000_test_logindex.pl builds it with a
 * low fillfactor so the blocks are cheap, checkpoints it so every block on
 * storage matches its buffer, and turns autovacuum off on it so they stay that
 * way while we compare the two.
 */
static void
test_sweep_set_relation(void)
{
	RangeVar   *rv = makeRangeVar(NULL, TEST_SWEEP_RELNAME, -1);
	Relation	rel;
	BlockNumber nblocks;

	rel = relation_openrv(rv, AccessShareLock);
	nblocks = RelationGetNumberOfBlocks(rel);

	if (nblocks <= LOG_INDEX_MEM_TBL_SEG_NUM)
		elog(PANIC, "%s has %u blocks, need more than the %d segments of a logindex memory table",
			 TEST_SWEEP_RELNAME, nblocks, LOG_INDEX_MEM_TBL_SEG_NUM);

	SpinLockAcquire(&sweep_meta->lock);
	sweep_meta->relid = RelationGetRelid(rel);
	sweep_meta->nblocks = nblocks;
	SpinLockRelease(&sweep_meta->lock);

	relation_close(rel, AccessShareLock);
}

/*
 * Lower the barrier so the sweeps below actually reach the removal loop.
 *
 * remove_old_fullpage_files() arms sweep_safe_lsn at the current insert
 * pointer on its first call and removes nothing until the logindex min lsn has
 * passed it, which will not happen inside a test.  test_sweep_barrier() covers
 * the arming itself; everywhere else we want the sweep to do its work.
 */
static void
test_sweep_disarm_barrier(polar_fullpage_ctl_t ctl)
{
	pg_atomic_write_u64(&ctl->sweep_safe_lsn, 1);
}

/*
 * Run the sweep, having first pushed the logindex min lsn as far up as it will
 * go.  The sweep reads its floor from the record sitting at that min lsn, so
 * this is what keeps the floor close behind the counter and the removal loop
 * working on the segments the writers are actually using.
 *
 * The flush matters as much as the truncate.  polar_logindex_truncate() only
 * drops tables that are already on storage, so without it the min lsn stays
 * pinned at the oldest record of the run: test_fullpage() writes its first
 * snapshots before it moves the counter, so that record carries a fullpage_no
 * of about zero, the sweep finds it below its own margin and returns having
 * done nothing at all.
 */
static void
test_sweep_run(polar_fullpage_ctl_t ctl, log_index_snapshot snapshot)
{
	polar_logindex_flush_table(snapshot, InvalidXLogRecPtr, true);
	polar_remove_old_fullpage_files(ctl,
									polar_get_logindex_snapshot_max_lsn(snapshot));
}

/*
 * Writers and a sweep at the same time.
 *
 * This is the part a single backend cannot do.  max_fullpage_no_gap is the
 * distance between a reservation and the counter when that reservation's
 * record reaches WAL, so it stays at one unless somebody else is reserving in
 * between; the inversion the sweep has to survive only exists for the same
 * reason.  Asserting the gap moved is what makes a pass here mean something.
 */
static void
test_sweep_under_writers(polar_fullpage_ctl_t ctl, log_index_snapshot snapshot)
{
	BackgroundWorkerHandle *handle[TEST_SWEEP_WRITERS];
	BackgroundWorker worker[TEST_SWEEP_WRITERS];
	TimestampTz start;
	uint64		gap;
	int			i;

	test_sweep_disarm_barrier(ctl);

	/*
	 * Start just below a segment boundary.  Crossing one makes a writer
	 * create the next segment, and polar_fullpage_file_init() zero fills
	 * 256MB and fsyncs it while the reservation is already taken and its
	 * record already in WAL.  That is a real several hundred millisecond
	 * window between the reservation and the write, on the production path,
	 * with no injection: it is the window the in-flight floor has to cover,
	 * and it is wide enough for a concurrent sweep to land in.
	 */
	polar_update_max_fullpage_no(ctl,
								 pg_atomic_read_u64(&ctl->max_fullpage_no) +
								 FULLPAGE_NUM_PER_FILE -
								 (pg_atomic_read_u64(&ctl->max_fullpage_no) %
								  FULLPAGE_NUM_PER_FILE) - 4);

	for (i = 0; i < TEST_SWEEP_WRITERS; i++)
		handle[i] = test_sweep_launch_worker(&worker[i], i);

	for (i = 0; i < TEST_SWEEP_WRITERS; i++)
	{
		pid_t		pid;

		if (WaitForBackgroundWorkerStartup(handle[i], &pid) != BGWH_STARTED)
			elog(ERROR, "could not start fullpage sweep writer");
	}

	test_sweep_set_status(TEST_SWEEP_RUN);

	start = GetCurrentTimestamp();

	/*
	 * Run until the writers have filled a memory table and it has been
	 * written out, which is what finally gives the logindex a min segment and
	 * the sweep a min lsn to read its floor from.  Flushing the active table
	 * (see test_sweep_run) is not enough: log_index_write_table() only
	 * records the min segment for a table that has gone inactive, so until
	 * one fills, polar_get_logindex_snapshot_storage_min_lsn() is invalid and
	 * the sweep returns before it looks at anything.
	 */
	for (;;)
	{
		TimestampTz now = GetCurrentTimestamp();

		test_sweep_run(ctl, snapshot);

		if (TimestampDifferenceExceeds(start, now, TEST_SWEEP_MAX_MSECS))
			break;

		if (TimestampDifferenceExceeds(start, now, TEST_SWEEP_MIN_MSECS) &&
			!XLogRecPtrIsInvalid(polar_get_logindex_snapshot_storage_min_lsn(snapshot)))
			break;

		pg_usleep(10 * 1000);
	}

	test_sweep_set_status(TEST_SWEEP_STOP);

	for (i = 0; i < TEST_SWEEP_WRITERS; i++)
		if (WaitForBackgroundWorkerShutdown(handle[i]) != BGWH_STOPPED)
			elog(ERROR, "fullpage sweep writer did not stop");

	gap = pg_atomic_read_u64(&ctl->max_fullpage_no_gap);

	elog(LOG, "test_fullpage_sweep: " UINT64_FORMAT " writes, max gap " UINT64_FORMAT,
		 sweep_meta->writes, gap);

	Assert(sweep_meta->writes > 0);

	/*
	 * Without this the sweep never gets past its own preconditions and the
	 * rest of the test would pass without exercising anything.
	 */
	if (XLogRecPtrIsInvalid(polar_get_logindex_snapshot_storage_min_lsn(snapshot)))
		elog(PANIC, "fullpage logindex rolled no table in " UINT64_FORMAT " writes, the sweep has no min lsn to work from",
			 sweep_meta->writes);

	/*
	 * Nothing here is timing dependent except this: with several writers
	 * inserting concurrently the counter must have moved past at least one of
	 * them between its reservation and its insert.  If this ever fails the
	 * writers were not actually overlapping, and the run proved nothing.
	 */
	if (gap <= 1)
		elog(ERROR, "fullpage writers did not overlap their reservations");
}

/*
 * The first sweep of an instance arms the barrier and removes nothing.
 */
static void
test_sweep_barrier(polar_fullpage_ctl_t ctl, log_index_snapshot snapshot,
				   uint64 seg_lo, uint64 seg_hi)
{
	uint64		seg_no;

	pg_atomic_write_u64(&ctl->sweep_safe_lsn, InvalidXLogRecPtr);

	test_sweep_run(ctl, snapshot);

	Assert(!XLogRecPtrIsInvalid(pg_atomic_read_u64(&ctl->sweep_safe_lsn)));

	for (seg_no = seg_lo; seg_no <= seg_hi; seg_no++)
		Assert(test_sweep_segment_exists(ctl, seg_no));
}

/*
 * The floor, exactly.
 *
 * The removal loop walks down from the floor and stops at the first segment
 * that is not there, so the placeholders have to sit immediately below the
 * counter to be reachable at all.  The first sweep tells us where the floor
 * actually landed; the two after it are the assertion, and they differ only in
 * whether a reservation is published below that floor.  Remove the in-flight
 * floor from remove_old_fullpage_files() and the second one deletes the
 * segments the parked reservation is sitting in.
 */
static void
test_sweep_inflight_floor(polar_fullpage_ctl_t ctl, log_index_snapshot snapshot)
{
	pg_atomic_uint64 *slot = &ctl->inflight_fullpage_no[MyProcNumber];
	int			keep_segments = polar_fullpage_keep_segments;
	uint64		seg_hi;
	uint64		seg_lo;
	uint64		seg_no;
	uint64		removed = 0;
	uint64		lowest_kept;

	seg_hi = FULLPAGE_FILE_SEG_NO(pg_atomic_read_u64(&ctl->max_fullpage_no));
	Assert(seg_hi > TEST_SWEEP_PLACEHOLDERS);
	seg_lo = seg_hi - TEST_SWEEP_PLACEHOLDERS;

	/*
	 * Recycling renames a swept segment into a future slot rather than
	 * unlinking it, which would install these empty placeholders where a
	 * later writer expects a full segment.  It also destroys the old snapshot
	 * just as removal does, which is the reason polar_fullpage_keep_segments
	 * is no protection here; the sweep floor is the whole safety net.
	 */
	polar_fullpage_keep_segments = 0;

	for (seg_no = seg_lo; seg_no <= seg_hi; seg_no++)
		test_sweep_make_placeholder(ctl, seg_no);

	test_sweep_barrier(ctl, snapshot, seg_lo, seg_hi);
	test_sweep_disarm_barrier(ctl);

	/* Where does the floor land with nothing published? */
	Assert(pg_atomic_read_u64(slot) == POLAR_FULLPAGE_NO_INFLIGHT);
	test_sweep_run(ctl, snapshot);

	lowest_kept = seg_hi + 1;

	for (seg_no = seg_lo; seg_no <= seg_hi; seg_no++)
	{
		if (test_sweep_segment_exists(ctl, seg_no))
		{
			lowest_kept = Min(lowest_kept, seg_no);
			continue;
		}

		removed++;
		test_sweep_make_placeholder(ctl, seg_no);
	}

	/*
	 * If the sweep reached none of them the rest proves nothing, so say why
	 * rather than passing quietly.
	 */
	if (removed == 0)
		elog(PANIC, "sweep removed no fullpage segment in [" UINT64_FORMAT ", " UINT64_FORMAT "], max_fullpage_no = " UINT64_FORMAT,
			 seg_lo, seg_hi, pg_atomic_read_u64(&ctl->max_fullpage_no));

	/* Park a reservation in the lowest segment the sweep just took */
	pg_atomic_write_u64(slot, seg_lo * FULLPAGE_NUM_PER_FILE);

	test_sweep_run(ctl, snapshot);

	for (seg_no = seg_lo; seg_no <= seg_hi; seg_no++)
	{
		if (!test_sweep_segment_exists(ctl, seg_no))
			elog(PANIC, "sweep removed fullpage segment " UINT64_FORMAT " below a published reservation at " UINT64_FORMAT,
				 seg_no, seg_lo * FULLPAGE_NUM_PER_FILE);
	}

	/* Release it and the same sweep gets back to where it was */
	pg_atomic_write_u64(slot, POLAR_FULLPAGE_NO_INFLIGHT);

	test_sweep_run(ctl, snapshot);

	Assert(!test_sweep_segment_exists(ctl, lowest_kept - 1));

	for (seg_no = seg_lo; seg_no <= seg_hi; seg_no++)
	{
		if (test_sweep_segment_exists(ctl, seg_no))
			test_sweep_remove_placeholder(ctl, seg_no);
	}

	polar_fullpage_keep_segments = keep_segments;
}

PG_FUNCTION_INFO_V1(test_fullpage_sweep);
/*
 * SQL-callable entry point.  Must run after test_fullpage(), which is what
 * initialises the fullpage logindex snapshot this works against.
 */
Datum
test_fullpage_sweep(PG_FUNCTION_ARGS)
{
	polar_fullpage_ctl_t ctl;
	log_index_snapshot snapshot;

	ctl = polar_logindex_redo_instance->fullpage_ctl;
	Assert(ctl != NULL && ctl->logindex_snapshot != NULL);
	snapshot = ctl->logindex_snapshot;

	test_sweep_attach_shmem();

	Assert(ctl->num_inflight_slots > MyProcNumber);

	test_sweep_set_relation();

	/*
	 * Start the fullpage logindex over, the way test_fullpage() does.
	 *
	 * The sweep reads its floor from the fullpage_no of the record sitting at
	 * the logindex min lsn, so every record the logindex still holds has to
	 * carry a number worth sweeping by.  test_fullpage() writes its first
	 * snapshots before it moves max_fullpage_no up, so the records it leaves
	 * behind carry numbers near zero: the sweep would find one below its own
	 * margin and return.  Everything written from here on is above the
	 * counter test_fullpage() left.
	 */
	pg_atomic_init_u32(&snapshot->state, 0);
	polar_logindex_snapshot_init(snapshot, 1000, 1, false, false);
	polar_logindex_set_start_lsn(snapshot, 1000);

	/*
	 * Writers first: the deterministic part below leaves empty placeholder
	 * files behind, and no writer should ever be handed one of those as a
	 * segment.
	 */
	test_sweep_under_writers(ctl, snapshot);
	test_sweep_inflight_floor(ctl, snapshot);

	PG_RETURN_INT32(0);
}

PG_FUNCTION_INFO_V1(test_fullpage_publish_inflight);
/*
 * Publish a reservation into this backend's own in-flight slot and leave it
 * there, the way polar_log_fullpage_begin() does before its fetch_add.
 *
 * Paired with a SIGKILL of this backend, this is the one death path that skips
 * polar_fullpage_inflight_shmem_exit(): quickdie() does not run proc_exit(),
 * and neither does a process the kernel removes outright.  What keeps such a
 * slot from pinning the sweep floor for the life of the instance is that the
 * postmaster answers an abnormal child exit with a crash restart, and
 * reset_shared() builds shared memory again from nothing
 * (postmaster.c, CreateSharedMemoryAndSemaphores()).
 */
Datum
test_fullpage_publish_inflight(PG_FUNCTION_ARGS)
{
	polar_fullpage_ctl_t ctl = polar_logindex_redo_instance->fullpage_ctl;
	uint64		fullpage_no = (uint64) PG_GETARG_INT64(0);

	Assert(ctl != NULL);
	Assert(MyProc != NULL && MyProcNumber < ctl->num_inflight_slots);

	pg_atomic_write_u64(&ctl->inflight_fullpage_no[MyProcNumber],
						fullpage_no);

	PG_RETURN_INT32(MyProcNumber);
}

PG_FUNCTION_INFO_V1(test_fullpage_inflight_occupied);
/*
 * How many in-flight slots currently hold a reservation.  A caller that knows
 * nothing is reserving can use this to see whether the array came back clean.
 */
Datum
test_fullpage_inflight_occupied(PG_FUNCTION_ARGS)
{
	polar_fullpage_ctl_t ctl = polar_logindex_redo_instance->fullpage_ctl;
	int			occupied = 0;
	int			i;

	Assert(ctl != NULL);

	for (i = 0; i < ctl->num_inflight_slots; i++)
	{
		if (pg_atomic_read_u64(&ctl->inflight_fullpage_no[i]) !=
			POLAR_FULLPAGE_NO_INFLIGHT)
			occupied++;
	}

	PG_RETURN_INT32(occupied);
}
