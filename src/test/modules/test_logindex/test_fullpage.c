/*-------------------------------------------------------------------------
 *
 * test_fullpage.c
 *
 * Copyright (c) 2024, Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * IDENTIFICATION
 *	  src/test/modules/test_logindex/test_fullpage.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "access/heapam.h"
#include "access/polar_logindex_redo.h"
#include "catalog/pg_control.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/postmaster.h"
#include "pgstat.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/polar_bitpos.h"
#include "utils/ps_status.h"


static void test_write_fullpage(polar_fullpage_ctl_t ctl);

static void
test_flush_fullpage_table(polar_fullpage_ctl_t ctl)
{
	log_index_snapshot logindex_snapshot = ctl->logindex_snapshot;
	log_index_mem_table active = LOG_INDEX_MEM_TBL_ACTIVE();
	log_index_table_t table;

	/* Wait for fullpage write to logindex */
	sleep(1);

	polar_logindex_flush_table(logindex_snapshot, InvalidXLogRecPtr, true);
	elog(LOG, "Read table which tid=%ld", LOG_INDEX_MEM_TBL_TID(active));

	log_index_read_table_data(logindex_snapshot, &table, LOG_INDEX_MEM_TBL_TID(active), LOG);
	Assert(table.idx_table_id == LOG_INDEX_MEM_TBL_TID(active));
	Assert(table.max_lsn == polar_get_logindex_snapshot_max_lsn(logindex_snapshot));
}

static void
test_fullpage_segment_file(polar_fullpage_ctl_t ctl)
{
	char		path[MAXPGPATH] = {0};
	uint64		min_fullpage_seg_no = 0;
	struct stat statbuf;
	int			i = 0;

	polar_fullpage_file_init(ctl, 10000000);
	polar_fullpage_file_init(ctl, 20000000);
	polar_fullpage_file_init(ctl, 30000000);
	polar_fullpage_file_init(ctl, 40000000);

	min_fullpage_seg_no = FULLPAGE_FILE_SEG_NO(10000000);
	FULLPAGE_SEG_FILE_NAME(ctl, path, min_fullpage_seg_no);
	polar_remove_old_fullpage_file(ctl, path, min_fullpage_seg_no);
	Assert(polar_lstat(path, &statbuf) != 0);

	min_fullpage_seg_no = FULLPAGE_FILE_SEG_NO(20000000);
	FULLPAGE_SEG_FILE_NAME(ctl, path, min_fullpage_seg_no);
	polar_remove_old_fullpage_file(ctl, path, min_fullpage_seg_no);
	Assert(polar_lstat(path, &statbuf) != 0);

	min_fullpage_seg_no = FULLPAGE_FILE_SEG_NO(30000000);
	FULLPAGE_SEG_FILE_NAME(ctl, path, min_fullpage_seg_no);
	polar_remove_old_fullpage_file(ctl, path, min_fullpage_seg_no);
	Assert(polar_lstat(path, &statbuf) != 0);

	min_fullpage_seg_no = FULLPAGE_FILE_SEG_NO(40000000);
	FULLPAGE_SEG_FILE_NAME(ctl, path, min_fullpage_seg_no);
	polar_remove_old_fullpage_file(ctl, path, min_fullpage_seg_no);
	Assert(polar_lstat(path, &statbuf) != 0);

	polar_update_max_fullpage_no(ctl, 500000000);

	for (i = 0; i < 10000; i++)
		test_write_fullpage(ctl);
}

static void
test_write_fullpage(polar_fullpage_ctl_t ctl)
{
	Relation	rel;
	Buffer		buf;
	XLogRecPtr	start_lsn;
	XLogRecPtr	end_lsn;
	static XLogReaderState *state = NULL;
	uint64		fullpage_no;
	Page		page;
	uint64		estimate_total_segment_size = 0;

	estimate_total_segment_size = polar_get_estimate_fullpage_segment_size(ctl->logindex_snapshot);
	Assert(estimate_total_segment_size <= polar_fullpage_max_segment_size);

	/* Open relation and check privileges. */
	rel = relation_open(1260, AccessShareLock);

	/* pg_authid block_no=0 */
	buf = ReadBufferExtended(rel, MAIN_FORKNUM, 0, RBM_NORMAL, NULL);

	end_lsn = polar_log_fullpage_snapshot_image(ctl, buf, InvalidXLogRecPtr);

	ReleaseBuffer(buf);

	start_lsn = ProcLastRecPtr;

	/* Close relation, release lock. */
	relation_close(rel, AccessShareLock);

	XLogFlush(end_lsn);

	if (state == NULL)
	{
		state = XLogReaderAllocate(wal_segment_size, NULL,
								   XL_ROUTINE(.page_read = &read_local_xlog_page,
											  .segment_open = wal_segment_open,
											  .segment_close = wal_segment_close),
								   NULL);

		if (!state)
		{
			ereport(FATAL,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory"),
					 errdetail("Failed while allocating a WAL reading processor.")));
		}
	}

	polar_logindex_read_xlog(state, start_lsn);

	Assert(state->record->header.xl_rmid == RM_XLOG_ID);
	Assert((state->record->header.xl_info & ~XLR_INFO_MASK) == POLAR_WAL);
	Assert(*((PolarWalType *) XLogRecGetData(state)) == PWT_FPSI);

	page = palloc0(BLCKSZ);
	/* get fullpage_no from record */
	memcpy(&fullpage_no, XLogRecGetData(state) + sizeof(PolarWalType), sizeof(uint64));
	/* read fullpage from file */
	polar_read_fullpage(ctl, page, fullpage_no);

	Assert(memcmp((char *) BufferGetPage(buf), (char *) page, BLCKSZ) == 0);
}

PG_FUNCTION_INFO_V1(test_fullpage);
/*
 * SQL-callable entry point to perform all tests.
 *
 * If a 1% false positive threshold is not met, emits WARNINGs.
 *
 * See README for details of arguments.
 */
Datum
test_fullpage(PG_FUNCTION_ARGS)
{

	polar_fullpage_ctl_t ctl;
	log_index_snapshot logindex_snapshot;

	Assert(polar_logindex_redo_instance->wal_logindex_snapshot->flush_active_table == false);
	Assert(polar_logindex_redo_instance->fullpage_logindex_snapshot->flush_active_table == true);

	ctl = polar_logindex_redo_instance->fullpage_ctl;
	logindex_snapshot = polar_logindex_redo_instance->fullpage_logindex_snapshot;

	Assert(ctl != NULL && ctl->logindex_snapshot != NULL);

	pg_atomic_init_u32(&logindex_snapshot->state, 0);

	polar_logindex_snapshot_init(logindex_snapshot, 1000, 1, false, false);
	polar_logindex_set_start_lsn(logindex_snapshot, 1000);

	polar_logindex_redo_instance->fullpage_ctl = ctl;
	polar_logindex_redo_instance->fullpage_logindex_snapshot = logindex_snapshot;

	/* test fullpage */
	test_write_fullpage(ctl);
	test_write_fullpage(ctl);
	test_write_fullpage(ctl);

	/* test flush active table */
	test_flush_fullpage_table(ctl);

	/* test fullpage segment file */
	test_fullpage_segment_file(ctl);

	MemoryContextReset(polar_logindex_memory_context());

	PG_RETURN_INT32(0);
}

PG_FUNCTION_INFO_V1(test_fullpage_roll_table);
/*
 * Retire the fullpage logindex's active table and put it on storage.
 *
 * The meta file only learns a max lsn from a table that has gone inactive:
 * log_index_write_table() skips the meta update for an active one.  Until that
 * happens a restart brings the snapshot back with no max lsn at all, and
 * polar_logindex_calc_max_fullpage_no() returns before it looks up any wal
 * file.  Rolling the table here is what lets a restart reach the code under
 * test without first writing the thousands of snapshots it would take to fill
 * the table honestly.
 */
Datum
test_fullpage_roll_table(PG_FUNCTION_ARGS)
{
	log_index_snapshot logindex_snapshot =
		polar_logindex_redo_instance->fullpage_logindex_snapshot;
	log_index_mem_table active;
	LWLock	   *lock;

	Assert(logindex_snapshot != NULL);

	active = LOG_INDEX_MEM_TBL_ACTIVE();
	lock = LOG_INDEX_MEM_TBL_LOCK(active);

	LWLockAcquire(lock, LW_EXCLUSIVE);
	LOG_INDEX_MEM_TBL_SET_STATE(active, LOG_INDEX_MEM_TBL_STATE_INACTIVE);
	log_index_force_save_table(logindex_snapshot, active);
	LWLockRelease(lock);

	PG_RETURN_INT64(LOG_INDEX_MEM_TBL_TID(active));
}

/* Inspect the production counter after startup/replay. */
PG_FUNCTION_INFO_V1(test_fullpage_max_no);
Datum
test_fullpage_max_no(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT64(pg_atomic_read_u64(&polar_logindex_redo_instance->fullpage_ctl->max_fullpage_no));
}

/* Exercise recovery against a private directory without changing the allocator. */
PG_FUNCTION_INFO_V1(test_fullpage_resume);
Datum
test_fullpage_resume(PG_FUNCTION_ARGS)
{
	polar_fullpage_ctl_data_t ctl = {0};
	log_index_snapshot_t snapshot = {0};
	uint64		floor = PG_GETARG_INT64(0);

	strlcpy(snapshot.dir, "test_fullpage_resume", sizeof(snapshot.dir));
	pg_atomic_init_u32(&snapshot.state, POLAR_LOGINDEX_STATE_WRITABLE);
	ctl.logindex_snapshot = &snapshot;
	pg_atomic_init_u64(&ctl.max_fullpage_no, floor);

	polar_logindex_calc_max_fullpage_no(&ctl);
	PG_RETURN_INT64(pg_atomic_read_u64(&ctl.max_fullpage_no));
}

PG_FUNCTION_INFO_V1(test_fullpage_replay_counter);
Datum
test_fullpage_replay_counter(PG_FUNCTION_ARGS)
{
	polar_fullpage_ctl_data_t ctl = {0};

	pg_atomic_init_u64(&ctl.max_fullpage_no, 0);
	polar_update_max_fullpage_no(&ctl, 100);
	polar_update_max_fullpage_no(&ctl, 50);
	if (pg_atomic_read_u64(&ctl.max_fullpage_no) != 101)
		elog(ERROR, "replay reused or moved backwards from a fullpage number");

	PG_RETURN_INT32(0);
}
