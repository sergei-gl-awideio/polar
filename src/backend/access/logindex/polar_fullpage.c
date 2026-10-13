/*-------------------------------------------------------------------------
 *
 * polar_fullpage.c
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
 *	  src/backend/access/logindex/polar_fullpage.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>
#include "miscadmin.h"

#include "access/polar_logindex_redo.h"
#include "access/polar_fullpage.h"
#include "access/polar_queue_manager.h"
#include "access/twophase.h"	/* max_prepared_xacts, via POLAR_TOTALPROCS */
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "catalog/pg_control.h"
#include "pgstat.h"
#include "replication/walreceiver.h"
#include "storage/buf_internals.h"
#include "storage/polar_copybuf.h"
#include "storage/ipc.h"
#include "storage/fd.h"
#include "storage/polar_fd.h"
#include "storage/proc.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

int			polar_fullpage_keep_segments = 16;
int			polar_fullpage_max_segment_size = 100 * 1024;
int			polar_fullpage_snapshot_oldest_lsn_delay_threshold = 0;
int			polar_fullpage_snapshot_replay_delay_threshold = 0;
int			polar_fullpage_snapshot_min_modified_count = 0;
double		polar_fullpage_logindex_mem_ratio = 0.125;

static bool polar_fullpage_online_promote = false;

static int	open_fullpage_file = -1;
static uint64 open_fullpage_file_seg_no = -1;

static void remove_old_fullpage_files(polar_fullpage_ctl_t ctl);
static uint64 polar_xlog_read_fullpage_no(log_index_snapshot logindex_snapshot, XLogRecPtr lsn);

void
polar_fullpage_logindex_init_lwlock_array(log_index_snapshot logindex_snapshot)
{
	int			mem_tbl_size = polar_logindex_mem_tbl_size(logindex_snapshot);

	POLAR_ASSERT_PANIC(mem_tbl_size > 0);

	polar_logindex_init_lwlock_array(logindex_snapshot, LOG_INDEX_MEMTBL_LOCK_OFFSET,
									 mem_tbl_size, LWTRANCHE_FULLPAGE_LOGINDEX_MEM_TBL);

	polar_logindex_init_lwlock_array(logindex_snapshot, LOG_INDEX_HASH_LOCK_OFFSET,
									 LOG_INDEX_MEM_TBL_HASH_LOCK_NUM,
									 LWTRANCHE_FULLPAGE_LOGINDEX_HASH_LOCK);

	polar_logindex_init_lwlock_array(logindex_snapshot, LOG_INDEX_IO_LOCK_OFFSET,
									 1, LWTRANCHE_FULLPAGE_LOGINDEX_IO);

	polar_logindex_init_lwlock_array(logindex_snapshot, LOG_INDEX_FLUSH_ACTIVE_TBL_LOCK_OFFSET,
									 1, LWTRANCHE_FULLPAGE_LOGINDEX_FLUSH_ACTIVE_TBL);
}

/*
 * Initialization of shared memory for fullpage control
 */
Size
polar_fullpage_shmem_size(void)
{
	Size		size;

	/*
	 * One in-flight slot per PGPROC, indexed by pgprocno.  POLAR_TOTALPROCS
	 * is the length of the array InitProcGlobal() numbers, so this covers
	 * every pgprocno that can exist.  It is max_prepared_xacts slots longer
	 * than it strictly needs to be, since the prepared xact PGPROCs at the
	 * top are never a running process, but that is 8 bytes each against not
	 * having to argue about which procs reach polar_log_fullpage_begin().
	 */
	size = offsetof(polar_fullpage_ctl_data_t, inflight_fullpage_no);
	size = add_size(size, mul_size(POLAR_TOTALPROCS, sizeof(pg_atomic_uint64)));

	return CACHELINEALIGN(size);
}

polar_fullpage_ctl_t
polar_fullpage_shmem_init(const char *name, polar_ringbuf_t queue, log_index_snapshot logindex_snapshot)
{
#define FULLPAGE_SUFFIX "_fp_ctl"
	bool		found = false;
	polar_fullpage_ctl_t ctl;
	char		item_name[POLAR_MAX_SHMEM_NAME];

	snprintf(item_name, POLAR_MAX_SHMEM_NAME, "%s%s", name, FULLPAGE_SUFFIX);

	ctl = (polar_fullpage_ctl_t)
		ShmemInitStruct(item_name, polar_fullpage_shmem_size(), &found);

	if (!IsUnderPostmaster)
	{
		int			i;

		POLAR_ASSERT_PANIC(!found);
		memset(ctl, 0, polar_fullpage_shmem_size());

		pg_atomic_init_u64(&ctl->max_fullpage_no, 0);
		pg_atomic_init_u64(&ctl->max_fullpage_no_gap, 0);
		pg_atomic_init_u64(&ctl->sweep_safe_lsn, InvalidXLogRecPtr);

		ctl->num_inflight_slots = POLAR_TOTALPROCS;

		for (i = 0; i < ctl->num_inflight_slots; i++)
			pg_atomic_init_u64(&ctl->inflight_fullpage_no[i], POLAR_FULLPAGE_NO_INFLIGHT);

		/* Init fullpage lock */
		LWLockInitialize(&ctl->file_lock.lock, LWTRANCHE_FULLPAGE_FILE);
		strlcpy(ctl->name, name, MAX_FULLPAGE_DIR_NAME_LEN);
		pg_atomic_init_u32(&ctl->procno, INVALID_PROC_NUMBER);

		/* init stat info */
		pg_atomic_init_u64(&ctl->stat.fullpage_bgworker_replay_lsn, InvalidXLogRecPtr);
		pg_atomic_init_u64(&ctl->stat.read_oldpage_count, 0);
		pg_atomic_init_u64(&ctl->stat.write_fullpage_count, 0);
		pg_atomic_init_u64(&ctl->stat.read_fullpage_count, 0);
		pg_atomic_init_u64(&ctl->stat.restore_oldpage_count, 0);
		pg_atomic_init_u64(&ctl->stat.insert_fullpage_lsn, InvalidXLogRecPtr);
		pg_atomic_init_u64(&ctl->stat.load_fullpage_lsn, InvalidXLogRecPtr);

		ctl->logindex_snapshot = logindex_snapshot;
		ctl->queue = queue;

		/* Validate that dir to save fullpage files is created */
		if (!polar_is_replica())
		{
			char		dir[MAXPGPATH];

			snprintf(dir, MAXPGPATH, "%s/%s", POLAR_DATA_DIR(), ctl->name);
			polar_validate_dir(dir);
		}
	}
	else
		POLAR_ASSERT_PANIC(found);

	return ctl;
}

/* Raise a shared counter without moving it backwards. */
static bool
atomic_raise_u64(pg_atomic_uint64 *value, uint64 next)
{
	uint64		old = pg_atomic_read_u64(value);

	while (next > old)
	{
		if (pg_atomic_compare_exchange_u64(value, &old, next))
			return true;
	}

	return false;
}

/*
 * Resume beyond every segment on storage, including preallocated segments.
 * Replay starts at the checkpoint redo point, so its counter need not cover
 * snapshots retained from before that point.  Nor can we infer the allocation
 * frontier from a single WAL record or from the first hole in the directory:
 * a crash during reclamation can leave holes below surviving segments.
 *
 * Scan names rather than probing consecutive files.  ReadDir reports open and
 * read failures as errors; an incomplete listing must never authorize reuse.
 * This also works when the fullpage logindex is empty or its WAL is on an old
 * timeline.  The replay counter remains a lower bound even without any files.
 * Called at the end of recovery, before this instance starts allocating.
 */
void
polar_logindex_calc_max_fullpage_no(polar_fullpage_ctl_t ctl)
{
	char		path[MAXPGPATH];
	DIR		   *dir;
	struct dirent *de;
	uint64		next;

	if (!ctl || !polar_logindex_check_state(ctl->logindex_snapshot, POLAR_LOGINDEX_STATE_WRITABLE))
		return;

	next = pg_atomic_read_u64(&ctl->max_fullpage_no);
	snprintf(path, sizeof(path), "%s/%s", POLAR_DATA_DIR(),
			 polar_get_logindex_snapshot_dir(ctl->logindex_snapshot));
	dir = AllocateDir(path);

	while ((de = ReadDir(dir, path)) != NULL)
	{
		size_t		hexlen = strspn(de->d_name, "0123456789ABCDEF");
		uint64		segno;

		/* %08lX is a minimum width, so large segment numbers grow wider. */
		if (hexlen < 8 || hexlen > 16 || strcmp(de->d_name + hexlen, ".fp") != 0)
			continue;

		segno = strtou64(de->d_name, NULL, 16);
		if (segno >= PG_UINT64_MAX / FULLPAGE_NUM_PER_FILE)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("fullpage segment number is too large: %s", de->d_name)));

		next = Max(next, (segno + 1) * FULLPAGE_NUM_PER_FILE);
	}

	if (FreeDir(dir) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close directory \"%s\": %m", path)));

	atomic_raise_u64(&ctl->max_fullpage_no, next);
	elog(LOG, "redo done at max_fullpage_no = " UINT64_FORMAT,
		 pg_atomic_read_u64(&ctl->max_fullpage_no));
}

/*
 * max_fullpage_no is the next number to allocate, not the last one replayed.
 * Keep the maximum: allocation order need not be the same as WAL order.
 */
void
polar_update_max_fullpage_no(polar_fullpage_ctl_t ctl, uint64 fullpage_no)
{
	if (!ctl)
		return;

	if (fullpage_no == PG_UINT64_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("fullpage snapshot counter exhausted")));

	atomic_raise_u64(&ctl->max_fullpage_no, fullpage_no + 1);
}

/*
 * Install a new FULLPAGE segment file as a current or future log segment.
 */
static bool
install_fullpage_file_segment(polar_fullpage_ctl_t ctl, uint64 *seg_no,
							  const char *tmppath, uint64 max_segno)
{
	char		path[MAXPGPATH];
	struct stat stat_buf;

	FULLPAGE_SEG_FILE_NAME(ctl, path, *seg_no);

	/*
	 * We want to be sure that only one process does this at a time.
	 */
	LWLockAcquire(LOG_INDEX_FULLPAGE_FILE_LOCK(ctl), LW_EXCLUSIVE);

	/* Find a free slot to put it in */
	while (polar_stat(path, &stat_buf) == 0)
	{
		if ((*seg_no) >= max_segno)
		{
			/* Failed to find a free slot within specified range */
			LWLockRelease(LOG_INDEX_FULLPAGE_FILE_LOCK(ctl));
			return false;
		}

		(*seg_no)++;
		FULLPAGE_SEG_FILE_NAME(ctl, path, *seg_no);
	}

	/*
	 * Perform the rename using link if available, paranoidly trying to avoid
	 * overwriting an existing file (there shouldn't be one).
	 */
	if (durable_rename(tmppath, path, LOG) != 0)
	{
		LWLockRelease(LOG_INDEX_FULLPAGE_FILE_LOCK(ctl));
		/* durable_link_or_rename already emitted log message */
		return false;
	}

	LWLockRelease(LOG_INDEX_FULLPAGE_FILE_LOCK(ctl));

	return true;
}

static void
fill_fullpage_file_zero_pages(int fd, char *tmppath)
{
#define ONE_MB (1024 * 1024L)
	char	   *data;
	int			nbytes = 0;

	data = palloc_aligned(ONE_MB, PG_IO_ALIGN_SIZE, MCXT_ALLOC_ZERO);

	pgstat_report_wait_start(WAIT_EVENT_FULLPAGE_FILE_INIT_WRITE);

	for (nbytes = 0; nbytes < FULLPAGE_SEGMENT_SIZE; nbytes += ONE_MB)
	{
		int			rc = 0;

		errno = 0;
		rc = (int) polar_pwrite(fd, data, ONE_MB, nbytes);

		if (rc != ONE_MB)
		{
			int			save_errno = errno;

			/*
			 * If we fail to make the file, delete it to release disk space
			 */
			polar_unlink(tmppath);

			polar_close(fd);

			/* if write didn't set errno, assume problem is no disk space */
			errno = save_errno ? save_errno : ENOSPC;

			pfree(data);
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write to file \"%s\": %m", tmppath)));
		}
	}

	pgstat_report_wait_end();

	pfree(data);

	if (polar_fsync(fd) != 0)
	{
		int			save_errno = errno;

		polar_close(fd);
		errno = save_errno;
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", tmppath)));
	}

	return;
}

/*
 * Create a new FULLPAGE file segment, or open a pre-existing one.
 */
int
polar_fullpage_file_init(polar_fullpage_ctl_t ctl, uint64 fullpage_no)
{
	char		path[MAXPGPATH];
	char		tmppath[MAXPGPATH];
	uint64		installed_segno;
	uint64		max_segno;
	int			fd;
	char		polar_tmppath[MAXPGPATH];

	FULLPAGE_FILE_NAME(ctl, path, fullpage_no);

	/*
	 * Try to use existent file (polar worker maker may have created it
	 * already)
	 */

	fd = BasicOpenFile(path, O_RDWR | PG_BINARY | get_sync_bit(wal_sync_method));

	if (fd < 0)
	{
		if (errno != ENOENT)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", path)));
	}
	else
		return fd;

	/*
	 * Initialize an empty (all zeroes) segment.  NOTE: it is possible that
	 * another process is doing the same thing.  If so, we will end up
	 * pre-creating an extra log segment.  That seems OK, and better than
	 * holding the lock throughout this lengthy process.
	 */
	elog(LOG, "creating and filling new FULLPAGE file");

	snprintf(polar_tmppath, MAXPGPATH, "%s/fullpagetemp.%d", ctl->name, (int) getpid());
	polar_make_file_path_level2(tmppath, polar_tmppath);
	polar_unlink(tmppath);

	/* do not use get_sync_bit() here --- want to fsync only at end of fill */
	fd = BasicOpenFile(tmppath, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);

	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", tmppath)));

#ifdef __linux__

	/*
	 * POLAR: use FALLOC_FL_NO_HIDE_STALE on PFS to optimize appending writes.
	 */
	if (polar_enable_fallocate_no_hide_stale &&
		polar_vfs_type(fd) == POLAR_VFS_PFS &&
		polar_fallocate(fd, FALLOC_FL_NO_HIDE_STALE, 0, FULLPAGE_SEGMENT_SIZE) != 0)
	{
		int			save_errno = errno;

		polar_unlink(tmppath);
		polar_close(fd);
		errno = save_errno;

		elog(ERROR, "fallocate failed \"%s\": %m", tmppath);
	}
	/* POLAR end */
#endif

	fill_fullpage_file_zero_pages(fd, tmppath);

	if (polar_close(fd))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", tmppath)));

	/*
	 * Now move the segment into place with its final name.
	 *
	 * If caller didn't want to use a pre-existing file, get rid of any
	 * pre-existing file.  Otherwise, cope with possibility that someone else
	 * has created the file while we were filling ours: if so, use ours to
	 * pre-create a future log segment.
	 */
	installed_segno = FULLPAGE_FILE_SEG_NO(fullpage_no);

	max_segno = FULLPAGE_FILE_SEG_NO(fullpage_no) + polar_fullpage_keep_segments;

	if (!install_fullpage_file_segment(ctl, &installed_segno, tmppath, max_segno))
	{
		/*
		 * No need for any more future segments, or
		 * install_fullpage_file_segment() failed to rename the file into
		 * place. If the rename failed, opening the file below will fail.
		 */
		polar_unlink(tmppath);
	}

	/* Now open original target segment (might not be file I just made) */
	fd = BasicOpenFile(path, O_RDWR | PG_BINARY | get_sync_bit(wal_sync_method));

	if (fd < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));

	elog(LOG, "done creating and filling new FULLPAGE file %s", path);

	return fd;
}


/*
 * Open a fullpage file segment for read or write.
 */
static int
fullpage_file_open(polar_fullpage_ctl_t ctl, uint64 fullpage_no)
{
	char		path[MAXPGPATH];
	int			fd;

	FULLPAGE_FILE_NAME(ctl, path, fullpage_no);

	fd = BasicOpenFile(path, O_RDWR | PG_BINARY);

	if (fd >= 0)
		return fd;

	if (errno != ENOENT)		/* unexpected failure? */
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", path)));

	return polar_fullpage_file_init(ctl, fullpage_no);
}

/*
 * Drop whatever reservation this process was holding.  Registered as a shmem
 * exit callback so that a proc_exit() that never unwinds through the
 * PG_FINALLY in polar_log_fullpage_snapshot_image() - a FATAL, say - does not
 * leave a slot pinning the sweep floor for the rest of the instance's life.
 */
static void
polar_fullpage_inflight_shmem_exit(int code, Datum arg)
{
	polar_fullpage_ctl_t ctl = (polar_fullpage_ctl_t) DatumGetPointer(arg);

	if (MyProc != NULL)
		pg_atomic_write_u64(&ctl->inflight_fullpage_no[MyProcNumber],
							POLAR_FULLPAGE_NO_INFLIGHT);
}

/*
 * Publish a lower bound before reserving a number.  The fetch_add's full
 * barrier orders this store before any later reservation can advance the
 * sweep floor past us.  The slot remains occupied until both the WAL record
 * and page image are written; remove_old_fullpage_files() also consults it.
 */
static uint64
polar_log_fullpage_begin(polar_fullpage_ctl_t ctl)
{
	static bool exit_callback_registered = false;

	Assert(MyProc != NULL);
	Assert(MyProcNumber < ctl->num_inflight_slots);

	if (unlikely(!exit_callback_registered))
	{
		on_shmem_exit(polar_fullpage_inflight_shmem_exit, PointerGetDatum(ctl));
		exit_callback_registered = true;
	}

	pg_atomic_write_u64(&ctl->inflight_fullpage_no[MyProcNumber],
						pg_atomic_read_u64(&ctl->max_fullpage_no));

	return pg_atomic_fetch_add_u64(&ctl->max_fullpage_no, 1);
}

/*
 * Done with the reservation: the record is in WAL, its gap is recorded and the
 * page image is written, so max_fullpage_no_gap covers it from here on.
 */
static void
polar_log_fullpage_end(polar_fullpage_ctl_t ctl)
{
	/* Publish the gap (or our observation of an existing bound) before idle. */
	pg_memory_barrier();
	pg_atomic_write_u64(&ctl->inflight_fullpage_no[MyProcNumber],
						POLAR_FULLPAGE_NO_INFLIGHT);
}

/*
 * POLAR: write old version page to fullpage file
 */
static void
polar_write_fullpage(polar_fullpage_ctl_t ctl, Page page, uint64 fullpage_no)
{
	if (open_fullpage_file < 0)
	{
		open_fullpage_file = fullpage_file_open(ctl, fullpage_no);
		open_fullpage_file_seg_no = FULLPAGE_FILE_SEG_NO(fullpage_no);
	}
	else if (open_fullpage_file_seg_no != FULLPAGE_FILE_SEG_NO(fullpage_no))
	{
		polar_close(open_fullpage_file);
		open_fullpage_file = fullpage_file_open(ctl, fullpage_no);
		open_fullpage_file_seg_no = FULLPAGE_FILE_SEG_NO(fullpage_no);
	}

	polar_pwrite(open_fullpage_file, (void *) page, BLCKSZ, FULLPAGE_FILE_OFFSET(fullpage_no));
}

/*
 * POLAR: read old version page from fullpage file
 */
void
polar_read_fullpage(polar_fullpage_ctl_t ctl, Page page, uint64 fullpage_no)
{
	if (open_fullpage_file < 0)
	{
		open_fullpage_file = fullpage_file_open(ctl, fullpage_no);
		open_fullpage_file_seg_no = FULLPAGE_FILE_SEG_NO(fullpage_no);
	}
	else if (open_fullpage_file_seg_no != FULLPAGE_FILE_SEG_NO(fullpage_no))
	{
		polar_close(open_fullpage_file);
		open_fullpage_file = fullpage_file_open(ctl, fullpage_no);
		open_fullpage_file_seg_no = FULLPAGE_FILE_SEG_NO(fullpage_no);
	}

	polar_pread(open_fullpage_file, (void *) page, BLCKSZ, FULLPAGE_FILE_OFFSET(fullpage_no));
}

/*
 * Preallocate fullpage files
 */
void
polar_prealloc_fullpage_files(polar_fullpage_ctl_t ctl)
{
	uint64		max_fullpage_no = 0;
	int			file = 0;
	int			count = 0;

	if (!ctl)
		return;

	max_fullpage_no = pg_atomic_read_u64(&ctl->max_fullpage_no);

	while (count < 3)
	{
		file = polar_fullpage_file_init(ctl, max_fullpage_no);
		polar_close(file);
		max_fullpage_no += FULLPAGE_NUM_PER_FILE;
		count++;
	}
}


/*
 * Recycle or remove a fullpage file that's no longer needed.
 */
void
polar_remove_old_fullpage_file(polar_fullpage_ctl_t ctl, const char *segname, uint64 min_fullpage_seg_no)
{
	struct stat statbuf;
	uint64		end_fullpage_seg_no = FULLPAGE_FILE_SEG_NO(pg_atomic_read_u64(&ctl->max_fullpage_no));
	uint64		recycle_seg_no = min_fullpage_seg_no + polar_fullpage_keep_segments;

	/*
	 * Before deleting the file, see if it can be recycled as a future
	 * fullpage segment.
	 */
	if (end_fullpage_seg_no <= recycle_seg_no &&
		polar_lstat(segname, &statbuf) == 0 && S_ISREG(statbuf.st_mode) &&
		install_fullpage_file_segment(ctl, &end_fullpage_seg_no, segname,
									  recycle_seg_no))
	{
		/* POLAR: force log */
		ereport(LOG,
				(errmsg("recycled fullpage file \"%s\"",
						segname)));
	}
	else
	{
		/* No need for any more future segments... */
		int			rc;

		/* POLAR: force log */
		ereport(LOG,
				(errmsg("removing fullpage file \"%s\"",
						segname)));

		rc = durable_unlink(segname, LOG);

		if (rc != 0)
		{
			/* Message already logged by durable_unlink() */
			return;
		}
	}
}

/*
 * Recycle or remove all fullpage files older to passed segno.
 * It's a extern function
 */
void
polar_remove_old_fullpage_files(polar_fullpage_ctl_t ctl, XLogRecPtr min_lsn)
{
	polar_logindex_truncate(ctl->logindex_snapshot, min_lsn);
	remove_old_fullpage_files(ctl);
}

static uint64
polar_xlog_read_fullpage_no(log_index_snapshot logindex_snapshot, XLogRecPtr lsn)
{
	XLogRecord *record;
	XLogReaderState *xlogreader;
	char	   *errormsg;
	uint64		fullpage_no = 0;

	xlogreader = XLogReaderAllocate(wal_segment_size, NULL,
									XL_ROUTINE(.page_read = &read_local_xlog_page,
											   .segment_open = &wal_segment_open,
											   .segment_close = &wal_segment_close),
									NULL);

	if (!xlogreader)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));

	XLogBeginRead(xlogreader, lsn);
	record = XLogReadRecord(xlogreader, &errormsg);

	if (record == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read fullpage wal state from WAL at %X/%X",
						LSN_FORMAT_ARGS(lsn))));

	if (XLogRecGetRmid(xlogreader) != RM_XLOG_ID ||
		(XLogRecGetInfo(xlogreader) & ~XLR_INFO_MASK) != POLAR_WAL ||
		*((PolarWalType *) XLogRecGetData(xlogreader)) != PWT_FPSI)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("expected fullpage wal state data is not present in WAL at %X/%X",
						LSN_FORMAT_ARGS(lsn))));

	/* get fullpage_no from record */
	memcpy(&fullpage_no, XLogRecGetData(xlogreader) + sizeof(PolarWalType), sizeof(uint64));
	XLogReaderFree(xlogreader);
	return fullpage_no;
}

/*
 * Recycle or remove all fullpage files older to passed segno.
 */
static void
remove_old_fullpage_files(polar_fullpage_ctl_t ctl)
{
	char		path[MAXPGPATH] = {0};
	uint64		min_fullpage_seg_no = 0;
	uint64		fullpage_no = 0;
	uint64		min_fullpage_no = 0;
	uint64		min_inflight_no = POLAR_FULLPAGE_NO_INFLIGHT;
	uint64		margin = 0;
	int			i;
	struct stat statbuf;
	XLogSegNo	xlog_seg_no = 0;
	XLogRecPtr	min_lsn = InvalidXLogRecPtr;
	XLogRecPtr	sweep_safe_lsn = InvalidXLogRecPtr;

	if (polar_is_replica())
		return;

	/*
	 * POLAR: the barrier below has to sit above everything an earlier
	 * instance made durable, so it can only be armed once recovery has ended.
	 * While recovery is in progress - primary crash recovery, or the tail of
	 * an online promote - the replay position is still below the previous
	 * instance's last record, and a barrier armed there would let that range
	 * be swept using only our own measurements.  Nothing requests a
	 * checkpoint that early today; this makes it structural rather than
	 * incidental.
	 */
	if (RecoveryInProgress())
		return;

	min_lsn = polar_get_logindex_snapshot_storage_min_lsn(ctl->logindex_snapshot);

	/*
	 * POLAR: the margin below is derived from max_fullpage_no_gap, which only
	 * describes the records this instance inserted; whatever an earlier
	 * instance left behind could be inverted by more than it says.  Rather
	 * than making the gap durable, arm a barrier at the first sweep and
	 * refuse to remove anything until min_lsn has passed it - by then every
	 * record still in scope is one of ours.  This costs some retained
	 * fullpage files after every restart and promotion, and nothing in steady
	 * state.
	 */
	sweep_safe_lsn = pg_atomic_read_u64(&ctl->sweep_safe_lsn);

	if (XLogRecPtrIsInvalid(sweep_safe_lsn))
	{
		/*
		 * GetXLogInsertRecPtr() names the reserve frontier, not a record
		 * boundary, so this can sit a little above the last record actually
		 * inserted.  That is the safe direction: it only holds the first
		 * sweep back until min_lsn passes a slightly higher point.
		 */
		sweep_safe_lsn = GetXLogInsertRecPtr();
		pg_atomic_write_u64(&ctl->sweep_safe_lsn, sweep_safe_lsn);
		elog(LOG, "fullpage files will be swept once the logindex min lsn reaches %X/%X",
			 LSN_FORMAT_ARGS(sweep_safe_lsn));
		return;
	}

	if (min_lsn < sweep_safe_lsn)
		return;

	/* check wal file is exist or not */
	XLByteToSeg(min_lsn, xlog_seg_no, wal_segment_size);
	XLogFilePath(path, GetWALInsertionTimeLine(), xlog_seg_no, wal_segment_size);

	/* wal file has been removed already, we cannot get fullpage_no by lsn */
	if (polar_lstat(path, &statbuf) < 0)
		return;

	fullpage_no = polar_xlog_read_fullpage_no(ctl->logindex_snapshot, min_lsn);

	/*
	 * POLAR: fullpage_no is reserved without holding a lock across XLogInsert
	 * (see polar_log_fullpage_begin), so the record at min_lsn does not carry
	 * the smallest fullpage_no still in use - a record with a higher lsn can
	 * carry a lower one.  Two things bound that inversion by measurement
	 * rather than by assumption, and the sweep floors by both.
	 *
	 * max_fullpage_no_gap covers the reservations that reached WAL.  It says
	 * nothing about one still in flight: the counter keeps advancing past a
	 * writer stalled between its fetch_add and its insert, and a whole
	 * segment of that is only a couple of seconds of a busy future-page
	 * workload, so without the second bound the floor could climb over a
	 * segment that is about to be written.  The reservation would then land
	 * on an unlinked inode through the writer's cached fd, and a replica
	 * would later read that slot back out of a freshly zero-filled file.  So
	 * also floor by the oldest reservation still in flight, which every
	 * writer publishes before it takes its number.
	 *
	 * The three reads have to happen in this order.  Take any reservation N
	 * below fullpage_no that must survive.  N came off the counter before the
	 * record at min_lsn took its own number, which happened before that
	 * record became durable, so N was published before we read fullpage_no
	 * above.  If its writer is still in flight when we scan the slots we see
	 * it there; if it finished first, it recorded its gap before we scan, and
	 * therefore before we read max_fullpage_no_gap.  Reading the gap first
	 * would leave a window in which a writer that finishes in between is
	 * covered by neither.
	 *
	 * Keep at least one whole segment as a conservative minimum margin.
	 *
	 * Note that polar_fullpage_keep_segments gives no protection here: a
	 * recycled segment is refilled with new fullpage_no values, so removal
	 * and recycling both destroy the old snapshots.  This floor is the only
	 * thing keeping an inverted-but-still-needed snapshot alive.
	 */
	pg_memory_barrier();

	for (i = 0; i < ctl->num_inflight_slots; i++)
	{
		uint64		inflight_no = pg_atomic_read_u64(&ctl->inflight_fullpage_no[i]);

		if (inflight_no < min_inflight_no)
			min_inflight_no = inflight_no;
	}

	pg_memory_barrier();

	margin = Max(FULLPAGE_NUM_PER_FILE, pg_atomic_read_u64(&ctl->max_fullpage_no_gap));

	if (fullpage_no <= margin)
		return;

	min_fullpage_no = Min(fullpage_no - margin, min_inflight_no);
	min_fullpage_seg_no = FULLPAGE_FILE_SEG_NO(min_fullpage_no);

	while (min_fullpage_seg_no > 0)
	{
		/* Must remove segments older than min_fullpage_seg_no */
		min_fullpage_seg_no--;

		FULLPAGE_SEG_FILE_NAME(ctl, path, min_fullpage_seg_no);

		/* File has been removed already */
		if (polar_lstat(path, &statbuf) < 0)
			break;

		elog(LOG, "attempting to remove FULLPAGE segments older than log file %s",
			 path);
		polar_remove_old_fullpage_file(ctl, path, min_fullpage_seg_no);
	}
}

/*
 * POLAR: write a WAL record containing a full snapshot image of a page. Caller is
 * responsible for writing the page to disk after calling this routine.
 */
XLogRecPtr
polar_log_fullpage_snapshot_image(polar_fullpage_ctl_t ctl, Buffer buffer, XLogRecPtr oldest_apply_lsn)
{
	SMgrRelation smgr = NULL;
	int			flags;
	RelFileLocator rlocator;
	ForkNumber	forkNum;
	BlockNumber blkno;
	XLogRecPtr	recptr;
	BufferDesc *buf_hdr = GetBufferDescriptor(buffer - 1);
	uint64		fullpage_no = 0;
	uint64		gap;
	static char *fullpage = NULL;
	PolarWalType wtype = PWT_FPSI;

	/*
	 * We allocate the old page space once and use it over on each subsequent
	 * call.
	 */
	if (fullpage == NULL)
		fullpage = MemoryContextAllocAligned(TopMemoryContext, BLCKSZ, PG_IO_ALIGN_SIZE, 0);

	Assert(polar_is_primary());

	flags = REGBUF_NO_IMAGE | REGBUF_NO_CHANGE;
	BufferGetTag(buffer, &rlocator, &forkNum, &blkno);

	/*
	 * If we have copy buffer, and copy buffer can be flushed, just treat copy
	 * buffer as fullpage, so we can avoid reading older page from storage
	 */
	if (buf_hdr->copy_buffer &&
		polar_copy_buffer_get_lsn(buf_hdr->copy_buffer) <= oldest_apply_lsn)
		memcpy(fullpage, (char *) CopyBufHdrGetBlock(buf_hdr->copy_buffer), BLCKSZ);
	else
	{
		/* Find smgr relation for buffer */
		smgr = smgropen(rlocator, INVALID_PROC_NUMBER);

		smgrread(smgr, forkNum, blkno, (char *) fullpage);

		/* check for garbage data, decrypt page if it has beed encrypted */
		if (!PageIsVerified((Page) fullpage, forkNum, blkno, smgr))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("invalid page in block %u of relation %u/%u/%u_%d",
							blkno,
							smgr->smgr_rlocator.locator.spcOid,
							smgr->smgr_rlocator.locator.dbOid,
							smgr->smgr_rlocator.locator.relNumber,
							forkNum)));

		pg_atomic_add_fetch_u64(&ctl->stat.read_oldpage_count, 1);
	}

	/*
	 * POLAR: reserve a fullpage_no.  This is not serialized against the
	 * XLogInsert below, so fullpage_no is only approximately ordered with
	 * lsn; see polar_log_fullpage_begin() and remove_old_fullpage_files().
	 */
	fullpage_no = polar_log_fullpage_begin(ctl);

	PG_TRY();
	{
		XLogBeginInsert();
		XLogRegisterBuffer(0, buffer, flags);
		XLogRegisterData((char *) (&wtype), sizeof(PolarWalType));
		XLogRegisterData((char *) (&fullpage_no), sizeof(uint64));

		recptr = XLogInsert(RM_XLOG_ID, POLAR_WAL);
		pg_atomic_write_u64(&ctl->stat.insert_fullpage_lsn, recptr);

		/*
		 * POLAR: record how far the counter ran ahead of us while we were
		 * inserting.  This is the bound remove_old_fullpage_files() sweeps by
		 * for reservations that are no longer in flight.
		 *
		 * Suppose a record s ends up with a lower fullpage_no than a record r
		 * that has a lower lsn - the inversion the sweep has to survive.  s
		 * reserved before r did, and r reserved before it took its WAL space,
		 * which it did before s took its own.  So by the time s reaches this
		 * line the counter is already past r's fullpage_no, and this gap is
		 * at least the distance between them.  The largest gap ever recorded
		 * therefore covers every inversion, with no assumption about how long
		 * a writer can stall.
		 *
		 * Record the gap before releasing the reservation below, so that the
		 * sweep never sees a fullpage_no neither protects: it scans the
		 * in-flight slots before it reads max_fullpage_no_gap.
		 */
		gap = pg_atomic_read_u64(&ctl->max_fullpage_no) - fullpage_no;

		if (atomic_raise_u64(&ctl->max_fullpage_no_gap, gap) &&
			gap > FULLPAGE_NUM_PER_FILE)
			elog(LOG, "fullpage_no reservation fell " UINT64_FORMAT " entries behind, more than one segment; the fullpage sweep margin now covers it",
				 gap);

		polar_write_fullpage(ctl, fullpage, fullpage_no);
		pg_atomic_add_fetch_u64(&ctl->stat.write_fullpage_count, 1);
	}
	PG_FINALLY();
	{
		polar_log_fullpage_end(ctl);
	}
	PG_END_TRY();

	return recptr;
}

/*
 * POLAR: this function is only used for polar worker pop record from queue
 */
static XLogRecord *
polar_fullpage_bgworker_xlog_recv_queue_pop(polar_fullpage_ctl_t ctl, polar_ringbuf_ref_t *ref,
											XLogReaderState *state, char **errormsg)
{
	XLogRecord *record = NULL;
	DecodedXLogRecord *decoded = NULL;
	uint32		pktlen = 0;
	uint32		xlog_len;
	XLogRecPtr	read_rec_ptr,
				end_rec_ptr;
	bool		pop_storage;

	*errormsg = NULL;
	state->errormsg_buf[0] = '\0';

	if (XLogRecPtrIsInvalid(state->EndRecPtr))
	{
		state->EndRecPtr = GetXLogReplayRecPtr(NULL);
		elog(LOG, "Fullpage redo start from %lX", state->EndRecPtr);
	}

	state->currRecPtr = state->EndRecPtr;

	do
	{
		ssize_t		offset = 0;
		uint8		pkt_type;

		record = NULL;
		pop_storage = false;
		pkt_type = polar_ringbuf_next_pkt_type(ref, &pktlen);

		switch (pkt_type)
		{
			case POLAR_RINGBUF_PKT_WAL_META:
				{
					POLAR_COPY_QUEUE_CONTENT(ref, offset, &end_rec_ptr, sizeof(XLogRecPtr));
					POLAR_COPY_QUEUE_CONTENT(ref, offset, &xlog_len, sizeof(uint32));
					read_rec_ptr = end_rec_ptr - xlog_len;

					XLogReleasePreviousRecord(state);
					POLAR_ASSERT_PANIC(!XLogReaderHasQueuedRecordOrError(state));
					decoded = polar_xlog_queue_decode_record(ref, offset, pktlen, read_rec_ptr, end_rec_ptr, state, false);
					decoded = XLogNextRecord(state, errormsg);
					record = &decoded->header;

					polar_ringbuf_update_ref(ref);
					break;
				}

			case POLAR_RINGBUF_PKT_WAL_STORAGE_BEGIN:
				{
					polar_ringbuf_update_ref(ref);
					pop_storage = true;
					break;
				}

			default:
				break;
		}
	}
	while ((record && (state->ReadRecPtr <= polar_get_logindex_snapshot_max_lsn(ctl->logindex_snapshot)
					   || state->ReadRecPtr < state->currRecPtr)) || pop_storage);

	return record;
}

/*
 * POLAR: polar worker replay fullpage wal record, only replica
 * can do this
 */
void
polar_bgworker_fullpage_snapshot_replay(polar_fullpage_ctl_t ctl)
{
	XLogRecord *record;
	char	   *errormsg = NULL;
	static TimestampTz last_load_logindex_time = 0;
	static polar_ringbuf_ref_t ref =
	{
		.slot = -1
	};
	static XLogReaderState *xlogreader = NULL;

	if (!polar_enable_fullpage_snapshot)
	{
		/* release ref when fullpage snapshot is disabled */
		if (unlikely(ref.slot != -1))
		{
			polar_ringbuf_release_ref(&ref);
			ref.slot = -1;
		}
		return;
	}

	/* wait util reach consistency mode */
	if (!HotStandbyActive())
		return;

	if (unlikely(ref.slot == -1))
	{
		if (polar_fullpage_online_promote)
			return;

		POLAR_XLOG_QUEUE_NEW_REF(&ref, ctl->queue, true, "polar_fullpage_xlog_queue");
	}

	/* Set up XLOG reader facility */
	if (!xlogreader)
		xlogreader = XLogReaderAllocate(wal_segment_size, NULL,
										XL_ROUTINE(.page_read = &read_local_xlog_page,
												   .segment_open = &wal_segment_open,
												   .segment_close = &wal_segment_close),
										NULL);

	if (!xlogreader)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));

	for (;;)
	{
		record = polar_fullpage_bgworker_xlog_recv_queue_pop(ctl, &ref, xlogreader, &errormsg);

		if (record && record->xl_rmid == RM_XLOG_ID &&
			record->xl_info == POLAR_WAL &&
			*((PolarWalType *) XLogRecGetData(xlogreader)) == PWT_FPSI)
		{
			BufferTag	tag;

			POLAR_GET_LOG_TAG(xlogreader, tag, 0);
			POLAR_LOG_INDEX_ADD_LSN(ctl->logindex_snapshot, xlogreader, &tag);
		}

		if (record == NULL)
		{
			TimestampTz now = GetCurrentTimestamp();
			XLogRecPtr	last_replay_end_ptr = polar_get_xlog_replay_recptr_nolock();

			CHECK_FOR_INTERRUPTS();

			if (polar_fullpage_get_online_promote())
			{
				polar_ringbuf_release_ref(&ref);
				ref.slot = -1;
				break;
			}

			/* update min read position for xlog recv queue */
			polar_ringbuf_update_keep_data(ctl->queue);

			if (errormsg != NULL)
			{
				elog(WARNING, "Failed to read xlog for fullpage, errmsg=%s", errormsg);
				break;
			}

			/*
			 * We load logindex from storage in following cases: 1. read from
			 * file instead of queue 2. wal receiver hang or lost for a long
			 * time(1s?) 3. periodly load logindex from storage(1s)
			 */
			if (TimestampDifferenceExceeds(
										   polar_get_walrcv_last_msg_receipt_time(), now, 1000) ||
				TimestampDifferenceExceeds(
										   last_load_logindex_time, now, 1000))
			{
				last_load_logindex_time = now;
				polar_load_logindex_snapshot_from_storage(ctl->logindex_snapshot, InvalidXLogRecPtr);
				pg_atomic_write_u64(&ctl->stat.load_fullpage_lsn, ctl->logindex_snapshot->max_lsn);
			}

			/*
			 * POLAR: when read from wal file instead of queue maybe very
			 * slow. we use replay_lsn for polar worker catch up with starup
			 * replay when replay fullpage wal, because polar worker don't
			 * need to replay wal lsn less than replay_lsn, the future page
			 * fullpage snapshot wal lsn must be bigger than replay_lsn
			 */

			if (xlogreader->EndRecPtr < last_replay_end_ptr)
				xlogreader->EndRecPtr = last_replay_end_ptr;

			break;
		}
		else
			pg_atomic_write_u64(&ctl->stat.fullpage_bgworker_replay_lsn, xlogreader->ReadRecPtr);
	}
}

void
polar_fullpage_bgworker_wait_notify(polar_fullpage_ctl_t ctl)
{
	if (ctl)
		pg_atomic_write_u32(&ctl->procno, MyProc->vxid.procNumber);
}

void
polar_fullpage_bgworker_wakeup(polar_fullpage_ctl_t ctl)
{
	int			procno;

	if (ctl)
	{
		procno = pg_atomic_read_u32(&ctl->procno);

		if (procno != INVALID_PROC_NUMBER)
		{
			pg_atomic_write_u32(&ctl->procno, INVALID_PROC_NUMBER);

			/*
			 * Not acquiring ProcArrayLock here which is slightly icky. It's
			 * actually fine because procLatch isn't ever freed, so we just
			 * can potentially set the wrong process' (or no process') latch.
			 */
			SetLatch(&ProcGlobal->allProcs[procno].procLatch);
		}
	}
}

void
polar_fullpage_set_online_promote(bool online_promote)
{
	polar_fullpage_online_promote = online_promote;
}

bool
polar_fullpage_get_online_promote(void)
{
	return polar_fullpage_online_promote;
}
