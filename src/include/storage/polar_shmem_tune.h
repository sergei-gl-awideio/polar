/*-------------------------------------------------------------------------
 *
 * polar_shmem_tune.h
 *	  Placement tuning for the main shared-memory segment.
 *
 * Helpers that place PolarDB's main shared-memory segment well on NUMA
 * hardware, regardless of how the segment is backed (anonymous mmap or the
 * zero-copy memfd, see polar_zc.h).
 *
 * Each is gated by its own PGC_POSTMASTER GUC and is a Linux-only, best-effort
 * no-op elsewhere.
 *
 * IDENTIFICATION
 *	  src/include/storage/polar_shmem_tune.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef POLAR_SHMEM_TUNE_H
#define POLAR_SHMEM_TUNE_H

/*
 * Interleave the main shared-memory segment across NUMA nodes. PGC_POSTMASTER.
 * Applies to any segment backing; a no-op on single-node hosts. See
 * polar_shmem_numa_interleave_range().
 */
extern PGDLLIMPORT bool polar_shmem_numa_interleave;

/*
 * Install an MPOL_INTERLEAVE policy on [base, base+len) before it is faulted,
 * so its pages are spread across all NUMA nodes the process may allocate from.
 * Gated by polar_shmem_numa_interleave; best-effort, a no-op on a single node,
 * and it stands aside (reporting why, at LOG) when the postmaster already
 * carries an explicit mempolicy. Returns true if the policy was actually
 * installed, false otherwise. Must run before the range is touched (i.e. at
 * segment creation).
 */
extern bool polar_shmem_numa_interleave_range(void *base, Size len);

/*
 * Prefault the whole main shared-memory segment at startup, when it is
 * backed by huge pages. PGC_POSTMASTER. See polar_shmem_prefault_range().
 */
extern PGDLLIMPORT bool polar_shmem_prefault;

/*
 * Fault in every page of [base, base+len) in the postmaster, under the
 * segment's NUMA policy, so placement is fixed before fork and huge-page
 * allocation and zeroing happen here rather than in backends. Gated by
 * polar_shmem_prefault and on huge pages actually being in use
 * (huge_pages_status = on); best-effort. Called once on the whole segment at
 * creation, from both segment backings.
 */
extern void polar_shmem_prefault_range(void *base, Size len);

#endif							/* POLAR_SHMEM_TUNE_H */
