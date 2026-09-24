/*-------------------------------------------------------------------------
 *
 * polar_shmem_tune.c
 *	  Placement tuning for the main shared-memory segment.
 *
 * Placement helpers applied to PolarDB's main shared-memory segment regardless
 * of its backing (anonymous mmap or the zero-copy memfd): a whole-segment
 * MPOL_INTERLEAVE policy, and (see polar_shmem_prefault_range) prefaulting
 * of the whole segment. Both are Linux-only and best-effort.
 *
 * IDENTIFICATION
 *	  src/backend/storage/ipc/polar_shmem_tune.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef __linux__
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

#include "port/pg_bitutils.h"
#include "storage/pg_shmem.h"	/* huge_pages, huge_pages_status */
#include "storage/polar_shmem_tune.h"
#include "utils/guc.h"

bool		polar_shmem_numa_interleave = true;
bool		polar_shmem_prefault = true;

/*
 * MADV_POPULATE_WRITE advice (kernel >= 5.14). Defined by hand rather
 * than testing for the glibc macro (only since 2.36), so builds on older
 * toolchains still prefault on modern kernels; the kernel's own EINVAL at
 * runtime is the real availability signal.
 */
#if defined(__linux__) && !defined(MADV_POPULATE_WRITE)
#define MADV_POPULATE_WRITE 23
#endif

/*
 * NUMA interleave constants. Use the syscalls directly rather than depend on
 * libnuma / <numaif.h>, which the build does not link; these are stable kernel
 * ABI values.
 */
#ifndef MPOL_DEFAULT
#define MPOL_DEFAULT 0
#endif
#ifndef MPOL_INTERLEAVE
#define MPOL_INTERLEAVE 3
#endif
#ifndef MPOL_F_MEMS_ALLOWED
#define MPOL_F_MEMS_ALLOWED (1 << 2)
#endif
#define POLAR_NUMA_NMASK_BITS	1024
#define POLAR_NUMA_NMASK_LONGS	(POLAR_NUMA_NMASK_BITS / (8 * (int) sizeof(unsigned long)))

/*
 * Apply MPOL_INTERLEAVE before pages are faulted. Anonymous mappings carry the
 * policy into forked children; regular memfds store it on the shmem inode, so
 * all mappings use it. Hugetlb memfds keep it VMA-local, so their pages must be
 * faulted through this mapping (see polar_shmem_prefault_range()).
 *
 * Do nothing with fewer than two allowed NUMA nodes or when the postmaster has
 * an explicit memory policy. Failures are nonfatal and leave first-touch
 * placement in effect.
 */
bool
polar_shmem_numa_interleave_range(void *base, Size len)
{
	if (!polar_shmem_numa_interleave)
		return false;

#if defined(__linux__) && defined(SYS_mbind) && defined(SYS_get_mempolicy)
	{
		unsigned long nodemask[POLAR_NUMA_NMASK_LONGS];
		int			mode = MPOL_DEFAULT;
		int			nnodes = 0;

		/*
		 * Leave an operator's explicit placement alone. A process mempolicy
		 * (numactl --membind / --interleave / --preferred, i.e.
		 * set_mempolicy) shows up in the mode returned by get_mempolicy()
		 * with flags 0; it is *not* reflected in MPOL_F_MEMS_ALLOWED below,
		 * which reports the cpuset mems (cgroup cpuset.mems, docker
		 * --cpuset-mems) instead. Both have to be consulted: mbind()
		 * validates its nodemask against the cpuset only, so without this
		 * check we would happily override a numactl --membind the operator
		 * asked for.
		 */
		if (syscall(SYS_get_mempolicy, &mode, NULL, 0ul, NULL, 0ul) != 0)
		{
			elog(WARNING, "shared-memory NUMA interleave: get_mempolicy() failed: %m");
			return false;
		}

		if (mode != MPOL_DEFAULT)
		{
			/*
			 * Do not fight it - but say so, because inheriting a mempolicy
			 * unintentionally (a numactl wrapper in a service unit, a
			 * container runtime) is otherwise invisible: the operator would
			 * just see the interleave silently not happen.
			 */
			ereport(LOG,
					(errmsg("skipping shared-memory NUMA interleave: the postmaster already has an explicit memory policy (MPOL mode %d)",
							mode),
					 errhint("Placement is left to that policy, typically inherited from numactl.")));
			return false;
		}

		memset(nodemask, 0, sizeof(nodemask));

		/* the set of nodes this process is allowed to allocate from */
		if (syscall(SYS_get_mempolicy, NULL, nodemask,
					(unsigned long) POLAR_NUMA_NMASK_BITS, NULL,
					(unsigned long) MPOL_F_MEMS_ALLOWED) != 0)
		{
			elog(WARNING, "shared-memory NUMA interleave: get_mempolicy(MEMS_ALLOWED) failed: %m");
			return false;
		}

		nnodes = pg_popcount((char *) nodemask, POLAR_NUMA_NMASK_BITS / 8);

		if (nnodes < 2)
			return false;		/* one usable node: nothing to interleave */

		if (syscall(SYS_mbind, base, (unsigned long) len, MPOL_INTERLEAVE,
					nodemask, (unsigned long) POLAR_NUMA_NMASK_BITS, 0u) != 0)
		{
			elog(WARNING, "shared-memory NUMA interleave: mbind(MPOL_INTERLEAVE) failed: %m");
			return false;
		}

		ereport(LOG,
				(errmsg("interleaved %zu-byte shared-memory segment across %d NUMA nodes",
						len, nnodes)));
		return true;
	}
#endif
	return false;
}

/*
 * Prefault the huge-page segment in the postmaster after NUMA policy setup and
 * before children are forked. This moves page allocation and zeroing to startup
 * and fixes placement, at the cost of longer startup and crash restarts.
 *
 * Segments not backed by huge pages are skipped. An unsupported or failed
 * madvise() is nonfatal; pages it did not populate retain normal demand-fault
 * behavior.
 */
void
polar_shmem_prefault_range(void *base, Size len)
{
	static bool reported_no_huge = false;

	if (!polar_shmem_prefault)
		return;

	/* prefault only when the segment really got huge pages (see above) */
	if (huge_pages_status != HUGE_PAGES_ON)
	{
		if (!reported_no_huge)
		{
			reported_no_huge = true;

			/*
			 * Only suggest configuring huge pages to someone who asked for
			 * them: under huge_pages = off the lazy faulting is what the
			 * operator chose, and telling them to turn huge pages on would be
			 * arguing with their own setting.
			 */
			if (huge_pages == HUGE_PAGES_OFF)
				ereport(LOG,
						(errmsg("not prefaulting shared memory: huge pages are "
								"disabled")));
			else
				ereport(LOG,
						(errmsg("not prefaulting shared memory: huge pages are "
								"not in use (huge_pages_status = \"%s\")",
								GetConfigOption("huge_pages_status", false, false)),
						 errhint("Configure a hugetlb pool so the shared-memory "
								 "segment can use huge pages, or set "
								 "polar_shmem_prefault = off.")));
		}
		return;
	}

#if defined(__linux__) && defined(MADV_POPULATE_WRITE)

	/*
	 * base is an mmap() result, so it is page-aligned as madvise() requires;
	 * the kernel rounds len up.
	 */
	{
		static bool warned_madvise = false;

		if (madvise(base, len, MADV_POPULATE_WRITE) == 0)
			return;

		if (!warned_madvise)
		{
			warned_madvise = true;
			if (errno == EINVAL)
				ereport(WARNING,
						(errmsg("MADV_POPULATE_WRITE unsupported by this kernel "
								"(needs 5.14+); shared memory left lazily faulted")));
			else
				ereport(WARNING,
						(errmsg("could not prefault %zu-byte shared-memory range, "
								"leaving it lazily faulted: %m", len)));
		}

	}
#endif
}
