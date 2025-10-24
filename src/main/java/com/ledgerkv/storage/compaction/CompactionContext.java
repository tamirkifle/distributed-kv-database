package com.ledgerkv.storage.compaction;

import com.ledgerkv.storage.lsm.SSTableHandle;
import java.util.List;

/**
 * The engine seam the {@link Compactor} drives. Implementations supply a point-in-time snapshot of
 * the live SSTable set and atomically swap a compaction's inputs for its outputs (copy-on-write, so
 * readers are never blocked). Kept tiny so compaction is testable without the full engine; the
 * {@code LsmEngine} (sub-plan 1d) provides the real copy-on-write implementation.
 */
public interface CompactionContext {

    /** A snapshot of the current SSTables. The returned list is owned by the caller. */
    List<SSTableHandle> currentTables();

    /** Atomically remove {@code result.obsolete()} and add {@code result.added()} to the live set. */
    void apply(CompactionResult result);
}
