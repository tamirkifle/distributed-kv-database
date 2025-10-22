package com.ledgerkv.storage.compaction;

import com.ledgerkv.storage.lsm.SSTableHandle;
import java.util.List;
import java.util.Optional;

/** Decides which tables to compact next, given a snapshot of the current SSTable set. */
public interface CompactionStrategy {

    /** The next compaction to run, or {@link Optional#empty()} if no compaction is warranted. */
    Optional<CompactionTask> planCompaction(List<SSTableHandle> tables);
}
