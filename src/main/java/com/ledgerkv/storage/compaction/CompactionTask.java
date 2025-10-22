package com.ledgerkv.storage.compaction;

import com.ledgerkv.storage.lsm.SSTableHandle;
import java.util.Collections;
import java.util.List;

/** A planned compaction: the input tables to merge, the level the output lands at, and whether
 *  tombstones may be dropped (only when the output is the bottom of the tree). */
public final class CompactionTask {

    private final List<SSTableHandle> inputs;
    private final int outputLevel;
    private final boolean dropTombstones;

    public CompactionTask(List<SSTableHandle> inputs, int outputLevel, boolean dropTombstones) {
        this.inputs = Collections.unmodifiableList(new java.util.ArrayList<>(inputs));
        this.outputLevel = outputLevel;
        this.dropTombstones = dropTombstones;
    }

    public List<SSTableHandle> inputs() {
        return inputs;
    }

    public int outputLevel() {
        return outputLevel;
    }

    public boolean dropTombstones() {
        return dropTombstones;
    }
}
