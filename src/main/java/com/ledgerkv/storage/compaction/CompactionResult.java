package com.ledgerkv.storage.compaction;

import com.ledgerkv.storage.lsm.SSTableHandle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** The outcome of executing a {@link CompactionTask}: the new SSTables produced and the input
 *  tables they replace (now obsolete and safe to remove once readers have moved on). */
public final class CompactionResult {

    private final List<SSTableHandle> added;
    private final List<SSTableHandle> obsolete;

    public CompactionResult(List<SSTableHandle> added, List<SSTableHandle> obsolete) {
        this.added = Collections.unmodifiableList(new ArrayList<>(added));
        this.obsolete = Collections.unmodifiableList(new ArrayList<>(obsolete));
    }

    public List<SSTableHandle> added() {
        return added;
    }

    public List<SSTableHandle> obsolete() {
        return obsolete;
    }
}
