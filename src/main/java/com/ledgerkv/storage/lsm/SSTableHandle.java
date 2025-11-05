package com.ledgerkv.storage.lsm;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * An open {@link SSTable} paired with the metadata compaction needs: its on-disk path, the level
 * it lives at, its file size in bytes, and its key range. Immutable; closing it closes the
 * underlying table channel.
 */
public final class SSTableHandle implements Closeable {

    private final SSTable table;
    private final Path path;
    private final int level;
    private final long sizeBytes;
    private final String firstKey;
    private final String lastKey;

    private SSTableHandle(SSTable table, Path path, int level, long sizeBytes,
                          String firstKey, String lastKey) {
        this.table = table;
        this.path = path;
        this.level = level;
        this.sizeBytes = sizeBytes;
        this.firstKey = firstKey;
        this.lastKey = lastKey;
    }

    public static SSTableHandle open(Path path, int level) throws IOException {
        SSTable table = SSTable.open(path);
        boolean ok = false;
        try {
            long size = Files.size(path);
            SSTableHandle handle =
                    new SSTableHandle(table, path, level, size, table.firstKey(), table.lastKey());
            ok = true;
            return handle;
        } finally {
            if (!ok) {
                table.close();
            }
        }
    }

    public SSTable table() {
        return table;
    }

    public Path path() {
        return path;
    }

    public int level() {
        return level;
    }

    public long sizeBytes() {
        return sizeBytes;
    }

    public String firstKey() {
        return firstKey;
    }

    public String lastKey() {
        return lastKey;
    }

    /** True if this table's inclusive key range overlaps {@code other}'s. Empty tables never overlap. */
    public boolean overlaps(SSTableHandle other) {
        if (firstKey == null || other.firstKey == null) {
            return false;
        }
        return firstKey.compareTo(other.lastKey) <= 0 && other.firstKey.compareTo(lastKey) <= 0;
    }

    @Override
    public void close() throws IOException {
        table.close();
    }
}
