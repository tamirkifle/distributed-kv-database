package com.ledgerkv.bench;

/**
 * One compaction strategy's amplification measurement over a workload run.
 *
 * <p>Write amplification = {@code sstableBytesWritten / userLogicalBytes} (WAL bytes excluded — a
 * constant ~1x common to both strategies). Space amplification = {@code liveOnDiskBytes /
 * logicalLiveBytes}. Read amplification is a structural proxy supplied at construction (the average
 * number of live SSTables whose key range covers a sampled key).
 */
public final class Measurement {

    private final String label;
    private final long userLogicalBytes;
    private final long sstableBytesWritten;
    private final long logicalLiveBytes;
    private final long liveOnDiskBytes;
    private final double readAmplification;
    private final int liveTableCount;

    public Measurement(String label, long userLogicalBytes, long sstableBytesWritten,
                       long logicalLiveBytes, long liveOnDiskBytes, double readAmplification,
                       int liveTableCount) {
        this.label = label;
        this.userLogicalBytes = userLogicalBytes;
        this.sstableBytesWritten = sstableBytesWritten;
        this.logicalLiveBytes = logicalLiveBytes;
        this.liveOnDiskBytes = liveOnDiskBytes;
        this.readAmplification = readAmplification;
        this.liveTableCount = liveTableCount;
    }

    public String label() {
        return label;
    }

    public long userLogicalBytes() {
        return userLogicalBytes;
    }

    public long sstableBytesWritten() {
        return sstableBytesWritten;
    }

    public long logicalLiveBytes() {
        return logicalLiveBytes;
    }

    public long liveOnDiskBytes() {
        return liveOnDiskBytes;
    }

    public double readAmplification() {
        return readAmplification;
    }

    public int liveTableCount() {
        return liveTableCount;
    }

    public double writeAmplification() {
        return userLogicalBytes == 0 ? 0.0 : (double) sstableBytesWritten / userLogicalBytes;
    }

    public double spaceAmplification() {
        return logicalLiveBytes == 0 ? 0.0 : (double) liveOnDiskBytes / logicalLiveBytes;
    }
}
