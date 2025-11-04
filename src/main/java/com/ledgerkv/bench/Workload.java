package com.ledgerkv.bench;

/**
 * YCSB core workloads, encoded as operation-mix proportions over {@link OpType}. The five
 * proportions (read, update, insert, scan, read-modify-write) sum to 1.0 per workload.
 *
 * <p>Workload D ("read-latest") is approximated: its 95% read / 5% insert mix runs under the
 * configured {@link Distribution}; modeling the true temporal "latest" skew is out of scope.
 */
public enum Workload {

    /** Update-heavy: 50% read, 50% update. */
    A(0.50, 0.50, 0.00, 0.00, 0.00),
    /** Read-mostly: 95% read, 5% update. */
    B(0.95, 0.05, 0.00, 0.00, 0.00),
    /** Read-only: 100% read. */
    C(1.00, 0.00, 0.00, 0.00, 0.00),
    /** Read-latest (approximated): 95% read, 5% insert. */
    D(0.95, 0.00, 0.05, 0.00, 0.00),
    /** Short-range scans: 95% scan, 5% insert. */
    E(0.00, 0.00, 0.05, 0.95, 0.00),
    /** Read-modify-write: 50% read, 50% read-modify-write. */
    F(0.50, 0.00, 0.00, 0.00, 0.50);

    public final double read;
    public final double update;
    public final double insert;
    public final double scan;
    public final double readModifyWrite;

    Workload(double read, double update, double insert, double scan, double readModifyWrite) {
        this.read = read;
        this.update = update;
        this.insert = insert;
        this.scan = scan;
        this.readModifyWrite = readModifyWrite;
    }

    /** Picks an op type for a uniform draw {@code r} in {@code [0,1)} using the cumulative mix. */
    public OpType pick(double r) {
        double c = read;
        if (r < c) {
            return OpType.READ;
        }
        c += update;
        if (r < c) {
            return OpType.UPDATE;
        }
        c += insert;
        if (r < c) {
            return OpType.INSERT;
        }
        c += scan;
        if (r < c) {
            return OpType.SCAN;
        }
        return OpType.READ_MODIFY_WRITE;
    }
}
