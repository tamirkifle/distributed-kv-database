package com.ledgerkv.metrics;

import java.util.Objects;

/**
 * Renders metric snapshots into Prometheus text-exposition format (v0.0.4) by hand — no Prometheus
 * Java client dependency. Pure and deterministic: identical snapshots produce byte-identical output.
 *
 * <p>Counters use the {@code _total} suffix per Prometheus naming conventions; latency percentiles
 * are emitted as a {@code quantile}-labeled gauge family so a Prometheus scrape reads them directly.
 */
public final class PrometheusExporter {

    private PrometheusExporter() {
    }

    public static String render(String nodeId, OperationMetrics ops, LatencySummary latency,
                                RepairMetrics repair) {
        return render(nodeId, ops, latency, repair, null);
    }

    /**
     * Renders the same families plus the Raft member gauges. {@code raft} is null in quorum mode,
     * and the Raft families are then omitted entirely rather than exported as zeroes — a zero term
     * on a node with no consensus is a reading, and it would be a false one.
     */
    public static String render(String nodeId, OperationMetrics ops, LatencySummary latency,
                                RepairMetrics repair, RaftStatus raft) {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(ops, "ops must not be null");
        Objects.requireNonNull(latency, "latency must not be null");
        Objects.requireNonNull(repair, "repair must not be null");

        String node = escapeLabelValue(nodeId);
        StringBuilder sb = new StringBuilder(1024);

        counter(sb, "ledgerkv_operations_total", "Total quorum operations.", node,
            ops.getOperationCount());
        counter(sb, "ledgerkv_reads_total", "Total quorum read operations.", node,
            ops.getReadCount());
        counter(sb, "ledgerkv_writes_total", "Total quorum write operations.", node,
            ops.getWriteCount());
        counter(sb, "ledgerkv_operation_success_total", "Operations that met quorum.", node,
            ops.getSuccessCount());
        counter(sb, "ledgerkv_operation_failure_total", "Operations that failed.", node,
            ops.getFailureCount());
        counter(sb, "ledgerkv_quorum_failure_total", "Operations that did not meet quorum.", node,
            ops.getQuorumFailureCount());
        counter(sb, "ledgerkv_stale_read_total", "Successful reads that observed a stale replica.",
            node, ops.getStaleReadCount());
        counter(sb, "ledgerkv_conflict_total", "Successful reads that observed conflicting values.",
            node, ops.getConflictCount());
        counter(sb, "ledgerkv_hedged_requests_total",
            "Backup (hedge) requests fired to tame tail latency.", node,
            ops.getHedgedRequestCount());

        sb.append("# HELP ledgerkv_operation_latency_ms Quorum operation latency quantiles (ms).\n");
        sb.append("# TYPE ledgerkv_operation_latency_ms gauge\n");
        quantile(sb, node, "0.5", latency.getP50Ms());
        quantile(sb, node, "0.95", latency.getP95Ms());
        quantile(sb, node, "0.99", latency.getP99Ms());

        counter(sb, "ledgerkv_repairs_total", "Read-repair / anti-entropy repairs performed.", node,
            repair.getRepairCount());
        counter(sb, "ledgerkv_replicas_repaired_total", "Replicas updated by repair.", node,
            repair.getReplicasRepaired());
        counter(sb, "ledgerkv_repair_latency_ms_total", "Cumulative repair latency (ms).", node,
            repair.getTotalRepairLatencyMs());

        if (raft != null) {
            sb.append("# HELP ledgerkv_raft_role This member's Raft role (1 for the role it holds)"
                + ".\n");
            sb.append("# TYPE ledgerkv_raft_role gauge\n");
            for (String role : new String[] {"leader", "candidate", "follower"}) {
                sb.append("ledgerkv_raft_role{node=\"").append(node)
                    .append("\",role=\"").append(role).append("\"} ")
                    .append(role.equalsIgnoreCase(raft.role()) ? 1 : 0).append('\n');
            }
            gauge(sb, "ledgerkv_raft_term", "Current Raft term.", node, raft.term());
            gauge(sb, "ledgerkv_raft_commit_index", "Highest log index known committed.", node,
                raft.commitIndex());
            gauge(sb, "ledgerkv_raft_applied_index",
                "Highest log index applied to the state machine.", node, raft.appliedIndex());
            gauge(sb, "ledgerkv_raft_snapshot_index",
                "Log base: entries at or below this live in a snapshot.", node,
                raft.lastIncludedIndex());
            sb.append("# HELP ledgerkv_raft_leader The leader this member recognizes.\n");
            sb.append("# TYPE ledgerkv_raft_leader gauge\n");
            sb.append("ledgerkv_raft_leader{node=\"").append(node)
                .append("\",leader=\"").append(escapeLabelValue(raft.leaderId())).append("\"} ")
                .append(raft.leaderId().isEmpty() ? 0 : 1).append('\n');
        }

        return sb.toString();
    }

    private static void gauge(StringBuilder sb, String name, String help, String node, long value) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(" gauge\n");
        sb.append(name).append("{node=\"").append(node).append("\"} ").append(value).append('\n');
    }

    private static void counter(StringBuilder sb, String name, String help, String node,
                                long value) {
        sb.append("# HELP ").append(name).append(' ').append(help).append('\n');
        sb.append("# TYPE ").append(name).append(" counter\n");
        sb.append(name).append("{node=\"").append(node).append("\"} ").append(value).append('\n');
    }

    private static void quantile(StringBuilder sb, String node, String q, long value) {
        sb.append("ledgerkv_operation_latency_ms{node=\"").append(node)
            .append("\",quantile=\"").append(q).append("\"} ").append(value).append('\n');
    }

    private static String escapeLabelValue(String raw) {
        StringBuilder sb = new StringBuilder(raw.length());
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            switch (c) {
                case '\\':
                    sb.append("\\\\");
                    break;
                case '"':
                    sb.append("\\\"");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                default:
                    sb.append(c);
            }
        }
        return sb.toString();
    }
}
