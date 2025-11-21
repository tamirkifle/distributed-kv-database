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

        return sb.toString();
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
