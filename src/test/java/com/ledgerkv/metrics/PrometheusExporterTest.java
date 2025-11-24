package com.ledgerkv.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

class PrometheusExporterTest {

    @Test
    void rendersEmptySnapshotWithHelpAndTypeLines() {
        String out = PrometheusExporter.render(
            "ledgerkv-node-0",
            OperationMetrics.empty(),
            LatencySummary.empty(),
            RepairMetrics.empty());

        assertTrue(out.contains("# HELP ledgerkv_operations_total Total quorum operations."),
            () -> out);
        assertTrue(out.contains("# TYPE ledgerkv_operations_total counter"), () -> out);
        assertTrue(out.contains("ledgerkv_operations_total{node=\"ledgerkv-node-0\"} 0"), () -> out);
        assertTrue(out.endsWith("\n"), () -> out);
    }

    @Test
    void rendersOperationCountersWithNodeLabel() {
        OperationMetrics ops = new OperationMetrics(
            10, 6, 4, 8, 2, 2, 1, 1, List.of(1L, 2L, 3L));
        String out = PrometheusExporter.render("n1", ops, LatencySummary.from(ops),
            RepairMetrics.empty());

        assertTrue(out.contains("ledgerkv_operations_total{node=\"n1\"} 10"), () -> out);
        assertTrue(out.contains("ledgerkv_reads_total{node=\"n1\"} 6"), () -> out);
        assertTrue(out.contains("ledgerkv_writes_total{node=\"n1\"} 4"), () -> out);
        assertTrue(out.contains("ledgerkv_operation_success_total{node=\"n1\"} 8"), () -> out);
        assertTrue(out.contains("ledgerkv_operation_failure_total{node=\"n1\"} 2"), () -> out);
        assertTrue(out.contains("ledgerkv_quorum_failure_total{node=\"n1\"} 2"), () -> out);
        assertTrue(out.contains("ledgerkv_stale_read_total{node=\"n1\"} 1"), () -> out);
        assertTrue(out.contains("ledgerkv_conflict_total{node=\"n1\"} 1"), () -> out);
    }

    @Test
    void rendersHedgedRequestsCounter() {
        OperationMetrics ops = new OperationMetrics(
            10, 6, 4, 8, 2, 2, 1, 1, 5, List.of(1L, 2L, 3L));
        String out = PrometheusExporter.render("n1", ops, LatencySummary.from(ops),
            RepairMetrics.empty());

        assertTrue(out.contains("# TYPE ledgerkv_hedged_requests_total counter"), () -> out);
        assertTrue(out.contains("ledgerkv_hedged_requests_total{node=\"n1\"} 5"), () -> out);
    }

    @Test
    void rendersLatencyQuantilesAsLabeledGauge() {
        // Samples 1..100 -> p50=50, p95=95, p99=99 under ceil-rank percentile.
        java.util.List<Long> samples = new java.util.ArrayList<>();
        for (long i = 1; i <= 100; i++) {
            samples.add(i);
        }
        LatencySummary latency = LatencySummary.fromSamples(samples);
        String out = PrometheusExporter.render("n1", OperationMetrics.empty(), latency,
            RepairMetrics.empty());

        assertTrue(out.contains("# TYPE ledgerkv_operation_latency_ms gauge"), () -> out);
        assertTrue(out.contains(
            "ledgerkv_operation_latency_ms{node=\"n1\",quantile=\"0.5\"} 50"), () -> out);
        assertTrue(out.contains(
            "ledgerkv_operation_latency_ms{node=\"n1\",quantile=\"0.95\"} 95"), () -> out);
        assertTrue(out.contains(
            "ledgerkv_operation_latency_ms{node=\"n1\",quantile=\"0.99\"} 99"), () -> out);
    }

    @Test
    void rendersRepairCounters() {
        RepairMetrics repair = new RepairMetrics(3, 5, 90);
        String out = PrometheusExporter.render("n1", OperationMetrics.empty(),
            LatencySummary.empty(), repair);

        assertTrue(out.contains("ledgerkv_repairs_total{node=\"n1\"} 3"), () -> out);
        assertTrue(out.contains("ledgerkv_replicas_repaired_total{node=\"n1\"} 5"), () -> out);
        assertTrue(out.contains(
            "ledgerkv_repair_latency_ms_total{node=\"n1\"} 90"), () -> out);
    }

    @Test
    void escapesNodeLabelValue() {
        String out = PrometheusExporter.render("a\"b\\c", OperationMetrics.empty(),
            LatencySummary.empty(), RepairMetrics.empty());
        assertTrue(out.contains("node=\"a\\\"b\\\\c\""), () -> out);
    }

    @Test
    void isDeterministic() {
        OperationMetrics ops = new OperationMetrics(2, 1, 1, 2, 0, 0, 0, 0, List.of(5L));
        String a = PrometheusExporter.render("n", ops, LatencySummary.from(ops),
            RepairMetrics.empty());
        String b = PrometheusExporter.render("n", ops, LatencySummary.from(ops),
            RepairMetrics.empty());
        assertEquals(a, b);
    }
}
