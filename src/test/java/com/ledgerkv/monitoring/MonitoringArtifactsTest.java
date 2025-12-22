package com.ledgerkv.monitoring;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

/**
 * Structural contract checks over the {@code monitoring/} artifacts. The dashboard JSON is parsed
 * with Gson (already on the test classpath transitively via the gRPC stack — no new dependency);
 * the YAML files are checked by string contract (no YAML parser on the classpath). Failures here
 * mean the metrics pipeline (node -> Prometheus -> Grafana) would be mis-wired.
 */
class MonitoringArtifactsTest {

    private String read(String relativePath) throws IOException {
        Path path = Paths.get(relativePath);
        assertTrue(Files.exists(path), () -> "missing artifact: " + path.toAbsolutePath());
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }

    @Test
    void dashboardJsonIsWellFormedAndQueriesLedgerKvSeries() throws IOException {
        JsonObject dashboard =
            JsonParser.parseString(read("monitoring/grafana-dashboard.json")).getAsJsonObject();
        assertTrue(dashboard.has("title"));
        JsonArray panels = dashboard.getAsJsonArray("panels");
        assertFalse(panels.isEmpty(), "dashboard must have at least one panel");

        boolean sawLedgerKvExpr = false;
        boolean sawP99 = false;
        for (int i = 0; i < panels.size(); i++) {
            JsonArray targets = panels.get(i).getAsJsonObject().getAsJsonArray("targets");
            assertFalse(targets.isEmpty(), "each panel must have targets");
            for (int j = 0; j < targets.size(); j++) {
                String expr = targets.get(j).getAsJsonObject().get("expr").getAsString();
                if (expr.contains("ledgerkv_")) {
                    sawLedgerKvExpr = true;
                }
                if (expr.contains("ledgerkv_operation_latency_ms") && expr.contains("0.99")) {
                    sawP99 = true;
                }
            }
        }
        assertTrue(sawLedgerKvExpr, "panels must query ledgerkv_* series");
        assertTrue(sawP99, "dashboard must include the p99 latency panel");
    }

    @Test
    void prometheusConfigScrapesAllFiveNodes() throws IOException {
        String config = read("monitoring/prometheus.yml");
        assertTrue(config.contains("job_name: ledgerkv"), () -> config);
        assertTrue(config.contains("metrics_path: /metrics"), () -> config);
        for (int i = 0; i < 5; i++) {
            String target = "node" + i + ":8080";
            assertTrue(config.contains(target), "missing scrape target " + target);
        }
    }

    @Test
    void monitoringComposeOverlayDefinesPrometheusAndGrafana() throws IOException {
        String compose = read("monitoring/docker-compose.monitoring.yml");
        assertTrue(compose.contains("prometheus:"), () -> compose);
        assertTrue(compose.contains("grafana:"), () -> compose);
        assertTrue(compose.contains("prometheus.yml"), () -> compose);
    }
}
