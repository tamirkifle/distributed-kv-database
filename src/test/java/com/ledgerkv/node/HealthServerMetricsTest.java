package com.ledgerkv.node;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class HealthServerMetricsTest {

    private String get(int port, String path) throws Exception {
        URL url = new URL("http://localhost:" + port + path);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        int code = conn.getResponseCode();
        assertEquals(200, code);
        StringBuilder body = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                body.append(line).append('\n');
            }
        }
        return body.toString();
    }

    @Test
    void servesMetricsAndHealth() throws Exception {
        AtomicInteger scrapes = new AtomicInteger();
        try (HealthServer server = HealthServer.start(0, () -> {
            scrapes.incrementAndGet();
            return "ledgerkv_operations_total{node=\"n\"} 7\n";
        })) {
            int port = server.port();
            assertTrue(get(port, "/health").contains("OK"));
            String metrics = get(port, "/metrics");
            assertTrue(metrics.contains("ledgerkv_operations_total{node=\"n\"} 7"), () -> metrics);
        }
        assertTrue(scrapes.get() >= 1);
    }

    @Test
    void metricsBodyIsRecomputedPerScrape() throws Exception {
        AtomicInteger counter = new AtomicInteger();
        try (HealthServer server = HealthServer.start(0,
                () -> "ledgerkv_operations_total{node=\"n\"} " + counter.incrementAndGet() + "\n")) {
            int port = server.port();
            assertTrue(get(port, "/metrics").contains(" 1"));
            assertTrue(get(port, "/metrics").contains(" 2"));
        }
    }

    @Test
    void legacyStartStillServesHealth() throws Exception {
        try (HealthServer server = HealthServer.start(0)) {
            assertTrue(get(server.port(), "/health").contains("OK"));
        }
    }
}
