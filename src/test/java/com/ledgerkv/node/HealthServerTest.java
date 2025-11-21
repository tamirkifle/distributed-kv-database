package com.ledgerkv.node;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class HealthServerTest {

    @Test
    void healthEndpointReturns200Ok() throws Exception {
        try (HealthServer health = HealthServer.start(0)) {
            URL url = new URL("http://localhost:" + health.port() + "/health");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            assertEquals(200, conn.getResponseCode());
            String body = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            assertEquals("OK", body);
        }
    }
}
