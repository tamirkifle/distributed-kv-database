package com.ledgerkv.node;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

/**
 * Liveness + metrics endpoints served by the JDK {@link HttpServer}:
 * {@code GET /health -> 200 "OK"} (container healthcheck) and
 * {@code GET /metrics -> 200 <Prometheus text>} (scrape target, added in 2g).
 */
public final class HealthServer implements AutoCloseable {

    private static final String PROM_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";

    private final HttpServer server;

    private HealthServer(HttpServer server) {
        this.server = server;
    }

    /** Starts a server serving {@code /health} only (metrics body empty). */
    public static HealthServer start(int port) throws IOException {
        return start(port, () -> "");
    }

    /** Starts a server serving {@code /health} and {@code /metrics} (body from {@code metricsBody}). */
    public static HealthServer start(int port, Supplier<String> metricsBody) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/health", exchange -> {
            byte[] body = "OK".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.createContext("/metrics", exchange -> {
            byte[] body = metricsBody.get().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", PROM_CONTENT_TYPE);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.start();
        return new HealthServer(server);
    }

    /** The actual bound port (resolves an OS-assigned port when started with 0). */
    public int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
