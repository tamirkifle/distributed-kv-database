package com.ledgerkv.node;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

/**
 * Liveness, readiness, and metrics endpoints served by the JDK {@link HttpServer}.
 *
 * <ul>
 *   <li>{@code GET /health} and {@code GET /livez} — 200 while the process is up. A restart is the
 *       only thing that could fix a failure here, so consensus state must not affect it. Killing a
 *       Raft follower because its leader went away would take down the very majority that elects
 *       the next one.
 *   <li>{@code GET /readyz} — 200 only when this node can actually serve or redirect a request,
 *       503 otherwise. This is what pulls a node out of the client Service during an election.
 *   <li>{@code GET /metrics} — the Prometheus scrape target.
 * </ul>
 *
 * <p>The split follows etcd, which added {@code /livez} and {@code /readyz} for exactly this
 * reason: its {@code /livez} omits the linearizable-read check that {@code /readyz} performs.
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
        return start(port, metricsBody, () -> true);
    }

    /**
     * Starts a server whose {@code /readyz} reflects {@code ready}. Quorum mode passes a constant
     * true: every node coordinates, so being up is being ready.
     */
    public static HealthServer start(int port, Supplier<String> metricsBody, BooleanSupplier ready)
            throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        HttpHandler alive = exchange -> {
            byte[] body = "OK".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        };
        server.createContext("/health", alive);
        server.createContext("/livez", alive);
        server.createContext("/readyz", exchange -> {
            boolean up = ready.getAsBoolean();
            byte[] body = (up ? "READY" : "NOT READY").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(up ? 200 : 503, body.length);
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
