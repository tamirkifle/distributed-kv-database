package com.ledgerkv.node;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * A minimal liveness endpoint served by the JDK {@link HttpServer}: {@code GET /health -> 200 "OK"}.
 *
 * <p>Used by the container healthcheck. Sub-plan 2g hangs {@code /metrics} off this same server, so
 * the "metrics HTTP port" the deployment exposes is introduced here a step early.
 */
public final class HealthServer implements AutoCloseable {

    private final HttpServer server;

    private HealthServer(HttpServer server) {
        this.server = server;
    }

    /** Starts an HTTP server on {@code port} (0 = OS-assigned) serving {@code /health}. */
    public static HealthServer start(int port) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/health", exchange -> {
            byte[] body = "OK".getBytes(StandardCharsets.UTF_8);
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
