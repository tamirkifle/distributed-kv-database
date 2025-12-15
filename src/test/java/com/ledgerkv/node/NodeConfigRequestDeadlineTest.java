package com.ledgerkv.node;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NodeConfigRequestDeadlineTest {

    private static Map<String, String> baseEnv() {
        Map<String, String> env = new HashMap<>();
        env.put("LEDGERKV_PEERS", "h0:9090,h1:9090,h2:9090");
        env.put("LEDGERKV_NODE_INDEX", "0");
        return env;
    }

    @Test
    void parsesRequestDeadlineFromEnv() {
        Map<String, String> env = baseEnv();
        env.put("LEDGERKV_REQUEST_DEADLINE_MS", "250");
        assertEquals(Duration.ofMillis(250), NodeConfig.fromEnv(env).requestDeadline());
    }

    @Test
    void defaultsRequestDeadlineToFiveSeconds() {
        assertEquals(Duration.ofMillis(5000), NodeConfig.fromEnv(baseEnv()).requestDeadline());
    }
}
