package com.ledgerkv.node;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

/**
 * Text-based contract checks over the Raft deployment manifests, in the same spirit as
 * {@link K8sManifestContractTest}: no YAML parser on the classpath, so these assert on the
 * load-bearing lines. A failure here means a Raft deploy would mis-wire peer DNS, identity,
 * storage, or probes.
 */
class RaftDeploymentContractTest {

    private static String read(String relPath) {
        try {
            return new String(Files.readAllBytes(Paths.get(relPath)));
        } catch (IOException e) {
            throw new RuntimeException("could not read manifest " + relPath, e);
        }
    }

    @Test
    void composeRaftStackSelectsRaftModeOnEveryNode() {
        String compose = read("docker-compose.raft.yml");
        assertTrue(compose.contains("LEDGERKV_MODE: raft"), "the stack must select raft mode");
        for (int i = 0; i < 5; i++) {
            assertTrue(compose.contains("LEDGERKV_NODE_INDEX: \"" + i + "\""),
                    "member " + i + " must be present");
            assertTrue(compose.contains("raft" + i + "-data:/data"),
                    "member " + i + " needs its own volume; a shared log is two members in one");
        }
        assertTrue(compose.contains("LEDGERKV_RAFT_PORT: \"9095\""),
                "consensus traffic needs a port of its own");
        assertFalse(compose.contains("LEDGERKV_REPLICATION_FACTOR"),
                "N/R/W are quorum-mode settings and would misdescribe a raft deployment");
    }

    @Test
    void composeRaftStackDoesNotCollideWithTheQuorumStack() {
        String compose = read("docker-compose.raft.yml");
        String quorum = read("docker-compose.yml");
        for (String hostPort : new String[] {"9090:", "9091:", "9092:", "9093:", "9094:",
                "8080:", "8081:", "8082:", "8083:", "8084:"}) {
            assertTrue(quorum.contains("\"" + hostPort),
                    "sanity: the quorum stack should publish " + hostPort);
            assertFalse(compose.contains("\"" + hostPort),
                    "both stacks must be able to run at once, but they share host port " + hostPort);
        }
    }

    @Test
    void raftStatefulSetCarriesIdentityStorageAndPeerWiring() {
        String ss = read("k8s/raft/statefulset.yaml");
        assertTrue(ss.contains("kind: StatefulSet"));
        assertTrue(ss.contains("name: ledgerkv-raft"));
        assertTrue(ss.contains("serviceName: ledgerkv-raft-headless"),
                "serviceName binds the StatefulSet to its headless Service for stable DNS");
        assertTrue(ss.contains("replicas: 5"), "NodeConfig accepts only 3 or 5 members");
        assertTrue(ss.contains("value: \"raft\""), "the pods must run in raft mode");

        assertTrue(ss.contains("HOSTNAME##*-"), "the member index comes from the pod ordinal");
        assertTrue(ss.contains("LEDGERKV_NODE_INDEX="));
        assertTrue(ss.contains("/app/ledgerkv.jar"));

        for (int i = 0; i < 5; i++) {
            assertTrue(ss.contains("ledgerkv-raft-" + i + ".ledgerkv-raft-headless:9090"),
                    "client endpoint for ordinal " + i + " over headless DNS");
            assertTrue(ss.contains("ledgerkv-raft-" + i + ".ledgerkv-raft-headless:9095"),
                    "peer endpoint for ordinal " + i + " over headless DNS");
        }

        assertTrue(ss.contains("volumeClaimTemplates"), "per-member PVC");
        assertTrue(ss.contains("mountPath: /data"));
        assertTrue(ss.contains("ReadWriteOnce"));
    }

    @Test
    void raftProbesSeparateLivenessFromConsensusAvailability() {
        String ss = read("k8s/raft/statefulset.yaml");
        assertTrue(ss.contains("path: /readyz"), "readiness must reflect whether a leader is known");
        assertTrue(ss.contains("path: /livez"), "liveness must reflect only the process");
        // The specific failure this guards: restarting a follower because its leader went away
        // removes a vote from the majority that has to elect the replacement.
        int readiness = ss.indexOf("readinessProbe");
        int liveness = ss.indexOf("livenessProbe");
        assertTrue(readiness > 0 && liveness > 0, "both probes required");
        assertFalse(ss.substring(liveness).contains("path: /readyz"),
                "the liveness probe must not depend on consensus state");
    }

    @Test
    void raftHeadlessServicePublishesPeersBeforeReadiness() {
        String svc = read("k8s/raft/headless-service.yaml");
        assertTrue(svc.contains("kind: Service"));
        assertTrue(svc.contains("name: ledgerkv-raft-headless"));
        assertTrue(svc.contains("clusterIP: None"));
        assertTrue(svc.contains("publishNotReadyAddresses: true"),
                "readiness means a leader is known, so requiring it before DNS deadlocks the "
                        + "first election");
        assertTrue(svc.contains("9090") && svc.contains("9095"),
                "a member needs both its client and its peer address published");
    }

    @Test
    void raftClientServiceIsSeparateFromTheQuorumFrontDoor() {
        String svc = read("k8s/raft/client-service.yaml");
        assertTrue(svc.contains("name: ledgerkv-raft"));
        assertTrue(svc.contains("app: ledgerkv-raft"),
                "distinct labels let both stacks live in one namespace");
        assertFalse(svc.contains("clusterIP: None"), "the client front door is not headless");
    }

    @Test
    void validateScriptCoversTheRaftManifests() {
        String script = read("scripts/k8s-validate.sh");
        assertTrue(script.contains("k8s/raft/"), "the raft manifests must be validated too");
    }
}
