package com.ledgerkv.node;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.Test;

/**
 * Text-based contract checks over the Kubernetes manifests in {@code k8s/}. We deliberately do
 * NOT parse YAML (no YAML parser is on the classpath, and adding one would be a new dependency for
 * a handful of structural assertions). Instead we assert on the load-bearing lines that wire the
 * StatefulSet to its headless Service, derive the per-pod ordinal, and honor the NodeConfig env
 * contract. Failures here mean a deploy would mis-wire peer DNS, identity, or storage.
 */
class K8sManifestContractTest {

    private static String read(String relPath) {
        try {
            // Tests run with the module dir as the working directory.
            Path p = Paths.get(relPath);
            return new String(Files.readAllBytes(p));
        } catch (IOException e) {
            throw new RuntimeException("could not read manifest " + relPath, e);
        }
    }

    @Test
    void headlessServiceDeclaresStableDnsContract() {
        String svc = read("k8s/headless-service.yaml");
        assertTrue(svc.contains("kind: Service"), "must be a Service");
        assertTrue(svc.contains("name: ledgerkv-headless"), "headless svc name must match StatefulSet serviceName");
        assertTrue(svc.contains("clusterIP: None"), "headless service requires clusterIP: None");
        assertTrue(svc.contains("app: ledgerkv"), "must select the StatefulSet pods by app=ledgerkv");
        assertTrue(svc.contains("publishNotReadyAddresses: true"),
                "peers must resolve before readiness so the cluster can form at startup");
        assertTrue(svc.contains("9090"), "must publish the gRPC port 9090");
        assertTrue(svc.contains("8080"), "must publish the health port 8080");
    }

    @Test
    void statefulSetHonorsNodeConfigContract() {
        String ss = read("k8s/statefulset.yaml");
        assertTrue(ss.contains("kind: StatefulSet"), "must be a StatefulSet");
        assertTrue(ss.contains("name: ledgerkv"), "StatefulSet name drives pod ordinal hostnames ledgerkv-0..N");
        assertTrue(ss.contains("serviceName: ledgerkv-headless"),
                "serviceName must bind the StatefulSet to the headless Service for stable DNS");
        assertTrue(ss.contains("replicas: 5"), "5-node cluster (N=3, W=R=2)");

        // Per-pod ordinal identity: the ordinal is the trailing number of the pod hostname.
        // StatefulSet env is identical across pods, so LEDGERKV_NODE_INDEX MUST be derived at runtime.
        assertTrue(ss.contains("HOSTNAME##*-") || ss.contains("HOSTNAME=~"),
                "node index must be derived from the pod ordinal in $HOSTNAME");
        assertTrue(ss.contains("LEDGERKV_NODE_INDEX="),
                "the derived ordinal must be exported as LEDGERKV_NODE_INDEX");
        assertTrue(ss.contains("/app/ledgerkv.jar"),
                "must exec the 2e image jar (reuse the image entrypoint payload)");

        // Static positional peer list = ledgerkv-<i>.ledgerkv-headless:9090 for i in 0..4,
        // matching NodeMain's positional peer -> clusterId-node-i mapping.
        assertTrue(ss.contains("LEDGERKV_PEERS"), "must set the peer list");
        for (int i = 0; i < 5; i++) {
            assertTrue(ss.contains("ledgerkv-" + i + ".ledgerkv-headless:9090"),
                    "peer list must include ordinal " + i + " over headless DNS");
        }
        assertTrue(ss.contains("LEDGERKV_NODE_COUNT") && ss.contains("\"5\""), "node count = 5");
        assertTrue(ss.contains("LEDGERKV_REPLICATION_FACTOR") && ss.contains("\"3\""), "N=3");
        assertTrue(ss.contains("LEDGERKV_WRITE_QUORUM"), "W set");
        assertTrue(ss.contains("LEDGERKV_READ_QUORUM"), "R set");
        assertTrue(ss.contains("LEDGERKV_DATA_DIR") && ss.contains("/data"), "data dir = /data");

        // Per-pod PVC for the LSM data dir, mounted at /data.
        assertTrue(ss.contains("volumeClaimTemplates"), "per-pod PVC via volumeClaimTemplates");
        assertTrue(ss.contains("mountPath: /data"), "PVC must mount at the LSM data dir /data");
        assertTrue(ss.contains("ReadWriteOnce"), "per-pod volume is RWO");

        // Liveness + readiness probes on /health (the 2e HealthServer; 2g hangs /metrics here too).
        assertTrue(ss.contains("livenessProbe"), "liveness probe required");
        assertTrue(ss.contains("readinessProbe"), "readiness probe required");
        assertTrue(ss.contains("path: /health"), "probes hit /health");
        assertTrue(ss.contains("port: 8080"), "probes hit the health port 8080");
    }

    @Test
    void clientServiceFrontsTheCluster() {
        String svc = read("k8s/client-service.yaml");
        assertTrue(svc.contains("kind: Service"), "must be a Service");
        assertTrue(svc.contains("name: ledgerkv"), "client front-door service named ledgerkv");
        // ClusterIP (not headless): any node coordinates, so round-robin across pods is correct.
        assertTrue(!svc.contains("clusterIP: None"), "client service must NOT be headless");
        assertTrue(svc.contains("app: ledgerkv"), "must select the StatefulSet pods");
        assertTrue(svc.contains("9090"), "must expose the client gRPC port");
    }

    @Test
    void validateScriptIsExecutableAndDryRunsManifests() {
        String script = read("scripts/k8s-validate.sh");
        assertTrue(script.startsWith("#!/"), "must have a shebang");
        assertTrue(script.contains("k8s/"), "must target the k8s manifests dir");
        assertTrue(script.contains("--dry-run=client") || script.contains("kubeval"),
                "must validate via kubectl client dry-run or kubeval");
        assertTrue(script.contains("command -v"),
                "must degrade gracefully when the tooling is absent (CI may lack a cluster)");
        assertTrue(java.nio.file.Files.isExecutable(java.nio.file.Paths.get("scripts/k8s-validate.sh")),
                "script must be chmod +x");
    }
}
