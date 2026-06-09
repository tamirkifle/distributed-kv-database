package com.ledgerkv.quorum;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ledgerkv.QuorumConfig;
import com.ledgerkv.QuorumResponse;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Concurrency regression: a single {@link LeaderlessKVCluster} instance is shared across concurrent
 * coordinator threads (the gRPC server dispatches each client call on its own thread). Before 6b the
 * per-key {@code versionMetadataByKey} was a plain {@code LinkedHashMap} and the version was assigned
 * via a non-atomic get-then-put, so concurrent writers on the same key lost increments (and could
 * corrupt the map). This test hammers one key from many threads and asserts no increment is lost.
 */
class LeaderlessKVClusterConcurrencyTest {

    @Test
    void concurrentWritesToSameKeyLoseNoVersionIncrement() throws Exception {
        ClusterMembership membership = ClusterMembership.create("test-cluster", 3, 3);
        QuorumConfig config = new QuorumConfig(3, 2, 2); // N=3, W=2, R=2
        Map<String, ReplicaClient> clients = InMemoryReplicaClient.clusterFor(membership);

        int writers = 8;
        int writesPerThread = 50;
        ExecutorService fanout = Executors.newCachedThreadPool();
        try (LeaderlessKVCluster cluster =
                 LeaderlessKVCluster.create(membership, config, clients, fanout, Duration.ofSeconds(5))) {

            ExecutorService drivers = Executors.newFixedThreadPool(writers);
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger successes = new AtomicInteger();
            List<Future<?>> tasks = new ArrayList<>();
            for (int t = 0; t < writers; t++) {
                tasks.add(drivers.submit(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    for (int i = 0; i < writesPerThread; i++) {
                        QuorumResponse r = cluster.write(0, "hot-key", "v");
                        if (r.isSuccessful()) {
                            successes.incrementAndGet();
                        }
                    }
                }));
            }
            start.countDown();
            for (Future<?> task : tasks) {
                task.get(30, TimeUnit.SECONDS);
            }
            drivers.shutdownNow();

            // Every successful write must have advanced the coordinator's vector-clock counter by
            // exactly one. With a lost-update race the final counter is < successes.
            QuorumResponse read = cluster.read(0, "hot-key");
            assertTrue(read.isSuccessful(), "read must succeed");
            String coordinatorId = membership.getNodes().get(0).getId();
            long counter = read.getValue().getVersionMetadata()
                .getVectorClock().getOrDefault(coordinatorId, 0L);
            assertEquals(successes.get(), counter,
                "every successful write must advance the version exactly once (no lost update)");
        } finally {
            fanout.shutdownNow();
        }
    }
}
