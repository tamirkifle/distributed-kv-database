package com.ledgerkv.quorum;

import com.ledgerkv.VersionedValue;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

/**
 * Test double: a {@link ReplicaClient} decorator whose calls block on a {@link CountDownLatch}
 * until {@link #release()} is invoked. Models a "slow" (latch closed) replica deterministically —
 * no {@code Thread.sleep}. If the waiting thread is interrupted (the coordinator gave up after the
 * deadline and cancelled the in-flight task), the call throws a {@link RuntimeException}, exactly as
 * a dropped RPC would, so the coordinator does not count it toward quorum.
 */
public final class LatchControlledReplicaClient implements ReplicaClient {

    private final ReplicaClient delegate;
    private final CountDownLatch gate = new CountDownLatch(1);
    private volatile boolean called;

    public LatchControlledReplicaClient(ReplicaClient delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
    }

    public void release() {
        gate.countDown();
    }

    boolean wasCalled() {
        return called;
    }

    @Override
    public String nodeId() {
        return delegate.nodeId();
    }

    @Override
    public Optional<VersionedValue> get(String key) {
        await();
        return delegate.get(key);
    }

    @Override
    public void put(String key, VersionedValue value) {
        await();
        delegate.put(key, value);
    }

    @Override
    public void deliverHint(String key, VersionedValue value) {
        await();
        delegate.deliverHint(key, value);
    }

    private void await() {
        called = true;
        try {
            gate.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("replica call interrupted (deadline exceeded)", e);
        }
    }
}
