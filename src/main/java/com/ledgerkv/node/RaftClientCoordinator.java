package com.ledgerkv.node;

import com.ledgerkv.consistency.VersionMetadata;
import com.ledgerkv.metrics.OperationMetrics;
import com.ledgerkv.metrics.OperationMetricsCollector;
import com.ledgerkv.raft.RaftNode;
import com.ledgerkv.raft.ProposalRejectedException;
import com.ledgerkv.raft.RaftReplicationDriver;
import com.ledgerkv.raft.kv.KvCommand;
import com.ledgerkv.raft.kv.KvOutcome;
import com.ledgerkv.raft.kv.RaftKvStateMachine;
import com.ledgerkv.transport.ClientCoordinator;
import com.ledgerkv.transport.MutationId;
import com.ledgerkv.transport.NotLeaderException;
import com.ledgerkv.transport.StoredValue;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Serves the public GET/PUT/DELETE surface from a Raft group. Writes go through the replicated log
 * and return once committed and applied; reads go through the ReadIndex barrier.
 *
 * <p>A node that is not the leader serves nothing and throws {@link NotLeaderException}, carrying
 * the leader's endpoint when it knows it so the client can retry in one hop. Forwarding the request
 * onward the way etcd does would spare the client that hop, but it would also nest this node's
 * deadline inside the leader's, and a follower would be queueing work for a leader whose state it
 * cannot see.
 *
 * <h2>Why a committed index is not enough</h2>
 *
 * <p>{@code propose} returns the index it assigned, and waiting for that index to apply looks like
 * the completion test. It is not. An appended but uncommitted entry can be overwritten by the next
 * leader, so by the time the index applies it may carry a different command. The state machine's
 * session table settles it: if it holds this exact {@code (clientId, sequence, fingerprint)}, this
 * command took effect. Nothing weaker proves that.
 */
public final class RaftClientCoordinator implements ClientCoordinator {

    private final RaftNode node;
    private final RaftReplicationDriver driver;
    private final RaftKvStateMachine state;
    private final Duration deadline;
    /** Node id to client endpoint, for turning {@code leaderId()} into something dialable. */
    private final Map<String, String> endpoints;
    private final OperationMetricsCollector metrics = new OperationMetricsCollector();

    public RaftClientCoordinator(RaftNode node, RaftReplicationDriver driver,
            RaftKvStateMachine state, Duration deadline, Map<String, String> endpoints) {
        this.node = Objects.requireNonNull(node, "node");
        this.driver = Objects.requireNonNull(driver, "driver");
        this.state = Objects.requireNonNull(state, "state");
        this.deadline = Objects.requireNonNull(deadline, "deadline");
        this.endpoints = Objects.requireNonNull(endpoints, "endpoints");
    }

    /** Live snapshot of this coordinator's request metrics (drives the Prometheus exporter). */
    public OperationMetrics operationMetrics() {
        return metrics.snapshot();
    }

    /**
     * Raft has no range iterator over its map, and a scan taken off applied state would not be
     * linearizable against the writes it overlaps. Saying so beats inventing an answer.
     */
    @Override
    public boolean supportsScan() {
        return false;
    }

    @Override
    public List<StoredValue> get(String key) {
        long start = System.nanoTime();
        long readIndex;
        try {
            readIndex = driver.readIndex(deadline);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted awaiting a read index", e);
        }
        if (readIndex < 0) {
            metrics.recordOperation(true, false, millisSince(start));
            throw notLeader();
        }
        byte[] value = state.get(key);
        long version = state.versionOf(key);
        metrics.recordOperation(true, true, millisSince(start));
        return value == null
                ? Collections.emptyList()
                : Collections.singletonList(stored(value, version));
    }

    @Override
    public StoredValue put(String key, byte[] value, MutationId id) {
        KvOutcome outcome = submit(KvCommand.put(require(id).clientId(), id.sequence(), key, value));
        return stored(value, outcome.appliedIndex());
    }

    @Override
    public boolean delete(String key, MutationId id) {
        KvOutcome outcome = submit(KvCommand.delete(require(id).clientId(), id.sequence(), key));
        byte[] previous = outcome.value();
        return previous != null && previous.length > 0;
    }

    /**
     * Proposes one command and reports what became of it: wait for the assigned index to apply,
     * then ask the session table whether this command is what applied there.
     */
    private KvOutcome submit(KvCommand command) {
        long start = System.nanoTime();
        if (!node.isLeader()) {
            metrics.recordOperation(false, false, millisSince(start));
            throw notLeader();
        }
        long index;
        try {
            index = driver.propose(command.encode(), deadline);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted awaiting commit of " + command, e);
        } catch (ProposalRejectedException rejected) {
            metrics.recordOperation(false, false, millisSince(start));
            throw notLeader(); // nothing was appended, so the write definitely did not happen
        } catch (IllegalStateException timedOut) {
            metrics.recordOperation(false, false, millisSince(start));
            // The entry is in the log and may still commit. Surfacing this as a redirect would
            // tell the caller it definitely did not happen, which is the one thing we do not know.
            throw timedOut;
        }

        KvOutcome outcome =
                state.outcomeFor(command.clientId(), command.sequenceNumber(), command.fingerprint());
        if (outcome.succeeded()) {
            metrics.recordOperation(false, true, millisSince(start));
            return outcome;
        }
        metrics.recordOperation(false, false, millisSince(start));
        switch (outcome.status()) {
            case STALE_SEQUENCE:
                throw new IllegalArgumentException("sequence " + command.sequenceNumber()
                        + " is behind what client " + command.clientId() + " has already applied");
            case SEQUENCE_CONFLICT:
                throw new IllegalArgumentException("sequence " + command.sequenceNumber()
                        + " was already used by a different command from " + command.clientId());
            default:
                // The index applied, but not with this command: a leader change replaced the entry,
                // or this client's session was evicted. Unknown, not failed.
                throw new IllegalStateException("entry " + index + " applied without "
                        + command.clientId() + "#" + command.sequenceNumber()
                        + "; the write may or may not have taken effect, retry with the same id");
        }
    }

    private static MutationId require(MutationId id) {
        if (!id.isPresent()) {
            throw new IllegalArgumentException("raft mode requires client_id and sequence on a "
                    + "mutation; without them a retry cannot be told from a second write");
        }
        return id;
    }

    /**
     * The redirect to send when this node cannot serve. Sending no hint when this node still
     * believes it is the leader is deliberate: reaching here means leadership could not be
     * confirmed or the entry did not commit, so that belief is exactly the thing in doubt.
     * Naming ourselves would tell the client to come straight back, and it would do so until its
     * deadline ran out.
     */
    private NotLeaderException notLeader() {
        String leader = node.leaderId();
        if (leader == null || leader.equals(node.nodeId())) {
            return new NotLeaderException(null, null);
        }
        return new NotLeaderException(leader, endpoints.get(leader));
    }

    private static StoredValue stored(byte[] value, long version) {
        // A Raft version is a log index: totally ordered by consensus, so there is no concurrency
        // for a vector clock to describe. legacy() carries the index without implying one.
        return new StoredValue(value, version, false, VersionMetadata.legacy(version));
    }

    private static long millisSince(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000L;
    }
}
