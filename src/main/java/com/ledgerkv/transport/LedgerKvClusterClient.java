package com.ledgerkv.transport;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * A cluster-aware client: it holds a connection to every member, remembers which one leads, follows
 * {@code NotLeader} hints, and falls back to sweeping the member list — all inside one deadline
 * that covers the whole call rather than each attempt.
 *
 * <p>Works against both modes. In quorum mode every node coordinates, so no redirect ever arrives
 * and this degrades to "retry elsewhere if a node is down".
 *
 * <h2>One outstanding mutation</h2>
 *
 * <p>Mutations are serialized per client. Ongaro §6.3 generalizes to concurrent requests by having
 * the session hold a set of sequence/response pairs and the client send "the lowest sequence number
 * for which it has not yet received a response"; the state machine here keeps a single slot per
 * client instead, so two in-flight mutations would let the second retire the first's record before
 * its retry could be recognized.
 *
 * <h2>Sequence numbers across a timeout</h2>
 *
 * <p>A retry inside one call reuses its sequence, which is what makes it a retry rather than a
 * second write. When the deadline expires the sequence is <em>consumed</em> anyway and the call
 * throws {@link UnknownOutcomeException}. Holding it back would deadlock the client against a
 * command it can no longer complete; releasing it means the abandoned command can still commit
 * before the next one, which is exactly the uncertainty the exception reports.
 */
public final class LedgerKvClusterClient implements AutoCloseable {

    /** How long to pause after a full sweep found nobody, so a retry loop is not a spin. */
    private static final long SWEEP_PAUSE_MS = 25;

    private final List<String> endpoints;
    /** One NodeClient per member, indexed alongside {@link #endpoints}. */
    private final List<NodeClient> members;
    private final String clientId;
    private final java.time.Duration deadline;

    private final ReentrantLock mutationLock = new ReentrantLock();
    private long nextSequence = 1;
    private volatile int preferred;

    private LedgerKvClusterClient(List<String> endpoints, List<NodeClient> members,
            String clientId, java.time.Duration deadline) {
        this.endpoints = endpoints;
        this.members = members;
        this.clientId = clientId;
        this.deadline = deadline;
    }

    /**
     * Connects to every {@code host:port} in {@code endpoints}. {@code clientId} must be stable for
     * as long as this client's writes matter: it is half the key the cluster deduplicates retries
     * on, so a fresh id per process turns a retry after a restart into a second write.
     */
    public static LedgerKvClusterClient connect(
            List<String> endpoints, String clientId, java.time.Duration deadline) {
        Objects.requireNonNull(endpoints, "endpoints");
        Objects.requireNonNull(clientId, "clientId");
        Objects.requireNonNull(deadline, "deadline");
        if (endpoints.isEmpty()) {
            throw new IllegalArgumentException("need at least one endpoint");
        }
        List<NodeClient> members = new ArrayList<>();
        for (String endpoint : endpoints) {
            String[] hostPort = endpoint.split(":", 2);
            // One attempt may not outlast the budget for all of them. Without this a single
            // unresponsive member — partitioned but still accepting connections — consumes the
            // whole deadline in one call, and the other members are never tried at all.
            members.add(NodeClient.connect(
                    hostPort[0], Integer.parseInt(hostPort[1]), deadline));
        }
        return new LedgerKvClusterClient(
                new ArrayList<>(endpoints), members, clientId, deadline);
    }

    /** The member this client currently believes is worth asking first. */
    public String preferredEndpoint() {
        return endpoints.get(preferred);
    }

    public Optional<byte[]> get(String key) {
        List<StoredValue> siblings = getSiblings(key);
        if (siblings.isEmpty()) {
            return Optional.empty();
        }
        if (siblings.size() > 1) {
            throw new ConflictingValuesException(key, siblings);
        }
        return Optional.of(siblings.get(0).value());
    }

    public List<StoredValue> getSiblings(String key) {
        return call(member -> member.getSiblings(key), "get " + key);
    }

    /** Writes {@code value} under {@code key}, retrying through leader changes under one sequence. */
    public long put(String key, byte[] value) {
        return mutate(id -> call(member -> member.put(key, value, id), "put " + key));
    }

    /** Deletes {@code key}; returns true if a live value existed. */
    public boolean delete(String key) {
        return mutate(id -> call(member -> member.delete(key, id), "delete " + key));
    }

    /**
     * Runs one mutation under this client's next sequence, holding the single-mutation lock for its
     * whole lifetime and consuming the sequence either way.
     */
    private <T> T mutate(Function<MutationId, T> operation) {
        mutationLock.lock();
        try {
            MutationId id = MutationId.of(clientId, nextSequence);
            try {
                return operation.apply(id);
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED
                        || e.getStatus().getCode() == Status.Code.UNAVAILABLE) {
                    throw new UnknownOutcomeException(
                            clientId, id.sequence(), e.getStatus().toString(), e);
                }
                throw e;
            } finally {
                nextSequence++;
            }
        } finally {
            mutationLock.unlock();
        }
    }

    /**
     * Calls {@code operation} against the preferred member, then wherever the cluster points, until
     * it answers or the deadline passes. A redirect retargets immediately; an unreachable member
     * advances to the next one. Only a call that a retry could not change — a malformed request —
     * fails straight through.
     */
    private <T> T call(Function<NodeClient, T> operation, String description) {
        long expiry = System.nanoTime() + deadline.toNanos();
        RuntimeException last = null;
        int attemptsSinceProgress = 0;
        int target = preferred;

        while (System.nanoTime() < expiry) {
            try {
                T result = operation.apply(members.get(target));
                preferred = target;
                return result;
            } catch (NotLeaderException redirect) {
                last = redirect;
                int hinted = indexOf(redirect.leaderEndpoint());
                if (hinted >= 0 && hinted != target) {
                    target = hinted;
                    attemptsSinceProgress = 0;
                    continue; // a named leader is worth going straight to
                }
                target = next(target);
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() == Status.Code.INVALID_ARGUMENT) {
                    throw e; // the request itself is wrong; another member would say the same
                }
                // Everything else is worth asking someone else, UNIMPLEMENTED included. This
                // client exposes no operation that a healthy member refuses, so UNIMPLEMENTED
                // means the address is not the member we think it is — a port reused after a
                // node died, say — which is precisely a reason to move on.
                last = e;
                target = next(target);
            }
            if (++attemptsSinceProgress >= members.size()) {
                // A full sweep with nobody able to serve: an election, or a lost majority. Pause
                // so the retry loop does not spin a core while the cluster sorts itself out.
                attemptsSinceProgress = 0;
                if (!pause(expiry)) {
                    break;
                }
            }
        }
        throw new StatusRuntimeException(Status.DEADLINE_EXCEEDED
                .withDescription("no member served '" + description + "' within " + deadline
                        + "; last was " + (last == null ? "no attempt" : last.getMessage()))
                .withCause(last));
    }

    /** Sleeps briefly, returning false if the deadline arrived first. */
    private static boolean pause(long expiry) {
        long remainingMs = (expiry - System.nanoTime()) / 1_000_000L;
        if (remainingMs <= 0) {
            return false;
        }
        try {
            Thread.sleep(Math.min(SWEEP_PAUSE_MS, remainingMs));
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private int indexOf(String endpoint) {
        return endpoint == null ? -1 : endpoints.indexOf(endpoint);
    }

    private int next(int target) {
        return (target + 1) % members.size();
    }

    @Override
    public void close() throws InterruptedException {
        for (NodeClient member : members) {
            member.close();
        }
    }
}
