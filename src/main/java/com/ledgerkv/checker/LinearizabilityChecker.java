package com.ledgerkv.checker;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Bounded Wing-&-Gong / Porcupine-style linearizability checker over an {@link OperationHistory}.
 *
 * <p>Given a concurrent history of READ/WRITE operations whose {@link OperationRecord}s carry
 * real-time {@code startTime}/{@code endTime} bounds, this searches for a total order that
 * (a) respects the real-time happens-before partial order (op A must precede op B iff
 * {@code A.endTime < B.startTime}) and (b) is a legal sequential execution of a last-write-wins
 * register, one register per key.
 *
 * <p>Practical optimizations (standard for this family of checkers): operations on independent keys
 * are checked in independent sub-searches (Lowe's partition-by-key); within a per-key search,
 * visited {@code (set-of-linearized-ops, register-value)} prefixes are memoized to prune revisits;
 * and a per-key {@code maxOpsPerKey} guardrail throws {@link LinearizabilityCheckException} rather
 * than risk silent exponential blowup.
 *
 * <p><b>This is a bounded bug-finder, not a soundness proof.</b> A LINEARIZABLE verdict means no
 * violation was found within the bounded history; it is not a proof that all executions of the
 * system are linearizable. A NOT-LINEARIZABLE verdict is a genuine counterexample — there is no
 * valid linearization of the supplied history — and comes with a witness for debugging. This is
 * exactly how Jepsen's Knossos/Porcupine checkers are positioned.
 *
 * <h2>Operations whose outcome the client never learned</h2>
 *
 * <p>Three results, treated three ways:
 *
 * <ul>
 *   <li>{@link OperationResult#SUCCESS} — must appear in the linearization, inside its real-time
 *       bounds.
 *   <li>{@link OperationResult#FAILURE} — definitely never took effect, so it is omitted. The
 *       client learned nothing about the register and the op constrains nothing.
 *   <li>{@link OperationResult#UNKNOWN} — a timeout or a killed node. The search may place it
 *       <em>or</em> leave it out, whichever admits a valid order, which is how Jepsen treats an
 *       {@code :info} op.
 * </ul>
 *
 * <p>An unknown write also has no end: the client stopped waiting, but the entry may commit long
 * afterwards, so nothing can be required to follow it. Only its invocation time constrains what
 * may precede it. An unknown <em>read</em> returned no value and is simply dropped.
 *
 * <p>Folding unknowns into failures is what makes a correct checker lie. The write disappears from
 * the history, a later read observes the value it wrote, no writer exists for that value, and the
 * checker reports a violation that never happened.
 */
public final class LinearizabilityChecker {

    private static final int DEFAULT_MAX_OPS_PER_KEY = 64;

    private final int maxOpsPerKey;

    public LinearizabilityChecker() {
        this(DEFAULT_MAX_OPS_PER_KEY);
    }

    public LinearizabilityChecker(int maxOpsPerKey) {
        if (maxOpsPerKey < 1) {
            throw new IllegalArgumentException("maxOpsPerKey must be >= 1");
        }
        this.maxOpsPerKey = maxOpsPerKey;
    }

    /**
     * Checks {@code history} for linearizability. Returns a {@link LinearizabilityResult}; throws
     * {@link LinearizabilityCheckException} if any single key has more than {@code maxOpsPerKey}
     * (non-failed) operations.
     */
    public LinearizabilityResult check(OperationHistory history) {
        Objects.requireNonNull(history, "history must not be null");

        // Partition by key (Lowe). LinkedHashMap so the first-failing key is deterministic.
        Map<String, List<OperationRecord>> byKey = new LinkedHashMap<>();
        for (OperationRecord op : history.getOperations()) {
            if (!constrains(op)) {
                continue;
            }
            byKey.computeIfAbsent(op.getKey(), k -> new ArrayList<>()).add(op);
        }

        for (Map.Entry<String, List<OperationRecord>> entry : byKey.entrySet()) {
            String key = entry.getKey();
            List<OperationRecord> ops = entry.getValue();
            if (ops.size() > maxOpsPerKey) {
                throw new LinearizabilityCheckException(
                    "key '" + key + "' has " + ops.size() + " operations, exceeding the bound of "
                        + maxOpsPerKey + "; this checker is a bounded bug-finder, not a proof");
            }
            LinearizabilityResult perKey = checkKey(key, ops);
            if (!perKey.isLinearizable()) {
                return perKey; // whole history is non-linearizable iff some key is
            }
        }
        return LinearizabilityResult.linearizable();
    }

    /**
     * Whether this op says anything about the register. A definite failure does not. Neither does
     * an unknown read: the client timed out holding no value, so there is nothing to explain.
     */
    private static boolean constrains(OperationRecord op) {
        if (op.getResult() == OperationResult.FAILURE) {
            return false;
        }
        return op.getResult() != OperationResult.UNKNOWN || op.getType() == OperationType.WRITE;
    }

    /** An unknown write may still be applied after the client gave up, so it has no end. */
    private static boolean optional(OperationRecord op) {
        return op.getResult() == OperationResult.UNKNOWN;
    }

    private LinearizabilityResult checkKey(String key, List<OperationRecord> ops) {
        int n = ops.size();
        // Precompute real-time-before: before[i][j] == true iff ops[i].end < ops[j].start. An
        // optional op has no end, so it is never required to precede anything — which also means
        // leaving it out of the order can never block an op that was waiting behind it.
        boolean[][] before = new boolean[n][n];
        for (int i = 0; i < n; i++) {
            if (optional(ops.get(i))) {
                continue;
            }
            for (int j = 0; j < n; j++) {
                if (i != j && ops.get(i).getEndTime().isBefore(ops.get(j).getStartTime())) {
                    before[i][j] = true;
                }
            }
        }

        int mandatory = 0;
        for (OperationRecord op : ops) {
            if (!optional(op)) {
                mandatory++;
            }
        }

        Set<MemoKey> failedPrefixes = new HashSet<>();
        List<String> prefix = new ArrayList<>();
        Set<Integer> linearized = new HashSet<>();
        Witness witness = new Witness();

        boolean ok =
                search(ops, before, mandatory, null, linearized, prefix, failedPrefixes, witness);
        if (ok) {
            return LinearizabilityResult.linearizable();
        }
        return LinearizabilityResult.notLinearizable(key, witness.prefix, witness.frontier);
    }

    /**
     * Recursive minimal-frontier search. {@code registerValue} is the current model value for the
     * key (null = never written). Returns true if the remaining ops can be linearized. On the way
     * back up a failing branch it leaves {@code prefix}/{@code stuckFrontier} describing the
     * deepest stuck point for the witness.
     */
    private boolean search(List<OperationRecord> ops,
                           boolean[][] before,
                           int mandatoryRemaining,
                           String registerValue,
                           Set<Integer> linearized,
                           List<String> prefix,
                           Set<MemoKey> failedPrefixes,
                           Witness witness) {
        // Done once every op the client got an answer for is placed. Unknown ops still pending at
        // that point are the ones that never took effect, which is a legal reading of the history.
        if (mandatoryRemaining == 0) {
            return true;
        }

        // Memoize on (set-of-linearized-ops, registerValue): if seen, it already failed.
        MemoKey memoKey = new MemoKey(linearized, registerValue);
        if (failedPrefixes.contains(memoKey)) {
            return false;
        }

        // Minimal frontier: pending op i is a candidate iff no other pending op j must precede it.
        List<Integer> frontier = new ArrayList<>();
        for (int i = 0; i < ops.size(); i++) {
            if (linearized.contains(i)) {
                continue;
            }
            boolean blocked = false;
            for (int j = 0; j < ops.size(); j++) {
                if (j != i && !linearized.contains(j) && before[j][i]) {
                    blocked = true;
                    break;
                }
            }
            if (!blocked) {
                frontier.add(i);
            }
        }

        // Frontier ops that do not fit the model become the witness if this branch dead-ends.
        List<String> nonFittingFrontier = new ArrayList<>();
        for (int i : frontier) {
            OperationRecord op = ops.get(i);
            if (!fits(op, registerValue)) {
                nonFittingFrontier.add(describe(op));
                continue;
            }
            String nextValue = (op.getType() == OperationType.WRITE) ? op.getValue() : registerValue;
            linearized.add(i);
            prefix.add(describe(op));
            if (search(ops, before, mandatoryRemaining - (optional(op) ? 0 : 1), nextValue,
                    linearized, prefix, failedPrefixes, witness)) {
                return true;
            }
            prefix.remove(prefix.size() - 1);
            linearized.remove(i);
        }

        failedPrefixes.add(memoKey);
        witness.record(prefix, nonFittingFrontier);
        return false;
    }

    /**
     * The best dead end seen so far, kept for the failure report.
     *
     * <p>It has to be a copy. {@code prefix} is one list mutated in place as the search descends
     * and unwinds, so by the time the top-level call returns false it is empty again — which is
     * what every non-linearizable verdict used to carry.
     */
    private static final class Witness {
        final List<String> prefix = new ArrayList<>();
        final List<String> frontier = new ArrayList<>();
        private int depth = -1;

        /**
         * Prefers a dead end that can name the operations that would not fit, and among those the
         * one reached after linearizing the most operations: that is the longest story the checker
         * can tell before it runs out of legal moves.
         */
        void record(List<String> linearizedSoFar, List<String> nonFitting) {
            boolean informative = !nonFitting.isEmpty();
            boolean haveInformative = !frontier.isEmpty();
            if (haveInformative && !informative) {
                return; // never trade a named blocker for an unexplained dead end
            }
            if (informative == haveInformative && linearizedSoFar.size() <= depth) {
                return;
            }
            depth = linearizedSoFar.size();
            prefix.clear();
            prefix.addAll(linearizedSoFar);
            frontier.clear();
            frontier.addAll(nonFitting);
        }
    }

    private static boolean fits(OperationRecord op, String registerValue) {
        if (op.getType() == OperationType.WRITE) {
            return true; // a write has no precondition
        }
        return Objects.equals(op.getValue(), registerValue);
    }

    private static String describe(OperationRecord op) {
        String suffix = optional(op) ? " (outcome unknown)" : "";
        return op.getType() + " " + op.getKey() + "=" + op.getValue() + suffix;
    }

    /**
     * Memoization cache key: the set of already-linearized op indices plus the current register
     * value. Two search states with the same consumed-op set and register value are equivalent, so
     * if one dead-ended the other will too.
     */
    private static final class MemoKey {
        private final Set<Integer> linearized;
        private final String registerValue;

        MemoKey(Set<Integer> linearized, String registerValue) {
            this.linearized = new HashSet<>(linearized);
            this.registerValue = registerValue;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof MemoKey)) {
                return false;
            }
            MemoKey that = (MemoKey) other;
            return linearized.equals(that.linearized) && Objects.equals(registerValue, that.registerValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(linearized, registerValue);
        }
    }
}
