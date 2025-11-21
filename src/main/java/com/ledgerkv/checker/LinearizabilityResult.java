package com.ledgerkv.checker;

import java.util.List;
import java.util.Objects;

/**
 * Immutable verdict from {@link LinearizabilityChecker}. On failure it carries a witness — the key
 * whose per-key sub-history could not be linearized, the prefix that was linearized before the
 * search got stuck, and the minimal-frontier ops that could not extend it — so a CI failure is
 * debuggable.
 */
public final class LinearizabilityResult {

    private final boolean linearizable;
    private final String stuckKey;
    private final List<String> linearizedPrefix;
    private final List<String> stuckFrontier;

    private LinearizabilityResult(boolean linearizable,
                                  String stuckKey,
                                  List<String> linearizedPrefix,
                                  List<String> stuckFrontier) {
        this.linearizable = linearizable;
        this.stuckKey = stuckKey;
        this.linearizedPrefix = List.copyOf(linearizedPrefix);
        this.stuckFrontier = List.copyOf(stuckFrontier);
    }

    public static LinearizabilityResult linearizable() {
        return new LinearizabilityResult(true, null, List.of(), List.of());
    }

    public static LinearizabilityResult notLinearizable(String stuckKey,
                                                        List<String> linearizedPrefix,
                                                        List<String> stuckFrontier) {
        Objects.requireNonNull(stuckKey, "stuckKey must not be null");
        return new LinearizabilityResult(false, stuckKey, linearizedPrefix, stuckFrontier);
    }

    public boolean isLinearizable() {
        return linearizable;
    }

    public String getStuckKey() {
        return stuckKey;
    }

    public List<String> getLinearizedPrefix() {
        return linearizedPrefix;
    }

    public List<String> getStuckFrontier() {
        return stuckFrontier;
    }

    public String describeWitness() {
        if (linearizable) {
            return "linearizable";
        }
        return "NOT linearizable on key=" + stuckKey
            + "; linearized prefix=" + linearizedPrefix
            + "; stuck frontier (none of these fit the model)=" + stuckFrontier;
    }

    @Override
    public String toString() {
        return "LinearizabilityResult{" + describeWitness() + '}';
    }
}
