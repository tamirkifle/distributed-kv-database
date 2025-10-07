package com.ledgerkv.consistency;

import com.ledgerkv.VersionedValue;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public final class VersionConflictResolver {
    private VersionConflictResolver() {
    }

    public static List<VersionedValue> conflictingSiblings(List<VersionedValue> values) {
        List<VersionedValue> survivingVersions = survivingVersions(values);
        if (survivingVersions.size() <= 1 || !hasConcurrentPair(survivingVersions)) {
            return List.of();
        }
        return survivingVersions;
    }

    public static VersionedValue resolvedValue(VersionedValue proposedValue, List<VersionedValue> values) {
        return resolvedValue(proposedValue, values, ConflictResolutionPolicy.PRESERVE_CONFLICTS);
    }

    public static VersionedValue resolvedValue(VersionedValue proposedValue, List<VersionedValue> values,
                                               ConflictResolutionPolicy policy) {
        Objects.requireNonNull(policy, "conflict resolution policy must not be null");

        List<VersionedValue> survivingVersions = survivingVersions(values);
        if (survivingVersions.size() == 1) {
            return survivingVersions.get(0);
        }
        if (survivingVersions.size() > 1 && hasConcurrentPair(survivingVersions)) {
            if (policy == ConflictResolutionPolicy.RESOLVE_DETERMINISTICALLY) {
                return deterministicWinner(survivingVersions);
            }
            return null;
        }
        return proposedValue;
    }

    private static List<VersionedValue> survivingVersions(List<VersionedValue> values) {
        Objects.requireNonNull(values, "values must not be null");

        List<VersionedValue> nonNullValues = new ArrayList<>();
        for (VersionedValue value : values) {
            if (value != null) {
                nonNullValues.add(value);
            }
        }

        Map<SiblingKey, VersionedValue> uniqueSurvivors = new LinkedHashMap<>();
        for (VersionedValue candidate : nonNullValues) {
            if (!isObsolete(candidate, nonNullValues)) {
                uniqueSurvivors.putIfAbsent(SiblingKey.from(candidate), candidate);
            }
        }

        List<VersionedValue> survivors = new ArrayList<>(uniqueSurvivors.values());
        survivors.sort(Comparator
            .comparing((VersionedValue value) -> value.getVersionMetadata().getVectorClock().toString())
            .thenComparing(VersionedValue::getValue)
            .thenComparingLong(VersionedValue::getVersion));
        return survivors;
    }

    private static boolean isObsolete(VersionedValue candidate, List<VersionedValue> values) {
        for (VersionedValue other : values) {
            if (candidate == other) {
                continue;
            }
            if (candidate.getVersionMetadata().happensBefore(other.getVersionMetadata())) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasConcurrentPair(List<VersionedValue> values) {
        for (int i = 0; i < values.size(); i++) {
            for (int j = i + 1; j < values.size(); j++) {
                if (values.get(i).getVersionMetadata().isConcurrentWith(values.get(j).getVersionMetadata())) {
                    return true;
                }
            }
        }
        return false;
    }

    private static VersionedValue deterministicWinner(List<VersionedValue> values) {
        return values.stream()
            .max(Comparator
                .comparing((VersionedValue value) -> metadataSortKey(value.getVersionMetadata()))
                .thenComparing(VersionedValue::getValue)
                .thenComparingLong(VersionedValue::getVersion))
            .orElse(null);
    }

    private static String metadataSortKey(VersionMetadata metadata) {
        return metadata.getVectorClock().entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(entry -> entry.getKey() + "=" + entry.getValue())
            .collect(Collectors.joining(","));
    }

    private static final class SiblingKey {
        private final String value;
        private final VersionMetadata metadata;

        private SiblingKey(String value, VersionMetadata metadata) {
            this.value = value;
            this.metadata = metadata;
        }

        static SiblingKey from(VersionedValue value) {
            return new SiblingKey(value.getValue(), value.getVersionMetadata());
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof SiblingKey)) {
                return false;
            }
            SiblingKey that = (SiblingKey) other;
            return Objects.equals(value, that.value) && metadata.equals(that.metadata);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value, metadata);
        }
    }
}
