package com.ledgerkv.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

class OperationMetricsTest {

    @Test
    void nineArgConstructorDefaultsHedgedCountToZero() {
        OperationMetrics m = new OperationMetrics(1, 1, 0, 1, 0, 0, 0, 0, List.of(1L));
        assertEquals(0, m.getHedgedRequestCount());
    }

    @Test
    void tenArgConstructorCarriesHedgedCount() {
        OperationMetrics m = new OperationMetrics(2, 1, 1, 2, 0, 0, 0, 0, 3, List.of(1L, 2L));
        assertEquals(3, m.getHedgedRequestCount());
    }

    @Test
    void emptyHasZeroHedgedCount() {
        assertEquals(0, OperationMetrics.empty().getHedgedRequestCount());
    }

    @Test
    void rejectsNegativeHedgedCount() {
        assertThrows(IllegalArgumentException.class,
            () -> new OperationMetrics(0, 0, 0, 0, 0, 0, 0, 0, -1, List.of()));
    }
}
