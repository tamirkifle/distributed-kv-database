package com.ledgerkv.bench;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import org.junit.jupiter.api.Test;

class AmplificationReportTest {

    @Test
    void rendersAMarkdownTableWithComputedAmplifications() {
        // userLogical=100, written=300 -> WA 3.00; logicalLive=200, onDisk=300 -> SA 1.50.
        Measurement st = new Measurement("size-tiered", 100, 300, 200, 300, 4.0, 8);
        Measurement lv = new Measurement("leveled", 100, 600, 200, 220, 1.0, 3);
        String md = AmplificationReport.render(Arrays.asList(st, lv));

        assertTrue(md.contains("| Strategy | Write Amp | Read Amp | Space Amp | Live Tables | On-disk MB |"),
                "missing header row:\n" + md);
        assertTrue(md.contains("| size-tiered | 3.00×"), "missing size-tiered WA:\n" + md);
        assertTrue(md.contains("| leveled | 6.00×"), "missing leveled WA:\n" + md);
        assertTrue(md.contains("1.50×"), "missing size-tiered SA:\n" + md);
    }

    @Test
    void writeAndSpaceAmplificationComputeCorrectly() {
        Measurement m = new Measurement("x", 100, 250, 200, 240, 2.0, 4);
        assertTrue(Math.abs(m.writeAmplification() - 2.5) < 1e-9);
        assertTrue(Math.abs(m.spaceAmplification() - 1.2) < 1e-9);
    }
}
