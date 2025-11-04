package com.ledgerkv.bench;

import java.util.List;
import java.util.Locale;

/** Renders {@link Measurement}s as a GitHub-flavored markdown table. */
public final class AmplificationReport {

    private AmplificationReport() {
    }

    public static String render(List<Measurement> measurements) {
        StringBuilder sb = new StringBuilder();
        sb.append("| Strategy | Write Amp | Read Amp | Space Amp | Live Tables | On-disk MB |\n");
        sb.append("|---|---|---|---|---|---|\n");
        for (Measurement m : measurements) {
            sb.append(String.format(Locale.ROOT, "| %s | %.2f× | %.2f | %.2f× | %d | %.2f |",
                            m.label(), m.writeAmplification(), m.readAmplification(),
                            m.spaceAmplification(), m.liveTableCount(),
                            m.liveOnDiskBytes() / (1024.0 * 1024.0)))
                    .append('\n');
        }
        return sb.toString();
    }
}
