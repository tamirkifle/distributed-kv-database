package com.ledgerkv.raft;

import java.nio.file.Paths;
import java.util.Collections;

/**
 * Child-JVM entry point for the Raft crash-recovery test. Opens a durable single-node Raft group
 * (no peers → self-elects), proposes {@code count} commands (each WAL-fsync'd in SYNC mode), then
 * dies hard via {@link Runtime#halt(int)} with no shutdown hook / close. Mirrors {@code LsmCrashHarness}.
 *
 * <p>Usage: {@code java -cp <cp> com.ledgerkv.raft.RaftCrashHarness <dir> <count>}
 */
public final class RaftCrashHarness {

    static String cmd(int i) {
        return String.format("cmd%06d", i);
    }

    private static final class NoopSm implements StateMachine {
        @Override public byte[] apply(byte[] command) { return command; }
    }

    public static void main(String[] args) throws Exception {
        String dir = args[0];
        int count = Integer.parseInt(args[1]);

        RaftState recovered = RaftPersistence.replay(Paths.get(dir));
        RaftPersistence persistence = RaftPersistence.open(Paths.get(dir));
        RaftNode node = new RaftNode("n0", Collections.emptyList(), new NoopSm(), () -> 1,
                persistence, recovered);

        // Single-node cluster: one tick fires the election timeout and self-elects (majority of 1).
        node.tick();
        if (!node.isLeader()) {
            throw new IllegalStateException("single-node should self-elect");
        }
        for (int i = 0; i < count; i++) {
            node.propose(cmd(i).getBytes());
        }
        // Every propose recorded a SYNC-fsync'd LOG_ENTRY; die hard with no cleanup.
        Runtime.getRuntime().halt(0);
    }
}
