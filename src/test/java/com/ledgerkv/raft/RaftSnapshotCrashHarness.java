package com.ledgerkv.raft;

import com.ledgerkv.raft.kv.KvCommand;
import com.ledgerkv.raft.kv.RaftKvStateMachine;
import java.nio.file.Paths;
import java.util.Collections;

/**
 * Child-JVM entry point for the Raft snapshot crash-recovery test. Self-elects a single-node group,
 * proposes a batch of KV PUTs, compacts (durable snapshot + WAL prefix truncation), proposes MORE
 * PUTs after the snapshot, then dies hard via {@link Runtime#halt(int)}.
 *
 * <p>Usage: {@code java -cp <cp> com.ledgerkv.raft.RaftSnapshotCrashHarness <dir> <pre> <post>}
 */
public final class RaftSnapshotCrashHarness {

    static String key(int i) {
        return String.format("k%04d", i);
    }

    static String value(int i) {
        return String.format("v%04d", i);
    }

    public static void main(String[] args) throws Exception {
        String dir = args[0];
        int pre = Integer.parseInt(args[1]);
        int post = Integer.parseInt(args[2]);

        RaftState recovered = RaftPersistence.replay(Paths.get(dir));
        RaftPersistence persistence = RaftPersistence.open(Paths.get(dir));
        RaftKvStateMachine sm = new RaftKvStateMachine();
        RaftNode node = new RaftNode("n0", Collections.emptyList(), sm, () -> 1, persistence, recovered);
        node.setCompactionThreshold(1); // compact eagerly after the pre batch
        node.tick();
        if (!node.isLeader()) {
            throw new IllegalStateException("single-node should self-elect");
        }

        for (int i = 0; i < pre; i++) {
            node.propose(KvCommand.put("c", i + 1, key(i), value(i).getBytes()).encode());
        }
        node.maybeCompact(); // durable snapshot through the pre batch + WAL prefix truncated
        for (int i = 0; i < post; i++) {
            int seq = pre + i + 1;
            node.propose(KvCommand.put("c", seq, key(pre + i), value(pre + i).getBytes()).encode());
        }
        Runtime.getRuntime().halt(0);
    }
}
