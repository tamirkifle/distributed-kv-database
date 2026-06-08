package com.ledgerkv.raft;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;

/** Converters between the {@code com.ledgerkv.raft} Raft RPC types and their proto twins. */
public final class RaftProtos {

    private RaftProtos() {}

    public static com.ledgerkv.transport.proto.RequestVoteRequest toProto(RequestVoteRequest r) {
        return com.ledgerkv.transport.proto.RequestVoteRequest.newBuilder()
                .setTerm(r.term())
                .setCandidateId(r.candidateId())
                .setLastLogIndex(r.lastLogIndex())
                .setLastLogTerm(r.lastLogTerm())
                .build();
    }

    public static RequestVoteRequest fromProto(com.ledgerkv.transport.proto.RequestVoteRequest p) {
        return RequestVoteRequest.of(p.getTerm(), p.getCandidateId(), p.getLastLogIndex(), p.getLastLogTerm());
    }

    public static com.ledgerkv.transport.proto.RequestVoteResponse toProto(RequestVoteResponse r) {
        return com.ledgerkv.transport.proto.RequestVoteResponse.newBuilder()
                .setTerm(r.term())
                .setVoteGranted(r.voteGranted())
                .build();
    }

    public static RequestVoteResponse fromProto(com.ledgerkv.transport.proto.RequestVoteResponse p) {
        return RequestVoteResponse.of(p.getTerm(), p.getVoteGranted());
    }

    public static com.ledgerkv.transport.proto.AppendEntriesRequest toProto(AppendEntriesRequest r) {
        com.ledgerkv.transport.proto.AppendEntriesRequest.Builder b =
                com.ledgerkv.transport.proto.AppendEntriesRequest.newBuilder()
                        .setTerm(r.term())
                        .setLeaderId(r.leaderId())
                        .setPrevLogIndex(r.prevLogIndex())
                        .setPrevLogTerm(r.prevLogTerm())
                        .setLeaderCommit(r.leaderCommit());
        for (LogEntry e : r.entries()) {
            b.addEntries(toProto(e));
        }
        return b.build();
    }

    public static AppendEntriesRequest fromProto(com.ledgerkv.transport.proto.AppendEntriesRequest p) {
        List<LogEntry> entries = new ArrayList<>();
        for (com.ledgerkv.transport.proto.RaftLogEntry e : p.getEntriesList()) {
            entries.add(fromProto(e));
        }
        return AppendEntriesRequest.of(p.getTerm(), p.getLeaderId(), p.getPrevLogIndex(),
                p.getPrevLogTerm(), entries, p.getLeaderCommit());
    }

    public static com.ledgerkv.transport.proto.AppendEntriesResponse toProto(AppendEntriesResponse r) {
        return com.ledgerkv.transport.proto.AppendEntriesResponse.newBuilder()
                .setTerm(r.term())
                .setSuccess(r.success())
                .setMatchIndex(r.matchIndex())
                .setConflictIndex(r.conflictIndex())
                .build();
    }

    public static AppendEntriesResponse fromProto(com.ledgerkv.transport.proto.AppendEntriesResponse p) {
        return p.getSuccess()
                ? AppendEntriesResponse.success(p.getTerm(), p.getMatchIndex())
                : AppendEntriesResponse.failure(p.getTerm(), p.getConflictIndex());
    }

    public static com.ledgerkv.transport.proto.InstallSnapshotRequest toProto(InstallSnapshotRequest r) {
        return com.ledgerkv.transport.proto.InstallSnapshotRequest.newBuilder()
                .setTerm(r.term())
                .setLeaderId(r.leaderId())
                .setLastIncludedIndex(r.lastIncludedIndex())
                .setLastIncludedTerm(r.lastIncludedTerm())
                .setData(ByteString.copyFrom(r.data()))
                .build();
    }

    public static InstallSnapshotRequest fromProto(com.ledgerkv.transport.proto.InstallSnapshotRequest p) {
        return InstallSnapshotRequest.of(p.getTerm(), p.getLeaderId(), p.getLastIncludedIndex(),
                p.getLastIncludedTerm(), p.getData().toByteArray());
    }

    public static com.ledgerkv.transport.proto.InstallSnapshotResponse toProto(InstallSnapshotResponse r) {
        return com.ledgerkv.transport.proto.InstallSnapshotResponse.newBuilder()
                .setTerm(r.term())
                .build();
    }

    public static InstallSnapshotResponse fromProto(com.ledgerkv.transport.proto.InstallSnapshotResponse p) {
        return InstallSnapshotResponse.of(p.getTerm());
    }

    public static com.ledgerkv.transport.proto.RaftLogEntry toProto(LogEntry e) {
        return com.ledgerkv.transport.proto.RaftLogEntry.newBuilder()
                .setTerm(e.term())
                .setIndex(e.index())
                .setCommand(ByteString.copyFrom(e.command()))
                .build();
    }

    public static LogEntry fromProto(com.ledgerkv.transport.proto.RaftLogEntry e) {
        return LogEntry.of(e.getTerm(), e.getIndex(), e.getCommand().toByteArray());
    }
}
