package com.ledgerkv.raft;

/**
 * Thrown when a proposal was refused before it entered the log, because this node was not the
 * leader. Nothing was appended, so the command definitely did not take effect.
 *
 * <p>Separate from the timeout case for one reason: after {@code propose} has appended an entry,
 * a timeout means the outcome is <em>unknown</em>, never "did not happen". Telling the two apart
 * by checking the node's role afterwards does not work — a leader that appends an entry and then
 * steps down looks exactly like one that never accepted the proposal at all, and reporting that
 * as a definite failure loses a write that may still commit.
 */
public final class ProposalRejectedException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    public ProposalRejectedException(String message) {
        super(message);
    }
}
