package com.ledgerkv.storage;

import java.util.Iterator;

/**
 * An {@link Iterator} that holds resources (pinned SSTable handles) for its lifetime and must be
 * released. {@link #close()} declares no checked exception so it composes with try-with-resources
 * without a catch. Closing is idempotent; a fully-drained iterator releases its resources
 * automatically, but a caller that abandons iteration early MUST {@code close()} it.
 */
public interface CloseableIterator<T> extends Iterator<T>, AutoCloseable {

    @Override
    void close();
}
