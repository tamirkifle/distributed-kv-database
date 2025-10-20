package com.ledgerkv.storage.lsm;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Thrown when an SSTable's footer or a data block fails its integrity check. Unchecked so it can
 * surface through the (future) StorageEngine.get/scan path, which declares no checked exception.
 */
public final class SSTableCorruptionException extends UncheckedIOException {
    public SSTableCorruptionException(String message) {
        super(message, new IOException(message));
    }
}
