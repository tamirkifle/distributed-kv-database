package com.ledgerkv.storage.wal;

import java.io.IOException;

public class WalCorruptionException extends IOException {
    public WalCorruptionException(String message) {
        super(message);
    }
}
