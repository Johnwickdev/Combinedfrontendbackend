package com.trader.backend.events;

import java.time.Instant;

/**
 * Event emitted by the LWP signal engine.
 */
public record LwpSignalEvent(Instant ts, String symbol, String side,
                             int level, double price, int persistenceCount, double confidence) { }
