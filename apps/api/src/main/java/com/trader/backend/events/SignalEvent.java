package com.trader.backend.events;

import java.time.Instant;

/**
 * Emitted when depth imbalance indicates a directional trade signal.
 */
public record SignalEvent(Instant ts, String symbol, String side, double dbi, double sl, double tp) {}
