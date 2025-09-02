package com.trader.backend.events;

import java.time.Instant;

/**
 * Published whenever a new tick is received from the WebSocket feed.
 */
public record TickEvent(String instrumentKey, double ltp, Instant ts) {}
