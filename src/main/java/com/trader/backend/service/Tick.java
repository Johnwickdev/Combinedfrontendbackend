package com.trader.backend.service;

import java.time.Instant;

/**
 * Simple snapshot of the last traded price for an instrument.
 */
public record Tick(String instrumentKey, double ltp, Instant ts) {}
