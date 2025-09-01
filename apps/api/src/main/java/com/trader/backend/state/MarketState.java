package com.trader.backend.state;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Shared mutable market state. Holds WebSocket connection status and
 * timestamp of the last received tick.
 */
@Component
public class MarketState {

    private final AtomicBoolean wsConnected = new AtomicBoolean(false);
    private final AtomicLong lastTickTs = new AtomicLong(0);

    public boolean isWsConnected() {
        return wsConnected.get();
    }

    public void setWsConnected(boolean connected) {
        wsConnected.set(connected);
    }

    public long getLastTickTs() {
        return lastTickTs.get();
    }

    public void setLastTickTs(long ts) {
        lastTickTs.set(ts);
    }
}
