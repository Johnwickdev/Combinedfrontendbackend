package com.trader.backend.state;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Shared mutable market state. Holds WebSocket connection status and
 * timestamps required for deriving the overall market online/offline state.
 */
@Component
public class MarketState {
    private final AtomicBoolean wsConnected = new AtomicBoolean(false);
    private final AtomicLong lastTickTs = new AtomicLong(0);
    private final AtomicLong lastMarketInfoTs = new AtomicLong(0);
    private final AtomicBoolean online = new AtomicBoolean(false);
    private final AtomicLong lastChangeTs = new AtomicLong(0);
    private volatile String reason = "init";

    public boolean isWsConnected() { return wsConnected.get(); }
    public void setWsConnected(boolean connected) { wsConnected.set(connected); }

    public long getLastTickTs() { return lastTickTs.get(); }
    public void setLastTickTs(long ts) { lastTickTs.set(ts); }

    public long getLastMarketInfoTs() { return lastMarketInfoTs.get(); }
    public void setLastMarketInfoTs(long ts) { lastMarketInfoTs.set(ts); }

    public boolean isOnline() { return online.get(); }
    public void setOnline(boolean value) { online.set(value); }

    public long getLastChangeTs() { return lastChangeTs.get(); }
    public void setLastChangeTs(long ts) { lastChangeTs.set(ts); }

    public String getReason() { return reason; }
    public void setReason(String r) { reason = r; }
}
