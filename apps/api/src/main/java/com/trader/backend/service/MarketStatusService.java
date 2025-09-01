package com.trader.backend.service;

import com.trader.backend.state.MarketState;
import com.trader.backend.util.TradingHoursUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * Tracks live market connection state and last tick timestamp.
 */
@Service
@RequiredArgsConstructor
public class MarketStatusService {

    private final MarketState marketState;

    /**
     * Update the timestamp of the latest received tick.
     */
    public void onTick(long ts) {
        marketState.setLastTickTs(ts);
    }

    public Status getStatus() {
        boolean wsConnected = marketState.isWsConnected();
        boolean open = TradingHoursUtil.isMarketOpen(Instant.now());
        boolean online = open && wsConnected;
        String reason = online ? "" : (open ? "ws-disconnected" : "market-closed");
        return new Status(online, wsConnected, marketState.getLastTickTs(), reason);
    }

    public record Status(boolean online, boolean wsConnected, long lastTickTs, String reason) {}
}
