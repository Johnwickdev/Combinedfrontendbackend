package com.trader.backend.service;

import com.trader.backend.util.TradingHoursUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks live market connection state and last tick timestamp.
 */
@Service
@RequiredArgsConstructor
public class MarketStatusService {

    private final LiveFeedService liveFeedService;
    private final AtomicLong lastTickTs = new AtomicLong(0);

    public void onTick(long ts) {
        lastTickTs.set(ts);
    }

    public Status getStatus() {
        boolean wsConnected = liveFeedService.isWsConnected();
        boolean open = TradingHoursUtil.isMarketOpen(Instant.now());
        boolean online = open && wsConnected;
        String reason = online ? "" : (open ? "ws-disconnected" : "market-closed");
        return new Status(online, wsConnected, lastTickTs.get(), reason);
    }

    public record Status(boolean online, boolean wsConnected, long lastTickTs, String reason) {}
}
