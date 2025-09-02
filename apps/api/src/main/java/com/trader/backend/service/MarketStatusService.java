package com.trader.backend.service;

import com.trader.backend.events.TickEvent;
import com.trader.backend.state.MarketState;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.time.Instant;

/**
 * Determines whether the market feed is online based on WebSocket connectivity
 * and recent market_info heartbeats.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class MarketStatusService {
    private final MarketState marketState;

    private void evaluate() {
        long now = System.currentTimeMillis();
        boolean ws = marketState.isWsConnected();
        boolean segOpen = now - marketState.getLastMarketInfoTs() < 60_000;
        boolean online = ws && segOpen;
        if (online != marketState.isOnline()) {
            marketState.setOnline(online);
            marketState.setLastChangeTs(now);
            String reason = online ? "" : (!ws ? "ws-disconnected" : "market-closed");
            marketState.setReason(reason);
            if (online) {
                log.info("[market] ONLINE (segment=NSE_FO)");
            } else {
                log.info("[market] OFFLINE (reason={})", reason);
            }
        }
    }

    public void setWsConnected(boolean connected) {
        marketState.setWsConnected(connected);
        evaluate();
    }

    public void onMarketInfo(boolean segmentOpen) {
        if (segmentOpen) {
            marketState.setLastMarketInfoTs(System.currentTimeMillis());
        }
        evaluate();
    }

    @EventListener
    public void onTick(TickEvent tick) {
        marketState.setLastTickTs(tick.ts().toEpochMilli());
        evaluate();
    }

    public Status getStatus() {
        return new Status(marketState.isOnline(), marketState.getLastChangeTs(), marketState.getReason());
    }

    public record Status(boolean online, long lastChangeTs, String reason) {}
}
