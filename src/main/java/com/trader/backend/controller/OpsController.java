package com.trader.backend.controller;

import com.trader.backend.service.MarketHours;
import com.trader.backend.service.LiveFeedService;
import com.trader.backend.service.InfluxTickService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.*;
import java.util.LinkedHashMap;
import java.util.Map;

@RestController
public class OpsController {
    private final LiveFeedService liveFeedService;
    private final InfluxTickService influxTickService;

    public OpsController(LiveFeedService liveFeedService, InfluxTickService influxTickService) {
        this.liveFeedService = liveFeedService;
        this.influxTickService = influxTickService;
    }

    @GetMapping("/ops/market-clock")
    public Map<String, Object> marketClock() {
        Map<String, Object> m = new LinkedHashMap<>();
        ZoneId marketTz = MarketHours.zone();
        Instant nowUtc = Instant.now();
        ZonedDateTime nowIst = nowUtc.atZone(marketTz);
        LocalTime open = MarketHours.openTime();
        LocalTime close = MarketHours.closeTime();
        int buffer = MarketHours.bufferMinutes();
        boolean isTradingWindowNow = MarketHours.isOpen(nowUtc);
        boolean todayIsTradingDay = MarketHours.isTradingDay(nowIst.toLocalDate());
        Instant nextAutoStart = null;
        if (!isTradingWindowNow) {
            nextAutoStart = MarketHours.nextOpenAfter(nowUtc);
        }
        m.put("nowUtc", nowUtc.toString());
        m.put("nowIst", nowIst.toString());
        m.put("marketTz", marketTz.getId());
        m.put("openIst", open.toString());
        m.put("closeIst", close.toString());
        m.put("bufferMin", buffer);
        m.put("isTradingWindowNow", isTradingWindowNow);
        m.put("todayIsTradingDay", todayIsTradingDay);
        m.put("nextAutoStartAtUtc", nextAutoStart != null ? nextAutoStart.toString() : null);
        m.put("serverZoneId", ZoneId.systemDefault().getId());
        m.put("serverInstant", Instant.now().toString());
        return m;
    }

    @GetMapping("/ops/live-status")
    public Map<String, Object> liveStatus() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("connected", liveFeedService.isConnected());
        Instant ts = liveFeedService.lastTickTs();
        m.put("lastTickTs", ts != null ? ts.toString() : null);
        m.put("ticksLast60s", liveFeedService.ticksLast60s());
        m.put("futSubscribed", liveFeedService.futSubscribed());
        m.put("optSubscribedCount", liveFeedService.optSubscribedCount());
        return m;
    }

    @GetMapping("/ops/influx-sanity")
    public Map<String, Object> influxSanity() {
        InfluxTickService.Sanity s = influxTickService.sanityLast2m();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("futPoints", s.futPoints());
        m.put("optPoints", s.optPoints());
        m.put("futLastTs", s.futLastTs() != null ? s.futLastTs().toString() : null);
        m.put("optLastTs", s.optLastTs() != null ? s.optLastTs().toString() : null);
        return m;
    }
}
