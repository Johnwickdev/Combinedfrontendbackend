package com.trader.backend.controller;

import com.trader.backend.service.MarketHours;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.*;
import java.util.LinkedHashMap;
import java.util.Map;

@RestController
public class OpsController {

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
}
