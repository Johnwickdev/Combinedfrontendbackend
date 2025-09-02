package com.trader.backend.controller;

import com.trader.backend.service.LiveFeedService;
import com.trader.backend.service.Tick;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequiredArgsConstructor
public class WsController {
    private final LiveFeedService live;

    @GetMapping("/ws/status")
    public Map<String, Object> status() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("connected", live.isWsConnected());
        m.put("lastError", live.lastError());
        Instant ts = live.lastConnectTs();
        m.put("lastConnectTs", ts != null ? ts.toString() : null);
        m.put("lastCloseReason", live.lastCloseReason());
        return m;
    }

    @GetMapping("/ticks/latest")
    public Map<String, Tick> latest() {
        return live.latestTicks();
    }
}
