package com.trader.backend.controller;

import com.trader.backend.service.Tick;
import com.trader.backend.service.TickStore;
import com.trader.backend.service.LiveFeedService;
import com.trader.backend.state.MarketState;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequiredArgsConstructor
public class WsController {
    private final LiveFeedService feed;
    private final TickStore tickStore;
    private final MarketState marketState;

    @GetMapping("/ws/status")
    public Map<String, Object> status() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("connected", feed.isConnected());
        m.put("symbols", feed.cachedKeys());
        m.put("lastError", feed.lastError());
        m.put("lastTickTs", marketState.getLastTickTs());
        return m;
    }

    @GetMapping("/ticks/latest")
    public Map<String, Tick> latest() {
        return tickStore.all();
    }
}
