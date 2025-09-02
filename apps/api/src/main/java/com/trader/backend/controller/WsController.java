package com.trader.backend.controller;

import com.trader.backend.service.Tick;
import com.trader.backend.service.TickStore;
import com.trader.backend.service.UpstoxFeedV3Client;
import com.trader.backend.state.MarketState;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequiredArgsConstructor
public class WsController {
    private final UpstoxFeedV3Client feed;
    private final TickStore tickStore;
    private final MarketState marketState;

    @GetMapping("/ws/status")
    public Map<String, Object> status() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("connected", feed.isConnected());
        m.put("mode", feed.mode());
        m.put("symbols", feed.symbols());
        m.put("lastError", feed.lastError());
        m.put("lastTickTs", marketState.getLastTickTs());
        return m;
    }

    @GetMapping("/ticks/latest")
    public Map<String, Tick> latest() {
        return tickStore.all();
    }
}
