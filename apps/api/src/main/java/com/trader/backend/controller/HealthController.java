package com.trader.backend.controller;

import com.trader.backend.service.UpstoxAuthService;
import com.trader.backend.service.UpstoxFeedV3Client;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequiredArgsConstructor
public class HealthController {
    private final UpstoxAuthService auth;
    private final UpstoxFeedV3Client feed;

    @GetMapping("/status/health")
    public Map<String, Object> health() {
        Map<String, Object> m = new HashMap<>();
        m.put("state", auth.state().name());
        m.put("hasToken", auth.currentToken() != null);
        m.put("wsConnected", feed.isConnected());
        m.put("symbols", feed.symbols().size());
        return m;
    }
}
