package com.trader.backend.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trader.backend.service.LiveFeedService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequiredArgsConstructor
public class SignalController {

    private final LiveFeedService live;
    private final ObjectMapper mapper = new ObjectMapper();

    @GetMapping(value = "/signals/live", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> live() {
        return Flux.interval(Duration.ofSeconds(1))
                .map(i -> {
                    try {
                        return mapper.writeValueAsString(buildPayload());
                    } catch (Exception e) {
                        return "{}";
                    }
                });
    }

    private Map<String, Object> buildPayload() {
        Map<String, Object> root = new LinkedHashMap<>();
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("connected", live.isWsConnected());
        status.put("marketOpen", live.isMarketOpen());
        Instant lt = live.lastTickTs();
        status.put("lastTickTs", lt != null ? lt.toString() : null);
        root.put("status", status);

        String futKey = live.currentFutKey();
        Map<String, Object> fut = new LinkedHashMap<>();
        if (futKey != null) {
            fut.put("key", futKey);
            fut.put("ltp", live.getLatestLtp(futKey));
        }
        root.put("fut", fut.isEmpty() ? null : fut);

        Map<String, Double> opts = new LinkedHashMap<>();
        live.latestTicks().forEach((k, v) -> {
            if (!k.equals(futKey)) {
                opts.put(k, v.ltp());
            }
        });
        root.put("opts", opts);
        return root;
    }
}
