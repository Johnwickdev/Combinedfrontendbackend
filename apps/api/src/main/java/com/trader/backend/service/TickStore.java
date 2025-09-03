package com.trader.backend.service;

import com.trader.backend.events.TickEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
public class TickStore {
    private final InfluxTickService influxTickService;

    @Value("${influx.url:}")
    private String influxUrl;

    private final Map<String, Tick> latest = new ConcurrentHashMap<>();

    @EventListener
    public void onTick(TickEvent event) {
        Tick t = new Tick(event.instrumentKey(), event.ltp(), event.ts());
        latest.put(event.instrumentKey(), t);
        // writing to Influx is handled elsewhere; this store just caches
    }

    public Optional<Tick> get(String key) {
        Tick t = latest.get(key);
        if (t != null) {
            return Optional.of(t);
        }
        if (influxUrl != null && !influxUrl.isBlank()) {
            return influxTickService.latestTick(key);
        }
        return Optional.empty();
    }

    public Map<String, Tick> all() {
        return Map.copyOf(latest);
    }
}
