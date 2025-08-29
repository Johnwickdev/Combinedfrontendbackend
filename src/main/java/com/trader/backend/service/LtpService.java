package com.trader.backend.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class LtpService {
    private final LiveFeedService liveFeedService;
    private final InfluxTickService influxTickService;

    public record Result(Double ltp, Instant ts, String source) {}

    public Result resolve(String instrumentKey) {
        Instant now = Instant.now();
        Optional<Tick> live = liveFeedService.getLatestTick(instrumentKey)
                .filter(t -> Duration.between(t.ts(), now).toMillis() <= 5000);
        if (live.isPresent()) {
            Tick t = live.get();
            liveFeedService.logResolvedLtp(instrumentKey, t.ltp(), "live");
            return new Result(t.ltp(), t.ts(), "live");
        }
        Optional<Tick> stored = influxTickService.latestTick(instrumentKey);
        if (stored.isPresent()) {
            Tick t = stored.get();
            liveFeedService.logResolvedLtp(instrumentKey, t.ltp(), "stored");
            return new Result(t.ltp(), t.ts(), "influx");
        }
        return new Result(null, null, "none");
    }
}
