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
        Optional<Tick> liveOpt = liveFeedService.getLatestTick(instrumentKey);

        // if we have a very recent tick, treat it as live
        if (liveOpt.isPresent() &&
                Duration.between(liveOpt.get().ts(), now).toMillis() <= 5000) {
            Tick t = liveOpt.get();
            liveFeedService.logResolvedLtp(instrumentKey, t.ltp(), "live");
            return new Result(t.ltp(), t.ts(), "live");
        }

        // otherwise, try the persisted store
        Optional<Tick> stored = influxTickService.latestTick(instrumentKey);
        if (stored.isPresent()) {
            Tick t = stored.get();
            liveFeedService.logResolvedLtp(instrumentKey, t.ltp(), "stored");
            return new Result(t.ltp(), t.ts(), "influx");
        }

        // fall back to the last cached tick even if it is stale
        if (liveOpt.isPresent()) {
            Tick t = liveOpt.get();
            liveFeedService.logResolvedLtp(instrumentKey, t.ltp(), "stale");
            return new Result(t.ltp(), t.ts(), "influx");
        }

        return new Result(null, null, "none");
    }
}
