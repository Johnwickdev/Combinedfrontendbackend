package com.trader.backend.service;

import com.trader.backend.config.LwpProperties;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

public class LiquidityWallServiceTest {

    private LiquidityWallService svc(LwpProperties p) {
        return new LiquidityWallService(p);
    }

    private List<DepthMetricsService.Level> bids(double q) {
        return List.of(new DepthMetricsService.Level(99, q),
                new DepthMetricsService.Level(98, 10),
                new DepthMetricsService.Level(97, 10),
                new DepthMetricsService.Level(96, 10),
                new DepthMetricsService.Level(95, 10));
    }

    private List<DepthMetricsService.Level> asks(double q) {
        return List.of(new DepthMetricsService.Level(101, q),
                new DepthMetricsService.Level(102, 10),
                new DepthMetricsService.Level(103, 10),
                new DepthMetricsService.Level(104, 10),
                new DepthMetricsService.Level(105, 10));
    }

    @Test
    void detectsAskWallAfterReappearances() {
        LwpProperties p = new LwpProperties();
        LiquidityWallService svc = svc(p);
        Instant t = Instant.now();
        StepVerifier.create(svc.stream())
                .then(() -> {
                    // baseline samples for median
                    for (int i = 0; i < 10; i++) {
                        svc.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(10), asks(10), 100, t.plusMillis(i * 100)));
                    }
                    long base = t.plusMillis(1000).toEpochMilli();
                    svc.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(10), asks(50), 100, Instant.ofEpochMilli(base)));
                    svc.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(10), asks(5), 100, Instant.ofEpochMilli(base + 100)));
                    svc.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(10), asks(50), 100, Instant.ofEpochMilli(base + 200)));
                    svc.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(10), asks(5), 100, Instant.ofEpochMilli(base + 300)));
                    svc.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(10), asks(50), 100, Instant.ofEpochMilli(base + 400)));
                    svc.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(10), asks(5), 100, Instant.ofEpochMilli(base + 500)));
                    svc.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(10), asks(50), 100, Instant.ofEpochMilli(base + 600)));
                })
                .expectNextMatches(ev -> ev.side().equals("ASK") && ev.persistenceCount() == 3)
                .thenCancel()
                .verify();
    }
}
