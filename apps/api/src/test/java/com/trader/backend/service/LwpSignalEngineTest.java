package com.trader.backend.service;

import com.trader.backend.config.LwpProperties;
import com.trader.backend.events.LwpSignalEvent;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

public class LwpSignalEngineTest {

    private LiquidityWallService wall(LwpProperties p) { return new LiquidityWallService(p); }

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
    void emitsShortAfterPersistence() {
        LwpProperties p = new LwpProperties();
        p.setPersistActiveMs(200);
        LiquidityWallService ws = wall(p);
        LwpSignalEngine eng = new LwpSignalEngine(ws, p, new SimpleMeterRegistry());
        Instant t = Instant.now();
        StepVerifier.create(eng.stream())
                .then(() -> {
                    for (int i = 0; i < 10; i++) {
                        ws.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(10), asks(10), 100, t.plusMillis(i * 100)));
                    }
                    long base = t.plusMillis(1000).toEpochMilli();
                    ws.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(10), asks(50), 100, Instant.ofEpochMilli(base)));
                    ws.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(10), asks(5), 100, Instant.ofEpochMilli(base + 100)));
                    ws.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(10), asks(50), 100, Instant.ofEpochMilli(base + 200)));
                    ws.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(10), asks(5), 100, Instant.ofEpochMilli(base + 300)));
                    ws.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(10), asks(50), 100, Instant.ofEpochMilli(base + 400)));
                    ws.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(10), asks(5), 100, Instant.ofEpochMilli(base + 500)));
                    ws.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(10), asks(50), 100, Instant.ofEpochMilli(base + 600)));
                    ws.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(10), asks(50), 100, Instant.ofEpochMilli(base + 700)));
                    ws.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(10), asks(50), 100, Instant.ofEpochMilli(base + 800)));
                })
                .expectNextMatches(ev -> ev.side().equals("LWP_SHORT"))
                .thenCancel()
                .verify();
    }

    @Test
    void emitsLongAfterPersistence() {
        LwpProperties p = new LwpProperties();
        p.setPersistActiveMs(200);
        LiquidityWallService ws = wall(p);
        LwpSignalEngine eng = new LwpSignalEngine(ws, p, new SimpleMeterRegistry());
        Instant t = Instant.now();
        StepVerifier.create(eng.stream())
                .then(() -> {
                    for (int i = 0; i < 10; i++) {
                        ws.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(10), asks(10), 100, t.plusMillis(i * 100)));
                    }
                    long base = t.plusMillis(1000).toEpochMilli();
                    ws.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(50), asks(10), 100, Instant.ofEpochMilli(base)));
                    ws.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(5), asks(10), 100, Instant.ofEpochMilli(base + 100)));
                    ws.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(50), asks(10), 100, Instant.ofEpochMilli(base + 200)));
                    ws.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(5), asks(10), 100, Instant.ofEpochMilli(base + 300)));
                    ws.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(50), asks(10), 100, Instant.ofEpochMilli(base + 400)));
                    ws.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(5), asks(10), 100, Instant.ofEpochMilli(base + 500)));
                    ws.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(50), asks(10), 100, Instant.ofEpochMilli(base + 600)));
                    ws.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(50), asks(10), 100, Instant.ofEpochMilli(base + 700)));
                    ws.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids(50), asks(10), 100, Instant.ofEpochMilli(base + 800)));
                })
                .expectNextMatches(ev -> ev.side().equals("LWP_LONG"))
                .thenCancel()
                .verify();
    }
}
