package com.trader.backend.service;

import com.trader.backend.config.SignalsProperties;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class SignalEngineTest {

    private DepthMetricsService service(SignalsProperties p) {
        return new DepthMetricsService(p);
    }

    private DepthMetricsService.DepthTick tickHigh() {
        var bids = List.of(new DepthMetricsService.Level(100, 20),
                new DepthMetricsService.Level(99, 20),
                new DepthMetricsService.Level(98, 20),
                new DepthMetricsService.Level(97, 20),
                new DepthMetricsService.Level(96, 20));
        var asks = List.of(new DepthMetricsService.Level(101, 10),
                new DepthMetricsService.Level(102, 10),
                new DepthMetricsService.Level(103, 10),
                new DepthMetricsService.Level(104, 10),
                new DepthMetricsService.Level(105, 10));
        return new DepthMetricsService.DepthTick("SYM", bids, asks, 100, Instant.now());
    }

    private DepthMetricsService.DepthTick tickLow() {
        var bids = List.of(new DepthMetricsService.Level(100, 5),
                new DepthMetricsService.Level(99, 5),
                new DepthMetricsService.Level(98, 5),
                new DepthMetricsService.Level(97, 5),
                new DepthMetricsService.Level(96, 5));
        var asks = List.of(new DepthMetricsService.Level(101, 20),
                new DepthMetricsService.Level(102, 20),
                new DepthMetricsService.Level(103, 20),
                new DepthMetricsService.Level(104, 20),
                new DepthMetricsService.Level(105, 20));
        return new DepthMetricsService.DepthTick("SYM", bids, asks, 100, Instant.now());
    }

    @Test
    void buyCeAfterPersistence() {
        SignalsProperties p = new SignalsProperties();
        p.setWindowMs(10);
        p.setPersistMs(80);
        DepthMetricsService svc = service(p);
        SignalEngine engine = new SignalEngine(svc, p);
        StepVerifier.withVirtualTime(engine::stream)
                .then(() -> Flux.interval(Duration.ofMillis(10)).take(8).subscribe(i -> svc.onDepthTick(tickHigh())))
                .thenAwait(Duration.ofMillis(80))
                .expectNextMatches(ev -> ev.side().equals("BUY_CE"))
                .thenCancel()
                .verify();
    }

    @Test
    void buyPeAfterPersistence() {
        SignalsProperties p = new SignalsProperties();
        p.setWindowMs(10);
        p.setPersistMs(80);
        DepthMetricsService svc = service(p);
        SignalEngine engine = new SignalEngine(svc, p);
        StepVerifier.withVirtualTime(engine::stream)
                .then(() -> Flux.interval(Duration.ofMillis(10)).take(8).subscribe(i -> svc.onDepthTick(tickLow())))
                .thenAwait(Duration.ofMillis(80))
                .expectNextMatches(ev -> ev.side().equals("BUY_PE"))
                .thenCancel()
                .verify();
    }

    @Test
    void noSignalWithoutPersistence() {
        SignalsProperties p = new SignalsProperties();
        p.setWindowMs(10);
        p.setPersistMs(80);
        DepthMetricsService svc = service(p);
        SignalEngine engine = new SignalEngine(svc, p);
        StepVerifier.withVirtualTime(engine::stream)
                .then(() -> Flux.interval(Duration.ofMillis(10)).take(7).subscribe(i -> svc.onDepthTick(tickHigh())))
                .thenAwait(Duration.ofMillis(70))
                .expectTimeout(Duration.ofMillis(10))
                .verify();
    }

    @Test
    void replayThirtySeconds() {
        SignalsProperties p = new SignalsProperties();
        p.setWindowMs(100);
        p.setPersistMs(800);
        DepthMetricsService svc = service(p);
        SignalEngine engine = new SignalEngine(svc, p);
        StepVerifier.withVirtualTime(engine::stream)
                .then(() -> Flux.interval(Duration.ofMillis(100)).take(300).subscribe(i -> svc.onDepthTick(tickHigh())))
                .thenAwait(Duration.ofSeconds(30))
                .expectNextMatches(ev -> ev.side().equals("BUY_CE"))
                .thenCancel()
                .verify();
    }
}
