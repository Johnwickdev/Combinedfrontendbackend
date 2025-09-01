package com.trader.backend.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.trader.backend.config.SignalsProperties;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class DepthMetricsServiceTest {

    private DepthMetricsService service(SignalsProperties props) {
        return new DepthMetricsService(props);
    }

    @Test
    void computesDbiCorrectly() {
        SignalsProperties p = new SignalsProperties();
        p.setWindowMs(10);
        DepthMetricsService svc = service(p);
        var bids = List.of(new DepthMetricsService.Level(100, 10),
                new DepthMetricsService.Level(99, 10),
                new DepthMetricsService.Level(98, 10),
                new DepthMetricsService.Level(97, 10),
                new DepthMetricsService.Level(96, 10));
        var asks = List.of(new DepthMetricsService.Level(101, 5),
                new DepthMetricsService.Level(102, 5),
                new DepthMetricsService.Level(103, 5),
                new DepthMetricsService.Level(104, 5),
                new DepthMetricsService.Level(105, 5));
        svc.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids, asks, 100, Instant.now()));
        StepVerifier.withVirtualTime(svc::metrics)
                .thenAwait(Duration.ofMillis(20))
                .expectNextMatches(m -> Math.abs(m.dbi() - 2.0) < 1e-6)
                .thenCancel()
                .verify();
    }

    @Test
    void respectsMaxLevels() {
        SignalsProperties p = new SignalsProperties();
        p.setWindowMs(10);
        p.setMaxLevels(2);
        DepthMetricsService svc = service(p);
        var bids = List.of(new DepthMetricsService.Level(100, 10),
                new DepthMetricsService.Level(99, 10),
                new DepthMetricsService.Level(98, 10));
        var asks = List.of(new DepthMetricsService.Level(101, 5),
                new DepthMetricsService.Level(102, 5),
                new DepthMetricsService.Level(103, 5));
        svc.onDepthTick(new DepthMetricsService.DepthTick("SYM", bids, asks, 100, Instant.now()));
        StepVerifier.withVirtualTime(svc::metrics)
                .thenAwait(Duration.ofMillis(20))
                .expectNextMatches(m -> Math.abs(m.dbi() - 4.0) < 1e-6) // (10+10)/(5+5)
                .thenCancel()
                .verify();
    }

    @Test
    void parsesJsonFeed() {
        SignalsProperties p = new SignalsProperties();
        p.setWindowMs(10);
        DepthMetricsService svc = service(p);
        ObjectMapper om = new ObjectMapper();
        ObjectNode feed = om.createObjectNode();
        ObjectNode full = feed.putObject("fullFeed").putObject("marketFF")
                .putObject("marketLevel");
        ArrayNode arr = full.putArray("bidAskQuote");
        for (int i = 0; i < 5; i++) {
            ObjectNode lvl = arr.addObject();
            lvl.put("bidP", 100 - i);
            lvl.put("bidQ", 10);
            lvl.put("askP", 101 + i);
            lvl.put("askQ", 5);
        }
        svc.onFeed("SYM", feed, 100, Instant.now());
        StepVerifier.withVirtualTime(svc::metrics)
                .thenAwait(Duration.ofMillis(20))
                .expectNextMatches(m -> Math.abs(m.dbi() - 2.0) < 1e-6)
                .thenCancel()
                .verify();
    }

    @Test
    void emitsPerWindow() {
        SignalsProperties p = new SignalsProperties();
        p.setWindowMs(10);
        DepthMetricsService svc = service(p);
        var bids = List.of(new DepthMetricsService.Level(100, 10));
        var asks = List.of(new DepthMetricsService.Level(101, 5));
        Flux.interval(Duration.ofMillis(10)).take(3)
                .doOnNext(i -> svc.onDepthTick(new DepthMetricsService.DepthTick("X", bids, asks, 100, Instant.now())))
                .subscribe();
        StepVerifier.withVirtualTime(svc::metrics)
                .thenAwait(Duration.ofMillis(40))
                .expectNextCount(3)
                .thenCancel()
                .verify();
    }
}
