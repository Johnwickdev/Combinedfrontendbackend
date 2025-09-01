package com.trader.backend.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.trader.backend.config.SignalsProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Computes depth based metrics such as the Depth Bid-Ask Imbalance (DBI5)
 * from streaming order book snapshots.
 */
@Service
@RequiredArgsConstructor
public class DepthMetricsService {

    private final SignalsProperties props;
    private final Sinks.Many<DepthTick> sink = Sinks.many().multicast().onBackpressureBuffer();

    /**
     * Accepts a raw feed payload and publishes a structured depth tick.
     */
    public void onFeed(String symbol, JsonNode feed, double ltp, Instant ts) {
        JsonNode quotes = feed.path("fullFeed").path("marketFF").path("marketLevel").path("bidAskQuote");
        List<Level> bids = new ArrayList<>();
        List<Level> asks = new ArrayList<>();
        if (quotes.isArray()) {
            int max = Math.min(props.getMaxLevels(), quotes.size());
            for (int i = 0; i < max; i++) {
                JsonNode level = quotes.get(i);
                bids.add(new Level(level.path("bidP").asDouble(0), level.path("bidQ").asDouble(0)));
                asks.add(new Level(level.path("askP").asDouble(0), level.path("askQ").asDouble(0)));
            }
        }
        onDepthTick(new DepthTick(symbol, bids, asks, ltp, ts));
    }

    /**
     * Publishes a structured depth tick.
     */
    public void onDepthTick(DepthTick tick) {
        sink.tryEmitNext(tick);
    }

    /**
     * Stream of DBI5 metrics per symbol sampled at the configured window.
     */
    public Flux<DepthMetric> metrics() {
        return sink.asFlux()
                .groupBy(DepthTick::symbol)
                .flatMap(g -> g.sample(Duration.ofMillis(props.getWindowMs()))
                        .map(this::computeMetric));
    }

    private DepthMetric computeMetric(DepthTick t) {
        int n = Math.min(props.getMaxLevels(), Math.min(t.bids().size(), t.asks().size()));
        double bidSum = 0;
        double askSum = 0;
        for (int i = 0; i < n; i++) {
            bidSum += t.bids().get(i).qty();
            askSum += t.asks().get(i).qty();
        }
        double dbi = askSum == 0 ? 0 : bidSum / askSum;
        return new DepthMetric(t.symbol(), dbi, t.ltp(), t.ts());
    }

    public record Level(double price, double qty) { }
    public record DepthTick(String symbol, List<Level> bids, List<Level> asks, double ltp, Instant ts) { }
    public record DepthMetric(String symbol, double dbi, double ltp, Instant ts) { }
}
