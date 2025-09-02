package com.trader.backend.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trader.backend.config.LwpProperties;
import com.trader.backend.config.SimProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import jakarta.annotation.PostConstruct;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Replays depth ticks from a dataset when simulation mode is enabled.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class SimRunner {

    private final SimProperties simProps;
    private final LwpProperties lwpProps;
    private final DepthMetricsService depthService;
    private final LiquidityWallService wallService;
    private final ObjectMapper mapper = new ObjectMapper();

    @PostConstruct
    void init() {
        if (!simProps.isEnabled()) return;
        try {
            String path = simProps.getDataset().replace("classpath:/", "");
            InputStream is = new ClassPathResource(path).getInputStream();
            List<SimTick> ticks = mapper.readValue(is, new TypeReference<>(){});
            Flux.fromIterable(ticks)
                    .delaySubscription(Duration.ofSeconds(1))
                    .delayElements(Duration.ofMillis(lwpProps.getWindowMs()))
                    .doOnNext(this::publish)
                    .doOnComplete(() -> log.info("Sim replay completed"))
                    .subscribe();
            log.info("Loaded {} sim ticks from {}", ticks.size(), path);
        } catch (Exception e) {
            log.error("Failed to load sim dataset", e);
        }
    }

    private void publish(SimTick t) {
        List<DepthMetricsService.Level> bids = t.bids.stream()
                .map(l -> new DepthMetricsService.Level(l.p, l.q)).toList();
        List<DepthMetricsService.Level> asks = t.asks.stream()
                .map(l -> new DepthMetricsService.Level(l.p, l.q)).toList();
        DepthMetricsService.DepthTick dt = new DepthMetricsService.DepthTick(
                t.symbol, bids, asks, t.ltp, Instant.ofEpochMilli(t.ts));
        depthService.onDepthTick(dt);
        wallService.onDepthTick(dt);
    }

    // Data model for JSON
    public static class SimTick { public long ts; public String symbol; public double ltp; public List<Lvl> bids; public List<Lvl> asks; }
    public static class Lvl { public double p; public double q; }
}
