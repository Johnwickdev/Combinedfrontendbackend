package com.trader.backend.service;

import com.trader.backend.config.SignalsProperties;
import com.trader.backend.events.SignalEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import jakarta.annotation.PostConstruct;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Consumes depth metrics and emits trading signals once configured
 * thresholds persist for the desired duration.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class SignalEngine {

    private final DepthMetricsService depthMetricsService;
    private final SignalsProperties props;
    private final Sinks.Many<SignalEvent> sink = Sinks.many().multicast().onBackpressureBuffer();

    private static class Counts { int ce; int pe; }
    private final Map<String, Counts> state = new ConcurrentHashMap<>();

    @PostConstruct
    void init() {
        depthMetricsService.metrics().subscribe(this::handleMetric);
    }

    private void handleMetric(DepthMetricsService.DepthMetric metric) {
        if (!props.isEnabled()) return;
        int required = props.getPersistWindows();
        Counts c = state.computeIfAbsent(metric.symbol(), k -> new Counts());
        if (metric.dbi() > 1.8) {
            c.ce++; c.pe = 0;
            if (c.ce >= required) {
                emit("BUY_CE", metric);
                c.ce = 0;
            }
        } else if (metric.dbi() < 0.55) {
            c.pe++; c.ce = 0;
            if (c.pe >= required) {
                emit("BUY_PE", metric);
                c.pe = 0;
            }
        } else {
            c.ce = 0; c.pe = 0;
        }
    }

    private void emit(String side, DepthMetricsService.DepthMetric m) {
        double sl = m.ltp() * 0.85;  // -15%
        double tp = m.ltp() * 1.25;  // +25%
        SignalEvent ev = new SignalEvent(Instant.now(), m.symbol(), side, m.dbi(), sl, tp);
        sink.tryEmitNext(ev);
        log.info("Signal {} for {} dbi={}", side, m.symbol(), m.dbi());
    }

    public Flux<SignalEvent> stream() {
        return sink.asFlux();
    }
}
