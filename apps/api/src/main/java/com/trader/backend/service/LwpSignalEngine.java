package com.trader.backend.service;

import com.trader.backend.config.LwpProperties;
import com.trader.backend.events.LwpSignalEvent;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Consumes liquidity wall events and emits directional bias signals once the
 * wall has persisted for the configured duration.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class LwpSignalEngine {

    private final LiquidityWallService wallService;
    private final LwpProperties props;
    private final MeterRegistry meterRegistry;
    private final Sinks.Many<LwpSignalEvent> sink = Sinks.many().multicast().onBackpressureBuffer();

    private static class State { Instant firstSeen; Instant lastSeen; }
    private final Map<String, State> state = new ConcurrentHashMap<>();

    @PostConstruct
    void init() {
        wallService.stream().subscribe(this::handle);
    }

    private void handle(LiquidityWallService.LwpWallEvent ev) {
        if (!props.isEnabled()) return;
        String key = ev.symbol() + ":" + ev.side();
        State st = state.computeIfAbsent(key, k -> new State());
        Instant ts = ev.ts();
        if (st.firstSeen == null) {
            st.firstSeen = ts;
        }
        st.lastSeen = ts;
        long activeMs = Duration.between(st.firstSeen, ts).toMillis();
        if (activeMs >= props.getPersistActiveMs()) {
            String side = ev.side().equals("BID") ? "LWP_LONG" : "LWP_SHORT";
            double confidence = (ev.persistenceCount() / 5.0) * (ev.qty() / ev.medianQty());
            confidence = Math.max(0, Math.min(1, confidence));
            LwpSignalEvent out = new LwpSignalEvent(ts, ev.symbol(), side, ev.level(), ev.price(), ev.persistenceCount(), confidence);
            sink.tryEmitNext(out);
            log.info("lwp.signal.emit side={} symbol={} level={} price={} count={} confidence={}",
                    side, ev.symbol(), ev.level(), ev.price(), ev.persistenceCount(), confidence);
            meterRegistry.counter("trading.lwp.signals_total", "side", side).increment();
            st.firstSeen = null;
            st.lastSeen = null;
        }
    }

    public Flux<LwpSignalEvent> stream() {
        return sink.asFlux();
    }
}
