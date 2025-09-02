package com.trader.backend.service;

import com.trader.backend.config.LwpProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Detects persistent liquidity walls within the top levels of the order book.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class LiquidityWallService {

    private final LwpProperties props;
    private final Sinks.Many<LwpWallEvent> sink = Sinks.many().multicast().onBackpressureBuffer();

    private static class QtySample {
        Instant ts; double qty;
        QtySample(Instant ts, double qty) { this.ts = ts; this.qty = qty; }
    }

    private static class State {
        final List<QtySample> samples = new ArrayList<>();
        boolean active = false;
        Instant lastGone = null;
        double lastPrice = 0;
        int persistenceCount = 0;
    }

    private final Map<String, State> states = new ConcurrentHashMap<>();

    public record LwpWallEvent(Instant ts, String symbol, String side, int level,
                               double price, int persistenceCount, double qty, double medianQty) { }

    public void onDepthTick(DepthMetricsService.DepthTick tick) {
        processSide(tick.symbol(), "BID", tick.bids(), tick.ts());
        processSide(tick.symbol(), "ASK", tick.asks(), tick.ts());
    }

    private void processSide(String symbol, String side, List<DepthMetricsService.Level> levels, Instant ts) {
        int max = Math.min(props.getMaxLevels(), levels.size());
        for (int i = 0; i < max; i++) {
            DepthMetricsService.Level lvl = levels.get(i);
            String key = symbol + ":" + side + ":" + i;
            State st = states.computeIfAbsent(key, k -> new State());
            updateSamples(st, ts, lvl.qty());
            double median = median(st.samples);
            double threshold = median * props.getWallThreshold();
            boolean candidate = median > 0 && lvl.qty() >= threshold;
            if (candidate) {
                if (!st.active) {
                    if (st.lastGone != null && Duration.between(st.lastGone, ts).toMillis() <= props.getReappearWindowMs()
                            && Math.abs(lvl.price() - st.lastPrice) <= 0.05) {
                        st.persistenceCount++;
                        log.info("lwp.wall.refresh symbol={} side={} level={} price={} count={}",
                                symbol, side, i, lvl.price(), st.persistenceCount);
                        if (st.persistenceCount >= props.getPersistenceCount()) {
                            log.info("lwp.wall.persistent symbol={} side={} level={} price={} count={} qty={} median={}",
                                    symbol, side, i, lvl.price(), st.persistenceCount, lvl.qty(), median);
                            sink.tryEmitNext(new LwpWallEvent(ts, symbol, side, i, lvl.price(),
                                    st.persistenceCount, lvl.qty(), median));
                        }
                    } else {
                        st.persistenceCount = 0;
                        st.lastPrice = lvl.price();
                        log.info("lwp.wall.candidate symbol={} side={} level={} price={} qty={} median={}",
                                symbol, side, i, lvl.price(), lvl.qty(), median);
                    }
                    st.active = true;
                } else {
                    // still active
                }
            } else {
                if (st.active) {
                    st.active = false;
                    st.lastGone = ts;
                }
            }
        }
    }

    private void updateSamples(State st, Instant ts, double qty) {
        st.samples.add(new QtySample(ts, qty));
        long cutoff = ts.minusMillis(5000).toEpochMilli();
        st.samples.removeIf(s -> s.ts.toEpochMilli() < cutoff);
    }

    private double median(List<QtySample> samples) {
        if (samples.isEmpty()) return 0;
        return samples.stream()
                .map(s -> s.qty)
                .sorted(Comparator.naturalOrder())
                .skip((samples.size() - 1) / 2)
                .limit(2 - samples.size() % 2)
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(0);
    }

    public Flux<LwpWallEvent> stream() {
        return sink.asFlux();
    }
}
