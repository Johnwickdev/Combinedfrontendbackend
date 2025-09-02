package com.trader.backend.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trader.backend.config.LwpProperties;
import com.trader.backend.events.LwpSignalEvent;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class LwpSimReplayTest {

    @Test
    void replayDatasetEmitsBothSides() throws Exception {
        LwpProperties p = new LwpProperties();
        LiquidityWallService ws = new LiquidityWallService(p);
        LwpSignalEngine eng = new LwpSignalEngine(ws, p, new SimpleMeterRegistry());
        List<LwpSignalEvent> events = new ArrayList<>();
        eng.stream().doOnNext(events::add).subscribe();

        ObjectMapper om = new ObjectMapper();
        try (InputStream is = getClass().getResourceAsStream("/ticks/lwp_synthetic.json")) {
            List<SimRunner.SimTick> ticks = om.readValue(is, new TypeReference<>(){});
            for (SimRunner.SimTick t : ticks) {
                List<DepthMetricsService.Level> bids = t.bids.stream().map(l -> new DepthMetricsService.Level(l.p, l.q)).toList();
                List<DepthMetricsService.Level> asks = t.asks.stream().map(l -> new DepthMetricsService.Level(l.p, l.q)).toList();
                DepthMetricsService.DepthTick dt = new DepthMetricsService.DepthTick(t.symbol, bids, asks, t.ltp, Instant.ofEpochMilli(t.ts));
                ws.onDepthTick(dt);
            }
        }
        assertEquals(2, events.size());
        assertEquals("LWP_SHORT", events.get(0).side());
        assertEquals("LWP_LONG", events.get(1).side());
    }
}
