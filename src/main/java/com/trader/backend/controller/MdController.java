package com.trader.backend.controller;

import com.trader.backend.service.CandleService;
import com.trader.backend.service.CandleService.CandleResponse;
import com.trader.backend.service.LiveFeedService;
import com.trader.backend.service.NseInstrumentService;
import com.trader.backend.events.LtpEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/md")
@RequiredArgsConstructor
public class MdController {
    private final CandleService candleService;
    private final LiveFeedService liveFeedService;
    private final NseInstrumentService nseInstrumentService;

    @GetMapping("/candles")
    public Mono<List<CandleResponse>> candles(@RequestParam("instrumentKey") List<String> instrumentKeys,
                                             @RequestParam(value = "tf", defaultValue = "1m") String tf,
                                             @RequestParam(value = "lookback", defaultValue = "120") int lookback) {
        if (instrumentKeys == null || instrumentKeys.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "instrumentKey required");
        }
        if (!Set.of("1m","3m","5m","15m").contains(tf)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "invalid tf");
        }
        int lb = Math.min(Math.max(lookback,1), 720);
        return candleService.fetchCandles(instrumentKeys, tf, lb);
    }

    @GetMapping("/selection")
    public ResponseEntity<Map<String, Object>> selection() {
        var sel = nseInstrumentService.currentSelectionData();
        String main = nseInstrumentService.nearestNiftyFutureKey().orElse("");
        List<String> opts = sel.keys().stream()
                .filter(k -> !k.equals(main))
                .toList();
        return ResponseEntity.ok(Map.of("mainInstrument", main, "options", opts));
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<Map<String, Object>>> stream(@RequestParam(value = "instrumentKey", required = false) List<String> keys,
                                                             ServerHttpResponse response) {
        response.getHeaders().set(HttpHeaders.CACHE_CONTROL, "no-cache");
        Flux<LtpEvent> flux = liveFeedService.ltpEvents();
        if (keys != null && !keys.isEmpty()) {
            Set<String> set = new HashSet<>(keys);
            flux = flux.filter(ev -> set.contains(ev.instrumentKey()));
        }
        Flux<ServerSentEvent<Map<String, Object>>> ticks = flux
                .map(ev -> {
                    Map<String, Object> data = Map.of(
                            "instrumentKey", ev.instrumentKey(),
                            "ts", ev.timestamp().toString(),
                            "ltp", ev.ltp()
                    );
                    return ServerSentEvent.<Map<String, Object>>builder(data).event("tick").build();
                });

        Flux<ServerSentEvent<Map<String, Object>>> heartbeat = Flux.interval(Duration.ofSeconds(15))
                .map(i -> ServerSentEvent.<Map<String, Object>>builder().comment("hb").build());

        return Flux.merge(ticks, heartbeat);
    }
}
