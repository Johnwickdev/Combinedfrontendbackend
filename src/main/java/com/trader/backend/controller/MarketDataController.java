package com.trader.backend.controller;

import com.trader.backend.service.MarketDataService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/data")
@RequiredArgsConstructor
public class MarketDataController {

    private final MarketDataService md;

    /* …ltp() & daily() keep working… */

    // GET /data/candle/NSE_EQ_AXISBANK/minutes/5?to=2025-05-12&from=2025-05-01
    @GetMapping("/candle/{key}/{unit}/{interval}")
    public Mono<String> candle(@PathVariable String key,
                               @PathVariable String unit,
                               @PathVariable int    interval,
                               @RequestParam String to,
                               @RequestParam(required = false) String from) {
        return md.candleV3(key, unit, interval, to, from);
    }
}
