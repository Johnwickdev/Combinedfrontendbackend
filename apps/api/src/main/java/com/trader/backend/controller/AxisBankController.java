package com.trader.backend.controller;

import com.trader.backend.service.AxisBankHistoryService;
import com.trader.backend.service.AxisBankHistoryService.Candle;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Endpoints dedicated to Axis Bank historical data. These endpoints are
 * additive and do not interfere with the existing trading workflow.
 */
@RestController
@RequestMapping("/axisbank")
@RequiredArgsConstructor
public class AxisBankController {
    private final AxisBankHistoryService service;

    /** Trigger fetch from Upstox and store into Influx. */
    @PostMapping("/fetch")
    public Mono<List<Candle>> fetch() {
        return service.fetchAndStore();
    }

    /** Return stored candles. */
    @GetMapping(value = "/candles", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<Candle> candles() {
        return service.readCandles();
    }
}
