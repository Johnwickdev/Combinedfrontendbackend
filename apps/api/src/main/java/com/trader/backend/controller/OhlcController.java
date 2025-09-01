package com.trader.backend.controller;

import com.trader.backend.service.OhlcService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/ohlc")
@RequiredArgsConstructor
public class OhlcController {
    private final OhlcService ohlcService;

    @GetMapping("/cepe")
    public List<OhlcService.Ohlc> cepe(@RequestParam String side,
                                       @RequestParam(defaultValue="1s") String gran,
                                       @RequestParam(defaultValue="900") int limit) {
        return ohlcService.fetch(side, gran, limit);
    }
}
