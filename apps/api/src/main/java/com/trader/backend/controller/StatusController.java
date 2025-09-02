package com.trader.backend.controller;

import com.trader.backend.service.MarketStatusService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class StatusController {
    private final MarketStatusService marketStatusService;

    @GetMapping("/status/market")
    public MarketStatusService.Status market() {
        return marketStatusService.getStatus();
    }
}
