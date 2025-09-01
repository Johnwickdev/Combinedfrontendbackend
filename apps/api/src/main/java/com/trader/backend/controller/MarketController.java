package com.trader.backend.controller;

import com.trader.backend.service.MarketStatusService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/market")
@RequiredArgsConstructor
public class MarketController {
    private final MarketStatusService marketStatusService;

    @GetMapping("/status")
    public MarketStatusService.Status status() {
        return marketStatusService.getStatus();
    }
}
