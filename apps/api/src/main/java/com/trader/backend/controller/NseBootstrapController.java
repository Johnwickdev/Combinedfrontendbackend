package com.trader.backend.controller;

import com.trader.backend.service.NSEBootstrapService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequiredArgsConstructor
public class NseBootstrapController {
    private final NSEBootstrapService nseBootstrapService;

    @GetMapping("/nse/bootstrap")
    public Map<String, Object> refresh() {
        int count = nseBootstrapService.refresh();
        return Map.of("loaded", count, "refreshedAt", java.time.Instant.now().toString());
        }
}
