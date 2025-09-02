package com.trader.backend.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class StartupLifecycle implements ApplicationRunner {
    private final NSEBootstrapService nseBootstrapService;

    @Override
    public void run(ApplicationArguments args) {
        log.info("[boot] app starting");
        nseBootstrapService.ensureLoaded();
    }
}
