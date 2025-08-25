package com.trader.backend.controller;

import com.trader.backend.service.NSEDownloaderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/nse")
@Slf4j
public class NSEDownloaderController {

    @Autowired
    private NSEDownloaderService service;

    // Manual Trigger
    @PostMapping("/download")
    public String manualDownload() {
        return service.downloadAndExtract();
    }

    // Scheduled Trigger – Every day at 8 AM
    @Scheduled(cron = "0 0 8 * * ?")
    public void scheduledDownload() {
        String result = service.downloadAndExtract();
        log.info("Scheduled Download at 8 AM → {}", result);
    }
}