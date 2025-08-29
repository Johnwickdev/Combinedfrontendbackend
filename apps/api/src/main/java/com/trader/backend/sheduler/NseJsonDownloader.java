package com.trader.backend.sheduler;

// File: com/trader/backend/scheduler/NseJsonDownloader.java


import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

@Slf4j
@Component
public class NseJsonDownloader {

    private static final String DOWNLOAD_URL = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz";
    private static final String DESTINATION_PATH = "src/main/resources/data/NSE.json.gz";

    @Scheduled(cron = "0 0 18 * * *") // Every day at 6 PM
    public void downloadNseJson() {
        log.info("📥 [START] Downloading NSE.json.gz from Upstox...");

        try {
            URL url = new URL(DOWNLOAD_URL);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            try (InputStream in = connection.getInputStream();
                 FileOutputStream out = new FileOutputStream(DESTINATION_PATH)) {

                byte[] buffer = new byte[1024];
                int bytesRead;
                while ((bytesRead = in.read(buffer)) != -1) {
                    out.write(buffer, 0, bytesRead);
                }
            }

            if (Files.exists(Paths.get(DESTINATION_PATH))) {
                log.info("✅ NSE.json.gz downloaded and saved at: {}", DESTINATION_PATH);
            } else {
                log.warn("⚠️ File not found after download. Something went wrong.");
            }

        } catch (Exception e) {
            log.error("❌ Failed to download NSE.json.gz", e);
        }
    }
}
