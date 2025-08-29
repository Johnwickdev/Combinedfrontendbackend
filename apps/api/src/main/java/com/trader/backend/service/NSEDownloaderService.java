package com.trader.backend.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.URL;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.zip.GZIPInputStream;

@Service
@Slf4j
public class NSEDownloaderService {

    public String downloadAndExtract() {
        try {
            String downloadUrl = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz";
            Path targetFolder = Paths.get("src/main/resources/data");
            Files.createDirectories(targetFolder);

            Path gzPath = targetFolder.resolve("NSE.json.gz");
            Path jsonPath = targetFolder.resolve("NSE.json");

            try (InputStream in = new URL(downloadUrl).openStream()) {
                Files.copy(in, gzPath, StandardCopyOption.REPLACE_EXISTING);
                log.info("NSE.json.gz downloaded.");
            }

            try (GZIPInputStream gis = new GZIPInputStream(new FileInputStream(gzPath.toFile()));
                 FileOutputStream out = new FileOutputStream(jsonPath.toFile())) {
                byte[] buffer = new byte[1024];
                int len;
                while ((len = gis.read(buffer)) != -1) {
                    out.write(buffer, 0, len);
                }
                log.info("NSE.json extracted.");
            }

            return "NSE.json downloaded and extracted at " + LocalDateTime.now();

        } catch (Exception e) {
            log.error("Failed to download/extract NSE file: {}", e.getMessage(), e);
            return "Failed to download/extract NSE file: " + e.getMessage();
        }
    }
}