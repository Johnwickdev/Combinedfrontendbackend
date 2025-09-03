package com.trader.backend.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trader.backend.entity.NseInstrument;
import jakarta.annotation.PostConstruct;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class NseDataInitializer {

    private final NSEDownloaderService downloader;
    private final MongoTemplate mongoTemplate;
    private final ObjectMapper mapper;

    @PostConstruct
    public void init() {
        try {
            downloader.downloadAndExtract();
            InputStream is = getClass().getClassLoader().getResourceAsStream("data/NSE.json");
            if (is == null) {
                log.error("NSE.json not found after download");
                return;
            }
            List<NseInstrument> all = Arrays.asList(mapper.readValue(is, NseInstrument[].class));

            List<NseInstrument> futures = all.stream()
                    .filter(i -> "NSE_INDEX|Nifty 50".equals(i.getUnderlyingKey()))
                    .filter(i -> i.getInstrumentType() != null && i.getInstrumentType().startsWith("FUT"))
                    .collect(Collectors.toList());
            mongoTemplate.dropCollection("nifty_futures");
            if (!futures.isEmpty()) {
                mongoTemplate.insert(futures, "nifty_futures");
            }

            List<NseInstrument> premium = all.stream()
                    .filter(i -> "NSE_INDEX|Nifty 50".equals(i.getUnderlyingKey()))
                    .filter(i -> "CE".equals(i.getInstrumentType()) || "PE".equals(i.getInstrumentType()))
                    .collect(Collectors.toList());
            mongoTemplate.dropCollection("premium");
            if (!premium.isEmpty()) {
                mongoTemplate.insert(premium, "premium");
            }

            log.info("NSE bootstrap: futures={} premium={}", futures.size(), premium.size());
        } catch (Exception e) {
            log.error("NSE bootstrap failed", e);
        }
    }
}
