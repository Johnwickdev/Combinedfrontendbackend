package com.trader.backend.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trader.backend.entity.Instrument;
import com.trader.backend.repository.InstrumentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.ExchangeStrategies;

import javax.annotation.PostConstruct;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class NSEBootstrapService {
    private final InstrumentRepository instrumentRepository;
    private final ObjectMapper mapper = new ObjectMapper();

    @Value("${NSE_FNO_JSON_URL:}")
    private String nseUrl;

    @Value("${NSE_CACHE_TTL_MIN:1440}")
    private long cacheTtlMin;

    private volatile long lastLoaded = 0L;

    private final WebClient webClient = WebClient.builder()
            .exchangeStrategies(ExchangeStrategies.builder()
                    .codecs(cfg -> cfg.defaultCodecs().maxInMemorySize(16 * 1024 * 1024))
                    .build())
            .build();

    @PostConstruct
    public void init() {
        ensureLoaded();
    }

    public synchronized int ensureLoaded() {
        long now = System.currentTimeMillis();
        if (now - lastLoaded < cacheTtlMin * 60_000 && instrumentRepository.count() > 0) {
            return (int) instrumentRepository.count();
        }
        return refresh();
    }

    public synchronized int refresh() {
        try {
            String json;
            if (nseUrl == null || nseUrl.isBlank()) {
                try (InputStream is = getClass().getResourceAsStream("/nse_fno_data.json")) {
                    if (is == null) {
                        log.error("Failed to load nse_fno_data.json from classpath");
                        return 0;
                    }
                    json = new String(is.readAllBytes(), StandardCharsets.UTF_8);
                }
            } else {
                json = webClient.get().uri(nseUrl).retrieve().bodyToMono(String.class).block();
            }
            JsonNode arr = mapper.readTree(json);
            List<Instrument> list = new ArrayList<>();
            for (JsonNode n : arr) {
                Instrument inst = new Instrument();
                inst.setKey(n.path("instrument_key").asText());
                inst.setSymbol(n.path("name").asText(""));
                String type = n.path("instrument_type").asText();
                inst.setKind(type.contains("FUT") ? "FUT" : "OPT");
                long expMs = n.path("expiry").asLong();
                if (expMs > 0) {
                    inst.setExpiry(Instant.ofEpochMilli(expMs).atZone(ZoneId.of("Asia/Kolkata")).toLocalDate());
                }
                if ("OPT".equals(inst.getKind())) {
                    inst.setStrike(n.path("strike_price").asDouble());
                    inst.setOptType(type);
                }
                inst.setSegment(n.path("segment").asText());
                inst.setSource("NSE_JSON");
                inst.setUpdatedAt(Instant.now());
                list.add(inst);
            }
            instrumentRepository.deleteAll();
            instrumentRepository.saveAll(list);
            lastLoaded = System.currentTimeMillis();
            log.info("[nse] cache loaded count={}", list.size());
            return list.size();
        } catch (Exception e) {
            log.error("Failed to refresh NSE instruments", e);
            return 0;
        }
    }

    @Scheduled(cron = "0 0 9 * * *", zone = "${APP_TIMEZONE:Asia/Kolkata}")
    public void scheduledRefresh() {
        refresh();
    }
}
