package com.trader.backend.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

@Service
@RequiredArgsConstructor
@Slf4j
public class HistoricalCandleScheduler {

    private final MarketDataService marketDataService;

    @Value("${HC_SCHED_KEYS:}")
    private String keysCsv;

    private final Map<String, Instant> nextAllowed = new ConcurrentHashMap<>();
    private final Map<String, Instant> pauseUntil = new ConcurrentHashMap<>();

    @PostConstruct
    void init() {
        if (keysCsv == null || keysCsv.isBlank()) {
            log.info("[hist] HC_SCHED_KEYS not set; scheduler disabled");
            return;
        }
        for (String s : keysCsv.split(",")) {
            String key = s.trim();
            if (!key.isEmpty()) {
                schedule(key);
            }
        }
    }

    private void schedule(String key) {
        Flux.interval(Duration.ZERO, Duration.ofSeconds(5))
                .flatMap(t -> poll(key))
                .subscribe();
    }

    private Mono<Void> poll(String key) {
        Instant now = Instant.now();
        Instant pause = pauseUntil.get(key);
        if (pause != null && now.isBefore(pause)) {
            return Mono.empty();
        }
        Instant next = nextAllowed.getOrDefault(key, Instant.EPOCH);
        if (now.isBefore(next)) {
            return Mono.empty();
        }
        nextAllowed.put(key, now.plusSeconds(60));
        log.info("[hist] {} next-at {}", key, nextAllowed.get(key));
        String today = LocalDate.now().toString();
        return marketDataService.candleV3(key, "minute", 1, today, null)
                .retryWhen(Retry.backoff(5, Duration.ofSeconds(5))
                        .maxBackoff(Duration.ofMinutes(1))
                        .jitter(0.3)
                        .filter(HistoricalCandleScheduler::isTransient))
                .doOnError(ex -> {
                    if (ex instanceof WebClientResponseException w && w.getStatusCode().is4xxClientError()) {
                        Instant resume = Instant.now().plus(Duration.ofMinutes(10 + ThreadLocalRandom.current().nextInt(6)));
                        pauseUntil.put(key, resume);
                        log.warn("[hist] {} {} -> pause until {}", key, w.getStatusCode().value(), resume);
                    }
                })
                .onErrorResume(ex -> Mono.empty())
                .then();
    }

    private static boolean isTransient(Throwable ex) {
        if (ex instanceof WebClientResponseException w) {
            int code = w.getStatusCode().value();
            return code == 429 || w.getStatusCode().is5xxServerError();
        }
        return ex instanceof TimeoutException || ex instanceof java.io.IOException;
    }
}
