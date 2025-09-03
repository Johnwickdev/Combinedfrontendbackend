package com.trader.backend.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.HttpStatusCode;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.net.URI;

// (exact same file you created earlier)
@Service
@RequiredArgsConstructor
@Slf4j
public class MarketDataService {

    private final UpstoxAuthService auth;

    /* …ltp() and dailyCandles() stay as-is … */

    /*public Mono<String> candleV3(String key, String unit, int interval,
                                 String to, @Nullable String from) {

        WebClient wc = buildClient("https://api.upstox.com/v3");

        if (from == null || from.isBlank()) {
            return wc.get()
                    .uri("/historical-candle/{k}/{u}/{i}/{to}",
                            key, unit, interval, to)
                    .accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .bodyToMono(String.class);
        }

        return wc.get()
                .uri("/historical-candle/{k}/{u}/{i}/{to}/{from}",
                        key, unit, interval, to, from)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(String.class);
    }
*/

    /* ---------- ONE-LINE FIX ---------- */
    private static String encodeKey(String key) {
        // only the instrument-key needs encoding; other parts are plain
        return key.replace("|", "%7C");
    }

    public Mono<String> candleV3(String key,
                                 String unit,
                                 int    interval,
                                 String to,
                                 @Nullable String from) {

        WebClient wc = buildClient("https://api.upstox.com/v3");

        /* encode the instrument-key */
        String safeKey = encodeKey(key);

        // build the raw URL manually (no extra encoding by WebClient)
        String url = from == null || from.isBlank()
                ? String.format(
                "https://api.upstox.com/v3/historical-candle/%s/%s/%d/%s",
                safeKey, unit, interval, to)
                : String.format(
                "https://api.upstox.com/v3/historical-candle/%s/%s/%d/%s/%s",
                safeKey, unit, interval, to, from);

        return wc.get()
                .uri(URI.create(url))     // <<< use URI to STOP extra encoding
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .onStatus(HttpStatusCode::isError, resp ->
                        resp.bodyToMono(String.class).defaultIfEmpty("")
                                .flatMap(body -> {
                                    String b = body.length() > 500 ? body.substring(0,500) + "..." : body;
                                    log.error("[upstox] HTTP {} body={}", resp.statusCode().value(), b);
                                    return resp.createException();
                                }))
                .bodyToMono(String.class);
    }

    /* ---------- helper ---------- */
    private WebClient buildClient(String baseUrl) {
        return WebClient.builder()
                .baseUrl(baseUrl)
                .defaultHeader(HttpHeaders.AUTHORIZATION,
                        "Bearer " + auth.currentToken())
                .build();
    }
}
