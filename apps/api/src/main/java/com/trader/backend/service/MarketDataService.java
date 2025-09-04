package com.trader.backend.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.HttpStatusCode;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Mono;

import static com.trader.backend.config.UpstoxApiEndpoints.API_V3_BASE_URL;

import java.net.URI;

@Service
@RequiredArgsConstructor
@Slf4j
public class MarketDataService {

    private final UpstoxAuthService auth;

    @Value("${HIST_INTERVAL_FORMAT:v3}")
    private String histFmt;

    public Mono<String> candleV3(String key,
                                 String unit,
                                 int interval,
                                 String to,
                                 @Nullable String from) {

        WebClient wc = buildClient(API_V3_BASE_URL);

        URI uri = buildHistUri(key, unit, interval, to, from);

        return wc.get()
                .uri(uri)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .onStatus(HttpStatusCode::isError, resp ->
                        resp.bodyToMono(String.class).defaultIfEmpty("")
                                .flatMap(body -> {
                                    String b = (body == null) ? "" : (body.length() > 500 ? body.substring(0,500) + "..." : body);
                                    log.error("[upstox] HTTP {} body={}", resp.statusCode().value(), b);
                                    return resp.createException();
                                }))
                .bodyToMono(String.class);
    }

    private URI buildHistUri(String instrumentKey,
                             String unit,
                             int interval,
                             String to,
                             @Nullable String from) {
        UriComponentsBuilder b = UriComponentsBuilder
                .fromPath("/historical-candle/{key}")
                .queryParam("to", to);
        if (from != null && !from.isBlank()) {
            b.queryParam("from", from);
        }
        if ("v3".equalsIgnoreCase(histFmt)) {
            b.queryParam("interval", interval + unit);
        } else {
            b.queryParam("unit", unit).queryParam("span", interval);
        }
        return b.buildAndExpand(instrumentKey).toUri();
    }

    private WebClient buildClient(String baseUrl) {
        return WebClient.builder()
                .baseUrl(baseUrl)
                .defaultHeader(HttpHeaders.AUTHORIZATION,
                        "Bearer " + auth.currentToken())
                .build();
    }
}

