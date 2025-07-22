package com.trader.backend.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.upstox.ApiClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Mono;
import org.springframework.context.annotation.Lazy;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Service
@Slf4j
public class UpstoxAuthService {

    private final WebClient webClient = WebClient.create("https://api.upstox.com/v2");
    private final ObjectMapper mapper = new ObjectMapper();
    private final ApiClient apiClient;
    private final LiveFeedService liveFeedService;
    private long tokenCreatedAt;
    private long expiresIn = 1800000; // 30 minutes in milliseconds

    public UpstoxAuthService(ApiClient apiClient, @Lazy LiveFeedService liveFeedService) {
        this.apiClient = apiClient;
        this.liveFeedService = liveFeedService;
    }

    @Value("${upstox.apiKey}")
    private String apiKey;

    @Value("${upstox.apiSecret}")
    private String apiSecret;

    @Value("${upstox.webhookUri}")
    private String webhookUri;

    private final AtomicReference<String> accessToken = new AtomicReference<>();
    private final AtomicReference<Long> expiryEpoch = new AtomicReference<>(0L);
    private final AtomicReference<String> refreshTokenRef = new AtomicReference<>();
    private final AtomicReference<String> currentToken = new AtomicReference<>();

    public Mono<Void> exchangeCode(String code) {
        return webClient.post()
                .uri("/login/authorization/token")
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_VALUE)
                .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                .body(BodyInserters.fromFormData("code", code)
                        .with("client_id", apiKey)
                        .with("client_secret", apiSecret)
                        .with("redirect_uri", webhookUri)
                        .with("grant_type", "authorization_code"))
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})
                .doOnNext(tok -> {
                    String token = (String) tok.get("access_token");
                    if (token == null) {
                        throw new IllegalStateException("Upstox response had no access_token: " + tok);
                    }

                    Integer ttlSec = (Integer) tok.get("expires_in");
                    if (ttlSec == null) {
                        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("Asia/Kolkata"));
                        ZonedDateTime next = now.withHour(3).withMinute(30).withSecond(0);
                        if (!next.isAfter(now)) next = next.plusDays(1);
                        ttlSec = (int) Duration.between(now, next).getSeconds();
                    }

                    accessToken.set(token);
                    expiryEpoch.set(System.currentTimeMillis() / 1000 + ttlSec);
                    apiClient.addDefaultHeader("Authorization", "Bearer " + token);

                    log.info("✅ access_token saved ({} min left, expires {})",
                            ttlSec / 60, Instant.ofEpochSecond(expiryEpoch.get())
                                    .atZone(ZoneId.of("Asia/Kolkata")));
                })
                .then(Mono.fromRunnable(this::initLiveWebSocket)); // ✅ Trigger WS after login
    }

    public void initLiveWebSocket() {
        log.info("⚡ initLiveWebSocket() called after successful login");
        try {
            // ✅ AXISBANK for now — will replace with dynamic list later
            liveFeedService.streamSingleInstrument("NSE_EQ|INE238A01034");
        } catch (Exception e) {
            log.error("🔥 Failed to start WebSocket stream", e);
        }
    }

    public String buildAuthUrl() {
        return UriComponentsBuilder
                .fromUriString("https://api.upstox.com/v2/login/authorization/dialog")
                .queryParam("response_type", "code")
                .queryParam("client_id", apiKey)
                .queryParam("redirect_uri", webhookUri)
                .queryParam("state", "botInit")
                .queryParam("scope", "profile marketdata")
                .build().toUriString();
    }

    public Mono<Map<String, Object>> status() {
        long now = System.currentTimeMillis() / 1000;
        long expiry = expiryEpoch.get();
        boolean ok = accessToken.get() != null && now < expiry;

        return Mono.just(Map.of(
                "ready", ok,
                "expiresInSec", ok ? (expiry - now) : 0
        ));
    }

    public String currentToken() {
        return accessToken.get();
    }

    public void setCurrentToken(String token) {
        currentToken.set(token);
        apiClient.addDefaultHeader("Authorization", "Bearer " + token);
    }

    public String getCurrentToken() {
        return currentToken.get();
    }

    public Mono<Map<String, Object>> fetchQuote(String exchange, String symbol) {
        String token = accessToken.get();
        if (token == null) {
            return Mono.error(new IllegalStateException("Access-token not ready. Hit /auth/exchange first."));
        }

        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/market-quote/quotes")
                        .queryParam("symbols", exchange + ":" + symbol)
                        .build())
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})
                .map(resp -> {
                    Map<String, Object> data = (Map<String, Object>) resp.get("data");
                    return (Map<String, Object>) data.get(exchange + ":" + symbol);
                });
    }

    public Mono<Void> ensureValidToken() {
        long now = System.currentTimeMillis() / 1000;
        if (accessToken.get() == null || now >= expiryEpoch.get()) {
            return refreshToken();
        }
        return Mono.empty();
    }

    public Mono<Void> refreshToken() {
        String rt = refreshTokenRef.get();
        if (rt == null) {
            return Mono.error(new IllegalStateException("No refresh token; must re-authenticate"));
        }
        return webClient.post()
                .uri("/login/authorization/token")
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_VALUE)
                .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                .body(BodyInserters.fromFormData("grant_type", "refresh_token")
                        .with("refresh_token", rt)
                        .with("client_id", apiKey)
                        .with("client_secret", apiSecret))
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})
                .doOnNext(tok -> {
                    String newAt = (String) tok.get("access_token");
                    Integer expires = (Integer) tok.get("expires_in");
                    String newRt = (String) tok.get("extended_token");

                    accessToken.set(newAt);
                    expiryEpoch.set(System.currentTimeMillis() / 1000 + expires);
                    if (newRt != null) refreshTokenRef.set(newRt);
                    apiClient.addDefaultHeader("Authorization", "Bearer " + newAt);

                    log.info("🔄 Refreshed access_token ({} sec left)", expires);
                })
                .then();
    }

    public Mono<TokenRequestResponse> requestAccessTokenPush() {
        return webClient.post()
                .uri("/login/auth/token/request/{clientId}", apiKey)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                .bodyValue(Map.of("client_secret", apiSecret))
                .retrieve()
                .bodyToMono(TokenRequestResponse.class);
    }

    public record TokenRequestResponse(
            String status,
            Data data
    ) {
        public record Data(String authorization_expiry, String notifier_url) {
        }
    }

    public Mono<Map<String, Object>> initiateAccessTokenRequest() {
        return webClient.post()
                .uri("/login/authorization/request_token")
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .bodyValue(Map.of(
                        "client_id", apiKey,
                        "client_secret", apiSecret,
                        "redirect_uri", webhookUri
                ))
                .exchangeToMono(res -> res.bodyToMono(String.class)
                        .map(body -> {
                            Map<String, Object> m = new HashMap<>();
                            m.put("code", res.statusCode().value());
                            m.put("rawBody", body);
                            return m;
                        }))
                .timeout(Duration.ofSeconds(5));
    }

    public Mono<Void> handleWebhook(String rawOrQueryJson) {
        String code;
        try {
            JsonNode node = mapper.readTree(rawOrQueryJson);
            code = node.path("code").asText(null);
            if (code == null || code.isBlank()) {
                log.warn("Webhook hit but no ?code= param found -> {}", rawOrQueryJson);
                return Mono.empty();
            }
        } catch (IOException e) {
            return Mono.error(e);
        }

        return exchangeCode(code);
    }
public Map<String, Object> getTokenStatus() {
    Map<String, Object> map = new HashMap<>();
    map.put("createdAt", this.tokenCreatedAt); // already present
    map.put("expiresIn", this.expiresIn); // already present
    return map;
}
}