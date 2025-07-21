package com.trader.backend.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.upstox.ApiClient;
import com.upstox.auth.OAuth;
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

    /* ------------------------------------------------------------
       Helpers & injected beans
     ------------------------------------------------------------ */
    private final WebClient webClient = WebClient.create("https://api.upstox.com/v2");
    private final ObjectMapper mapper = new ObjectMapper();
    private final ApiClient apiClient;          // injected via constructor

    public UpstoxAuthService(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    /* ------------------------------------------------------------
       Config values from application.properties
     ------------------------------------------------------------ */
    @Value("${upstox.apiKey}")
    private String apiKey;

    @Value("${upstox.apiSecret}")
    private String apiSecret;

    @Value("${upstox.webhookUri}")
    private String webhookUri;

    /* ------------------------------------------------------------
       In-memory token store (we’ll move to Redis later)
     ------------------------------------------------------------ */
    private final AtomicReference<String> accessToken = new AtomicReference<>();
    private final AtomicReference<Long> expiryEpoch = new AtomicReference<>(0L);
    /**
     * store the “extended_token” returned on initial exchange
     **/
    private final AtomicReference<String> refreshTokenRef = new AtomicReference<>();
    private final AtomicReference<String> currentToken = new AtomicReference<>();


    /* ============================================================
       1️⃣  Trigger push-notification in Upstox mobile app
     ============================================================ */
    public Mono<Map<String, Object>> initiateAccessTokenRequest() {

        return webClient.post()
                .uri("/login/authorization/request_token")        // ← new path
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
                .timeout(Duration.ofSeconds(5))
                /* .doOnNext(r -> log.info("Upstox raw response: {}", r))
                 .doOnError(err -> log.error("Upstox call failed", err))*/
                .doOnNext(tok -> {
                    String token = (String) tok.get("access_token");
                    String extToken = (String) tok.get("extended_token"); // <—
                    Integer expiry = (Integer) tok.get("expires_in");
                    accessToken.set(token);
                    expiryEpoch.set(System.currentTimeMillis() / 1000 + expiry);
                    apiClient.addDefaultHeader("Authorization", "Bearer " + token);

                    if (extToken != null) {
                        refreshTokenRef.set(extToken);                    // <— store it
                    }
                    log.info("✅ access_token saved ({} sec left)", expiry);
                });
    }


    /* ============================================================
       2️⃣  Webhook endpoint → exchange code for token
     ============================================================ */
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

        return webClient      // already pointing to https://api.upstox.com/v2
                .post()
                .uri("/login/authorization/token")                  // ⬅️ correct path
                .header(HttpHeaders.CONTENT_TYPE,
                        MediaType.APPLICATION_FORM_URLENCODED_VALUE)
                .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                .body(BodyInserters
                        .fromFormData("code", code)
                        .with("client_id", apiKey)
                        .with("client_secret", apiSecret)
                        .with("redirect_uri", webhookUri)  // EXACT match
                        .with("grant_type", "authorization_code")) // ⬅️ not “true”
                .retrieve()
                .bodyToMono(Map.class)
                .doOnNext(tok -> {

                    String token = (String) tok.get("access_token");
                    String extToken = (String) tok.get("extended_token"); // optional
                    Integer expiry = (Integer) tok.get("expires_in");    // seconds until 03:30
                    Integer ttlSec = (Integer) tok.get("expires_in");
                    if (ttlSec == null) {
                        var now = ZonedDateTime.now(ZoneId.of("Asia/Kolkata"));
                        var next = now.withHour(3).withMinute(30).withSecond(0);
                        if (!next.isAfter(now)) next = next.plusDays(1);
                        ttlSec = (int) Duration.between(now, next).getSeconds();
                    }
                    log.debug("🔑 raw token map: {}", tok);
                    log.info("✔️  stored accessToken={}, expiresIn={}");
                    accessToken.set(token);
                    expiryEpoch.set(System.currentTimeMillis() / 1000 + ttlSec);

                    apiClient.addDefaultHeader("Authorization", "Bearer " + token);

                    log.info("✅ access_token saved ({} min left, expires {})",
                            ttlSec / 60,
                            Instant.ofEpochSecond(expiryEpoch.get())
                                    .atZone(ZoneId.of("Asia/Kolkata")));
                })
                .then();
    }


    /* ============================================================
       3️⃣  Status endpoint for front-end polling
     ============================================================ */
    public Mono<Map<String, Object>> status() {
        long now = System.currentTimeMillis() / 1000;
        long expiry = expiryEpoch.get();
        boolean ok = accessToken.get() != null && now < expiry;

        return Mono.just(Map.of(
                "ready", ok,
                "expiresInSec", ok ? (expiry - now) : 0
        ));
    }

    /* Utility for other beans later */
    public String currentToken() {
        return accessToken.get();
    }

    public String buildAuthUrl() {
        return UriComponentsBuilder
                .fromUriString("https://api.upstox.com/v2/login/authorization/dialog")
                .queryParam("response_type", "code")
                .queryParam("client_id", apiKey)
                .queryParam("redirect_uri", webhookUri)   // e.g. https://my-app.up.railway.app/auth/webhook
                .queryParam("state", "botInit")           // optional
                .queryParam("scope", "profile marketdata")
                .build().toUriString();
    }

    public Mono<Void> exchangeCode(String code) {

        return webClient.post()
                .uri("/login/authorization/token")
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_VALUE)
                .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                .body(BodyInserters.fromFormData("code", code)
                        .with("client_id", apiKey)
                        .with("client_secret", apiSecret)
                        .with("redirect_uri", webhookUri)          // root “/”
                        .with("grant_type", "authorization_code")) // ← fix
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {
                })
                .doOnNext(tok -> {

                    String token = (String) tok.get("access_token");
                    if (token == null) {
                        throw new IllegalStateException("Upstox response had no access_token: " + tok);
                    }

                    // 1️⃣  compute TTL
                    Integer ttlSec = (Integer) tok.get("expires_in");   // may be null
                    if (ttlSec == null) {
                        // seconds until next 03:30 IST
                        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("Asia/Kolkata"));
                        ZonedDateTime next = now.withHour(3).withMinute(30).withSecond(0);
                        if (!next.isAfter(now)) next = next.plusDays(1);
                        ttlSec = (int) Duration.between(now, next).getSeconds();
                    }

                    // 2️⃣  store & inject
                    accessToken.set(token);
                    expiryEpoch.set(System.currentTimeMillis() / 1000 + ttlSec);
                    apiClient.addDefaultHeader("Authorization", "Bearer " + token);

                    log.info("✅ access_token saved ({} min left, expires {})",
                            ttlSec / 60, Instant.ofEpochSecond(expiryEpoch.get())
                                    .atZone(ZoneId.of("Asia/Kolkata")));
                })
                .then();
    }

    /**
     * ------------------------------------------------------------------
     * SIMPLE helper – fetch a spot quote for one instrument
     * Example:  fetchQuote("NSE_EQ", "HDFCBANK")
     * ------------------------------------------------------------------
     */
    public Mono<Map<String, Object>> fetchQuote(String exchange, String symbol) {

        String token = accessToken.get();
        if (token == null) {
            return Mono.error(new IllegalStateException("Access-token not ready. Hit /auth/exchange first."));
        }

        // API:  GET /market-quote/quotes?symbols=NSE_EQ:HDFCBANK
        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/market-quote/quotes")
                        .queryParam("symbols", exchange + ":" + symbol)
                        .build())
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {
                })
                .map(resp -> {
                /* Upstox wraps the actual quote inside:
                   { "data": { "NSE_EQ:HDFCBANK": { ...quote... } } }   */
                    Map<String, Object> data = (Map<String, Object>) resp.get("data");
                    return (Map<String, Object>) data.get(exchange + ":" + symbol);
                });
    }

    /**
     * before any V3 call, make sure currentToken() isn’t expired
     **/
    public Mono<Void> ensureValidToken() {
        long now = System.currentTimeMillis() / 1000;
        if (accessToken.get() == null || now >= expiryEpoch.get()) {
            return refreshToken();
        }
        return Mono.empty();
    }

    /**
     * 4️⃣ Refresh via your stored refresh-token
     **/
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
                .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {
                })
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

    /**
     * Step 1: Trigger push-notification flow
     **/
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

    public void setCurrentToken(String token) {
        currentToken.set(token);
        // also add to your ApiClient or WebClient default header:
        apiClient.addDefaultHeader("Authorization", "Bearer " + token);
    }

    public String getCurrentToken() {
        return currentToken.get();
    }
}
