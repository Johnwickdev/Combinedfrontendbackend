package com.trader.backend;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

public class UpstoxFeedTester {

    // ← FILL THESE IN
    private static final String UPSTOX_API_KEY       = "7fde129-556b-4a84-9083-9fea5eb53fb0";
    private static final String UPSTOX_API_SECRET    = "ofye1h1t8e";
    private static final String REDIRECT_URI         = "https://e186-2406-7400-ce-e9fa-65d2-c857-98a7-9171.ngrok-free.app/";
    private static final String ONE_TIME_CODE        = "-LELG-";

    public static void main(String[] args) throws InterruptedException {
        ObjectMapper mapper = new ObjectMapper();
        WebClient authClient = WebClient.builder()
                .baseUrl("https://api.upstox.com/v2")
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_VALUE)
                .build();

        // 1️⃣ Exchange code for access_token
        Map<String, Object> tok = authClient.post()
                .uri("/login/authorization/token")
                .body(BodyInserters
                        .fromFormData("code", ONE_TIME_CODE)
                        .with("client_id", UPSTOX_API_KEY)
                        .with("client_secret", UPSTOX_API_SECRET)
                        .with("redirect_uri", REDIRECT_URI)
                        .with("grant_type", "authorization_code"))
                .retrieve()
                .bodyToMono(Map.class)
                .block(Duration.ofSeconds(10));

        if (tok == null || tok.get("access_token") == null) {
            System.err.println("❌ Failed to exchange code: " + tok);
            System.exit(1);
        }
        String accessToken = tok.get("access_token").toString();
        System.out.println("✅ Access token: " + accessToken);

        // 2️⃣ Call authorize to get WS URL
        WebClient feedClient = WebClient.builder()
                .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken)
                .build();

        String wsUrl = feedClient.get()
                .uri("https://api.upstox.com/v3/feed/market-data-feed/authorize")
                .exchangeToMono(resp -> {
                    if (resp.statusCode().is3xxRedirection()) {
                        return Mono.just(resp.headers().asHttpHeaders().getLocation().toString());
                    }
                    return resp.bodyToMono(JsonNode.class)
                            .map(j -> "wss://api.upstox.com/v3/feed/market-data-feed?authorization="
                                    + j.at("/data/feed_token").asText());
                })
                .block(Duration.ofSeconds(10));

        System.out.println("📡 WS URL = " + wsUrl);

        // 3️⃣ Open WebSocket and print incoming messages for 20s
        ReactorNettyWebSocketClient wsClient = new ReactorNettyWebSocketClient();
        wsClient.execute(URI.create(wsUrl), session ->
                session.receive()
                        .map(WebSocketMessage::getPayloadAsText)
                        .doOnNext(msg -> System.out.println("⏳ tick → " + msg))
                        .then()
        ).subscribe();

        // keep JVM alive to see a few ticks
        Thread.sleep(20_000);
        System.exit(0);
    }
}
