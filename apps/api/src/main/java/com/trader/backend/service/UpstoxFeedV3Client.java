package com.trader.backend.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.trader.backend.service.Tick;
import com.upstox.marketdatafeederv3udapi.rpc.proto.MarketDataFeed;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Service
@Slf4j
@RequiredArgsConstructor
public class UpstoxFeedV3Client {

    private final UpstoxAuthService auth;

    @Value("${FEED_V3_MODE:ltpc}")
    private String mode;

    @Value("${TICK_SYMBOLS:}")
    private String symbolsCsv;

    private final ObjectMapper om = new ObjectMapper();
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicReference<String> lastError = new AtomicReference<>(null);
    private final Map<String, Tick> latest = new ConcurrentHashMap<>();
    private List<String> symbols = new ArrayList<>();

    @PostConstruct
    void start() {
        if (symbolsCsv != null && !symbolsCsv.isBlank()) {
            for (String s : symbolsCsv.split(",")) {
                String t = s.trim();
                if (!t.isEmpty()) {
                    symbols.add(t);
                }
            }
        }
        if (!symbols.isEmpty()) {
            connectLoop();
        } else {
            log.warn("No TICK_SYMBOLS configured; Upstox feed will not start");
        }
    }

    public boolean isConnected() { return connected.get(); }
    public String mode() { return mode; }
    public List<String> symbols() { return Collections.unmodifiableList(symbols); }
    public String lastError() { return lastError.get(); }
    public Map<String, Tick> latestTicks() { return new LinkedHashMap<>(latest); }

    private void connectLoop() {
        authorizeAndConnect()
                .doOnError(e -> {
                    lastError.set(e.getMessage());
                    log.warn("WS error: {}", e.toString());
                })
                .doFinally(sig -> Mono.delay(Duration.ofSeconds(1)).subscribe(v -> connectLoop()))
                .subscribe();
    }

    private Mono<Void> authorizeAndConnect() {
        String token = auth.currentToken();
        if (token == null || token.isBlank()) {
            return Mono.error(new IllegalStateException("Upstox token not available"));
        }
        WebClient client = WebClient.builder()
                .baseUrl("https://api-v2.upstox.com")
                .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .defaultHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                .build();
        return client.get()
                .uri("/feed/market-data-feed/authorize-v3")
                .retrieve()
                .bodyToMono(JsonNode.class)
                .map(resp -> {
                    JsonNode n = resp.path("data").path("authorized_redirect_uri");
                    if (n.isMissingNode()) {
                        n = resp.path("authorized_redirect_uri");
                    }
                    return n.asText();
                })
                .flatMap(this::openWebSocket);
    }

    private Mono<Void> openWebSocket(String wsUrl) {
        byte[] frame = buildSubFrame();
        ReactorNettyWebSocketClient client = new ReactorNettyWebSocketClient();

        return client.execute(URI.create(wsUrl), session -> {
            // sender: send subscribe frame once
            Mono<Void> sender = session.send(
                    Mono.just(session.binaryMessage(factory -> factory.wrap(frame)))
            );

            // receiver: consume messages, collapse Flux to Mono<Void>
            Mono<Void> receiver = session.receive()
                    .doOnSubscribe(s -> {
                        connected.set(true);
                        lastError.set(null);
                        log.info("[ws] v3 authorized and connected");
                    })
                    .map(WebSocketMessage::getPayload)
                    .doOnNext(this::handlePayload)
                    .then() // important: convert Flux<?> to Mono<Void>
                    .doFinally(sig -> session.closeStatus()
                            .doOnNext(cs -> {
                                connected.set(false);
                                log.info("[ws] closed code={} reason={}", cs.getCode(), cs.getReason());
                            })
                            .subscribe()
                    );

            // return Mono<Void>
            return Mono.when(sender, receiver);
        });
    }

    private byte[] buildSubFrame() {
        ObjectNode frame = om.createObjectNode();
        frame.put("guid", UUID.randomUUID().toString());
        frame.put("method", "sub");
        ObjectNode data = frame.putObject("data");
        data.put("mode", mode);
        ArrayNode arr = data.putArray("instrumentKeys");
        for (String s : symbols) {
            arr.add(s);
        }
        return frame.toString().getBytes(StandardCharsets.UTF_8);
    }

    private void handlePayload(DataBuffer buf) {
        try {
            byte[] b = new byte[buf.readableByteCount()];
            buf.read(b);
            MarketDataFeed.FeedResponse resp = MarketDataFeed.FeedResponse.parseFrom(b);
            if (resp.getType() == MarketDataFeed.Type.market_info) {
                log.info("market_info: {}", resp.getMarketInfo().getSegmentStatusMap());
                return;
            }
            resp.getFeedsMap().forEach((key, feed) -> {
                double price = 0;
                long ts = 0;
                if (feed.hasLtpc()) {
                    price = feed.getLtpc().getLtp();
                    ts = feed.getLtpc().getLtt();
                } else if (feed.hasFullFeed()) {
                    MarketDataFeed.FullFeed ff = feed.getFullFeed();
                    if (ff.hasMarketFF()) {
                        MarketDataFeed.MarketFullFeed mf = ff.getMarketFF();
                        price = mf.getLtpc().getLtp();
                        ts = mf.getLtpc().getLtt();
                    } else if (ff.hasIndexFF()) {
                        MarketDataFeed.IndexFullFeed idx = ff.getIndexFF();
                        price = idx.getLtpc().getLtp();
                        ts = idx.getLtpc().getLtt();
                    }
                } else if (feed.hasFirstLevelWithGreeks()) {
                    price = feed.getFirstLevelWithGreeks().getLtpc().getLtp();
                    ts = feed.getFirstLevelWithGreeks().getLtpc().getLtt();
                }
                Instant tsInstant = Instant.ofEpochMilli(ts);
                latest.put(key, new Tick(key, price, tsInstant));
                log.info("[tick] {} ltp={} ts={}", key, price, tsInstant);
            });
        } catch (Exception e) {
            log.error("Failed to parse feed", e);
        } finally {
            DataBufferUtils.release(buf);
        }
    }
}
