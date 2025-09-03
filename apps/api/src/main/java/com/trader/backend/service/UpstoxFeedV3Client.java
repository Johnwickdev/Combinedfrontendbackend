package com.trader.backend.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.trader.backend.events.TickEvent;
import com.upstox.marketdatafeederv3udapi.rpc.proto.MarketDataFeed;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.ExchangeFilterFunction;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Service
@Slf4j
@RequiredArgsConstructor
public class UpstoxFeedV3Client {
    private final UpstoxAuthService auth;
    private final ApplicationEventPublisher publisher;
    private final MarketStatusService marketStatusService;
    @Value("${FEED_V3_MODE:ltpc}")
    private String mode;

    @Value("${TICK_SYMBOLS:}")
    private String symbolsCsv;

    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicReference<String> lastError = new AtomicReference<>(null);
    private final Map<String, Tick> latest = new ConcurrentHashMap<>();
    private final Set<String> symbols = ConcurrentHashMap.newKeySet();
    private final AtomicReference<WebSocketSession> sessionRef = new AtomicReference<>();
    private static final ObjectMapper om = new ObjectMapper();

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
            log.info("[sub] added key={} mode={} (startup)", symbols, mode);
            connectLoop();
        } else {
            log.warn("No TICK_SYMBOLS configured; Upstox feed will not start");
        }
    }

    public boolean isConnected() { return connected.get(); }
    public String mode() { return mode; }
    public Set<String> symbols() { return Collections.unmodifiableSet(symbols); }
    public String lastError() { return lastError.get(); }
    public Map<String, Tick> latestTicks() { return new ConcurrentHashMap<>(latest); }

    public void subscribe(String key) {
        if (symbols.contains(key)) {
            log.info("[sub] already-subscribed key={}", key);
            return;
        }
        symbols.add(key);
        WebSocketSession session = sessionRef.get();
        if (session != null) {
            ObjectNode frame = om.createObjectNode();
            frame.put("guid", UUID.randomUUID().toString());
            frame.put("method", "sub");
            ObjectNode data = frame.putObject("data");
            data.put("mode", mode);
            ArrayNode arr = data.putArray("instrumentKeys");
            arr.add(key);
            byte[] b = frame.toString().getBytes(StandardCharsets.UTF_8);
            session.send(Mono.just(session.binaryMessage(f -> f.wrap(b)))).subscribe();
            log.info("[sub] added key={} mode={}", key, mode);
        }
    }

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
                .filter(logRequest())
                .filter(logResponse())
                .build();
        return client.get()
                .uri("/feed/market-data-feed/authorize-v3")
                .retrieve()
                .onStatus(HttpStatusCode::isError, resp ->
                        resp.bodyToMono(String.class).defaultIfEmpty("")
                                .flatMap(body -> {
                                    String b = (body == null) ? "" : (body.length() > 500 ? body.substring(0,500) + "..." : body);
                                    log.error("[upstox] HTTP {} body={}", resp.statusCode().value(), b);
                                    return resp.createException();
                                }))
                .bodyToMono(JsonNode.class)
                .map(resp -> {
                    JsonNode n = resp.path("data").path("authorized_redirect_uri");
                    if (n.isMissingNode()) {
                        n = resp.path("authorized_redirect_uri");
                    }
                    return n.asText();
                })
                .doOnNext(v -> log.info("[ws] authorize-v3 ok; redirect_uri=present"))
                .flatMap(this::openWebSocket);
    }

    private Mono<Void> openWebSocket(String wsUrl) {
        byte[] frame = buildSubFrame();
        ReactorNettyWebSocketClient client = new ReactorNettyWebSocketClient();

        return client.execute(URI.create(wsUrl), session -> {
            sessionRef.set(session);
            Mono<Void> sender = session.send(
                    Mono.just(session.binaryMessage(factory -> factory.wrap(frame)))
            );

            Mono<Void> receiver = session.receive()
                    .doOnSubscribe(s -> {
                        connected.set(true);
                        lastError.set(null);
                        marketStatusService.setWsConnected(true);
                        log.info("[ws] v3 authorized and connected");
                    })
                    .map(WebSocketMessage::getPayload)
                    .doOnNext(this::handlePayload)
                    .then()
                    .doFinally(sig -> session.closeStatus()
                            .doOnNext(cs -> {
                                connected.set(false);
                                marketStatusService.setWsConnected(false);
                                log.info("[ws] closed code={} reason={}", cs.getCode(), cs.getReason());
                            })
                            .subscribe()
                    );

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
        for (String s : symbols) arr.add(s);
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
                Tick t = new Tick(key, price, tsInstant);
                latest.put(key, t);
                publisher.publishEvent(new TickEvent(key, price, tsInstant));
                log.info("[tick] {} ltp={} ts={}", key, price, tsInstant);
            });
        } catch (Exception e) {
            log.error("Failed to parse feed", e);
        } finally {
            DataBufferUtils.release(buf);
        }
    }

    private static ExchangeFilterFunction logRequest() {
        return ExchangeFilterFunction.ofRequestProcessor(req -> {
            log.info("[http->] {} {}", req.method(), req.url());
            return Mono.just(req);
        });
    }

    private static ExchangeFilterFunction logResponse() {
        return ExchangeFilterFunction.ofResponseProcessor(resp -> {
            log.info("[http<-] status={}", resp.statusCode().value());
            return Mono.just(resp);
        });
    }
}
