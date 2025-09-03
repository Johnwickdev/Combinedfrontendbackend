package com.trader.backend.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.util.JsonFormat;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.trader.backend.entity.NseInstrument;
import com.trader.backend.state.MarketState;
import com.upstox.marketdatafeederv3udapi.rpc.proto.MarketDataFeed;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;
import reactor.netty.http.client.HttpClient;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLParameters;

import javax.annotation.PostConstruct;
import java.net.URI;
import java.time.Duration;

import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.beans.factory.annotation.Value;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.ThreadLocalRandom;
import java.util.LinkedHashMap;
import java.util.Map;
import com.trader.backend.events.LtpEvent;

import java.security.KeyStore;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;


@Service
@Slf4j
@RequiredArgsConstructor
public class LiveFeedService {

    @Autowired(required = false)
    private WriteApiBlocking writeApi;

    private final UpstoxAuthService auth;
    private final NseInstrumentService nseInstrumentService;
    private final ObjectMapper om = new ObjectMapper();
    private final MongoTemplate mongoTemplate;
    private final QuantAnalysisService quantAnalysisService;
    private final MarketState marketState;
    private final DepthMetricsService depthMetricsService;
    @Value("${APP_MOCK:false}")
    private boolean mockMode;
private final Sinks.Many<JsonNode> sink = Sinks.many().multicast().onBackpressureBuffer();
private final AtomicBoolean optionsStreamStarted = new AtomicBoolean(false);

    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicBoolean everConnected = new AtomicBoolean(false);
    private final AtomicBoolean futSubscribed = new AtomicBoolean(false);
    private final AtomicInteger optSubscribedCount = new AtomicInteger(0);
    private final AtomicLong ticksLast60s = new AtomicLong();
    private final AtomicReference<Instant> lastTickTs = new AtomicReference<>(null);
    private final AtomicReference<String> currentFutKey = new AtomicReference<>(null);
    private final AtomicBoolean futLtpLogged = new AtomicBoolean(false);

private final AtomicLong lastAutoStartLog = new AtomicLong(0);

    private final AtomicBoolean marketWasOpen = new AtomicBoolean(false);

public enum OrchestratorState { IDLE, RUNNING, READY }
private final AtomicReference<OrchestratorState> orchestratorState = new AtomicReference<>(OrchestratorState.IDLE);
private final AtomicReference<String> lastSelectionSignature = new AtomicReference<>(null);
private final AtomicBoolean selectionComputed = new AtomicBoolean(false);
private final Set<String> currentlySubscribedKeys = ConcurrentHashMap.newKeySet();

    private final ConcurrentHashMap<String, Tick> lastTick = new ConcurrentHashMap<>();
    private final AtomicBoolean instrumentsInitialized = new AtomicBoolean(false);
    private final AtomicInteger ceLoadedCount = new AtomicInteger(0);
    private final AtomicInteger peLoadedCount = new AtomicInteger(0);
    private final AtomicLong currentExpiryMs = new AtomicLong(0);

    private final boolean wsDebug = Boolean.parseBoolean(System.getenv().getOrDefault("WS_DEBUG", "false"));
    private final AtomicReference<String> lastError = new AtomicReference<>(null);
    private final AtomicReference<Instant> lastConnectTs = new AtomicReference<>(null);
    private final AtomicReference<String> lastCloseReason = new AtomicReference<>(null);

    public Optional<Tick> getLatestTick(String key) {
        return Optional.ofNullable(lastTick.get(key));
    }

    public Double getLatestLtp(String key) {
        return getLatestTick(key).map(Tick::ltp).orElse(null);
    }

    public Set<String> cachedKeys() {
        return lastTick.keySet();
    }

    public boolean isMarketOpen() {
        return MarketHours.isOpen(Instant.now());
    }

    public boolean hasRecentFutWrites() {
        return futWrites.get() > 0;
    }

    public boolean isConnected() { return connected.get(); }
    public boolean isWsConnected() { return marketState.isWsConnected(); }
    public Instant lastTickTs() { return lastTickTs.get(); }
    public long ticksLast60s() { return ticksLast60s.get(); }
    public boolean futSubscribed() { return futSubscribed.get(); }
    public int optSubscribedCount() { return optSubscribedCount.get(); }
    public String lastError() { return lastError.get(); }
    public Instant lastConnectTs() { return lastConnectTs.get(); }
    public String lastCloseReason() { return lastCloseReason.get(); }
    public Map<String, Tick> latestTicks() { return new LinkedHashMap<>(lastTick); }
    public String currentFutKey() { return currentFutKey.get(); }

    private final Sinks.Many<LtpEvent> ltpSink = Sinks.many().multicast().onBackpressureBuffer();
    public Flux<LtpEvent> ltpEvents() { return ltpSink.asFlux(); }

    @Value("${INFLUX_BUCKET:}")
    private String influxBucket;
    @Value("${INFLUX_ORG:}")
    private String influxOrg;

    private final Map<String, NseInstrument> instrumentCache = new ConcurrentHashMap<>();
    private final AtomicLong futWrites = new AtomicLong();
    private final AtomicLong optWrites = new AtomicLong();

    private final Map<String, ConcurrentLinkedDeque<OptTick>> optionBuffers = new ConcurrentHashMap<>();

    public record OptTick(Instant ts, String instrumentKey, String symbol, double ltp, int qty, int oi) {}
    /**
     * Exposed for your controllers to subscribe
     **/
    public Flux<JsonNode> stream() {
        return sink.asFlux();
    }

    @PostConstruct
    void logTrustStoreInfo() {
        String ts = System.getProperty("javax.net.ssl.trustStore");
        if (ts == null || ts.isBlank()) {
            ts = System.getProperty("java.home") + "/lib/security/cacerts";
        }
        try (InputStream in = new FileInputStream(ts)) {
            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(in, "changeit".toCharArray());
            log.info("Trust store: {} ({} CAs)", ts, ks.size());
        } catch (Exception e) {
            log.warn("Unable to read trust store at {}", ts, e);
        }
        if (wsDebug) {
            try {
                java.net.http.HttpClient client = java.net.http.HttpClient.newHttpClient();
                HttpRequest req = HttpRequest.newBuilder(URI.create("https://repo1.maven.org"))
                        .GET().build();
                client.send(req, HttpResponse.BodyHandlers.discarding());
                log.info("TLS check to repo1.maven.org succeeded");
            } catch (Exception e) {
                log.error("TLS check to repo1.maven.org failed", e);
            }
        }
    }
    @PostConstruct
    public void subscribeToAuthEvents() {
        auth.events()
                .filter(e -> e == UpstoxAuthService.AuthEvent.READY)
                .subscribe(ev -> startOrchestration());

        auth.events()
                .filter(e -> e == UpstoxAuthService.AuthEvent.EXPIRED)
                .subscribe(e -> {
                    orchestratorState.set(OrchestratorState.IDLE);
                    selectionComputed.set(false);
                });

        Flux.interval(Duration.ofSeconds(15))
                .subscribe(i -> {
                    boolean open = MarketHours.isOpen(Instant.now());
                    boolean wasOpen = marketWasOpen.getAndSet(open);
                    if (wasOpen && !open) {
                        onMarketClose();
                    }
                    boolean futOn = futSubscribed.get();
                    int optCount = optSubscribedCount.get();
                    long fut = futWrites.getAndSet(0);
                    long opt = optWrites.getAndSet(0);
                    ticksLast60s.set(fut + opt);
                    String source = connected.get() ? "live" : "influx";
                    log.info("HEARTBEAT market={} liveFut={} liveOpts={} writesFut={}/15s writesOpt={}/15s sourceUsed={}",
                            open ? "open" : "closed", futOn ? "on" : "off", optCount, fut, opt, source);
                });
    }

    private void startOrchestration() {
        nseInstrumentService.ensureNseJsonLoaded();
        log.info("FLOW 1/8: NSE JSON loaded (size={})", nseInstrumentService.getCachedNse().size());
        nseInstrumentService.purgeExpiredOptionDocs();
        log.info("FLOW 2/8: Purged expired option docs");
        long count = nseInstrumentService.saveNiftyFuturesToMongo();
        log.info("FLOW 3/8: NIFTY FUT contracts saved → mongo collection=nifty_futures count={}", count);
        connectIfOpenOrSchedule();
    }

    public void connectIfOpenOrSchedule() {
        Instant now = Instant.now();
        boolean isTradingWindowNow = MarketHours.isOpen(now);
        boolean todayIsTradingDay = MarketHours.isTradingDay(now.atZone(MarketHours.zone()).toLocalDate());
        log.info("FLOW 4/8: Market window check → isOpen={} todayTradingDay={}", isTradingWindowNow, todayIsTradingDay);
        if (!isTradingWindowNow) {
            Instant next = MarketHours.nextOpenAfter(now);
            log.info("FLOW 5/8: Market closed — scheduling live feed at {}", next.atZone(MarketHours.zone()));
            long delay = Duration.between(now, next).toMillis();
            Mono.delay(Duration.ofMillis(delay)).subscribe(v -> startLive());
        } else {
            log.info("FLOW 5/8: Market open — starting live feed");
            startLive();
        }
    }

    public void startLive() {
        if (mockMode) {
            log.info("🧪 Mock mode enabled - skipping live feed startup");
            return;
        }
        orchestratorState.set(OrchestratorState.RUNNING);
        streamNiftyFutAndTriggerCEPE();
        orchestratorState.set(OrchestratorState.READY);
    }

    private void ensureOptionStream() {
        NseInstrumentService.OptionBatch batch = nseInstrumentService.loadCurrentWeekOptionInstruments();
        ceLoadedCount.set(batch.ce().size());
        peLoadedCount.set(batch.pe().size());
        currentExpiryMs.set(batch.expiry());
        if (batch.ce().isEmpty() && batch.pe().isEmpty()) {
            log.info("OPTION-STREAM wait: no instruments yet — attempting JSON fallback");
            try {
                Double ltp = nseInstrumentService.getNearestExpiryNiftyFutureLtp().block(Duration.ofSeconds(5));
                if (ltp != null) {
                    NseInstrumentService.OptionBatch fb = nseInstrumentService.filterStrikesAroundLtpFromJson(ltp);
                    ceLoadedCount.set(fb.ce().size());
                    peLoadedCount.set(fb.pe().size());
                    currentExpiryMs.set(fb.expiry());
                }
            } catch (Exception ex) {
                log.error("⚠️ Failed JSON fallback for options", ex);
            }
            if (ceLoadedCount.get() == 0 && peLoadedCount.get() == 0) {
                Mono.delay(Duration.ofSeconds(30)).subscribe(i -> ensureOptionStream());
                return;
            }
        }
        streamFilteredNiftyOptions();
    }

    /**
     * Public entry to start or resume feeds ensuring option instruments exist.
     */
    public void startOrResume() {
        ZonedDateTime nowIst = ZonedDateTime.now(MarketHours.zone());
        nseInstrumentService.ensureOptionsLoaded(nowIst);
        startLive();
    }

    /**
     * STEP 6.1: fetch the actual WS URL (handles redirect or JSON token)
     **/
    public Mono<String> fetchWebSocketUrl() {
        String wsUrl = "wss://api-v2.upstox.com/feed";
        log.info("▶︎ connecting to WS at {}", wsUrl);
        return Mono.just(wsUrl);
    }

    private ReactorNettyWebSocketClient createWsClient() {
        HttpClient http = HttpClient.create();
        if (wsDebug) {
            http = http.wiretap(true);
        }
        return new ReactorNettyWebSocketClient(http);
    }

    private HttpHeaders createWsHeaders() {
        HttpHeaders h = new HttpHeaders();
        String token = auth.currentToken();
        if (token != null) {
            h.set(HttpHeaders.AUTHORIZATION, "Bearer " + token);
        }
        h.set(HttpHeaders.USER_AGENT, "trader-backend/1.0");
        h.set(HttpHeaders.CONNECTION, "Upgrade");
        h.set(HttpHeaders.UPGRADE, "websocket");
        h.set("Sec-WebSocket-Version", "13");
        if (wsDebug) {
            HttpHeaders logHeaders = new HttpHeaders();
            logHeaders.addAll(h);
            if (logHeaders.containsKey(HttpHeaders.AUTHORIZATION)) {
                logHeaders.set(HttpHeaders.AUTHORIZATION, "Bearer ****");
            }
            log.info("WS headers: {}", logHeaders);
        }
        return h;
    }

    private void logWsError(Throwable t, String wsUrl) {
        Throwable root = Exceptions.unwrap(t);
        String host = URI.create(wsUrl).getHost();
        String msg = root.getMessage();
        String cls = root.getClass().getSimpleName();
        lastError.set(cls + ": " + msg);
        if (root instanceof SSLException se) {
            try {
                SSLContext ctx = SSLContext.getDefault();
                SSLParameters params = ctx.getSupportedSSLParameters();
                log.error("WS connect SSL error host={} msg={} prot={} cipher={}", host, msg,
                        String.join(" ", params.getProtocols()),
                        String.join(" ", params.getCipherSuites()),
                        se);
            } catch (Exception e) {
                log.error("WS connect SSL error host={} msg={} (failed to fetch SSL params)", host, msg, se);
            }
        } else {
            log.error("WS connect error host={} msg={} class={}", host, msg, cls, root);
        }
    }

    private Retry retrySpec() {
        long[] delays = {1,2,5,10,20,30};
        return Retry.from(companion -> companion
                .zipWith(Flux.range(0, Integer.MAX_VALUE), (sig, idx) -> idx)
                .flatMap(i -> {
                    long base = delays[Math.min(i, delays.length - 1)];
                    long jitter = (long)(base * 0.25);
                    long actual = base - jitter + ThreadLocalRandom.current().nextLong(jitter * 2 + 1);
                    return Mono.delay(Duration.ofSeconds(actual));
                }));
    }


    private static final byte[] SUB_FRAME = """
            {"guid":"someguid","method":"sub",
             "data":{"mode":"full",
                     "instrumentKeys":["NSE_FO|44874"]}}
            """.getBytes(StandardCharsets.UTF_8);

    private Flux<JsonNode> openWebSocket(String wsUrl) {
        ReactorNettyWebSocketClient client = createWsClient();
        HttpHeaders headers = createWsHeaders();
        Sinks.Many<JsonNode> local = Sinks.many().multicast().onBackpressureBuffer();

        client.execute(URI.create(wsUrl), headers, session ->
                session.send(Mono.just(session.binaryMessage(bb -> bb.wrap(SUB_FRAME))))
                        .doOnSuccess(v -> {
                            log.info("▶︎ subscribe frame sent");
                            lastConnectTs.set(Instant.now());
                            lastError.set(null);
                        })
                        .thenMany(session.receive()
                                .map(WebSocketMessage::getPayload)
                                .map(this::parseProtoFeedResponse)
                                .doOnNext(local::tryEmitNext)
                                .doFinally(sig -> session.closeStatus().doOnNext(cs -> {
                                    log.info("WS closed code={} reason={}", cs.getCode(), cs.getReason());
                                    lastCloseReason.set(cs.getCode() + ":" + cs.getReason());
                                }).subscribe())
                        )
                        .then()
        ).doOnError(t -> logWsError(t, wsUrl)).subscribe();

        return local.asFlux();
    }

    /**
     * replace your old parseProtoTick with this:
     **/
    private JsonNode parseProtoFeedResponse(DataBuffer buf) {
        try {
            byte[] b = new byte[buf.readableByteCount()];
            buf.read(b);

            // Upstox sample parses FeedResponse
            var resp = MarketDataFeed.FeedResponse.parseFrom(b);

            // convert to JSON with protobuf’s JsonFormat
            String json = JsonFormat.printer()
                    .omittingInsignificantWhitespace()
                    .print(resp);

            return om.readTree(json);
        } catch (Exception ex) {
            throw Exceptions.propagate(ex);
        }
    }

    private Point toPoint(JsonNode tick) {
        // 1) discover the instrument key (first field under "feeds")
        JsonNode feeds = tick.path("feeds");
        Iterator<String> it = feeds.fieldNames();
        String instr = it.hasNext() ? it.next() : "UNKNOWN";

        // 2) pick timestamp: currentTs if present, else now()
        long tms = tick.hasNonNull("currentTs")
                ? tick.get("currentTs").asLong()
                : Instant.now().toEpochMilli();

        // 3) build your InfluxDB point, saving the whole JSON as a string
        return Point
                .measurement("ticks")
                .addTag("instrument", instr)
                .time(Instant.ofEpochMilli(tms), WritePrecision.MS)
                .addField("raw", tick.toString());
    }
    public void setupNiftyOptionsLiveFeed() {
        log.info("🚀 Starting Nifty Option Chain setup...");
        log.info("🚀 [INIT] setupNiftyOptionsLiveFeed() CALLED");

        WebClient.builder()
                .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + auth.currentToken())
                .build()
                .get()
                .uri("https://api.upstox.com/v2/option/chain/index/NIFTY")
                .retrieve()
                .bodyToMono(JsonNode.class)
                .flatMapMany(response -> {
                    log.debug("🔍 Full response from option chain: {}", response.toPrettyString());

                    JsonNode records = response.path("data").path("records");
                    if (records.isMissingNode() || !records.isArray()) {
                        log.error("⚠️ Option chain response invalid or empty");
                        return Flux.empty();
                    }

                    String nearestExpiry = "";
                    List<String> instrumentKeys = new ArrayList<>();
                    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyMMMdd").withLocale(Locale.ENGLISH);
                    String segment = "NSE_INDEX_OPT";
                    String symbol = "NIFTY";

                    for (JsonNode record : records) {
                        String expiry = record.path("expiryDate").asText(); // yyyy-MM-dd
                        if (nearestExpiry.isEmpty()) {
                            nearestExpiry = expiry;
                            log.info("📅 Nearest expiry detected: {}", nearestExpiry);
                        }
                        if (!expiry.equals(nearestExpiry)) continue;

                        LocalDate expDate = LocalDate.parse(expiry);
                        String formattedExpiry = expDate.format(fmt).toUpperCase();

                        JsonNode ce = record.path("CE");
                        JsonNode pe = record.path("PE");

                        if (ce != null && ce.has("lastPrice") && ce.has("strikePrice")) {
                            double ceLtp = ce.path("lastPrice").asDouble();
                            double strike = ce.path("strikePrice").asDouble();
                            if (ceLtp < 50) {
                                String key = String.format("%s|%s%s%sCE", segment, symbol, formattedExpiry, (int) strike);
                                log.info("📘 CE → LTP: {}, Key: {}", ceLtp, key);
                                instrumentKeys.add(key);
                            }
                        }

                        if (pe != null && pe.has("lastPrice") && pe.has("strikePrice")) {
                            double peLtp = pe.path("lastPrice").asDouble();
                            double strike = pe.path("strikePrice").asDouble();
                            if (peLtp < 50) {
                                String key = String.format("%s|%s%s%sPE", segment, symbol, formattedExpiry, (int) strike);
                                log.info("📕 PE → LTP: {}, Key: {}", peLtp, key);
                                instrumentKeys.add(key);
                            }
                        }
                    }

                    log.info("✅ Total filtered option keys: {}", instrumentKeys.size());
                    for (String key : instrumentKeys) {
                        log.info("📦 Subscribing to option key: {}", key);
                    }

                    if (instrumentKeys.isEmpty()) {
                        log.warn("⚠️ No options found under ₹50. Nothing to subscribe.");
                        return Flux.empty();
                    }

                    ObjectMapper localMapper = new ObjectMapper();
                    ObjectNode frame = localMapper.createObjectNode();
                    frame.put("guid", "nifty-options-guid");
                    frame.put("method", "sub");

                    ObjectNode data = frame.putObject("data");
                    data.put("mode", "full");
                    ArrayNode keysArray = data.putArray("instrumentKeys");
                    for (String key : instrumentKeys) {
                        keysArray.add(key);
                    }

                    byte[] frameBytes;
                    try {
                        frameBytes = frame.toString().getBytes(StandardCharsets.UTF_8);
                        log.debug("🧾 Final SUB_FRAME for Nifty Options: {}", frame.toPrettyString());
                    } catch (Exception e) {
                        log.error("❌ Failed to build SUB_FRAME", e);
                        return Flux.empty();
                    }

                    return fetchWebSocketUrl()
                            .flatMapMany(wsUrl -> openWebSocketForOptions(wsUrl, frameBytes));
                })
                .subscribe(
                        tick -> sink.tryEmitNext((JsonNode) tick),
                        error -> log.error("❌ Option WS failed: ", error)
                );
    }


    public Flux<JsonNode> openWebSocketForOptions(String wsUrl, byte[] subFrame) {
        ReactorNettyWebSocketClient client = createWsClient();
        HttpHeaders headers = createWsHeaders();
        Sinks.Many<JsonNode> local = Sinks.many().multicast().onBackpressureBuffer();

        client.execute(URI.create(wsUrl), headers, session ->
                session.send(Mono.just(session.binaryMessage(bb -> bb.wrap(subFrame))))
                        .doOnSuccess(v -> {
                            log.info("▶︎ Nifty options subscription frame sent");
                            lastConnectTs.set(Instant.now());
                            lastError.set(null);
                            if (connected.compareAndSet(false, true)) {
                                if (everConnected.getAndSet(true)) {
                                    log.info("LIVE RECONNECTED");
                                }
                                log.info("LIVE CONNECTED");
                            }
                            marketState.setWsConnected(true);
                        })
                        .thenMany(session.receive()
                                .map(WebSocketMessage::getPayload)
                                .map(this::parseProtoFeedResponse)
                                .doOnNext(local::tryEmitNext)
                                .doOnSubscribe(s -> log.info("📡 Subscribed to Nifty options WebSocket feed"))
                                .doFinally(sig -> {
                                    if (connected.getAndSet(false)) {
                                        log.info("LIVE DISCONNECTED");
                                    }
                                    marketState.setWsConnected(false);
                                    session.closeStatus().doOnNext(cs -> {
                                        log.info("WS closed code={} reason={}", cs.getCode(), cs.getReason());
                                        lastCloseReason.set(cs.getCode() + ":" + cs.getReason());
                                    }).subscribe();
                                })

                        )
                        .then()
        ).doOnError(t -> logWsError(t, wsUrl)).subscribe();

        return local.asFlux();
    }
    // inside LiveFeedService.java (below your existing SUB_FRAME):
    private static final byte[] OPTION_SUB_FRAME = """
    {
      "guid":"someguid‐options",
      "method":"sub",
      "data":{
        "mode":"full",
        "instrumentKeys":[
          "NSE_FO|60131"
        ]
      }
    }
    """.getBytes(StandardCharsets.UTF_8);
    private Flux<JsonNode> openOptionWebSocket(String wsUrl) {
        ReactorNettyWebSocketClient client = createWsClient();
        HttpHeaders headers = createWsHeaders();
        Sinks.Many<JsonNode> local = Sinks.many().multicast().onBackpressureBuffer();

        client.execute(URI.create(wsUrl), headers, session ->
                session.send(Mono.just(session.binaryMessage(bb -> bb.wrap(OPTION_SUB_FRAME))))
                        .doOnSuccess(v -> {
                            log.info("▶︎ option‐subscribe frame sent");
                            lastConnectTs.set(Instant.now());
                            lastError.set(null);
                        })
                        .thenMany(session.receive()
                                .map(WebSocketMessage::getPayload)
                                .map(this::parseProtoFeedResponse)
                                .doOnNext(local::tryEmitNext)
                                .doFinally(sig -> session.closeStatus().doOnNext(cs -> {
                                    log.info("WS closed code={} reason={}", cs.getCode(), cs.getReason());
                                    lastCloseReason.set(cs.getCode() + ":" + cs.getReason());
                                }).subscribe())
                        )
                        .then()
        ).doOnError(t -> logWsError(t, wsUrl)).subscribe();

        return local.asFlux();
    }
public void streamFilteredNiftyOptions() {
    NseInstrumentService.SelectionData sel = nseInstrumentService.currentSelectionData();
    ceLoadedCount.set(sel.ceCount());
    peLoadedCount.set(sel.peCount());
    currentExpiryMs.set(sel.expiry());
    List<String> desired = sel.keys();
    if (desired.isEmpty()) {
        log.info("OPTION-STREAM wait: no instruments yet (will retry)");
        optionsStreamStarted.set(false);
        Mono.delay(Duration.ofSeconds(30)).subscribe(i -> streamFilteredNiftyOptions());
        return;
    }
    Set<String> toAdd = new HashSet<>(desired);
    toAdd.removeAll(currentlySubscribedKeys);
    Set<String> toRemove = new HashSet<>(currentlySubscribedKeys);
    toRemove.removeAll(new HashSet<>(desired));

    if (toAdd.isEmpty() && toRemove.isEmpty() && optionsStreamStarted.get()) {
        log.info("Orchestration skipped — selection unchanged (expiry={})",
                nseInstrumentService.formatExpiry(sel.expiry()));
        return;
    }

    currentlySubscribedKeys.addAll(toAdd);
    currentlySubscribedKeys.removeAll(toRemove);
    log.info("Subscriptions updated: +{} / -{} (total={})", toAdd.size(), toRemove.size(), desired.size());

    if (!optionsStreamStarted.compareAndSet(false, true)) {
        return;
    }

    log.info("OPTION-STREAM started keys={} expiry={}", desired.size(), sel.expiry());

    auth.ensureValidToken()
        .flatMap(valid -> {
            if (!valid) {
                optionsStreamStarted.set(false);
                log.warn("⚠️ Upstox token not ready — skipping option stream start");
                return Mono.empty();
            }
            return fetchWebSocketUrl();
        })
        .flatMapMany(wsUrl -> openWebSocketWithDynamicSub(wsUrl, () -> buildSubFrame(desired), desired.size()))
        .retryWhen(retrySpec())
        .doOnSubscribe(s -> log.info("📡 Subscribed to filtered CE/PE (auto-resub on reconnect)"))
        .doOnNext(tick -> {
            sink.tryEmitNext(tick);
            JsonNode feeds = tick.path("feeds");
            feeds.fields().forEachRemaining(entry -> {
                String instrumentKey = entry.getKey();
                JsonNode feed = entry.getValue();
                JsonNode ltpNode = feed
                        .path("fullFeed")
                        .path("marketFF")
                        .path("ltpc")
                        .path("ltp");
                if (ltpNode.isNumber()) {
                    double ltp = ltpNode.asDouble();
                    long ts = extractTimestamp(feed, tick);
                    ltpSink.tryEmitNext(new LtpEvent(instrumentKey, ltp, Instant.ofEpochMilli(ts)));
                    logLtp(instrumentKey, ltp, ts);
                    writeTickToInflux(instrumentKey, feed, ts);
                    lastTick.put(instrumentKey, new Tick(instrumentKey, ltp, Instant.ofEpochMilli(ts)));
                    bufferOptionTick(instrumentKey, feed, ts, ltp);

                    depthMetricsService.onFeed(instrumentKey, feed, ltp, Instant.ofEpochMilli(ts));

                    var result = quantAnalysisService.analyze(instrumentKey, feed);
                    if (result.signal() != QuantAnalysisService.Signal.NONE) {
                        log.info("🎯 {} for {} → momentum={} volSpike={} imbalance={} noise={}",
                                result.signal(), instrumentKey,
                                String.format("%.4f", result.momentum()),
                                String.format("%.2f", result.volumeSpike()),
                                String.format("%.4f", result.imbalance()),
                                String.format("%.2f", result.noise()));
                    }
                }
            });
        })
        .doOnError(err -> {
            optionsStreamStarted.set(false);
            log.error("❌ filtered option feed failed:", err);
        })
        .doFinally(sig -> {
            optionsStreamStarted.set(false);
            log.info("🧹 filtered CE/PE stream terminated: {}", sig);
        })
        .subscribe();
}
public void streamSingleInstrument(String instrumentKey) {
    log.info("🚀 Starting live stream for instrument → {}", instrumentKey);

    // Step 1: Create a sub frame
    ObjectNode frame = om.createObjectNode();
    frame.put("guid", "single-instrument-guid");
    frame.put("method", "sub");

    ObjectNode data = frame.putObject("data");
    data.put("mode", "full");
    data.putArray("instrumentKeys").add(instrumentKey);

    byte[] subFrame = frame.toString().getBytes(StandardCharsets.UTF_8);

    // Step 2: Connect and stream
    fetchWebSocketUrl()
        .flatMapMany(wsUrl -> openWebSocketForOptions(wsUrl, subFrame))
        .doOnNext(tick -> {
            try {
                // ✅ Parse LTP from incoming tick
                // ✅ Correct path:
JsonNode ltpNode = tick.path("feeds")
    .path(instrumentKey)
    .path("fullFeed")
    .path("marketFF")
    .path("ltpc")
    .path("ltp");
                if (!ltpNode.isMissingNode()) {
                    double ltp = ltpNode.asDouble();
                    long ts = extractTimestamp(tick.path("feeds").path(instrumentKey), tick);
                    logLtp(instrumentKey, ltp, ts);
                    ltpSink.tryEmitNext(new LtpEvent(instrumentKey, ltp, Instant.ofEpochMilli(ts)));
                    JsonNode singleFeed = tick.path("feeds").path(instrumentKey);
                    writeTickToInflux(instrumentKey, singleFeed, ts);
                    lastTick.put(instrumentKey, new Tick(instrumentKey, ltp, Instant.ofEpochMilli(ts)));
                    bufferOptionTick(instrumentKey, singleFeed, ts, ltp);
                    depthMetricsService.onFeed(instrumentKey, singleFeed, ltp, Instant.ofEpochMilli(ts));
                }

                sink.tryEmitNext(tick);
            } catch (Exception e) {
                log.error("❌ Error parsing tick JSON or filtering: ", e);
            }
        })
        .doOnError(err -> log.error("❌ WebSocket stream failed:", err))
        .subscribe();
}

public byte[] buildSubFrame(String instrumentKey) {
    ObjectNode frame = om.createObjectNode();
    frame.put("guid", "nifty-fut-guid");
    frame.put("method", "sub");

    ObjectNode data = frame.putObject("data");
    data.put("mode", "full");
    data.putArray("instrumentKeys").add(instrumentKey);

    return frame.toString().getBytes(StandardCharsets.UTF_8);
}

private byte[] buildSubFrame(List<String> keys) {
    ObjectNode frame = om.createObjectNode();
    frame.put("guid", "filtered-options-guid");
    frame.put("method", "sub");

    ObjectNode data = frame.putObject("data");
    data.put("mode", "full");
    ArrayNode arr = data.putArray("instrumentKeys");
    keys.forEach(arr::add);

    return frame.toString().getBytes(StandardCharsets.UTF_8);
}
public MongoTemplate getMongoTemplate() {
    return mongoTemplate;
}
public void streamNiftyFutAndTriggerCEPE() {
    if (auth.currentToken() == null) {
        log.warn("⚠️ Cannot start NIFTY FUT stream — token not available");
        return;
    }

    Optional<String> optKey = nseInstrumentService.getCurrentMonthNiftyFutKey();
    if (optKey.isEmpty()) {
        log.error("ERR FUT: current-month NIFTY FUT not found — cannot subscribe");
        return;
    }
    String instrumentKey = optKey.get();
    currentFutKey.set(instrumentKey);
    futLtpLogged.set(false);
    selectionComputed.set(false);

    fetchWebSocketUrl()
            .flatMapMany(wsUrl -> openWebSocketForOptions(wsUrl, buildSubFrame(instrumentKey)))
            .doOnSubscribe(s -> futSubscribed.set(true))
            .doFinally(sig -> futSubscribed.set(false))
            .doOnNext(tick -> {
                try {
                    JsonNode feed = tick.path("feeds").path(instrumentKey);
                    JsonNode ltpNode = feed
                            .path("fullFeed")
                            .path("marketFF")
                            .path("ltpc")
                            .path("ltp");

                    if (ltpNode.isNumber()) {
                        double ltp = ltpNode.asDouble();
                        long ts = extractTimestamp(feed, tick);
                        ltpSink.tryEmitNext(new LtpEvent(instrumentKey, ltp, Instant.ofEpochMilli(ts)));
                        logLtp(instrumentKey, ltp, ts);
                        writeTickToInflux(instrumentKey, feed, ts);
                        lastTick.put(instrumentKey, new Tick(instrumentKey, ltp, Instant.ofEpochMilli(ts)));
                        marketState.setLastTickTs(ts);
                        bufferOptionTick(instrumentKey, feed, ts, ltp);

                        if (futLtpLogged.compareAndSet(false, true)) {
                            log.info("FLOW 6/8: FUT LTP resolved key={} ltp={} ts={}", instrumentKey, ltp, Instant.ofEpochMilli(ts));
                            NseInstrumentService.OptionBatch batch = nseInstrumentService.filterStrikesAroundLtpFromJson(ltp);
                            int ce = batch.ce().size();
                            int pe = batch.pe().size();
                            String expiry = nseInstrumentService.formatExpiry(batch.expiry());
                            String sig = expiry + ":" + ce + ":" + pe;
                            if (sig.equals(lastSelectionSignature.get())) {
                                log.info("FLOW 7/8: Selection unchanged — expiry={} ce={} pe={}", expiry, ce, pe);
                            } else {
                                lastSelectionSignature.set(sig);
                                log.info("FLOW 7/8: Options selected from NSE JSON — expiry={} ce={} pe={}", expiry, ce, pe);
                                streamFilteredNiftyOptions();
                            }
                        }
                    } else {
                        log.warn("⚠️ LTP not found in tick — instrumentKey={}", instrumentKey);
                    }
                } catch (Exception ex) {
                    log.error("⚠️ Failed to extract LTP or trigger filtering", ex);
                }
            })
            .doOnError(err -> log.error("❌ WebSocket stream failed:", err))
            .subscribe();
}
/** Builds a fresh SUB frame from the current filtered_nifty_premiums (15 CE + 15 PE). */
/**
 * Opens a WS and sends a fresh SUB frame per connection (frameSupplier is called on every connect).
 * Use this for dynamic lists that may change between reconnects.
 */
private Flux<JsonNode> openWebSocketWithDynamicSub(String wsUrl, java.util.function.Supplier<byte[]> frameSupplier, int subsCount) {
    ReactorNettyWebSocketClient client = createWsClient();
    HttpHeaders headers = createWsHeaders();
    Sinks.Many<JsonNode> local = Sinks.many().multicast().onBackpressureBuffer();

    client.execute(URI.create(wsUrl), headers, session ->
            session.send(Mono.just(session.binaryMessage(bb -> bb.wrap(frameSupplier.get()))))
                   .doOnSuccess(v -> {
                       lastConnectTs.set(Instant.now());
                       lastError.set(null);
                       int total = subsCount + (futSubscribed.get() ? 1 : 0);
                       if (connected.compareAndSet(false, true)) {
                           if (everConnected.getAndSet(true)) {
                               log.info("LIVE RECONNECTED");
                           }
                           log.info("LIVE CONNECTED (subs={})", total);
                       }
                       optSubscribedCount.set(subsCount);
                       marketState.setWsConnected(true);
                   })
                   .thenMany(session.receive()
                           .map(WebSocketMessage::getPayload)
                           .map(this::parseProtoFeedResponse)
                           .doOnNext(local::tryEmitNext)
                           .doFinally(sig -> {
                               if (connected.getAndSet(false)) {
                                   log.info("LIVE DISCONNECTED");
                               }
                               optSubscribedCount.set(0);
                               marketState.setWsConnected(false);
                               session.closeStatus().doOnNext(cs -> {
                                   log.info("WS closed code={} reason={}", cs.getCode(), cs.getReason());
                                   lastCloseReason.set(cs.getCode() + ":" + cs.getReason());
                               }).subscribe();
                           }))
                   .then()
    ).doOnError(t -> logWsError(t, wsUrl)).subscribe();

    return local.asFlux();
}

    private void logLtp(String instrumentKey, double ltp, long ts) {
        logResolvedLtp(instrumentKey, ltp, "live");
    }

    public void logResolvedLtp(String instrumentKey, double ltp, String source) {
        NseInstrument info = instrumentCache.computeIfAbsent(instrumentKey,
                k -> mongoTemplate.findById(k, NseInstrument.class));
        if (info != null) {
            String type = info.getInstrumentType();
            if ("CE".equalsIgnoreCase(type) || "PE".equalsIgnoreCase(type)) {
                log.info("OPT LTP key={} ltp={}", instrumentKey, ltp);
                return;
            }
        }
        log.info("LTP [{}] {}={}", instrumentKey, source, ltp);
    }

    private void onMarketClose() {
        double futPrice = 0.0;
        long optRows = 0;
        String futKey = nseInstrumentService.nearestNiftyFutureKey().orElse(null);
        if (futKey != null) {
            Tick t = lastTick.get(futKey);
            if (t != null) {
                ObjectNode feed = om.createObjectNode().put("ltp", t.ltp());
                writeTickToInflux(futKey, feed, t.ts().toEpochMilli());
                logLtp(futKey, t.ltp(), t.ts().toEpochMilli());
                futPrice = t.ltp();
            }
        }
        for (var dq : optionBuffers.values()) {
            OptTick t = dq.peekFirst();
            if (t == null) continue;
            ObjectNode feed = om.createObjectNode().put("ltp", t.ltp());
            if (t.qty() > 0) feed.put("qty", t.qty());
            if (t.oi() > 0) feed.put("oi", t.oi());
            writeTickToInflux(t.instrumentKey(), feed, t.ts().toEpochMilli());
            logLtp(t.instrumentKey(), t.ltp(), t.ts().toEpochMilli());
            optRows++;
        }
        log.info("FLOW 8/8: MARKET CLOSED — last snapshot futLtp={} optRows={}", futPrice, optRows);
        lastTick.clear();
        optionBuffers.clear();
        connected.set(false);
        marketState.setWsConnected(false);
        futSubscribed.set(false);
        optSubscribedCount.set(0);
    }

    private void writeTickToInflux(String instrumentKey, JsonNode feed, long ts) {
        lastTickTs.set(Instant.ofEpochMilli(ts));
        if (writeApi == null) return;
        NseInstrument info = instrumentCache.computeIfAbsent(instrumentKey,
                k -> mongoTemplate.findById(k, NseInstrument.class));
        if (info == null) return;
        boolean isFut = info.getInstrumentType() != null && info.getInstrumentType().toUpperCase().contains("FUT");
        String measurement = isFut ? "nifty_fut_ltp" : "nifty_option_ticks";

        long exp = info.getExpiry();
        if (exp < 1_000_000_000_000L) exp *= 1000L;
        String expiry = Instant.ofEpochMilli(exp).toString().substring(0, 10);

        Point p = Point.measurement(measurement)
                .addTag("instrumentKey", instrumentKey)
                .addTag("symbol", "NIFTY")
                .addTag("segment", isFut ? "FUTIDX" : "OPTIDX")
                .addTag("expiry", expiry);

        String t = info.getInstrumentType();
        if (!isFut && t != null) {
            p.addTag("type", t.toUpperCase());
            Integer sp = info.getStrikePrice();
            int strike = (sp != null) ? sp : 0;
            p.addTag("strike", String.valueOf(strike));
        }

        JsonNode ltpNode = findField(feed, "ltp");
        if (ltpNode != null && ltpNode.isNumber()) {
            p.addField("ltp", ltpNode.asDouble());
        }
        int qty = findIntField(feed, "qty");
        int oi = findIntField(feed, "oi");
        if (qty != 0) p.addField("qty", qty);
        if (oi != 0) p.addField("oi", oi);

        p.time(Instant.ofEpochMilli(ts), WritePrecision.MS);

        try {
            writeApi.writePoint(influxBucket, influxOrg, p);
            if (isFut) futWrites.incrementAndGet(); else optWrites.incrementAndGet();
        } catch (Exception e) {
            // ignore write failures but do not block main loop
        }
    }

    private void bufferOptionTick(String instrumentKey, JsonNode feed, long ts, double ltp) {
        NseInstrument info = instrumentCache.computeIfAbsent(instrumentKey,
                k -> mongoTemplate.findById(k, NseInstrument.class));
        if (info == null) return;
        String type = info.getInstrumentType();
        if (type == null || !(type.equalsIgnoreCase("CE") || type.equalsIgnoreCase("PE"))) return;
        String symbol = info.getTradingSymbol();
        if (symbol == null) return;
        int qty = findIntField(feed, "qty");
        int oi = findIntField(feed, "oi");
        OptTick tick = new OptTick(Instant.ofEpochMilli(ts), instrumentKey, symbol, ltp, qty, oi);
        ConcurrentLinkedDeque<OptTick> dq = optionBuffers.computeIfAbsent(symbol, k -> new ConcurrentLinkedDeque<>());
        dq.addFirst(tick);
        while (dq.size() > 200) {
            dq.removeLast();
        }
    }

    private int findIntField(JsonNode node, String name) {
        JsonNode n = findField(node, name);
        return n != null && n.isNumber() ? n.intValue() : 0;
    }

    private JsonNode findField(JsonNode node, String name) {
        if (node.has(name) && node.get(name).isNumber()) {
            return node.get(name);
        }
        var it = node.fields();
        while (it.hasNext()) {
            var e = it.next();
            JsonNode val = e.getValue();
            if (val.isObject()) {
                JsonNode found = findField(val, name);
                if (found != null) return found;
            }
        }
        return null;
    }

    public List<OptTick> recentOptionTicks(String symbol) {
        ConcurrentLinkedDeque<OptTick> dq = optionBuffers.get(symbol);
        if (dq == null) return List.of();
        Instant cutoff = Instant.now().minusSeconds(60);
        dq.removeIf(t -> t.ts().isBefore(cutoff));
        return new ArrayList<>(dq);
    }

    private long extractTimestamp(JsonNode feed, JsonNode tick) {
        String[] ptrs = {"/fullFeed/marketFF/ts", "/fullFeed/ts", "/ts", "/timestamp"};
        for (String ptr : ptrs) {
            JsonNode n = feed.at(ptr);
            if (n.isNumber()) {
                long v = n.asLong();
                return v < 1_000_000_000_000L ? v * 1000L : v;
            }
        }
        JsonNode c = tick.path("currentTs");
        if (c.isNumber()) {
            long v = c.asLong();
            return v < 1_000_000_000_000L ? v * 1000L : v;
        }
        return System.currentTimeMillis();
    }


}
