package com.trader.backend.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility service that can fetch historical candles for Axis Bank from the
 * Upstox V3 API, persist them into InfluxDB and run a tiny analysis producing
 * a report under src/main/resources/report.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class AxisBankHistoryService {
    private final MarketDataService marketDataService;
    private final InfluxDBClient influxDBClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final String AXIS_KEY = "NSE_EQ|INE238A01034";

    @Value("${influx.org:}")
    private String influxOrg;
    @Value("${influx.bucket:}")
    private String influxBucket;

    /**
     * Fetch 15-minute candles for the last 5 years from Upstox and store them
     * into Influx. This will aggregate a much longer history than the
     * previous one-day fetch.
     */
    public Mono<List<Candle>> fetchAndStore() {
        LocalDate today = LocalDate.now();
        String to = today.toString();
        // five years back from today
        String from = today.minusYears(5).toString();
        return marketDataService
                .candleV3(AXIS_KEY, "minute", 15, to, from)
                .flatMap(json -> Mono.fromCallable(() -> parseAndStore(json)));
    }

    private List<Candle> parseAndStore(String json) throws IOException {
        JsonNode node = objectMapper.readTree(json);
        JsonNode data = node.path("data").path("candles");
        List<Candle> candles = new ArrayList<>();
        if (data.isArray()) {
            WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();
            for (JsonNode c : data) {
                String ts = c.get(0).asText();
                double open = c.get(1).asDouble();
                double high = c.get(2).asDouble();
                double low = c.get(3).asDouble();
                double close = c.get(4).asDouble();
                long volume = c.get(5).asLong();
                candles.add(new Candle(ts, open, high, low, close, volume));
                Point p = Point.measurement("axisbank_candles")
                        .time(Instant.parse(ts), WritePrecision.MS)
                        .addField("o", open)
                        .addField("h", high)
                        .addField("l", low)
                        .addField("c", close)
                        .addField("v", volume);
                writeApi.writePoint(influxBucket, influxOrg, p);
            }
        }
        analyseAndWriteReport(candles);
        return candles;
    }

    /**
     * Query stored candles from Influx.
     */
    public List<Candle> readCandles() {
        QueryApi queryApi = influxDBClient.getQueryApi();
        String flux = String.format("from(bucket: \"%s\") |> range(start: -5y) |> " +
                "filter(fn: (r) => r._measurement == \"axisbank_candles\") |> sort(columns:[\"_time\"])",
                influxBucket);
        List<Candle> result = new ArrayList<>();
        queryApi.query(flux, influxOrg).forEach(table -> table.getRecords().forEach(rec -> {
            Instant t = (Instant) rec.getValueByKey("_time");
            Double o = toDouble(rec.getValueByKey("o"));
            Double h = toDouble(rec.getValueByKey("h"));
            Double l = toDouble(rec.getValueByKey("l"));
            Double c = toDouble(rec.getValueByKey("c"));
            Long v = toLong(rec.getValueByKey("v"));
            if (o != null && h != null && l != null && c != null && v != null) {
                result.add(new Candle(t.toString(), o, h, l, c, v));
            }
        }));
        return result;
    }

    private Double toDouble(Object value) {
        return value instanceof Number ? ((Number) value).doubleValue() : null;
    }

    private Long toLong(Object value) {
        return value instanceof Number ? ((Number) value).longValue() : null;
    }

    private void analyseAndWriteReport(List<Candle> candles) {
        if (candles.isEmpty()) return;
        double avgClose = candles.stream().mapToDouble(c -> c.close).average().orElse(0);
        double maxHigh = candles.stream().mapToDouble(c -> c.high).max().orElse(0);
        double minLow = candles.stream().mapToDouble(c -> c.low).min().orElse(0);
        String report = String.format("candles=%d\navgClose=%.2f\nmaxHigh=%.2f\nminLow=%.2f\n",
                candles.size(), avgClose, maxHigh, minLow);
        try {
            Path dir = Path.of("src/main/resources/report");
            Files.createDirectories(dir);
            Files.writeString(dir.resolve("axisbank-analysis.txt"), report);
        } catch (IOException e) {
            log.error("Failed writing report", e);
        }
    }

    public record Candle(String time, double open, double high, double low, double close, long volume) {}
}
