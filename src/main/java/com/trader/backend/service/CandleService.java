package com.trader.backend.service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.trader.backend.entity.NseInstrument;
import com.trader.backend.repository.NseInstrumentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

@Service
@RequiredArgsConstructor
public class CandleService {
    private final InfluxDBClient influxDBClient;
    private final NseInstrumentRepository repo;

    @Value("${influx.org:}")
    private String influxOrg;

    @Value("${influx.bucket:}")
    private String influxBucket;

    private static final Set<String> ALLOWED_TF = Set.of("1m","3m","5m","15m");

    public Mono<List<CandleResponse>> fetchCandles(List<String> keys, String tf, int lookback) {
        if (keys == null || keys.isEmpty()) {
            return Mono.just(List.of());
        }
        if (!ALLOWED_TF.contains(tf)) {
            return Mono.error(new IllegalArgumentException("Unknown timeframe"));
        }
        int lb = Math.min(Math.max(lookback,1), 720);
        Duration unit = switch (tf) {
            case "3m" -> Duration.ofMinutes(3);
            case "5m" -> Duration.ofMinutes(5);
            case "15m" -> Duration.ofMinutes(15);
            default -> Duration.ofMinutes(1);
        };
        String start = "-" + (unit.toMinutes()*lb) + "m";
        QueryApi queryApi = influxDBClient.getQueryApi();
        return Flux.fromIterable(keys)
                .flatMap(key -> Mono.fromCallable(() -> queryForInstrument(queryApi, key, tf, start)))
                .collectList();
    }

    public Double readLatestLtpFromInflux(String instrumentKey) {
        Optional<NseInstrument> opt = repo.findById(instrumentKey);
        if (opt.isEmpty()) {
            return null;
        }
        String measurement = "FUT".equalsIgnoreCase(opt.get().getInstrumentType()) ?
                "nifty_future_ticks" : "nifty_option_ticks";
        String flux = String.format("from(bucket: \"%s\") |> range(start: -1d) |> " +
                        "filter(fn: (r) => r._measurement == \"%s\" and r.instrumentKey == \"%s\" and r._field == \"ltp\") |> last()",
                influxBucket, measurement, instrumentKey);
        QueryApi queryApi = influxDBClient.getQueryApi();
        List<FluxTable> tables = queryApi.query(flux, influxOrg);
        for (FluxTable table : tables) {
            for (FluxRecord rec : table.getRecords()) {
                Object val = rec.getValueByKey("_value");
                if (val instanceof Number num) {
                    return num.doubleValue();
                }
            }
        }
        return null;
    }

    private CandleResponse queryForInstrument(QueryApi queryApi, String key, String tf, String start) {
        Optional<NseInstrument> opt = repo.findById(key);
        if (opt.isEmpty()) {
            return new CandleResponse(key, tf, List.of());
        }
        String measurement = "FUT".equalsIgnoreCase(opt.get().getInstrumentType()) ?
                "nifty_future_ticks" : "nifty_option_ticks";

        Map<Instant, CandleBuilder> map = new HashMap<>();

        String tpl = """
from(bucket: "%s")
  |> range(start: %s)
  |> filter(fn: (r) => r._measurement == "%s" and r.instrumentKey == "%s" and r._field == "%s")
  |> aggregateWindow(every: %s, fn: %s, createEmpty: false)
  |> keep(columns: ["_time","_value"])
  |> rename(columns: {_value: "%s"})
""";

        merge(queryApi.query(String.format(tpl, influxBucket, start, measurement, key, "ltp", tf, "last", "c"), influxOrg), "c", map);
        merge(queryApi.query(String.format(tpl, influxBucket, start, measurement, key, "ltp", tf, "first", "o"), influxOrg), "o", map);
        merge(queryApi.query(String.format(tpl, influxBucket, start, measurement, key, "ltp", tf, "max", "h"), influxOrg), "h", map);
        merge(queryApi.query(String.format(tpl, influxBucket, start, measurement, key, "ltp", tf, "min", "l"), influxOrg), "l", map);
        merge(queryApi.query(String.format(tpl, influxBucket, start, measurement, key, "volume", tf, "sum", "v"), influxOrg), "v", map);

        List<Candle> candles = map.entrySet().stream()
                .filter(e -> e.getValue().complete())
                .sorted(Map.Entry.comparingByKey())
                .map(e -> e.getValue().toCandle(e.getKey()))
                .toList();

        return new CandleResponse(key, tf, candles);
    }

    private void merge(List<FluxTable> tables, String column, Map<Instant, CandleBuilder> map) {
        for (FluxTable table : tables) {
            for (FluxRecord rec : table.getRecords()) {
                Instant t = (Instant) rec.getValueByKey("_time");
                CandleBuilder b = map.computeIfAbsent(t, k -> new CandleBuilder());
                Object val = rec.getValueByKey(column);
                switch (column) {
                    case "o" -> b.o = toDouble(val);
                    case "h" -> b.h = toDouble(val);
                    case "l" -> b.l = toDouble(val);
                    case "c" -> b.c = toDouble(val);
                    case "v" -> b.v = toLong(val);
                }
            }
        }
    }

    static Double toDouble(Object v) {
        return Optional.ofNullable(v).map(x -> ((Number) x).doubleValue()).orElse(null);
    }

    static Long toLong(Object v) {
        return Optional.ofNullable(v).map(x -> ((Number) x).longValue()).orElse(null);
    }

    private static class CandleBuilder {
        Double o;
        Double h;
        Double l;
        Double c;
        Long v;

        boolean complete() {
            return o != null && h != null && l != null && c != null && v != null;
        }

        Candle toCandle(Instant t) {
            return new Candle(t.toString(), o, h, l, c, v);
        }
    }

    public record CandleResponse(String instrumentKey, String tf, List<Candle> candles) {}
    public record Candle(String t, double o, double h, double l, double c, long v) {}
}
