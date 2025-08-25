package com.trader.backend.service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.trader.backend.entity.NseInstrument;
import com.trader.backend.repository.NseInstrumentRepository;
import lombok.RequiredArgsConstructor;
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
        String bucket = influxDBClient.getOptions().getBucket();
        return Flux.fromIterable(keys)
                .flatMap(key -> Mono.fromCallable(() -> queryForInstrument(queryApi, bucket, key, tf, start)))
                .collectList();
    }

    private CandleResponse queryForInstrument(QueryApi queryApi, String bucket, String key, String tf, String start) {
        Optional<NseInstrument> opt = repo.findById(key);
        if (opt.isEmpty()) {
            return new CandleResponse(key, tf, List.of());
        }
        String measurement = "FUT".equalsIgnoreCase(opt.get().getInstrumentType()) ?
                "nifty_fut_ticks" : "nifty_option_ticks";

        String fluxTemplate = """
price = from(bucket: "%s")
  |> range(start: %s)
  |> filter(fn: (r) => r._measurement == "%s" and r.instrumentKey == "%s" and r._field == "ltp")
  |> aggregateWindow(every: %s, fn: (tables=<-) => tables |> ohlc())
  |> rename(columns: {open: "o", high: "h", low: "l", close: "c"})
  |> keep(columns: ["_time","o","h","l","c"])
vol = from(bucket: "%s")
  |> range(start: %s)
  |> filter(fn: (r) => r._measurement == "%s" and r.instrumentKey == "%s" and r._field == "volume")
  |> aggregateWindow(every: %s, fn: sum)
  |> rename(columns: {_value: "v"})
  |> keep(columns: ["_time","v"])
join(tables: {price: price, vol: vol}, on: ["_time"])
  |> sort(columns: ["_time"])
""";

        String flux = String.format(fluxTemplate,
                bucket, start, measurement, key, tf,
                bucket, start, measurement, key, tf);

        List<FluxTable> tables = queryApi.query(flux);
        List<Candle> candles = new ArrayList<>();
        for (FluxTable table : tables) {
            for (FluxRecord rec : table.getRecords()) {
                Instant t = (Instant) rec.getValue("_time");
                Double o = (Double) rec.getValue("o");
                Double h = (Double) rec.getValue("h");
                Double l = (Double) rec.getValue("l");
                Double c = (Double) rec.getValue("c");
                Number v = (Number) rec.getValue("v");
                candles.add(new Candle(t.toString(), o, h, l, c, v == null ? 0L : v.longValue()));
            }
        }
        return new CandleResponse(key, tf, candles);
    }

    public record CandleResponse(String instrumentKey, String tf, List<Candle> candles) {}
    public record Candle(String t, double o, double h, double l, double c, long v) {}
}
