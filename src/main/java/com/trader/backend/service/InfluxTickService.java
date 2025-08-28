package com.trader.backend.service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import com.trader.backend.entity.NseInstrument;
import com.trader.backend.repository.NseInstrumentRepository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Lightweight reader for fetching the last known tick from InfluxDB.
 */
@Service
@RequiredArgsConstructor
public class InfluxTickService {
    private final InfluxDBClient influxDBClient;
    private final NseInstrumentRepository repo;

    @Value("${influx.org:}")
    private String influxOrg;

    @Value("${influx.bucket:}")
    private String influxBucket;

    public record Sanity(long futPoints, long optPoints, Instant futLastTs, Instant optLastTs) {}

    /**
     * Fetches the latest tick for the given instrument key.
     */
    public Optional<Tick> latestTick(String instrumentKey) {
        Optional<NseInstrument> opt = repo.findById(instrumentKey);
        if (opt.isEmpty()) {
            return Optional.empty();
        }
        String measurement = "FUT".equalsIgnoreCase(opt.get().getInstrumentType()) ?
                "nifty_future_ticks" : "nifty_option_ticks";
        String flux = String.format("from(bucket: \"%s\") |> range(start: -30d) |> " +
                        "filter(fn: (r) => r._measurement == \"%s\" and r.instrumentKey == \"%s\" and r._field == \"ltp\") |> last()",
                influxBucket, measurement, instrumentKey);
        QueryApi queryApi = influxDBClient.getQueryApi();
        List<FluxTable> tables = queryApi.query(flux, influxOrg);
        for (FluxTable table : tables) {
            for (FluxRecord rec : table.getRecords()) {
                Object val = rec.getValueByKey("_value");
                Object timeObj = rec.getValueByKey("_time");
                if (val instanceof Number num && timeObj instanceof Instant ts) {
                    return Optional.of(new Tick(instrumentKey, num.doubleValue(), ts));
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Backwards compat: futures only.
     */
    public Optional<Tick> latestFutTick(String instrumentKey) {
        return latestTick(instrumentKey);
    }

    /**
     * Fetches latest stored NIFTY future LTP from the standalone measurement.
     */
    public Optional<Double> latestNiftyFutLtp() {
        String flux = "from(bucket: \"Ticks\") |> range(start: -30d) |> " +
                "filter(fn: (r) => r._measurement == \"nifty_fut_ltp\" and r.symbol == \"NIFTY\" and r._field == \"ltp\") |> last()";
        QueryApi queryApi = influxDBClient.getQueryApi();
        List<FluxTable> tables = queryApi.query(flux, influxOrg);
        for (FluxTable table : tables) {
            for (FluxRecord rec : table.getRecords()) {
                Object val = rec.getValueByKey("_value");
                if (val instanceof Number num) {
                    return Optional.of(num.doubleValue());
                }
            }
        }
        return Optional.empty();
    }

    public Sanity sanityLast2m() {
        QueryApi queryApi = influxDBClient.getQueryApi();
        String futFlux = String.format("from(bucket: \"%s\") |> range(start: -2m) |> filter(fn: (r) => r._measurement == \"nifty_future_ticks\" and r._field == \"ltp\") |> sort(columns:[\"_time\"])", influxBucket);
        List<FluxTable> futTables = queryApi.query(futFlux, influxOrg);
        long futCount = 0; Instant futLast = null;
        for (FluxTable t : futTables) {
            for (FluxRecord rec : t.getRecords()) {
                futCount++;
                Object timeObj = rec.getValueByKey("_time");
                if (timeObj instanceof Instant ts) futLast = ts;
            }
        }

        String optFlux = String.format("from(bucket: \"%s\") |> range(start: -2m) |> filter(fn: (r) => r._measurement == \"nifty_option_ticks\" and r._field == \"ltp\") |> sort(columns:[\"_time\"])", influxBucket);
        List<FluxTable> optTables = queryApi.query(optFlux, influxOrg);
        long optCount = 0; Instant optLast = null;
        for (FluxTable t : optTables) {
            for (FluxRecord rec : t.getRecords()) {
                optCount++;
                Object timeObj = rec.getValueByKey("_time");
                if (timeObj instanceof Instant ts) optLast = ts;
            }
        }
        return new Sanity(futCount, optCount, futLast, optLast);
    }
}
