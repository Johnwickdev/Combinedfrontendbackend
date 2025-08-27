package com.trader.backend.service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

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

    @Value("${influx.org:}")
    private String influxOrg;

    @Value("${influx.bucket:}")
    private String influxBucket;

    /**
     * Fetches the latest NIFTY future tick for the given instrument key.
     */
    public Optional<Tick> latestFutTick(String instrumentKey) {
        String flux = String.format("from(bucket: \"%s\") |> range(start: -30d) |> " +
                        "filter(fn: (r) => r._measurement == \"nifty_fut_ticks\" and r.instrumentKey == \"%s\" and r._field == \"ltp\") |> last()",
                influxBucket, instrumentKey);
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
}
