package com.trader.backend.service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.trader.backend.entity.NseInstrument;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;

@Service
@RequiredArgsConstructor
public class OhlcService {
    private final InfluxDBClient influxDBClient;
    private final MongoTemplate mongoTemplate;

    @Value("${influx.org:}")
    private String influxOrg;
    @Value("${influx.bucket:}")
    private String influxBucket;

    public List<Ohlc> fetch(String side, String gran, int limit) {
        if (!"CE".equalsIgnoreCase(side) && !"PE".equalsIgnoreCase(side)) return List.of();
        int lim = Math.max(1, Math.min(limit, 1000));
        String start = "-" + lim + "s";
        QueryApi queryApi = influxDBClient.getQueryApi();
        Query q = new Query(Criteria.where("instrumentType").is(side));
        List<NseInstrument> insts = mongoTemplate.find(q, NseInstrument.class, "filtered_nifty_premiums");
        List<Ohlc> out = new ArrayList<>();
        for (NseInstrument inst : insts) {
            out.addAll(queryForInstrument(queryApi, inst.getInstrumentKey(), gran, start));
        }
        return out;
    }

    private List<Ohlc> queryForInstrument(QueryApi queryApi, String key, String gran, String start) {
        Map<Instant, Builder> map = new HashMap<>();
        String measurement = "nifty_option_ticks";
        String tpl = """
from(bucket: \"%s\")
  |> range(start: %s)
  |> filter(fn: (r) => r._measurement == \"%s\" and r.instrumentKey == \"%s\" and r._field == \"%s\")
  |> aggregateWindow(every: %s, fn: %s, createEmpty: false)
  |> keep(columns: [\"_time\",\"_value\"])
  |> rename(columns: {_value: \"%s\"})
""";
        merge(queryApi.query(String.format(tpl, influxBucket, start, measurement, key, "ltp", gran, "last", "c"), influxOrg), "c", map);
        merge(queryApi.query(String.format(tpl, influxBucket, start, measurement, key, "ltp", gran, "first", "o"), influxOrg), "o", map);
        merge(queryApi.query(String.format(tpl, influxBucket, start, measurement, key, "ltp", gran, "max", "h"), influxOrg), "h", map);
        merge(queryApi.query(String.format(tpl, influxBucket, start, measurement, key, "ltp", gran, "min", "l"), influxOrg), "l", map);
        merge(queryApi.query(String.format(tpl, influxBucket, start, measurement, key, "volume", gran, "sum", "v"), influxOrg), "v", map);
        List<Ohlc> list = new ArrayList<>();
        for (var e : map.entrySet()) {
            if (e.getValue().complete()) {
                list.add(e.getValue().toOhlc(key, e.getKey()));
            }
        }
        list.sort(Comparator.comparingLong(o -> o.t));
        return list;
    }

    private void merge(List<FluxTable> tables, String col, Map<Instant, Builder> map) {
        for (FluxTable table : tables) {
            for (FluxRecord rec : table.getRecords()) {
                Instant t = (Instant) rec.getValueByKey("_time");
                Builder b = map.computeIfAbsent(t, k -> new Builder());
                Object val = rec.getValueByKey(col);
                switch (col) {
                    case "o" -> b.o = toDouble(val);
                    case "h" -> b.h = toDouble(val);
                    case "l" -> b.l = toDouble(val);
                    case "c" -> b.c = toDouble(val);
                    case "v" -> b.v = toLong(val);
                }
            }
        }
    }

    private static Double toDouble(Object v) { return Optional.ofNullable(v).map(x -> ((Number)x).doubleValue()).orElse(null); }
    private static Long toLong(Object v) { return Optional.ofNullable(v).map(x -> ((Number)x).longValue()).orElse(null); }

    private static class Builder {
        Double o; Double h; Double l; Double c; Long v;
        boolean complete(){ return o!=null && h!=null && l!=null && c!=null && v!=null; }
        Ohlc toOhlc(String key, Instant ts){
            return new Ohlc(key, ts.toEpochMilli(), o, h, l, c, v);
        }
    }

    public record Ohlc(String instrumentKey,long t,double o,double h,double l,double c,long v) {}
}
