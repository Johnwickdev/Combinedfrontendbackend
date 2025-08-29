package com.trader.backend.config;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InfluxConfig {

    @Value("${influx.url:}")
    private String url;

    @Value("${influx.token:}")
    private String token;

    @Value("${influx.org:}")
    private String org;

    @Value("${influx.bucket:}")
    private String bucket;

    /**
     * Create a singleton InfluxDBClient that knows your URL, token, org & bucket.
     */
    @Bean
    @ConditionalOnProperty(prefix = "influx", name = "url")
    public InfluxDBClient influxDBClient() {
        // note: token must be passed as char[] for security
        return InfluxDBClientFactory.create(url, token.toCharArray(), org, bucket);
    }

    /**
     * Expose the WriteApiBlocking so you can inject it into your service.
     */
    @Bean
    @ConditionalOnBean(InfluxDBClient.class)
    public WriteApiBlocking writeApi(InfluxDBClient influxDBClient) {
        return influxDBClient.getWriteApiBlocking();
    }
}
