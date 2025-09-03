package com.trader.backend.config;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class StartupConfig {

    @Value("${SPRING_DATA_MONGODB_URI:}")
    private String mongoUri;

    @Value("${influx.url:}")
    private String influxUrl;

    @Value("${influx.token:}")
    private String influxToken;

    @Value("${influx.org:}")
    private String influxOrg;

    @Value("${influx.bucket:}")
    private String influxBucket;

    @Value("${APP_CORS_ALLOWED_ORIGINS:}")
    private String corsOrigins;

    @Value("${APP_MOCK:false}")
    private boolean mock;

    @PostConstruct
    public void boot() {
        String mongoPresence = (mongoUri != null && !mongoUri.isBlank()) ? "present" : "absent";
        log.info("BOOT Config: mongodb.uri={} influx.org={} bucket={} cors.origins={} mock={}",
                mongoPresence,
                influxOrg,
                influxBucket,
                corsOrigins,
                mock);

        if (mongoUri == null || mongoUri.isBlank()) {
            log.error("Missing MongoDB URI");
            System.exit(0);
        }
        boolean influxOk = influxUrl != null && !influxUrl.isBlank()
                && influxToken != null && !influxToken.isBlank()
                && influxOrg != null && !influxOrg.isBlank()
                && influxBucket != null && !influxBucket.isBlank();
        if (!influxOk) {
            log.warn("Influx disabled: writing skipped");
        }
    }
}
