package com.trader.backend.config;

import com.trader.backend.service.UpstoxAuthService;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class StartupConfig {

    private final UpstoxAuthService auth;

    @Value("${UPSTOX_WEBHOOK_URI:}")
    private String webhookUri;

    @Value("${SPRING_DATA_MONGODB_URI:}")
    private String mongoUri;

    @Value("${INFLUX_URL:}")
    private String influxUrl;

    @Value("${INFLUX_TOKEN:}")
    private String influxToken;

    @Value("${INFLUX_ORG:}")
    private String influxOrg;

    @Value("${INFLUX_BUCKET:}")
    private String influxBucket;

    @Value("${APP_CORS_ALLOWED_ORIGINS:}")
    private String corsOrigins;

    @Value("${APP_MOCK:false}")
    private boolean mock;

    @PostConstruct
    public void boot() {
        String mongoPresence = (mongoUri != null && !mongoUri.isBlank()) ? "present" : "absent";
        log.info("BOOT Config: webhookUri={} mongodb.uri={} influx.org={} bucket={} cors.origins={} mock={}",
                webhookUri,
                mongoPresence,
                influxOrg,
                influxBucket,
                corsOrigins,
                mock);

        if (System.getenv("UPSTOX_API_KEY") == null || System.getenv("UPSTOX_API_SECRET") == null || webhookUri == null || webhookUri.isBlank()) {
            log.error("Missing Upstox credentials");
            System.exit(0);
        }
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
        auth.init();
    }
}
