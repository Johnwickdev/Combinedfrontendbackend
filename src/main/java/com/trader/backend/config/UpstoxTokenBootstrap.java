package com.trader.backend.config;

import com.upstox.ApiClient;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class UpstoxTokenBootstrap {

    private final ApiClient apiClient;

    @Value("${upstox.accessToken}")
    private String accessToken;

    public UpstoxTokenBootstrap(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    @PostConstruct
    public void init() {
        // safest way: set default header directly
        apiClient.addDefaultHeader("Authorization", "Bearer " + accessToken);
        log.info("✅ Sandbox access-token attached as default header: {}", accessToken);
    }

}
