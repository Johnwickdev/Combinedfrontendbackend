package com.trader.backend.config;

import com.upstox.ApiClient;
import com.upstox.auth.OAuth;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
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
        System.out.println("✅  Sandbox access-token attached as default header");
    }

}
