package com.trader.backend.config;

import com.upstox.ApiClient;
import com.upstox.Configuration;
import org.springframework.context.annotation.Bean;

import static com.trader.backend.config.UpstoxApiEndpoints.API_V2_BASE_URL;

@org.springframework.context.annotation.Configuration
public class UpstoxClientConfig {

    @Bean
    public ApiClient upstoxApiClient() {
        // Just create once; we’ll inject the token later
        ApiClient client = Configuration.getDefaultApiClient();
        client.setBasePath(API_V2_BASE_URL);
        return client;
    }
}
