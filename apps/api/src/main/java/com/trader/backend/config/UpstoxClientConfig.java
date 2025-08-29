package com.trader.backend.config;

import com.upstox.ApiClient;
import com.upstox.Configuration;
import com.upstox.auth.OAuth;
import org.springframework.context.annotation.Bean;


@org.springframework.context.annotation.Configuration
public class UpstoxClientConfig {

    @Bean
    public ApiClient upstoxApiClient() {
        // Just create once; we’ll inject the token later
        ApiClient client = Configuration.getDefaultApiClient();
        client.setBasePath("https://api.upstox.com/v2");
        return client;
    }
}
