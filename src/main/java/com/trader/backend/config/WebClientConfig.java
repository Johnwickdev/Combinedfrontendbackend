package com.trader.backend.config;



import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class WebClientConfig {

    @Bean
    @Primary
    public WebClient webClient() {
        return WebClient.builder().build();
    }

    @Bean
    @Qualifier("weatherClient")
    public WebClient weatherClient(WebClient.Builder builder) {
        return builder.baseUrl("https://api.open-meteo.com/v1/forecast").build();
    }
}
