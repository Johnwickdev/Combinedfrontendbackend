package com.trader.backend.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class CorsConfig implements WebMvcConfigurer {

    @Value("${CORS_ORIGINS:*}")
    private String origins;

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        String[] allowed = origins.split(",");
        registry.addMapping("/**")
            .allowedOriginPatterns(allowed)
            .allowedMethods("*")
            .allowedHeaders("*")
            .exposedHeaders("X-CORS-Check")
            .allowCredentials(true);
    }
}

