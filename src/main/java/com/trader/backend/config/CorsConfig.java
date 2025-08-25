package com.trader.backend.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.web.filter.CorsFilter;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import java.util.List;
@ComponentScan
@Configuration
@EnableWebMvc
public class CorsConfig {

    @Value("${FRONTEND_ORIGIN:https://frontendfortheautobot-7u7woqq2m-johnwicks-projects-025aea65.vercel.app}")
    private String frontendOrigin;

    @Bean
    public CorsFilter corsFilter() {
        CorsConfiguration config = new CorsConfiguration();
        config.setAllowCredentials(true);

        // Allow the deployed frontend and local development
        config.setAllowedOrigins(List.of(frontendOrigin, "http://localhost:4200"));

        config.setAllowedHeaders(List.of("*"));
        config.setAllowedMethods(List.of("GET", "POST", "PUT", "DELETE", "OPTIONS"));

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/auth/url", config);
        source.registerCorsConfiguration("/auth/status", config);

        return new CorsFilter(source);
    }
}
