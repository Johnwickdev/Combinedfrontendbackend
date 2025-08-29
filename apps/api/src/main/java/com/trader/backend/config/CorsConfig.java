package com.trader.backend.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.web.filter.CorsFilter;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import java.util.Arrays;
import java.util.List;
@ComponentScan
@Configuration
@EnableWebMvc
public class CorsConfig {

    @Value("${cors.allowedOrigins:https://combinedfrontendbackend.vercel.app,http://localhost:4200}")
    private String allowedOrigins;

    @Bean
    public CorsFilter corsFilter() {
        CorsConfiguration config = new CorsConfiguration();
        config.setAllowCredentials(false);
        config.setAllowedOrigins(Arrays.asList(allowedOrigins.split(",")));
        config.setAllowedHeaders(List.of("Content-Type"));
        config.setAllowedMethods(List.of("GET"));

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        List<String> paths = List.of(
                "/auth/**",
                "/md/selection",
                "/md/candles",
                "/md/stream",
                "/md/last-ltp",
                "/md/ltp",
                "/md/sector-trades",
                "/ops/live-status",
                "/ops/influx-sanity",
                "/ops/market-clock",
                "/ops/upstox-balance"
        );
        paths.forEach(p -> source.registerCorsConfiguration(p, config));

        return new CorsFilter(source);
    }
}
