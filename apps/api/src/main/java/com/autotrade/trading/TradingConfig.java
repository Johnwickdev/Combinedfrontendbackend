package com.autotrade.trading;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TradingConfig {
    @Bean
    public OrderRouter orderRouter() {
        return new LoggingOrderRouter();
    }

    @Bean
    public RiskManager riskManager() {
        return new RiskManager(5000.0, 3, 1);
    }
}
