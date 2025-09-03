package com.trader.backend;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.trader.backend.config.LwpProperties;
import com.trader.backend.config.SignalsProperties;
import com.trader.backend.config.SimProperties;

@SpringBootApplication(scanBasePackages = {"com.trader.backend", "com.autotrade.trading"})
@EnableScheduling
@EnableConfigurationProperties({SignalsProperties.class, LwpProperties.class, SimProperties.class})
public class BackendApplication {

    public static void main(String[] args) {
        SpringApplication.run(BackendApplication.class, args);
    }

}
