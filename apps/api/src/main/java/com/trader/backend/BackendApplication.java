package com.trader.backend;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.trader.backend.config.SignalsProperties;

@SpringBootApplication(scanBasePackages = "com.trader.backend")
@EnableScheduling
@EnableConfigurationProperties(SignalsProperties.class)
public class BackendApplication {

    public static void main(String[] args) {
        SpringApplication.run(BackendApplication.class, args);
    }

}
