package com.trader.backend.service;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link WeatherService}'s fallback behaviour.
 */
public class WeatherServiceTest {

    @Test
    void returnsDefaultWhenApiFails() {
        WebClient client = WebClient.builder()
                .exchangeFunction(req -> Mono.error(new RuntimeException("boom")))
                .build();

        WeatherService svc = new WeatherService(client);
        WeatherResponse r = svc.currentWeather(0, 0).block();

        assertEquals("unavailable", r.description());
        assertTrue(Double.isNaN(r.temperature()));
    }

    @Test
    void cachesLastSuccessfulResponse() {
        AtomicInteger calls = new AtomicInteger();
        ExchangeFunction fn = req -> {
            if (calls.getAndIncrement() == 0) {
                return Mono.just(ClientResponse.create(HttpStatus.OK)
                        .header("Content-Type", "application/json")
                        .body("{\"temperature\":25.0,\"description\":\"Sunny\"}")
                        .build());
            }
            return Mono.error(new RuntimeException("API down"));
        };

        WebClient client = WebClient.builder().exchangeFunction(fn).build();
        WeatherService svc = new WeatherService(client);

        WeatherResponse first = svc.currentWeather(0, 0).block();
        WeatherResponse second = svc.currentWeather(0, 0).block();

        assertEquals(2, calls.get());
        assertEquals(25.0, first.temperature());
        assertEquals("Sunny", first.description());
        assertEquals(first, second, "Should return cached value after failure");
    }
}
