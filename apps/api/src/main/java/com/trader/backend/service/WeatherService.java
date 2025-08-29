package com.trader.backend.service;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Retrieves weather information from a remote API.  The service keeps the last
 * successful response in memory and falls back to it whenever the remote call
 * fails.  If no cached value exists a minimal {@link WeatherResponse#unavailable()}
 * instance is returned instead.
 */
@Service
@RequiredArgsConstructor
public class WeatherService {

    private final @Qualifier("weatherClient") WebClient webClient;
    private final AtomicReference<WeatherResponse> cache = new AtomicReference<>();

    /**
     * Fetches current weather for the given coordinates.
     *
     * @param lat latitude
     * @param lon longitude
     * @return a mono emitting the latest weather information.  In case of an
     * API error or timeout, the previously cached value (if any) is returned;
     * otherwise {@link WeatherResponse#unavailable()} is emitted.
     */
    public Mono<WeatherResponse> currentWeather(double lat, double lon) {
        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .queryParam("latitude", lat)
                        .queryParam("longitude", lon)
                        .queryParam("current_weather", true)
                        .build())
                .retrieve()
                .onStatus(HttpStatusCode::isError, resp -> Mono.error(new IllegalStateException("API error")))
                .bodyToMono(WeatherResponse.class)
                .doOnNext(cache::set)
                .timeout(Duration.ofSeconds(5))
                .onErrorResume(ex -> {
                    WeatherResponse last = cache.get();
                    return last != null ? Mono.just(last) : Mono.just(WeatherResponse.unavailable());
                });
    }
}
