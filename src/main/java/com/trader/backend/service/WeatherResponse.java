package com.trader.backend.service;

/**
 * Simple weather data holder returned by {@link WeatherService}.
 */
public record WeatherResponse(double temperature, String description) {
    /**
     * Fallback instance used when the remote API cannot be reached
     * and no cached value is available.
     */
    public static WeatherResponse unavailable() {
        return new WeatherResponse(Double.NaN, "unavailable");
    }
}
