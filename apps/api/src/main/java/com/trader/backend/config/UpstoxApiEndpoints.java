package com.trader.backend.config;

/**
 * Central place for Upstox API base URLs. Upstox requires that
 * OAuth and most REST endpoints use the v2 base URL while
 * market data feeds are served from v3 endpoints.
 */
public final class UpstoxApiEndpoints {
    private UpstoxApiEndpoints() {}

    /** Base URL for Upstox v2 REST APIs including OAuth. */
    public static final String API_V2_BASE_URL = "https://api.upstox.com/v2";

    /** Base URL for Upstox v3 APIs such as the market data feed. */
    public static final String API_V3_BASE_URL = "https://api.upstox.com/v3";

    /** WebSocket base URL for the v3 market data feed. */
    public static final String FEED_WS_V3_BASE_URL = "wss://api.upstox.com/v3/feed/market-data-feed";
}
