package com.trader.backend.service;

/** Simple holder for persisted auth tokens. */
public class TokenBundle {
    public String accessToken;
    public String refreshToken;
    public long expiresAt; // epoch seconds

    public TokenBundle() {}

    public TokenBundle(String accessToken, String refreshToken, long expiresAt) {
        this.accessToken = accessToken;
        this.refreshToken = refreshToken;
        this.expiresAt = expiresAt;
    }
}
