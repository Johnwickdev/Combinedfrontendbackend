package com.trader.backend.events;


import org.springframework.context.ApplicationEvent;

public class FilteredPremiumsUpdatedEvent extends ApplicationEvent {
    private final long expiryEpochMs;   // the expiry you saved for
    private final int count;            // how many docs saved

    public FilteredPremiumsUpdatedEvent(Object source, long expiryEpochMs, int count) {
        super(source);
        this.expiryEpochMs = expiryEpochMs;
        this.count = count;
    }

    public long getExpiryEpochMs() { return expiryEpochMs; }
    public int getCount() { return count; }
}
