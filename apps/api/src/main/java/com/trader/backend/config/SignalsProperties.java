package com.trader.backend.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration knobs for depth-based signal generation.
 */
@ConfigurationProperties(prefix = "signals")
public class SignalsProperties {
    /** maximum depth levels to consider per side */
    private int maxLevels = 5;
    /** sampling window in milliseconds */
    private int windowMs = 100;
    /** required persistence in milliseconds */
    private int persistMs = 800;
    /** master switch for engine */
    private boolean enabled = true;

    public int getMaxLevels() {
        return maxLevels;
    }
    public void setMaxLevels(int maxLevels) {
        this.maxLevels = maxLevels;
    }
    public int getWindowMs() {
        return windowMs;
    }
    public void setWindowMs(int windowMs) {
        this.windowMs = windowMs;
    }
    public int getPersistMs() {
        return persistMs;
    }
    public void setPersistMs(int persistMs) {
        this.persistMs = persistMs;
    }
    public boolean isEnabled() {
        return enabled;
    }
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /** number of consecutive windows required */
    public int getPersistWindows() {
        return persistMs / windowMs;
    }
}
