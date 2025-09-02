package com.trader.backend.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration for Liquidity Wall Persistence (LWP-lite).
 */
@ConfigurationProperties(prefix = "lwp")
public class LwpProperties {
    /** master switch */
    private boolean enabled = true;
    /** sampling window in milliseconds */
    private int windowMs = 100;
    /** wall reappearance window */
    private int reappearWindowMs = 400;
    /** required reappearances */
    private int persistenceCount = 3;
    /** active duration before signal emission */
    private int persistActiveMs = 800;
    /** multiplier over median depth to qualify as wall */
    private double wallThreshold = 5.0;
    /** maximum depth levels considered */
    private int maxLevels = 5;

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public int getWindowMs() { return windowMs; }
    public void setWindowMs(int windowMs) { this.windowMs = windowMs; }

    public int getReappearWindowMs() { return reappearWindowMs; }
    public void setReappearWindowMs(int reappearWindowMs) { this.reappearWindowMs = reappearWindowMs; }

    public int getPersistenceCount() { return persistenceCount; }
    public void setPersistenceCount(int persistenceCount) { this.persistenceCount = persistenceCount; }

    public int getPersistActiveMs() { return persistActiveMs; }
    public void setPersistActiveMs(int persistActiveMs) { this.persistActiveMs = persistActiveMs; }

    public double getWallThreshold() { return wallThreshold; }
    public void setWallThreshold(double wallThreshold) { this.wallThreshold = wallThreshold; }

    public int getMaxLevels() { return maxLevels; }
    public void setMaxLevels(int maxLevels) { this.maxLevels = maxLevels; }
}
