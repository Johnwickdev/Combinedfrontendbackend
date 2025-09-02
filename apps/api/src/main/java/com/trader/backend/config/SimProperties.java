package com.trader.backend.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Simulation mode configuration. When enabled, order book ticks are replayed
 * from a dataset instead of a live broker feed.
 */
@ConfigurationProperties(prefix = "sim")
public class SimProperties {
    private boolean enabled = true;
    private String dataset = "classpath:/ticks/lwp_synthetic.json";

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public String getDataset() { return dataset; }
    public void setDataset(String dataset) { this.dataset = dataset; }
}
