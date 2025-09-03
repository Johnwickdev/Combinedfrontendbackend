package com.autotrade.trading;

import java.time.LocalDate;
import java.time.ZoneId;

public class RiskManager {
    private final double dailyLossCap;
    private final int maxOpenPositions;
    private final int maxQuantityPerOrder;

    private double dailyPnl = 0.0;
    private int openPositions = 0;
    private LocalDate lastReset = LocalDate.now(ZoneId.of("Asia/Kolkata"));

    public RiskManager(double dailyLossCap, int maxOpenPositions, int maxQuantityPerOrder) {
        this.dailyLossCap = dailyLossCap;
        this.maxOpenPositions = maxOpenPositions;
        this.maxQuantityPerOrder = maxQuantityPerOrder;
    }

    public synchronized boolean canEnter(int quantity) {
        resetIfNewDay();
        if (quantity > maxQuantityPerOrder) return false;
        if (openPositions >= maxOpenPositions) return false;
        if (dailyPnl <= -dailyLossCap) return false;
        return true;
    }

    public synchronized void recordEntry() {
        openPositions++;
    }

    public synchronized void recordExit(double pnl) {
        if (openPositions > 0) openPositions--;
        dailyPnl += pnl;
    }

    public synchronized void resetIfNewDay() {
        LocalDate today = LocalDate.now(ZoneId.of("Asia/Kolkata"));
        if (!today.equals(lastReset)) {
            dailyPnl = 0.0;
            openPositions = 0;
            lastReset = today;
        }
    }

    public double getDailyPnl() { return dailyPnl; }
    public int getOpenPositions() { return openPositions; }
}
