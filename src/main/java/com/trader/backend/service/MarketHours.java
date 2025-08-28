package com.trader.backend.service;

import java.time.*;

/**
 * Utility for India market hours. Assumes trading days are Monday–Friday
 * and ignores exchange holidays.
 */
public class MarketHours {
    private static final String TZ_ID = System.getenv().getOrDefault("MARKET_TZ", "Asia/Kolkata");
    private static final ZoneId TZ = ZoneId.of(TZ_ID);
    private static final LocalTime OPEN = LocalTime.parse(System.getenv().getOrDefault("MARKET_OPEN_HHMM", "09:15"));
    private static final LocalTime CLOSE = LocalTime.parse(System.getenv().getOrDefault("MARKET_CLOSE_HHMM", "15:30"));
    private static final int BUFFER_MIN = Integer.parseInt(System.getenv().getOrDefault("MARKET_BUFFER_MIN", "2"));

    public static ZoneId zone() { return TZ; }
    public static LocalTime openTime() { return OPEN; }
    public static LocalTime closeTime() { return CLOSE; }
    public static int bufferMinutes() { return BUFFER_MIN; }

    /**
     * Returns true if the given instant falls within regular market hours
     * (09:15–15:30 IST, Monday–Friday).
     */
    public static boolean isOpen(Instant now) {
        ZonedDateTime z = now.atZone(TZ);
        if (!isTradingDay(z.toLocalDate())) {
            return false;
        }
        ZonedDateTime open = z.with(OPEN).withSecond(0).withNano(0).minusMinutes(BUFFER_MIN);
        ZonedDateTime close = z.with(CLOSE).withSecond(0).withNano(0).plusMinutes(BUFFER_MIN);
        return !z.isBefore(open) && !z.isAfter(close);
    }

    /**
     * Returns the next market open instant strictly after the given instant.
     * Holidays are ignored.
     */
    public static Instant nextOpenAfter(Instant now) {
        ZonedDateTime z = now.atZone(TZ);
        ZonedDateTime candidate = z.with(OPEN).withSecond(0).withNano(0).minusMinutes(BUFFER_MIN);
        if (!z.isBefore(candidate)) {
            candidate = candidate.plusDays(1);
        }
        while (!isTradingDay(candidate.toLocalDate())) {
            candidate = candidate.plusDays(1);
        }
        return candidate.toInstant();
    }

    public static boolean isTradingDay(LocalDate date) {
        DayOfWeek dow = date.getDayOfWeek();
        return dow != DayOfWeek.SATURDAY && dow != DayOfWeek.SUNDAY;
    }
}

