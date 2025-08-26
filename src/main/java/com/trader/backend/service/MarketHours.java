package com.trader.backend.service;

import java.time.*;

/**
 * Utility for India market hours. Assumes trading days are Monday–Friday
 * and ignores exchange holidays.
 */
public class MarketHours {
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final LocalTime OPEN = LocalTime.of(9, 15);
    private static final LocalTime CLOSE = LocalTime.of(15, 30);

    /**
     * Returns true if the given instant falls within regular market hours
     * (09:15–15:30 IST, Monday–Friday).
     */
    public static boolean isOpen(Instant now) {
        ZonedDateTime z = now.atZone(IST);
        DayOfWeek dow = z.getDayOfWeek();
        if (dow == DayOfWeek.SATURDAY || dow == DayOfWeek.SUNDAY) {
            return false;
        }
        LocalTime t = z.toLocalTime();
        return !t.isBefore(OPEN) && !t.isAfter(CLOSE);
    }

    /**
     * Returns the next market open instant strictly after the given instant.
     * Holidays are ignored.
     */
    public static Instant nextOpenAfter(Instant now) {
        ZonedDateTime z = now.atZone(IST);
        ZonedDateTime candidate = z.with(OPEN);
        if (!z.toLocalTime().isBefore(OPEN)) {
            candidate = candidate.plusDays(1);
        }
        while (candidate.getDayOfWeek() == DayOfWeek.SATURDAY ||
               candidate.getDayOfWeek() == DayOfWeek.SUNDAY) {
            candidate = candidate.plusDays(1);
        }
        return candidate.toInstant();
    }
}

