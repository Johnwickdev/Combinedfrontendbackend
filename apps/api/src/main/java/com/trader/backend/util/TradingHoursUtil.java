package com.trader.backend.util;

import java.time.*;

/** Utility to check Indian market trading hours (09:15-15:30 IST, Mon-Fri). */
public final class TradingHoursUtil {
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final LocalTime OPEN = LocalTime.of(9, 15);
    private static final LocalTime CLOSE = LocalTime.of(15, 30);

    private TradingHoursUtil() {}

    public static boolean isMarketOpen(Instant now) {
        ZonedDateTime z = now.atZone(IST);
        DayOfWeek dow = z.getDayOfWeek();
        if (dow == DayOfWeek.SATURDAY || dow == DayOfWeek.SUNDAY) return false;
        LocalTime t = z.toLocalTime();
        return !t.isBefore(OPEN) && !t.isAfter(CLOSE);
    }

    public static boolean isMarketOpen() {
        return isMarketOpen(Instant.now());
    }
}
