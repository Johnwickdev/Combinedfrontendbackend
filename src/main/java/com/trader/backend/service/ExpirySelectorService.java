package com.trader.backend.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.*;
import java.time.temporal.TemporalAdjusters;

/**
 * Selects the current weekly NIFTY options expiry based on
 * India trading calendar rules. All times are evaluated in
 * Asia/Kolkata (IST).
 */
@Service
@Slf4j
public class ExpirySelectorService {

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final LocalTime ROLLOVER_TIME = LocalTime.of(15, 30);

    private LocalDate currentExpiry;

    /**
     * Picks the current weekly expiry for the given instant.
     * Logs whenever the selected expiry changes.
     */
    public synchronized LocalDate pickCurrentExpiry(Instant now) {
        ZonedDateTime z = now.atZone(IST);
        LocalDate next = computeExpiry(z);
        if (currentExpiry == null || !currentExpiry.equals(next)) {
            String reason = currentExpiry == null ? "DAILY" : "ROLLOVER";
            log.info("EXPIRY current={} reason={}", next, reason);
            currentExpiry = next;
        }
        return currentExpiry;
    }

    /** Public entry for option expiry selection with explicit IST instant. */
    public LocalDate selectCurrentOptionExpiry(Instant now) {
        return pickCurrentExpiry(now);
    }

    private LocalDate computeExpiry(ZonedDateTime z) {
        DayOfWeek dow = z.getDayOfWeek();
        LocalDate date = z.toLocalDate();

        if (dow == DayOfWeek.THURSDAY) {
            if (z.toLocalTime().isAfter(ROLLOVER_TIME)) {
                return date.with(TemporalAdjusters.next(DayOfWeek.THURSDAY));
            }
            return date;
        }
        if (dow == DayOfWeek.FRIDAY) {
            return date.with(TemporalAdjusters.next(DayOfWeek.THURSDAY));
        }
        if (dow.getValue() < DayOfWeek.THURSDAY.getValue()) {
            return date.with(TemporalAdjusters.nextOrSame(DayOfWeek.THURSDAY));
        }
        // weekend
        return date.with(TemporalAdjusters.next(DayOfWeek.THURSDAY));
    }
}

