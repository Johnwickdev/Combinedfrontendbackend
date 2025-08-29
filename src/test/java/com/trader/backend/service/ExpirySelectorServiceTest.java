package com.trader.backend.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.*;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ExpirySelectorServiceTest {

    private ExpirySelectorService service;
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    @BeforeEach
    void setUp() {
        service = new ExpirySelectorService();
    }

    @Test
    void mondayUsesThisThursday() {
        ZonedDateTime monday = LocalDate.of(2024, 1, 1).atTime(10, 0).atZone(IST);
        assertEquals(LocalDate.of(2024, 1, 4), service.selectCurrentOptionExpiry(monday));
    }

    @Test
    void thursdayBeforeCloseUsesToday() {
        ZonedDateTime thursday = LocalDate.of(2024, 1, 4).atTime(10, 0).atZone(IST);
        assertEquals(LocalDate.of(2024, 1, 4), service.selectCurrentOptionExpiry(thursday));
    }

    @Test
    void thursdayAfterCloseRollsOver() {
        ZonedDateTime thursday = LocalDate.of(2024, 1, 4).atTime(16, 0).atZone(IST);
        assertEquals(LocalDate.of(2024, 1, 11), service.selectCurrentOptionExpiry(thursday));
    }

    @Test
    void fridayUsesNextThursday() {
        ZonedDateTime friday = LocalDate.of(2024, 1, 5).atTime(10, 0).atZone(IST);
        assertEquals(LocalDate.of(2024, 1, 11), service.selectCurrentOptionExpiry(friday));
    }

    @Test
    void monthlyRolloverHandled() {
        // 25 April 2024 is the last Thursday of the month
        ZonedDateTime afterMonthly = LocalDate.of(2024, 4, 26).atTime(10, 0).atZone(IST);
        assertEquals(LocalDate.of(2024, 5, 2), service.selectCurrentOptionExpiry(afterMonthly));
    }

    @Test
    void pickWeeklyExpiryBeforeAndAfter1530() {
        ZoneId zone = ZoneId.of("Asia/Kolkata");
        Instant before = LocalDate.of(2024, 1, 3).atTime(15, 0).atZone(zone).toInstant();
        Instant after = LocalDate.of(2024, 1, 4).atTime(16, 0).atZone(zone).toInstant();
        long exp1 = LocalDate.of(2024, 1, 4).atTime(15, 30).atZone(zone).toInstant().toEpochMilli();
        long exp2 = LocalDate.of(2024, 1, 11).atTime(15, 30).atZone(zone).toInstant().toEpochMilli();
        List<Long> expiries = List.of(exp1, exp2);
        Instant pickedBefore = service.pickCurrentWeeklyExpiry(before, expiries, zone);
        Instant pickedAfter = service.pickCurrentWeeklyExpiry(after, expiries, zone);
        assertEquals(exp1, pickedBefore.toEpochMilli());
        assertEquals(exp2, pickedAfter.toEpochMilli());
    }
}

