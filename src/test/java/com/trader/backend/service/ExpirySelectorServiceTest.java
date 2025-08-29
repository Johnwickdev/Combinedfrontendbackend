package com.trader.backend.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.*;

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
        Instant monday = LocalDate.of(2024, 1, 1).atTime(10, 0).atZone(IST).toInstant();
        assertEquals(LocalDate.of(2024, 1, 4), service.pickCurrentExpiry(monday));
    }

    @Test
    void thursdayBeforeCloseUsesToday() {
        Instant thursday = LocalDate.of(2024, 1, 4).atTime(10, 0).atZone(IST).toInstant();
        assertEquals(LocalDate.of(2024, 1, 4), service.pickCurrentExpiry(thursday));
    }

    @Test
    void thursdayAfterCloseRollsOver() {
        Instant thursday = LocalDate.of(2024, 1, 4).atTime(16, 0).atZone(IST).toInstant();
        assertEquals(LocalDate.of(2024, 1, 11), service.pickCurrentExpiry(thursday));
    }

    @Test
    void fridayUsesNextThursday() {
        Instant friday = LocalDate.of(2024, 1, 5).atTime(10, 0).atZone(IST).toInstant();
        assertEquals(LocalDate.of(2024, 1, 11), service.pickCurrentExpiry(friday));
    }

    @Test
    void monthlyRolloverHandled() {
        // 25 April 2024 is the last Thursday of the month
        Instant afterMonthly = LocalDate.of(2024, 4, 26).atTime(10, 0).atZone(IST).toInstant();
        assertEquals(LocalDate.of(2024, 5, 2), service.pickCurrentExpiry(afterMonthly));
    }
}

