package com.trader.backend.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for numeric casting helpers in {@link CandleService}.
 */
class CandleServiceTest {

    @Test
    void testToDoubleAndLong() {
        assertEquals(1.5d, CandleService.toDouble(1.5d));
        assertEquals(2.0d, CandleService.toDouble(2));
        assertNull(CandleService.toDouble(null));

        assertEquals(5L, CandleService.toLong(5));
        assertEquals(7L, CandleService.toLong(7.8d));
        assertNull(CandleService.toLong(null));
    }
}

