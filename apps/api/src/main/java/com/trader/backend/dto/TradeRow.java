package com.trader.backend.dto;

import java.time.Instant;

public record TradeRow(
        Instant ts,
        String instrumentKey,
        OptionType optionType,
        int strike,
        double ltp,
        Double changePct,
        Integer qty,
        Integer oi,
        Double iv,
        String txId
) {}
