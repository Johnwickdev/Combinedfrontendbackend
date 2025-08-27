package com.trader.backend.dto;

import java.time.Instant;

public record TradeRow(
        Instant ts,
        String instrumentKey,
        OptionType optionType,
        int strike,
        double ltp,
        double changePct,
        int qty,
        Integer oi,
        String txId
) {}
