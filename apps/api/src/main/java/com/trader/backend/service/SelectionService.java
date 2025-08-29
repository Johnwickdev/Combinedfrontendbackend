package com.trader.backend.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class SelectionService {
    private final NseInstrumentService nseInstrumentService;

    /**
     * Returns the instrument-key for the current month NIFTY future
     * (earliest non-expired expiry).
     */
    public String getCurrentNiftyFutureKey() {
        return nseInstrumentService.nearestNiftyFutureKey().orElse(null);
    }
}
