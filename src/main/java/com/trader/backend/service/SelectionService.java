package com.trader.backend.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class SelectionService {
    private final NseInstrumentService nseInstrumentService;

    public String getMainFutureKey() {
        return nseInstrumentService.nearestNiftyFutureKey().orElse(null);
    }
}
