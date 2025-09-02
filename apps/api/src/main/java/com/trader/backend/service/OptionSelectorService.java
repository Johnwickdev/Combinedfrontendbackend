package com.trader.backend.service;

import com.trader.backend.entity.Instrument;
import com.trader.backend.entity.Selection;
import com.trader.backend.events.TickEvent;
import com.trader.backend.repository.InstrumentRepository;
import com.trader.backend.repository.SelectionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
public class OptionSelectorService {
    private final InstrumentRepository instrumentRepository;
    private final SelectionRepository selectionRepository;
    private final UpstoxFeedV3Client feed;

    @Value("${ATM_STEP:50}")
    private int atmStep;

    private Double lastAtm = null;

    @EventListener
    public void onTick(TickEvent event) {
        if (!feed.symbols().contains(event.instrumentKey())) {
            return; // ignore non-subscribed
        }
        // compute ATM for future ticks only
        if (!event.instrumentKey().contains("FUT")) {
            return;
        }
        double ltp = event.ltp();
        double atm = Math.round(ltp / atmStep) * atmStep;
        if (lastAtm != null && Math.abs(atm - lastAtm) < atmStep * 0.5) {
            return;
        }
        lastAtm = atm;
        LocalDate today = Instant.now().atZone(ZoneId.of("Asia/Kolkata")).toLocalDate();
        Optional<Instrument> ce = instrumentRepository
                .findFirstBySymbolAndStrikeAndOptTypeAndExpiryAfterOrderByExpiry("NIFTY", atm, "CE", today.minusDays(1));
        Optional<Instrument> pe = instrumentRepository
                .findFirstBySymbolAndStrikeAndOptTypeAndExpiryAfterOrderByExpiry("NIFTY", atm, "PE", today.minusDays(1));
        if (ce.isEmpty() || pe.isEmpty()) {
            return;
        }
        Selection sel = new Selection();
        sel.setBaseKey(event.instrumentKey());
        sel.setBaseLtp(ltp);
        sel.setAtmStrike(atm);
        sel.setCeKey(ce.get().getKey());
        sel.setPeKey(pe.get().getKey());
        sel.setCreatedAt(Instant.now());
        selectionRepository.save(sel);
        log.info("[atm] base={} ltp={} atm={} ce={} pe={}", event.instrumentKey(), ltp, atm, sel.getCeKey(), sel.getPeKey());
        feed.subscribe(sel.getCeKey());
        feed.subscribe(sel.getPeKey());
    }
}
