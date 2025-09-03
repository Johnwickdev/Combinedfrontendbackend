package com.autotrade.trading;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.PostConstruct;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.trader.backend.events.TickEvent;
import com.trader.backend.service.ExpirySelectorService;
import com.trader.backend.service.LiveFeedService;
import com.trader.backend.service.NseInstrumentService;
import com.trader.backend.service.NseInstrumentService.OptionBatch;
import com.trader.backend.service.Tick;

@Component
public class SessionOrchestrator {

    private final ExpirySelectorService expirySelectorService;
    private final NseInstrumentService nseInstrumentService;
    private final LiveFeedService liveFeedService;
    private final StrategyEngine strategyEngine;
    private final RiskManager riskManager;

    public SessionOrchestrator(ExpirySelectorService expirySelectorService,
                               NseInstrumentService nseInstrumentService,
                               LiveFeedService liveFeedService,
                               StrategyEngine strategyEngine,
                               RiskManager riskManager) {
        this.expirySelectorService = expirySelectorService;
        this.nseInstrumentService = nseInstrumentService;
        this.liveFeedService = liveFeedService;
        this.strategyEngine = strategyEngine;
        this.riskManager = riskManager;
    }

    @PostConstruct
    public void init() {
        expirySelectorService.ensureCurrentWeeklyExpiry();
        Optional<String> futKeyOpt = nseInstrumentService.getCurrentMonthNiftyFutKey();
        futKeyOpt.ifPresent(key -> {
            strategyEngine.setInstrument(key);
            liveFeedService.subscribe(key);
        });

        OptionBatch batch = nseInstrumentService.filterStrikesAroundLtpFromJson(0.0);
        List<String> optionKeys = new ArrayList<>();
        optionKeys.addAll(batch.ce().stream().map(i -> i.getInstrumentKey()).toList());
        optionKeys.addAll(batch.pe().stream().map(i -> i.getInstrumentKey()).toList());
        if (!optionKeys.isEmpty()) {
            liveFeedService.subscribe(optionKeys);
        }
    }

    @EventListener
    public void onTick(TickEvent event) {
        Tick tick = new Tick(event.instrumentKey(), event.ltp(), event.ts());
        strategyEngine.onTick(tick);
    }

    @Scheduled(cron = "0 1 0 * * *", zone = "Asia/Kolkata")
    public void resetDaily() {
        riskManager.resetIfNewDay();
    }
}
