package com.trader.backend.controller;


import com.trader.backend.entity.NiftyPriceDTO;
import com.trader.backend.service.LiveFeedService;
import com.trader.backend.service.NseInstrumentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequestMapping("/api/nse")
@RequiredArgsConstructor
@Slf4j
public class NseInstrumentController {

    private final NseInstrumentService nseInstrumentService;
    private final LiveFeedService liveFeedService;

    @PostMapping("/load")
    public Mono<ResponseEntity<String>> loadNseJsonToMongo() {
        log.info("🚀 /api/nse/load called. Filtering and saving NIFTY CE/PE...");
        nseInstrumentService.saveFilteredInstrumentsToMongo();
        return Mono.just(ResponseEntity.ok("Filtered NIFTY CE/PE saved to MongoDB ✅"));
    }
    @PostMapping("/filter-strikes")
    public Mono<ResponseEntity<String>> filterStrikePrices(@RequestBody NiftyPriceDTO request) {
        log.info("🎯 Received Nifty LTP: {}", request.getNiftyLtp());
        nseInstrumentService.filterAndSaveStrikesAroundLtp(request.getNiftyLtp());
        return Mono.just(ResponseEntity.ok("✅ Filtered 15 CE + 15 PE instruments saved."));
    }
    @PostMapping("/set-nifty-ltp")
    public Mono<ResponseEntity<String>> setNiftyLtpAndFilterStrikes(@RequestParam double niftyLtp) {
        log.info("📈 Received Nifty 50 LTP: {}", niftyLtp);
        nseInstrumentService.filterStrikesAroundLtp(niftyLtp);
        return Mono.just(ResponseEntity.ok("✅ Filtered CE/PE around Nifty LTP saved"));
    }
    @PostMapping("/load-nifty-options")
    public Mono<ResponseEntity<String>> loadNiftyOptionsFromJson() {
        nseInstrumentService.saveAllNiftyOptionsFromJson();
        return Mono.just(ResponseEntity.ok("✅ All NIFTY CE/PE saved to MongoDB"));
    }
    @GetMapping("/get-live-keys")
    public Mono<ResponseEntity<List<String>>> getLiveInstrumentKeys() {
        List<String> keys = nseInstrumentService.getInstrumentKeysForLiveSubscription();
        return Mono.just(ResponseEntity.ok(keys));
    }
    @PostMapping("/start-filtered-stream")
    public ResponseEntity<String> startFilteredCEPEStream() {
        liveFeedService.streamFilteredNiftyOptions();
        return ResponseEntity.ok("📡 Started live stream for filtered CE/PE instruments.");
    }
@PostMapping("/nifty-fut-stream")
public ResponseEntity<String> startNiftyFutStream() {
    liveFeedService.streamNiftyFutAndTriggerCEPE();
    return ResponseEntity.ok("📡 NIFTY FUT stream started");
}
@PostMapping("/save-nifty-futures")
public Mono<ResponseEntity<String>> saveNiftyFutures() {
    log.info("🚀 Triggered /api/nse/save-nifty-futures");
    nseInstrumentService.saveNiftyFuturesToMongo();
    return Mono.just(ResponseEntity.ok("✅ NIFTY FUTURES saved to MongoDB"));
}
@GetMapping("/nifty-future-ltp")
public Mono<ResponseEntity<Double>> getNiftyFutureLtp() {
    return nseInstrumentService.getNearestExpiryNiftyFutureLtp()
            .map(ResponseEntity::ok)
            .onErrorResume(e -> {
                log.error("❌ Failed to fetch NIFTY FUT LTP", e);
                return Mono.just(ResponseEntity.status(500).build());
            });
}
@PostMapping("/nifty-fut-auto-stream")
public ResponseEntity<String> autoStreamWithLtpAndSubscribe() {
    liveFeedService.streamNiftyFutAndTriggerCEPE();
    return ResponseEntity.ok("📡 NIFTY FUT LTP extracted, CE/PE filtered, and live stream started ✅");
}
    @PostMapping("/refresh-current-week")
public ResponseEntity<String> refreshCurrentWeek() {
    // Uses IST Fri→Wed rule to keep only this Wednesday’s expiry in nse_instruments
    nseInstrumentService.refreshNiftyOptionsByNearestExpiryFromJson();
    return ResponseEntity.ok("✅ Refreshed nse_instruments for THIS WEEK (Fri→Wed IST)");
}

@PostMapping("/purge-expired")
public ResponseEntity<String> purgeExpired() {
    nseInstrumentService.purgeExpiredOptionDocs();
    return ResponseEntity.ok("🧹 Purged expired docs from nse_instruments & filtered_nifty_premiums");
}

}
