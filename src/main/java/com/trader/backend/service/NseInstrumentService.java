package com.trader.backend.service;
    import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.http.HttpHeaders;
import reactor.core.publisher.Mono;
import org.springframework.beans.factory.annotation.Value;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.trader.backend.entity.NseInstrument;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Criteria;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trader.backend.entity.NseInstrument;
import com.trader.backend.repository.NseInstrumentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;
@RequiredArgsConstructor
@Slf4j
@Service
public class NseInstrumentService {
    private final WebClient webClient;
    private final UpstoxAuthService upstoxAuthService;
    private final NseInstrumentRepository repo;
    private final ObjectMapper mapper;
    private final MongoTemplate mongoTemplate;

    public void saveFilteredInstrumentsToMongo() {
        try {
            log.info("📂 Reading NSE.json for filtering...");
            File jsonFile = new File("src/main/resources/data/NSE.json");

            List<NseInstrument> allInstruments = Arrays.asList(mapper.readValue(jsonFile, NseInstrument[].class));

            log.info("🔍 Filtering only NIFTY CE/PE between ₹10 and ₹20 (simulated)...");

            List<NseInstrument> filtered = allInstruments.stream()
                    .filter(ins -> "NSE_FO".equals(ins.getSegment()))
                    .filter(ins -> "NIFTY".equals(ins.getName()))
                    .filter(ins -> {
                        String type = ins.getInstrumentType();
                        return "CE".equals(type) || "PE".equals(type);
                    })
                    .filter(ins -> {
                        // NOTE: Actual premium filtering must use live LTP; here we just simulate by strike range
                        double strike = ins.getStrikePrice();
                        return strike >= 23000 && strike <= 24000;
                    })
                    .collect(Collectors.toList());

            log.info("💾 Saving filtered instruments: {}", filtered.size());
            repo.saveAll(filtered);
            log.info("✅ Done saving filtered NIFTY CE/PE instruments to MongoDB.");

        } catch (Exception e) {
            log.error("❌ Failed during filtering/saving: ", e);
        }
    }


public void filterAndSaveStrikesAroundLtp(double niftyLtp) {
    try {
        log.info("📊 Calculating strike range around LTP: {}", niftyLtp);

        double baseStrike = Math.round(niftyLtp / 50.0) * 50;

        // 🎯 Create set of target strikes from baseStrike - 750 to +750 (15 each side)
        Set<Double> targetStrikes = new HashSet<>();
        for (int i = -15; i <= 15; i++) {
            targetStrikes.add(baseStrike + (i * 50));
        }

        // 🔍 Load NSE.json and parse
        InputStream jsonStream = new FileInputStream("src/main/resources/data/NSE.json"); // 🔄 Fixed path
        ObjectMapper mapper = new ObjectMapper();
        List<NseInstrument> all = Arrays.asList(mapper.readValue(jsonStream, NseInstrument[].class));

        // ⚙️ Filter CE and PE instruments for NIFTY
        List<NseInstrument> filtered = all.stream()
            .filter(i -> "NSE_INDEX|Nifty 50".equals(i.getUnderlying_key()))
            .filter(i -> "CE".equals(i.getInstrumentType()) || "PE".equals(i.getInstrumentType()))
            .filter(i -> targetStrikes.contains(i.getStrikePrice()))
            .collect(Collectors.toList());

        // 💾 Save to MongoDB
        log.info("💾 Saving {} instruments to 'filtered_nifty_premiums' collection...", filtered.size());
        mongoTemplate.dropCollection("filtered_nifty_premiums");
        mongoTemplate.insert(filtered, "filtered_nifty_premiums");
        log.info("✅ CE/PE filtering and save complete ✅");

    } catch (Exception e) {
        log.error("❌ Error during CE/PE strike filtering and saving", e);
    }
}
    public void filterStrikesAroundLtp(double niftyLtp) {
        log.info("🔍 [STEP 1] Start filtering strikes around Nifty LTP: {}", niftyLtp);

        // Round to nearest 50
        double baseStrike = Math.round(niftyLtp / 50.0) * 50;
   /*     Set<Double> strikeSet = new HashSet<>();
        for (int i = -15; i <= 15; i++) {
            strikeSet.add(baseStrike + (i * 50));
        }*/
        Set<Integer> strikeSet = new HashSet<>();
        for (int i = -15; i <= 15; i++) {
            strikeSet.add((int) (baseStrike + (i * 50)));
        }

        log.info("🎯 Strike price range: {}", strikeSet);

        // Find nearest expiry
        Query expiryQuery = new Query();
        expiryQuery.addCriteria(Criteria.where("underlying_key").is("NSE_INDEX|Nifty 50"));
        expiryQuery.with(Sort.by(Sort.Direction.ASC, "expiry"));
        expiryQuery.limit(1);

        NseInstrument nearest = mongoTemplate.findOne(expiryQuery, NseInstrument.class, "nse_instruments");

        if (nearest == null) {
            log.warn("⚠️ No instruments found with underlying_key=NSE_INDEX|Nifty 50 to detect nearest expiry.");
            return;
        }

        long nearestExpiry = nearest.getExpiry();
        log.info("📆 Nearest expiry detected: {}", nearestExpiry);

        // Filter instruments
        Query query = new Query();
        query.addCriteria(Criteria.where("underlying_key").is("NSE_INDEX|Nifty 50")
                .and("segment").is("NSE_FO")
                .and("instrumentType").in("CE", "PE")
                .and("strikePrice").in(strikeSet)
                .and("expiry").is(nearestExpiry)
        );

        /*List<NseInstrument> filtered = mongoTemplate.find(query, NseInstrument.class, "nse_instruments");
        filtered.forEach(i ->
                log.info("🎯 Instrument: {} | Strike: {} | Expiry: {}", i.getInstrument_key(), i.getStrikePrice(), i.getExpiry())
        );

        log.info("🔎 Total instruments matched: {}", filtered.size());*/
        List<NseInstrument> filtered = mongoTemplate.find(query, NseInstrument.class, "nse_instruments");

// Separate CE and PE by strike
        Map<Double, NseInstrument> ceMap = new LinkedHashMap<>();
        Map<Double, NseInstrument> peMap = new LinkedHashMap<>();

        for (NseInstrument inst : filtered) {
            double strike = inst.getStrikePrice();
            if ("CE".equals(inst.getInstrumentType()) && !ceMap.containsKey(strike)) {
                ceMap.put(strike, inst);
            }
            if ("PE".equals(inst.getInstrumentType()) && !peMap.containsKey(strike)) {
                peMap.put(strike, inst);
            }
        }

// Take first 15 CE + 15 PE
        List<NseInstrument> selected = new ArrayList<>();
        selected.addAll(ceMap.values().stream().limit(15).toList());
        selected.addAll(peMap.values().stream().limit(15).toList());

        log.info("🎯 Final selected instruments (15 CE + 15 PE): {}", selected.size());
        selected.forEach(i ->
                log.info("📌 {} | Strike: {} | Type: {}", i.getInstrument_key(), i.getStrikePrice(), i.getInstrumentType())
        );

// Save
        if (!selected.isEmpty()) {
            mongoTemplate.dropCollection("filtered_nifty_premiums");
            mongoTemplate.insert(selected, "filtered_nifty_premiums");
            log.info("✅ Saved {} filtered instruments to collection: filtered_nifty_premiums", selected.size());
        }


        if (!filtered.isEmpty()) {
            // Optional: clean up existing if any
            if (mongoTemplate.collectionExists("filtered_nifty_premiums")) {
                mongoTemplate.dropCollection("filtered_nifty_premiums");
            }
            mongoTemplate.insert(filtered, "filtered_nifty_premiums");
            log.info("✅ Saved {} filtered instruments to collection: filtered_nifty_premiums", filtered.size());
        } else {
            log.warn("⚠️ No instruments matched — collection not created/overwritten.");
        }
    }

    public void saveAllNiftyOptionsFromJson() {
        log.info("📂 Reading NSE.json to save all NIFTY CE/PE options...");

        try {
            File jsonFile = new File("src/main/resources/data/NSE.json");
            List<NseInstrument> all = mapper.readValue(jsonFile, new TypeReference<>() {});

            // Filter only CE/PE where underlying is Nifty 50
            List<NseInstrument> niftyOptions = all.stream()
                    .filter(inst ->
                            "NSE_INDEX|Nifty 50".equals(inst.getUnderlying_key()) &&
                                    "NSE_FO".equals(inst.getSegment()) &&
                                    ("CE".equals(inst.getInstrumentType()) || "PE".equals(inst.getInstrumentType()))
                    )
                    .toList();

            log.info("✅ Total NIFTY CE/PE instruments found: {}", niftyOptions.size());

            mongoTemplate.dropCollection("nse_instruments");
            mongoTemplate.insertAll(niftyOptions);
            log.info("💾 Saved to 'nse_instruments' collection.");

        } catch (IOException e) {
            log.error("❌ Error reading NSE.json or saving to MongoDB", e);
        }
    }
    public List<String> getInstrumentKeysForLiveSubscription() {
        log.info("📡 Fetching instrument keys from filtered_nifty_premiums...");
        List<NseInstrument> instruments = mongoTemplate.findAll(NseInstrument.class, "filtered_nifty_premiums");
        List<String> instrumentKeys = instruments.stream()
                .map(NseInstrument::getInstrument_key)
                .collect(Collectors.toList());

        log.info("✅ Total instrument keys fetched: {}", instrumentKeys.size());
        return instrumentKeys;
    }
public void saveNiftyFuturesToMongo() {
    log.info("📂 Extracting NIFTY FUTURE records from NSE.json...");

    try {
        File jsonFile = new File("src/main/resources/data/NSE.json");
        List<NseInstrument> all = mapper.readValue(jsonFile, new TypeReference<>() {});
for (NseInstrument i : all){
    if(i.getName() !=null) i.setName(i.getName().trim());
    if (i.getSegment() !=null) i.setSegment(i.getSegment().trim());
    if (i.getInstrumentType() !=null) i.setInstrumentType(i.getInstrumentType().trim());
    if (i.getUnderlying_key() !=null) i.setUnderlying_key(i.getUnderlying_key().trim());
}
        // Step 1: Filter valid NIFTY FUTs
        List<NseInstrument> niftyFutures = all.stream()
                .filter(inst -> "FUT".equalsIgnoreCase(inst.getInstrumentType()))
                .filter(inst -> "NSE_FO".equalsIgnoreCase(inst.getSegment()))
                .filter(inst -> "NIFTY".equalsIgnoreCase(inst.getName()))
                .filter(inst -> inst.getLot_size() == 75)
                .filter(inst -> "NSE_INDEX|Nifty 50".equals(inst.getUnderlying_key()))
                .sorted(Comparator.comparingLong(NseInstrument::getExpiry))
                .toList();

        log.info("First record: name={}, type={}, segment={}, underlaying_key={}",all.get(0).getName(),all.get(0).getInstrumentType(),all.get(0).getSegment(),all.get(0).getUnderlying_key());

        log.info("✅ Found {} NIFTY FUT contracts with lot size 75", niftyFutures.size());
        niftyFutures.forEach(i ->
                log.info("📄 {} | expiry={} | key={}", i.getTrading_symbol(), i.getExpiry(), i.getInstrument_key())

        );

        // Step 2: Save to separate collection
        if (!niftyFutures.isEmpty()) {
            mongoTemplate.dropCollection("nifty_futures");
            mongoTemplate.insert(niftyFutures, "nifty_futures");
            log.info("💾 Saved to MongoDB collection: nifty_futures");
        } else {
            log.warn("⚠️ No matching NIFTY FUT records found.");
        }

    } catch (IOException e) {
        log.error("❌ Failed to parse or save NIFTY FUT records", e);
    }
}
public Mono<Double> getNearestExpiryNiftyFutureLtp() {
    // 1. Fetch all saved NIFTY FUT contracts
    Query query = new Query();
    query.addCriteria(Criteria.where("segment").is("NSE_FO")
            .and("instrumentType").is("FUT")
            .and("lot_size").is(75));  // Ensure it's real NIFTY, not BANKNIFTY or others

    List<NseInstrument> niftyFuts = mongoTemplate.find(query, NseInstrument.class, "nifty_futures");

    if (niftyFuts.isEmpty()) {
        log.error("❌ No NIFTY FUT found in DB.");
        return Mono.error(new RuntimeException("No NIFTY FUT data in DB"));
    }

    // 2. Filter contracts whose expiry is after now
    long now = Instant.now().toEpochMilli();
    Optional<NseInstrument> nearestExpiry = niftyFuts.stream()
        .filter(f -> f.getExpiry() > now)
        .sorted(Comparator.comparingLong(NseInstrument::getExpiry))
        .findFirst();

    if (nearestExpiry.isEmpty()) {
        log.error("❌ No valid NIFTY FUT contract with future expiry.");
        return Mono.error(new RuntimeException("No future expiry NIFTY FUT found"));
    }

    String instrumentKey = nearestExpiry.get().getInstrument_key();
    log.info("📌 Selected NIFTY FUT key for LTP: " + instrumentKey);

    // 3. Call Upstox LTP API
    String url = "https://api.upstox.com/v2/market-quote/ltp?instrument_key=" + instrumentKey;

    return webClient.get()
            .uri(url)
            .header(HttpHeaders.AUTHORIZATION, "Bearer " + upstoxAuthService.currentToken())
            .header(HttpHeaders.ACCEPT, "application/json")
            .retrieve()
            .bodyToMono(String.class)
            .flatMap(response -> {
                // Parse LTP value from JSON (simplified)
                try {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode root = mapper.readTree(response);
                    JsonNode ltpNode = root.path("data").path(instrumentKey).path("ltp");

                    if (!ltpNode.isMissingNode()) {
                        double ltp = ltpNode.asDouble();
                        log.info("📈 NIFTY FUT LTP: " + ltp);
                        return Mono.just(ltp);
                    } else {
                        return Mono.error(new RuntimeException("LTP not found in response"));
                    }
                } catch (Exception e) {
                    return Mono.error(e);
                }
            });
}
}
