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
    log.info("🔍 Filtering around LTP={} for CURRENT-WEEK expiry (15 CE + 15 PE, ATM-first)", niftyLtp);

    Long currentWeekExpiry = mongoTemplate.find(
                    new Query(Criteria.where("underlying_key").is("NSE_INDEX|Nifty 50"))
                            .with(Sort.by(Sort.Direction.ASC, "expiry"))
                            .limit(1),
                    NseInstrument.class,
                    "nse_instruments")
            .stream()
            .map(NseInstrument::getExpiry)
            .findFirst()
            .orElse(null);

    if (currentWeekExpiry == null) {
        log.warn("⚠️ nse_instruments is empty or stale. Call refreshNiftyOptionsCurrentWeek() first.");
        return;
    }

    double base = Math.round(niftyLtp / 50.0) * 50;
    Set<Integer> strikeSet = new LinkedHashSet<>();
    for (int i = -30; i <= 30; i++) {
        strikeSet.add((int) (base + i * 50));
    }

    Query q = new Query(new Criteria().andOperator(
            Criteria.where("underlying_key").is("NSE_INDEX|Nifty 50"),
            Criteria.where("segment").is("NSE_FO"),
            Criteria.where("instrumentType").in("CE", "PE"),
            Criteria.where("strikePrice").in(strikeSet),
            Criteria.where("expiry").is(currentWeekExpiry)
    ));
    List<NseInstrument> pool = mongoTemplate.find(q, NseInstrument.class, "nse_instruments");

    // ✅ Explicit types in comparator to avoid Object inference
Comparator<NseInstrument> byDistance = Comparator
        .comparingDouble((NseInstrument i) -> Math.abs(i.getStrikePrice() - base))
        .thenComparingDouble(NseInstrument::getStrikePrice);

    List<NseInstrument> ce = pool.stream()
            .filter(i -> "CE".equals(i.getInstrumentType()))
            .sorted(byDistance)
            .collect(Collectors.collectingAndThen(
                    Collectors.toMap(
                            NseInstrument::getStrikePrice,
                            i -> i,
                            (a, b) -> a,
                            LinkedHashMap::new
                    ),
                    m -> new ArrayList<>(m.values())
            ));

    List<NseInstrument> pe = pool.stream()
            .filter(i -> "PE".equals(i.getInstrumentType()))
            .sorted(byDistance)
            .collect(Collectors.collectingAndThen(
                    Collectors.toMap(
                            NseInstrument::getStrikePrice,
                            i -> i,
                            (a, b) -> a,
                            LinkedHashMap::new
                    ),
                    m -> new ArrayList<>(m.values())
            ));

    List<NseInstrument> selected = new ArrayList<>();
    selected.addAll(ce.stream().limit(15).toList());
    selected.addAll(pe.stream().limit(15).toList());

    if (selected.isEmpty()) {
        log.warn("⚠️ No instruments selected for filtered_nifty_premiums");
        return;
    }

    mongoTemplate.dropCollection("filtered_nifty_premiums");
    mongoTemplate.insert(selected, "filtered_nifty_premiums");
    log.info("✅ Saved {} instruments (15 CE + 15 PE) to filtered_nifty_premiums", selected.size());
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
    long now = System.currentTimeMillis();
    List<NseInstrument> instruments = mongoTemplate.findAll(NseInstrument.class, "filtered_nifty_premiums");

    List<String> keys = instruments.stream()
            .filter(i -> i.getExpiry() > now)          // expiry is long primitive
            .map(NseInstrument::getInstrument_key)
            .filter(Objects::nonNull)
            .distinct()
            .limit(30)
            .toList();

    if (keys.isEmpty()) {
        log.warn("⚠️ No valid (non-expired) keys found in filtered_nifty_premiums.");
    } else {
        log.info("✅ Live subscription keys ready: {}", keys.size());
    }
    return keys;
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
public void refreshNiftyOptionsCurrentWeek() {
    log.info("🔁 Refreshing nse_instruments with CURRENT-WEEK NIFTY CE/PE from NSE.json...");
    try {
        File jsonFile = new File("src/main/resources/data/NSE.json");
        List<NseInstrument> all = mapper.readValue(jsonFile, new TypeReference<>() {});

        // normalize
        for (NseInstrument i : all) {
            if (i.getName() != null) i.setName(i.getName().trim());
            if (i.getSegment() != null) i.setSegment(i.getSegment().trim());
            if (i.getInstrumentType() != null) i.setInstrumentType(i.getInstrumentType().trim());
            if (i.getUnderlying_key() != null) i.setUnderlying_key(i.getUnderlying_key().trim());
        }

        long currentWeekExpiry = detectCurrentWeekExpiryEpoch(all);

        List<NseInstrument> currentWeek = all.stream()
                .filter(i -> "NSE_INDEX|Nifty 50".equals(i.getUnderlying_key()))
                .filter(i -> "NSE_FO".equals(i.getSegment()))
                .filter(i -> "CE".equals(i.getInstrumentType()) || "PE".equals(i.getInstrumentType()))
                .filter(i -> i.getExpiry() == currentWeekExpiry)
                .toList();

        mongoTemplate.dropCollection("nse_instruments");
        mongoTemplate.insert(currentWeek, "nse_instruments");

        log.info("✅ nse_instruments refreshed with {} CURRENT-WEEK CE/PE", currentWeek.size());
    } catch (Exception e) {
        log.error("❌ Failed to refresh CURRENT-WEEK instruments", e);
    }
}

// NEW: tiny helper used by refreshNiftyOptionsCurrentWeek()
private long detectCurrentWeekExpiryEpoch(List<NseInstrument> all) {
    long now = System.currentTimeMillis();
    return all.stream()
            .filter(i -> "NSE_INDEX|Nifty 50".equals(i.getUnderlying_key()))
            .filter(i -> "NSE_FO".equals(i.getSegment()))
            .filter(i -> "CE".equals(i.getInstrumentType()) || "PE".equals(i.getInstrumentType()))
            .mapToLong(NseInstrument::getExpiry)   // primitive long, no nulls
            .filter(exp -> exp >= now)
            .sorted()
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No future expiry found in NSE.json"));
}
}
