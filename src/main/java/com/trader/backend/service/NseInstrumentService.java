package com.trader.backend.service;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.temporal.TemporalAdjusters;
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
// at top of the class body:
private static final org.slf4j.Logger log =
        org.slf4j.LoggerFactory.getLogger(LiveFeedService.class);
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
    // make sure nse_instruments has the right week loaded
    int universeCount = ensureWeeklyUniverseLoaded();
    if (universeCount == 0) {
        log.error("❌ Aborting CE/PE selection because nse_instruments is still empty.");
        return;
    }

    LocalDate targetWed = resolveCurrentCycleExpiryWednesdayIST();
    long dayStart = istStartOfDayMs(targetWed);
    long dayEnd   = istEndOfDayMs(targetWed);
    log.info("🔍 Filtering around LTP={} for cycle Wed={} (IST) window [{}..{})",
            niftyLtp, targetWed, formatIstDateTime(dayStart), formatIstDateTime(dayEnd));

    double base = Math.round(niftyLtp / 50.0) * 50;

    // build a broad ladder (+/- 30 strikes of 50pts)
    Set<Integer> strikeSet = new LinkedHashSet<>();
    for (int i = -30; i <= 30; i++) strikeSet.add((int)(base + i*50));

    Query q = new Query(new Criteria().andOperator(
            Criteria.where("underlying_key").is("NSE_INDEX|Nifty 50"),
            Criteria.where("segment").is("NSE_FO"),
            Criteria.where("instrumentType").in("CE", "PE"),
            Criteria.where("strikePrice").in(strikeSet),
            Criteria.where("expiry").gte(dayStart).lt(dayEnd)
    ));

    List<NseInstrument> pool = mongoTemplate.find(q, NseInstrument.class, "nse_instruments");
    log.info("🧮 Candidates in window for base={} → {} rows ({} unique strikes)",
            base, pool.size(),
            pool.stream().map(NseInstrument::getStrikePrice).distinct().count());

    Comparator<NseInstrument> byDistance = Comparator
            .comparingDouble((NseInstrument i) -> Math.abs(i.getStrikePrice() - base))
            .thenComparingDouble(NseInstrument::getStrikePrice);

    List<NseInstrument> ce = pool.stream()
            .filter(i -> "CE".equals(i.getInstrumentType()))
            .sorted(byDistance)
            .collect(Collectors.collectingAndThen(
                    Collectors.toMap(
                            NseInstrument::getStrikePrice, i -> i, (a,b)->a, LinkedHashMap::new),
                    m -> new ArrayList<>(m.values())
            ));

    List<NseInstrument> pe = pool.stream()
            .filter(i -> "PE".equals(i.getInstrumentType()))
            .sorted(byDistance)
            .collect(Collectors.collectingAndThen(
                    Collectors.toMap(
                            NseInstrument::getStrikePrice, i -> i, (a,b)->a, LinkedHashMap::new),
                    m -> new ArrayList<>(m.values())
            ));

    List<NseInstrument> selected = new ArrayList<>();
    selected.addAll(ce.stream().limit(15).toList());
    selected.addAll(pe.stream().limit(15).toList());

    if (selected.isEmpty()) {
        log.warn("⚠️ No instruments selected for filtered_nifty_premiums (Wed={}). poolSize={}", targetWed, pool.size());
        return;
    }

    mongoTemplate.dropCollection("filtered_nifty_premiums");
    mongoTemplate.insert(selected, "filtered_nifty_premiums");

    log.info("✅ Saved {} ({} CE + {} PE) into filtered_nifty_premiums for Wed={}",
            selected.size(),
            Math.min(15, ce.size()),
            Math.min(15, pe.size()),
            targetWed);

    // print the keys we'll subscribe to
    selected.stream()
            .sorted(Comparator.comparingDouble(NseInstrument::getStrikePrice)
                    .thenComparing(NseInstrument::getInstrumentType))
            .limit(10)
            .forEach(i -> log.info("   🔑 {} {} {} | key={}",
                    i.getTrading_symbol(), i.getInstrumentType(), i.getStrikePrice(), i.getInstrument_key()));
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
    LocalDate targetWed = resolveCurrentCycleExpiryWednesdayIST();
    long dayStart = istStartOfDayMs(targetWed);
    long dayEnd   = istEndOfDayMs(targetWed);
    long now = System.currentTimeMillis();

    Query q = new Query(new Criteria().andOperator(
            Criteria.where("expiry").gte(dayStart).lt(dayEnd)  // only this cycle
    ));
    List<NseInstrument> instruments = mongoTemplate.find(q, NseInstrument.class, "filtered_nifty_premiums");

    List<String> keys = instruments.stream()
            .filter(i -> i.getExpiry() > now)
            .map(NseInstrument::getInstrument_key)
            .filter(Objects::nonNull)
            .distinct()
            .limit(30)
            .toList();

    if (keys.isEmpty()) {
        log.warn("⚠️ No valid keys for Wed={} (filtered_nifty_premiums).", targetWed);
    } else {
        log.info("✅ Keys ready for Wed={}: {}", targetWed, keys.size());
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
    //private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
private static final LocalTime EXPIRY_CUTOFF = LocalTime.of(15, 30); // 3:30 PM IST

private LocalDate resolveCurrentCycleExpiryWednesdayIST() {
    ZonedDateTime now = ZonedDateTime.now(IST);
    if (now.getDayOfWeek() == DayOfWeek.WEDNESDAY) {
        return now.toLocalTime().isAfter(EXPIRY_CUTOFF)
                ? now.toLocalDate().plusWeeks(1).with(DayOfWeek.WEDNESDAY)
                : now.toLocalDate();
    }
    return now.toLocalDate().with(TemporalAdjusters.next(DayOfWeek.WEDNESDAY));
}

private long istStartOfDayMs(LocalDate d) {
    return d.atStartOfDay(IST).toInstant().toEpochMilli();
}
private long istEndOfDayMs(LocalDate d) {
    return d.plusDays(1).atStartOfDay(IST).toInstant().toEpochMilli();
}
public void refreshNiftyOptionsCurrentWeekByLocalRule() {
    log.info("🔁 Refreshing nse_instruments by IST cycle (Fri→Wed)...");
    try {
        // 1) Load JSON
        File jsonFile = new File("src/main/resources/data/NSE.json");
        List<NseInstrument> all = mapper.readValue(jsonFile, new com.fasterxml.jackson.core.type.TypeReference<>() {});
        log.info("📦 Loaded {} rows from NSE.json (path={})", all.size(), jsonFile.getAbsolutePath());

        // 2) Quick expiry stats from file (before any filtering)
        if (!all.isEmpty()) {
            long minExp = all.stream().mapToLong(NseInstrument::getExpiry).min().orElse(Long.MAX_VALUE);
            long maxExp = all.stream().mapToLong(NseInstrument::getExpiry).max().orElse(Long.MIN_VALUE);
            log.info("🗓️ File expiry range (IST): {} .. {}",
                    formatIstDateTime(minExp), formatIstDateTime(maxExp));

            var byDay = all.stream().collect(
                    java.util.stream.Collectors.groupingBy(
                            i -> Instant.ofEpochMilli(i.getExpiry()).atZone(IST).toLocalDate(),
                            java.util.stream.Collectors.counting()
                    )
            );
            log.info("📊 Expiry counts by IST day (top 7):");
            byDay.entrySet().stream()
                    .sorted(java.util.Map.Entry.<java.time.LocalDate, Long>comparingByValue().reversed())
                    .limit(7)
                    .forEach(e -> log.info("   • {}  → {} instruments", e.getKey(), e.getValue()));
        }

        // 3) Normalize strings (whitespace in dumps can break equals())
        for (NseInstrument i : all) {
            if (i.getName() != null) i.setName(i.getName().trim());
            if (i.getSegment() != null) i.setSegment(i.getSegment().trim());
            if (i.getInstrumentType() != null) i.setInstrumentType(i.getInstrumentType().trim());
            if (i.getUnderlying_key() != null) i.setUnderlying_key(i.getUnderlying_key().trim());
        }

        // 4) Determine THIS cycle’s Wednesday in IST and the strict [start, end) window
        LocalDate targetWed = resolveCurrentCycleExpiryWednesdayIST();
        long dayStart = istStartOfDayMs(targetWed);
        long dayEnd   = istEndOfDayMs(targetWed);
        log.info("📅 Target cycle Wednesday (IST) = {} | window [{} .. {})",
                targetWed, formatIstDateTime(dayStart), formatIstDateTime(dayEnd));

        // 5) Step-by-step filter with counters (to explain every drop reason)
        int keep = 0, dropUnderlying = 0, dropSegment = 0, dropType = 0, dropExpiryWindow = 0;
        List<NseInstrument> currentWeek = new java.util.ArrayList<>();
        List<NseInstrument> sampleOutOfWindow = new java.util.ArrayList<>();

        for (NseInstrument i : all) {
            if (!"NSE_INDEX|Nifty 50".equals(i.getUnderlying_key())) { dropUnderlying++; continue; }
            if (!"NSE_FO".equals(i.getSegment())) { dropSegment++; continue; }
            String t = i.getInstrumentType();
            if (!"CE".equals(t) && !"PE".equals(t)) { dropType++; continue; }
            long exp = i.getExpiry();
            if (exp < dayStart || exp >= dayEnd) {
                dropExpiryWindow++;
                if (sampleOutOfWindow.size() < 3) sampleOutOfWindow.add(i);
                continue;
            }
            currentWeek.add(i);
            keep++;
        }

        log.info("🔎 Filter summary → keep={}, drop: underlying={}, segment={}, type={}, expiryWindow={}",
                keep, dropUnderlying, dropSegment, dropType, dropExpiryWindow);

        if (!sampleOutOfWindow.isEmpty()) {
            log.warn("⚠️ Examples out-of-window ({} of {}):",
                    sampleOutOfWindow.size(), dropExpiryWindow);
            for (NseInstrument s : sampleOutOfWindow) {
                log.warn("   ⏱️ trading_symbol='{}' | expiry(IST)={} | epoch={}",
                        s.getTrading_symbol(),
                        formatIstDateTime(s.getExpiry()),
                        s.getExpiry());
            }
        }

        // 6) If nothing matched, warn loudly and bail (don’t nuke collection)
        if (currentWeek.isEmpty()) {
            log.error("❌ No CURRENT-WEEK options matched for Wed={} within [{} .. {}). " +
                      "Likely NSE.json doesn’t contain that day’s expiry epoch or the IST window is wrong.",
                      targetWed, formatIstDateTime(dayStart), formatIstDateTime(dayEnd));
            return;
        }

        // 7) Stats on the result set
        long ceCount = currentWeek.stream().filter(x -> "CE".equals(x.getInstrumentType())).count();
        long peCount = currentWeek.stream().filter(x -> "PE".equals(x.getInstrumentType())).count();
        double minStrike = currentWeek.stream().mapToDouble(NseInstrument::getStrikePrice).min().orElse(Double.NaN);
        double maxStrike = currentWeek.stream().mapToDouble(NseInstrument::getStrikePrice).max().orElse(Double.NaN);
        log.info("✅ CURRENT-WEEK universe ready: total={}, CE={}, PE={}, strikeRange=[{}, {}]",
                currentWeek.size(), ceCount, peCount, minStrike, maxStrike);

        // 8) Replace collection
        mongoTemplate.dropCollection("nse_instruments");
        mongoTemplate.insert(currentWeek, "nse_instruments");
        log.info("💾 nse_instruments saved for Wed={} ({} docs)", targetWed, currentWeek.size());

    } catch (Exception e) {
        log.error("❌ Failed refreshing by local rule", e);
    }
}

public void purgeExpiredOptionDocs() {
    long now = System.currentTimeMillis();
    Query expired = new Query(Criteria.where("expiry").lt(now));

    long del1 = mongoTemplate.remove(expired, "nse_instruments").getDeletedCount();
    long del2 = mongoTemplate.remove(expired, "filtered_nifty_premiums").getDeletedCount();
    log.info("🧹 Purged expired: nse_instruments={}, filtered_nifty_premiums={}", del1, del2);
}

private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

// NEW: choose current‑month FUT key (fallback to nearest)
public Optional<String> getCurrentMonthNiftyFutKey() {
    List<NseInstrument> futs = mongoTemplate.findAll(NseInstrument.class, "nifty_futures");
    if (futs == null || futs.isEmpty()) return Optional.empty();

    ZonedDateTime now = ZonedDateTime.now(IST);
    int y = now.getYear(), m = now.getMonthValue();

    return futs.stream()
            .sorted(Comparator.comparingLong(NseInstrument::getExpiry))
            .filter(f -> {
                LocalDate d = Instant.ofEpochMilli(f.getExpiry()).atZone(IST).toLocalDate();
                return d.getYear() == y && d.getMonthValue() == m;
            })
            .map(NseInstrument::getInstrument_key)
            .findFirst()
            .or(() -> futs.stream()
                    .sorted(Comparator.comparingLong(NseInstrument::getExpiry))
                    .map(NseInstrument::getInstrument_key)
                    .findFirst());
}
// NEW: returns [startMs, endMs] for the nearest future option expiry DATE present in NSE.json (IST)
private long[] nearestOptionExpiryDayWindowFromJsonIST() {
    try {
        File jsonFile = new File("src/main/resources/data/NSE.json");
        List<NseInstrument> all = mapper.readValue(jsonFile, new com.fasterxml.jackson.core.type.TypeReference<>() {});

        long now = System.currentTimeMillis();
        List<LocalDate> days = all.stream()
                .filter(i -> "NSE_FO".equals(i.getSegment()))
                .filter(i -> "NIFTY".equalsIgnoreCase(i.getName()))
                .filter(i -> "NSE_INDEX|Nifty 50".equals(i.getUnderlying_key()))
                .filter(i -> {
                    String t = i.getInstrumentType();
                    return "CE".equalsIgnoreCase(t) || "PE".equalsIgnoreCase(t);
                })
                .map(i -> Instant.ofEpochMilli(i.getExpiry()).atZone(IST).toLocalDate())
                .distinct()
                .sorted()
                .toList();

        LocalDate target = days.stream()
                .filter(d -> d.atStartOfDay(IST).toInstant().toEpochMilli() >= now)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No future option expiry date in NSE.json"));

        long start = target.atStartOfDay(IST).toInstant().toEpochMilli();
        long end   = target.plusDays(1).atStartOfDay(IST).toInstant().toEpochMilli();
        log.info("🎯 Nearest option expiry date (IST) from JSON: {} ({}..{})", target, start, end);
        return new long[]{start, end};
    } catch (Exception e) {
        throw new RuntimeException("nearestOptionExpiryDayWindowFromJsonIST failed", e);
    }
}
// NEW: compute 10 CE + 10 PE for nearest expiry day and save -> filtered_nifty_premiums
public void filterTenTenAroundLtpAndSave(double niftyLtp) {
    long[] win = nearestOptionExpiryDayWindowFromJsonIST();
    long start = win[0], end = win[1];

    // Try DB universe for this day; fallback to JSON if empty
    var q = new Query(Criteria.where("underlying_key").is("NSE_INDEX|Nifty 50")
            .and("segment").is("NSE_FO")
            .and("instrumentType").in("CE","PE")
            .and("expiry").gte(start).lt(end));
    List<NseInstrument> universe = mongoTemplate.find(q, NseInstrument.class, "nse_instruments");

    if (universe.isEmpty()) {
        try {
            File jsonFile = new File("src/main/resources/data/NSE.json");
            List<NseInstrument> all = mapper.readValue(jsonFile, new com.fasterxml.jackson.core.type.TypeReference<>() {});
            for (NseInstrument i : all) {
                if (i.getName()!=null) i.setName(i.getName().trim());
                if (i.getSegment()!=null) i.setSegment(i.getSegment().trim());
                if (i.getInstrumentType()!=null) i.setInstrumentType(i.getInstrumentType().trim());
                if (i.getUnderlying_key()!=null) i.setUnderlying_key(i.getUnderlying_key().trim());
            }
            universe = all.stream()
                    .filter(i -> "NSE_INDEX|Nifty 50".equals(i.getUnderlying_key()))
                    .filter(i -> "NSE_FO".equals(i.getSegment()))
                    .filter(i -> {
                        String t = i.getInstrumentType();
                        return "CE".equals(t) || "PE".equals(t);
                    })
                    .filter(i -> i.getExpiry()>=start && i.getExpiry()<end)
                    .toList();

            mongoTemplate.dropCollection("nse_instruments");
            mongoTemplate.insert(universe, "nse_instruments");
            log.info("ℹ️ Refreshed nse_instruments for nearest expiry day ({} docs)", universe.size());
        } catch (Exception e) {
            log.error("❌ JSON fallback failed", e);
            return;
        }
    }

    int atm = (int) (Math.round(niftyLtp / 50.0) * 50);

    // CE buckets (ITM below ATM, OTM above)
    var ceATM = universe.stream().filter(i -> "CE".equals(i.getInstrumentType()) && i.getStrikePrice()==atm).toList();
    var ceITM = universe.stream().filter(i -> "CE".equals(i.getInstrumentType()) && i.getStrikePrice()<atm)
            .sorted(Comparator.comparingDouble(NseInstrument::getStrikePrice).reversed()).toList();
    var ceOTM = universe.stream().filter(i -> "CE".equals(i.getInstrumentType()) && i.getStrikePrice()>atm)
            .sorted(Comparator.comparingDouble(NseInstrument::getStrikePrice)).toList();

    // PE buckets (ITM above ATM, OTM below)
    var peATM = universe.stream().filter(i -> "PE".equals(i.getInstrumentType()) && i.getStrikePrice()==atm).toList();
    var peITM = universe.stream().filter(i -> "PE".equals(i.getInstrumentType()) && i.getStrikePrice()>atm)
            .sorted(Comparator.comparingDouble(NseInstrument::getStrikePrice)).toList();
    var peOTM = universe.stream().filter(i -> "PE".equals(i.getInstrumentType()) && i.getStrikePrice()<atm)
            .sorted(Comparator.comparingDouble(NseInstrument::getStrikePrice).reversed()).toList();

    List<NseInstrument> pick = new ArrayList<>(20);

    // CE: ATM1 + ITM2 + OTM7
    if (!ceATM.isEmpty()) pick.add(ceATM.get(0));
    pick.addAll(ceITM.stream().limit(2).toList());
    pick.addAll(ceOTM.stream().limit(7).toList());

    // PE: ATM1 + ITM2 + OTM7
    if (!peATM.isEmpty()) pick.add(peATM.get(0));
    pick.addAll(peITM.stream().limit(2).toList());
    pick.addAll(peOTM.stream().limit(7).toList());

    // de‑dupe by instrument_key, keep order
    LinkedHashMap<String,NseInstrument> uniq = new LinkedHashMap<>();
    for (NseInstrument i : pick) {
        if (i.getInstrument_key()!=null) uniq.putIfAbsent(i.getInstrument_key(), i);
    }
    List<NseInstrument> finalSel = new ArrayList<>(uniq.values());

    long ceCount = finalSel.stream().filter(i -> "CE".equals(i.getInstrumentType())).count();
    long peCount = finalSel.stream().filter(i -> "PE".equals(i.getInstrumentType())).count();
    log.info("🎯 Ten+Ten selection: CE={} PE={} (target 10+10), ATM={}, window=[{}..{}]", ceCount, peCount, atm, start, end);

    mongoTemplate.dropCollection("filtered_nifty_premiums");
    mongoTemplate.insert(finalSel, "filtered_nifty_premiums");
    log.info("✅ Stored {} instruments to filtered_nifty_premiums", finalSel.size());
}
private String formatIstDateTime(long epochMs) {
    try {
        return Instant.ofEpochMilli(epochMs).atZone(IST).toLocalDateTime().toString() + " IST";
    } catch (Exception e) {
        return String.valueOf(epochMs);
    }
}
// Ensures nse_instruments has rows for the intended cycle.
// 1) Try strict Wed IST window (Fri→Wed logic)
// 2) If still empty, fall back to "nearest future expiry in NSE.json"
// Returns how many docs are present after the attempt.
public int ensureWeeklyUniverseLoaded() {
    // --- target window by local rule ---
    LocalDate targetWed = resolveCurrentCycleExpiryWednesdayIST();
    long dayStart = istStartOfDayMs(targetWed);
    long dayEnd   = istEndOfDayMs(targetWed);

    // count existing
    Query cntQ = new Query(new Criteria().andOperator(
            Criteria.where("underlying_key").is("NSE_INDEX|Nifty 50"),
            Criteria.where("segment").is("NSE_FO"),
            Criteria.where("instrumentType").in("CE","PE"),
            Criteria.where("expiry").gte(dayStart).lt(dayEnd)
    ));
    long existing = mongoTemplate.count(cntQ, "nse_instruments");
    if (existing > 0) {
        log.info("🧮 nse_instruments already has {} docs for Wed={} ({}..{})",
                existing, targetWed, formatIstDateTime(dayStart), formatIstDateTime(dayEnd));
        return (int)existing;
    }

    log.warn("⚠️ nse_instruments empty for Wed={} — attempting refresh by local rule…", targetWed);
    refreshNiftyOptionsCurrentWeekByLocalRule();

    // re-count
    existing = mongoTemplate.count(cntQ, "nse_instruments");
    if (existing > 0) {
        log.info("✅ nse_instruments loaded by local rule: {} docs for Wed={}", existing, targetWed);
        return (int)existing;
    }

    // --- fallback: nearest future expiry present in NSE.json ---
    log.warn("⚠️ Local rule still empty. Falling back to nearest-future-expiry found in NSE.json …");

    try {
        File jsonFile = new File("src/main/resources/data/NSE.json");
        List<NseInstrument> all = mapper.readValue(jsonFile, new com.fasterxml.jackson.core.type.TypeReference<>() {});

        // normalize
        for (NseInstrument i : all) {
            if (i.getName() != null) i.setName(i.getName().trim());
            if (i.getSegment() != null) i.setSegment(i.getSegment().trim());
            if (i.getInstrumentType() != null) i.setInstrumentType(i.getInstrumentType().trim());
            if (i.getUnderlying_key() != null) i.setUnderlying_key(i.getUnderlying_key().trim());
        }

        long now = System.currentTimeMillis();
        OptionalLong nearestFuture = all.stream()
                .filter(i -> "NSE_INDEX|Nifty 50".equals(i.getUnderlying_key()))
                .filter(i -> "NSE_FO".equals(i.getSegment()))
                .filter(i -> {
                    String t = i.getInstrumentType();
                    return "CE".equals(t) || "PE".equals(t);
                })
                .mapToLong(NseInstrument::getExpiry)
                .filter(exp -> exp >= now)
                .sorted()
                .findFirst();

        if (nearestFuture.isEmpty()) {
            log.error("❌ NSE.json has no future CE/PE expiry for NIFTY. Cannot build universe.");
            return 0;
        }

        long chosenExpiry = nearestFuture.getAsLong();
        LocalDate chosenDay = Instant.ofEpochMilli(chosenExpiry).atZone(IST).toLocalDate();
        long chosenStart = istStartOfDayMs(chosenDay);
        long chosenEnd   = istEndOfDayMs(chosenDay);
        log.warn("🔁 Fallback selecting expiry day={} ({}..{})",
                chosenDay, formatIstDateTime(chosenStart), formatIstDateTime(chosenEnd));

        List<NseInstrument> current = all.stream()
                .filter(i -> "NSE_INDEX|Nifty 50".equals(i.getUnderlying_key()))
                .filter(i -> "NSE_FO".equals(i.getSegment()))
                .filter(i -> {
                    String t = i.getInstrumentType();
                    return "CE".equals(t) || "PE".equals(t);
                })
                .filter(i -> i.getExpiry() >= chosenStart && i.getExpiry() < chosenEnd)
                .toList();

        if (current.isEmpty()) {
            log.error("❌ Fallback day also yields zero rows. Check NSE.json content.");
            return 0;
        }

        mongoTemplate.dropCollection("nse_instruments");
        mongoTemplate.insert(current, "nse_instruments");
        log.info("💾 nse_instruments populated by fallback: {} docs for day={}", current.size(), chosenDay);

        return current.size();

    } catch (Exception e) {
        log.error("❌ ensureWeeklyUniverseLoaded fallback failed", e);
        return 0;
    }
}
}
