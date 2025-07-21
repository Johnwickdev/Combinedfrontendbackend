package com.trader.backend.service;

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
            Set<Double> targetStrikes = new HashSet<>();
            for (int i = -15; i <= 15; i++) {
                targetStrikes.add(baseStrike + (i * 50));
            }

            InputStream jsonStream = new FileInputStream("src/main/resources/NSE.json");
            ObjectMapper mapper = new ObjectMapper();
            List<NseInstrument> all = Arrays.asList(mapper.readValue(jsonStream, NseInstrument[].class));

            List<NseInstrument> filtered = all.stream()
                    .filter(i -> "NSE_INDEX|Nifty 50".equals(i.getUnderlying_key()))
                    .filter(i -> "CE".equals(i.getInstrumentType()) || "PE".equals(i.getInstrumentType()))
                    .filter(i -> targetStrikes.contains(i.getStrikePrice()))
                    .collect(Collectors.toList());

            log.info("💾 Saving {} filtered instruments to 'filtered_nifty_premiums' collection...", filtered.size());
            mongoTemplate.dropCollection("filtered_nifty_premiums");
            mongoTemplate.insert(filtered, "filtered_nifty_premiums");
            log.info("✅ Save complete.");

        } catch (Exception e) {
            log.error("❌ Error during strike filtering", e);
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

}
