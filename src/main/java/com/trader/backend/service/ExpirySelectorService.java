package com.trader.backend.service;

import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import java.time.*;
import java.time.temporal.TemporalAdjusters;
import java.util.List;

/**
 * Selects and persists the current NIFTY option expiry.
 */
@Service
@Slf4j
public class ExpirySelectorService {

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final LocalTime ROLLOVER = LocalTime.of(15, 30);

    private final MongoTemplate mongoTemplate;

    public ExpirySelectorService(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    /**
     * No-arg constructor for tests where persistence isn't needed.
     */
    public ExpirySelectorService() {
        this.mongoTemplate = null;
    }

    /**
     * Selects current weekly expiry based on trading rules and stores in meta_config.
     * Logs when the value changes.
     */
    public synchronized LocalDate selectCurrentOptionExpiry(ZonedDateTime nowIst) {
        ZonedDateTime z = nowIst.withZoneSameInstant(IST);
        LocalDate next;
        DayOfWeek dow = z.getDayOfWeek();
        if (dow == DayOfWeek.THURSDAY) {
            if (z.toLocalTime().isBefore(ROLLOVER)) {
                next = z.toLocalDate();
            } else {
                next = z.toLocalDate().with(TemporalAdjusters.next(DayOfWeek.THURSDAY));
            }
        } else if (dow.getValue() >= DayOfWeek.FRIDAY.getValue()) {
            next = z.toLocalDate().with(TemporalAdjusters.next(DayOfWeek.THURSDAY));
        } else {
            next = z.toLocalDate().with(TemporalAdjusters.nextOrSame(DayOfWeek.THURSDAY));
        }

        if (mongoTemplate != null) {
            Document prev = mongoTemplate.findById("options_current_expiry", Document.class, "meta_config");
            String prevVal = prev != null ? prev.getString("value") : null;
            if (prevVal == null || !prevVal.equals(next.toString())) {
                Document d = new Document("_id", "options_current_expiry").append("value", next.toString());
                mongoTemplate.save(d, "meta_config");
                log.info("OPTIONS expiry -> {} [rollover]", next);
            }
        }
        return next;
    }

    public LocalDate pickCurrentExpiry(ZonedDateTime nowIst) {
        return selectCurrentOptionExpiry(nowIst);
    }

    public LocalDate pickCurrentExpiry(Instant nowUtc) {
        return pickCurrentExpiry(ZonedDateTime.ofInstant(nowUtc, IST));
    }

    /**
     * Picks the current weekly expiry in a fully IST-aware manner.
     * @param nowUtc current instant in UTC
     * @param expiriesEpochMs candidate expiry epochs in milliseconds
     * @param zoneIst zone id representing IST
     * @return chosen expiry instant (at 15:30 IST)
     */
    public Instant pickCurrentWeeklyExpiry(Instant nowUtc, List<Long> expiriesEpochMs, ZoneId zoneIst) {
        ZonedDateTime nowIst = ZonedDateTime.ofInstant(nowUtc, zoneIst);
        ZonedDateTime marketCloseIst = nowIst.withHour(15).withMinute(30).withSecond(0).withNano(0);
        ZonedDateTime pivotIst = nowIst.isBefore(marketCloseIst)
                ? nowIst
                : nowIst.plusDays(1).withHour(0).withMinute(0).withSecond(0).withNano(0);

        List<Long> sorted = expiriesEpochMs.stream().distinct().sorted().toList();
        Instant chosen = null;
        for (Long expMs : sorted) {
            ZonedDateTime expiryIst = Instant.ofEpochMilli(expMs)
                    .atZone(zoneIst)
                    .withHour(15).withMinute(30).withSecond(0).withNano(0);
            if (!expiryIst.isBefore(pivotIst)) {
                chosen = expiryIst.toInstant();
                break;
            }
        }
        if (chosen == null && !sorted.isEmpty()) {
            Long last = sorted.get(sorted.size() - 1);
            chosen = Instant.ofEpochMilli(last)
                    .atZone(zoneIst)
                    .withHour(15).withMinute(30).withSecond(0).withNano(0)
                    .toInstant();
        }
        String candidates = summarise(sorted);
        log.info("EXPIRY-PICK nowIst={} pivotIst={} chosenExpiryIst={} (epochMs={}) poolSize={} candidates={}",
                nowIst, pivotIst, chosen.atZone(zoneIst), chosen.toEpochMilli(), sorted.size(), candidates);
        return chosen;
    }

    private static String summarise(List<Long> sorted) {
        if (sorted.isEmpty()) return "[]";
        if (sorted.size() <= 6) return sorted.toString();
        List<Long> first3 = sorted.subList(0, 3);
        List<Long> last3 = sorted.subList(sorted.size() - 3, sorted.size());
        return first3 + "..." + last3;
    }
}

