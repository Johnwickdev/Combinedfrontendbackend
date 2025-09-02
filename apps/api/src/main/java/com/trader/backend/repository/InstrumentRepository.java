package com.trader.backend.repository;

import com.trader.backend.entity.Instrument;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.time.LocalDate;
import java.util.Optional;

public interface InstrumentRepository extends MongoRepository<Instrument, String> {
    Optional<Instrument> findFirstBySymbolAndStrikeAndOptTypeAndExpiryAfterOrderByExpiry(
            String symbol, double strike, String optType, LocalDate expiryAfter);
}
