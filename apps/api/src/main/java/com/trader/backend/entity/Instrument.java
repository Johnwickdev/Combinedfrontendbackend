package com.trader.backend.entity;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.time.LocalDate;

/**
 * Simplified representation of an exchange instrument used for runtime
 * selections. Persisted in the {@code instruments} collection.
 */
@Data
@Document(collection = "instruments")
public class Instrument {
    @Id
    private String key;
    private String symbol;
    private String kind; // FUT or OPT
    private LocalDate expiry;
    private Double strike; // null for futures
    private String optType; // CE, PE or null
    private String segment;
    private String source; // e.g. NSE_JSON
    private Instant updatedAt;
}
