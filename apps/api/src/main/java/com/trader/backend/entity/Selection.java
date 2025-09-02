package com.trader.backend.entity;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/**
 * Stores the currently selected ATM option instruments derived from the
 * underlying future.
 */
@Data
@Document(collection = "selections")
public class Selection {
    @Id
    private String id;
    private String baseKey;
    private double baseLtp;
    private double atmStrike;
    private String ceKey;
    private String peKey;
    private Instant createdAt;
}
