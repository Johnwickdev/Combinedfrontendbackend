package com.trader.backend.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.trader.backend.repository.NseInstrumentRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.mongo.DataMongoTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;

import static org.junit.jupiter.api.Assertions.assertTrue;

@DataMongoTest
@Import({ExpirySelectorService.class})
public class NseInstrumentServiceIntegrationTest {

    @Autowired
    private MongoTemplate mongoTemplate;
    @Autowired
    private NseInstrumentRepository repo;

    private NseInstrumentService service;

    @BeforeEach
    void setUp() {
        service = new NseInstrumentService(null, null, repo, new ObjectMapper(), mongoTemplate, null, new ExpirySelectorService());
    }

    @Test
    void loadsAndPersistsCurrentWeekOptions() {
        NseInstrumentService.OptionBatch batch = service.loadCurrentWeekOptionInstruments();
        assertTrue(repo.countByExpiry(batch.expiry()) > 0);
    }
}
