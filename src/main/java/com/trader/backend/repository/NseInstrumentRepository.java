package com.trader.backend.repository;


import com.trader.backend.entity.NseInstrument;
import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface NseInstrumentRepository extends MongoRepository<NseInstrument, String> {
   /* List<NseInstrument> findBySegmentAndInstrumentTypeAndStrikePriceBetween(
            String segment, String instrumentType, double min, double max
    );*/
   List<NseInstrument> findBySegmentAndInstrumentTypeAndStrikePriceBetween(String segment, String instrumentType, double min, double max);

   long countByExpiry(long expiry);

   // List<NseInstrument> findBySegmentAndInstrument_type(String segment, String instrument_type);

}
