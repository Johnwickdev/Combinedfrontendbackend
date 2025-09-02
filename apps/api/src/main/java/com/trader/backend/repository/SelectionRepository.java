package com.trader.backend.repository;

import com.trader.backend.entity.Selection;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface SelectionRepository extends MongoRepository<Selection, String> {
}
