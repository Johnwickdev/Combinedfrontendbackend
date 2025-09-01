package com.trader.backend.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trader.backend.events.SignalEvent;
import com.trader.backend.service.SignalEngine;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

/**
 * Server Sent Events endpoint exposing live trading signals.
 */
@RestController
@RequiredArgsConstructor
public class SignalController {

    private final SignalEngine engine;
    private final ObjectMapper mapper = new ObjectMapper();

    @GetMapping(value = "/signals/live", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> live() {
        return engine.stream().map(this::toJson);
    }

    private String toJson(SignalEvent ev) {
        try {
            return mapper.writeValueAsString(ev);
        } catch (JsonProcessingException e) {
            return "{}";
        }
    }
}
