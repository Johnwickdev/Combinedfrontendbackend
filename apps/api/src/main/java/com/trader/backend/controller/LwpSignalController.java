package com.trader.backend.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trader.backend.events.LwpSignalEvent;
import com.trader.backend.service.LwpSignalEngine;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

/**
 * SSE endpoint for LWP signals.
 */
@RestController
@RequiredArgsConstructor
public class LwpSignalController {

    private final LwpSignalEngine engine;
    private final ObjectMapper mapper = new ObjectMapper();

    @GetMapping(value = "/signals/lwp", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> stream() {
        return engine.stream().map(this::toJson);
    }

    private String toJson(LwpSignalEvent ev) {
        try {
            return mapper.writeValueAsString(ev);
        } catch (JsonProcessingException e) {
            return "{}";
        }
    }
}
