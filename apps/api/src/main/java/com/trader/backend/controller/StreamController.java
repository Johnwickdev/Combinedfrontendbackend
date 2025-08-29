package com.trader.backend.controller;


import com.fasterxml.jackson.databind.JsonNode;
import com.trader.backend.service.LiveFeedService;   // or whatever you named it
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@Slf4j
@RestController
@RequiredArgsConstructor
public class StreamController {

    private final LiveFeedService live;

    //   GET /stream/ticks   →  text/event-stream
    @GetMapping(value = "/stream/ticks", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> ticks() {
        return live.stream()           // Flux<JsonNode>
                .map(Object::toString);   // plain JSON string for the browser
    }

}
