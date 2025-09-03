package com.trader.backend.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.reactive.function.client.WebClientResponseException;

@ControllerAdvice
@Slf4j
public class GlobalErrorHandler {

    @ExceptionHandler(Throwable.class)
    public ResponseEntity<String> handleException(Throwable ex) {
        if (ex instanceof WebClientResponseException w) {
            String body = w.getResponseBodyAsString();
            if (body == null) body = "";
            body = body.replaceAll("\n", " ");
            if (body.length() > 300) body = body.substring(0,300) + "...";
            log.error("Unhandled exception HTTP {} body={}", w.getStatusCode().value(), body, w);
        } else {
            log.error("Unhandled exception", ex);
        }
        return new ResponseEntity<>("Server Error: " + ex.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
