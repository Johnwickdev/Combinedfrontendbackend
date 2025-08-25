// 🔥 Paste inside a new file: GlobalErrorHandler.java
package com.trader.backend.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@ControllerAdvice
@Slf4j
public class GlobalErrorHandler {

    @ExceptionHandler(Throwable.class)
    public ResponseEntity<String> handleException(Throwable ex) {
        log.error("Unhandled exception", ex);
        return new ResponseEntity<>("Server Error: " + ex.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
    }
}