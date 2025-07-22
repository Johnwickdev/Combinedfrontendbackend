// 🔥 Paste inside a new file: GlobalErrorHandler.java
package com.trader.backend.config;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@ControllerAdvice
public class GlobalErrorHandler {

    @ExceptionHandler(Throwable.class)
    public ResponseEntity<String> handleException(Throwable ex) {
        ex.printStackTrace(); // 🔥 This will go to Railway logs
        return new ResponseEntity<>("Server Error: " + ex.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
    }
}