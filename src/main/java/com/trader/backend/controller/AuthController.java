package com.trader.backend.controller;

import com.trader.backend.service.UpstoxAuthService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import jakarta.servlet.http.HttpServletResponse;
import java.util.Map;

@RestController
@RequestMapping("/auth")
@RequiredArgsConstructor
@Slf4j
public class AuthController {

    private final UpstoxAuthService auth;

    @Value("${FRONTEND_REDIRECT_URL:https://frontendfortheautobot-7u7woqq2m-johnwicks-projects-025aea65.vercel.app/dashboard}")
    private String frontendRedirectUrl;

    /** Return the OAuth dialog URL for the frontend. */
    @GetMapping("/url")
    public Map<String, String> loginUrl() {
        return Map.of("url", auth.buildAuthUrl());
    }

    @GetMapping("/redirect-url")
    public Map<String, String> redirectUrl() {
        return Map.of("url", frontendRedirectUrl);
    }

    /** Endpoint used by the Upstox redirect after login. */
    @RequestMapping(value = "", method = RequestMethod.GET)
    public void handleUpstoxRedirect(@RequestParam Map<String, String> qs,
                                     HttpServletResponse response) {
        String code = qs.get("code");
        log.info("Received code: {}", code);

        if (code != null && !code.isBlank()) {
            auth.exchangeCode(code).subscribe();
            try {
                response.sendRedirect(frontendRedirectUrl);
            } catch (Exception e) {
                log.error("Failed to redirect to frontend", e);
            }
        } else {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        }
    }

    /** REST endpoint for the frontend to exchange the auth code. */
    @GetMapping("/exchange")
    public Mono<ResponseEntity<Void>> exchangeCode(@RequestParam("code") String code) {
        return auth.exchangeCode(code).thenReturn(ResponseEntity.ok().build());
    }

    /** Status endpoint for the frontend. */
    @GetMapping("/status")
    public Mono<Map<String, Object>> status() {
        return auth.status();
    }

    /** Convenience proxy to fetch a single quote. */
    @GetMapping("/quote/{exchange}/{symbol}")
    public Mono<Map<String, Object>> quote(@PathVariable String exchange,
                                           @PathVariable String symbol) {
        return auth.fetchQuote(exchange, symbol);
    }
}

