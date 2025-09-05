package com.trader.backend.controller;

import com.trader.backend.service.UpstoxAuthService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Map;
import java.util.LinkedHashMap;

@RestController
@RequestMapping("/auth")
@RequiredArgsConstructor
@Slf4j
public class AuthController {

    private final UpstoxAuthService auth;

    @Value("${frontend.dashboard-url}")
    private String frontendDashboardUrl;

    /** Redirect straight to the Upstox OAuth dialog. */
    @GetMapping("/login")
    public void login(HttpServletResponse response) throws IOException {
        String url = auth.buildAuthUrl();
        if (url == null) {
            throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Upstox credentials not configured");
        }
        response.sendRedirect(url);
    }

    /** Convenience endpoint that also redirects to the OAuth dialog. */
    @GetMapping("/url")
    public void loginUrl(HttpServletResponse response) throws IOException {
        login(response);
    }

    @GetMapping("/redirect-url")
    public Map<String, String> redirectUrl() {
        Map<String, String> body = new LinkedHashMap<>();
        body.put("url", frontendDashboardUrl);
        return body;
    }

    /** Endpoint used by the Upstox redirect after login. */
    @RequestMapping(value = "", method = RequestMethod.GET)
    public void handleUpstoxRedirect(@RequestParam Map<String, String> qs,
                                     HttpServletResponse response) {
        String code = qs.get("code");
        log.info("Received code: {}", code);

        if (code != null && !code.isBlank()) {
            try {
                auth.exchangeCode(code).block();
            } catch (Exception e) {
                log.error("Token exchange failed", e);
            }
            try {
                response.sendRedirect(frontendDashboardUrl);
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

