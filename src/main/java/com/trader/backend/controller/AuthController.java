package com.trader.backend.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trader.backend.service.UpstoxAuthService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.Map;

@RestController
@RequestMapping("/auth")
@RequiredArgsConstructor
public class AuthController {

    private final UpstoxAuthService auth;
    private final ObjectMapper mapper = new ObjectMapper();

    @GetMapping("/url")
    public Map<String, String> loginUrl() {
        return Map.of("url", auth.buildAuthUrl());
    }

   @RequestMapping(value = "", method = RequestMethod.GET)
public Mono<Void> handleUpstoxRedirect(@RequestParam Map<String, String> qs,
                                       ServerHttpResponse response) {
    String code = qs.get("code");
    System.out.println("🔵 Received code: " + code);

    if (code != null && !code.isBlank()) {
        return auth.exchangeCode(code)
                .doOnSuccess(unused -> {
                    System.out.println("🟢 Exchange successful, now calling initLiveWebSocket()");
                    try {
                        auth.initLiveWebSocket();
                    } catch (Exception e) {
                        System.out.println("🔴 WebSocket crash: " + e.getMessage());
                        e.printStackTrace();
                    }
                })
                .then(Mono.fromRunnable(() -> {
                    System.out.println("🟡 Redirecting to dashboard...");
                    response.setStatusCode(HttpStatus.FOUND);
                    response.getHeaders().setLocation(URI.create("https://frontendfortesting.vercel.app/dashboard"));
                }))
                .then();
    } else {
        response.setStatusCode(HttpStatus.BAD_REQUEST);
        return Mono.empty();
    }
}
    @PostMapping("/exchange")
    public Mono<ResponseEntity<Object>> exchangeCode(@RequestParam("code") String code) {
        return auth.exchangeCode(code)
                .doOnSuccess(unused -> auth.initLiveWebSocket())
                .then(Mono.just(ResponseEntity.ok().build()))
                .onErrorResume(e -> Mono.just(ResponseEntity.status(HttpStatus.UNAUTHORIZED).build()));
    }

    @GetMapping("/status")
    public Mono<Map<String, Object>> status() {
        return auth.status();
    }

    @GetMapping("/quote/{exchange}/{symbol}")
    public Mono<Map<String, Object>> quote(@PathVariable String exchange,
                                           @PathVariable String symbol,
                                           UpstoxAuthService auth) {
        return auth.fetchQuote(exchange, symbol);
    }

    @PostMapping("/auth/request-token")
    public Mono<UpstoxAuthService.TokenRequestResponse> requestToken() {
        return auth.requestAccessTokenPush();
    }

    @PostMapping("/auth/webhook")
    public Mono<Void> handleWebhook(@RequestBody WebhookPayload p) {
        if ("access_token".equals(p.message_type())) {
            auth.setCurrentToken(p.access_token());
        }
        return Mono.empty();
    }

    public record WebhookPayload(
            String client_id,
            String user_id,
            String access_token,
            String token_type,
            String expires_at,
            String issued_at,
            String message_type
    ) {
    }
}