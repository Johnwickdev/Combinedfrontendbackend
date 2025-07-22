package com.trader.backend.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trader.backend.service.UpstoxAuthService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.Map;

/*

@RestController
@RequestMapping("/auth")
@RequiredArgsConstructor
public class AuthController {

    private final UpstoxAuthService auth;
    private final ObjectMapper mapper = new ObjectMapper();

    */
/**
     * 1️⃣  Login-link for the front-end
     *//*

    @GetMapping("/url")
    public Map<String, String> loginUrl() {
        return Map.of("url", auth.buildAuthUrl());
    }

   */
/* @RequestMapping(value = "", method = {RequestMethod.GET, RequestMethod.POST})
    public Mono<Void> rootWebhook(@RequestParam Map<String, String> qs,
                                  @RequestBody(required = false) String body) throws JsonProcessingException {
        // same body as before
        String json = (body == null || body.isBlank())
                ? mapper.writeValueAsString(qs)
                : body;
        return auth.handleWebhook(json);
    }*//*

//   @RequestMapping(value = "", method = {RequestMethod.GET, RequestMethod.POST})
//   public Mono<ResponseEntity<String>> rootWebhook(@RequestParam Map<String, String> qs,
//                                                   @RequestBody(required = false) String body) throws JsonProcessingException {
//       String json = (body == null || body.isBlank())
//               ? mapper.writeValueAsString(qs)
//               : body;
//
//       // Log or exchange code if needed
//       System.out.println("Received OAuth code: " + qs.get("code"));
//
//       // Trigger your webhook logic
//       return auth.handleWebhook(json)
//               .thenReturn(ResponseEntity.ok("✅ Login successful. You can close this tab now."));
//   }
//
   @RequestMapping(value = "", method = {RequestMethod.GET})
   public Mono<Void> redirectToFrontend(@RequestParam Map<String, String> qs,
                                        ServerHttpResponse response) {
       String code = qs.get("code");
       if (code != null) {
           return auth.exchangeCode(code)
                   .then(Mono.fromRunnable(() -> {
                       // Redirect to Angular login page
                       response.setStatusCode(HttpStatus.FOUND);
                       response.getHeaders().setLocation(URI.create("https://frontendfortheautobot.vercel.app/login"));
                   }));
       } else {
           response.setStatusCode(HttpStatus.BAD_REQUEST);
           return Mono.empty();
       }
   }


    */
/**
     * 3️⃣  Polled by the UI
     *//*

    @GetMapping("/status")
    public Mono<Map<String, Object>> status() {
        return auth.status();
    }

    */
/* @PostMapping("/exchange")
     public Mono<Void> exchange(@RequestParam String code) {
         return auth.exchangeCode(code);
     }*//*

    @PostMapping("/exchange")
    public Mono<ResponseEntity<Object>> exchangeCode(@RequestParam("code") String code) {
        return auth.exchangeCode(code)
                .then(Mono.just(ResponseEntity.ok().build()))
                .onErrorResume(e -> {
                    // log.error("Exchange failed", e);
                    return Mono.just(ResponseEntity.status(HttpStatus.UNAUTHORIZED).build());
                });
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
*/

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trader.backend.service.UpstoxAuthService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpResponse;
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

    if (code != null && !code.isBlank()) {
        return auth.exchangeCode(code) // Step 1: Exchange code
                .then(auth.initLiveWebSocket()) // ✅ Step 2: Init WebSocket feed right here (you implement this method)
                .then(Mono.defer(() -> {
                    response.setStatusCode(HttpStatus.FOUND);
                    response.getHeaders().setLocation(URI.create("https://frontendfortheautobot.vercel.app/login"));
                    return Mono.empty();
                }));
    } else {
        response.setStatusCode(HttpStatus.BAD_REQUEST);
        return Mono.empty();
    }
}
@PostMapping("/exchange")
public Mono<ResponseEntity<Object>> exchangeCode(@RequestParam("code") String code) {
    return auth.exchangeCode(code)
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
