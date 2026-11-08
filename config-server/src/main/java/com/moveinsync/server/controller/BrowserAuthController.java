package com.moveinsync.server.controller;

import com.moveinsync.server.security.SecurityContextHolder;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/auth")
public class BrowserAuthController {
    private final SecurityContextHolder security;

    public BrowserAuthController(SecurityContextHolder security) {
        this.security = security;
    }

    public record LoginRequest(String apiKey) {}

    @PostMapping("/login")
    public ResponseEntity<?> login(@RequestBody LoginRequest request, HttpServletRequest servletRequest) {
        try {
            String token = security.createBrowserSession(request.apiKey());
            boolean secure = servletRequest.isSecure()
                    || "https".equalsIgnoreCase(servletRequest.getHeader("X-Forwarded-Proto"));
            String cookie = "mis_session=" + token + "; Path=/; Max-Age=7200; HttpOnly; SameSite=Strict"
                    + (secure ? "; Secure" : "");
            return ResponseEntity.ok().header(HttpHeaders.SET_COOKIE, cookie).body(Map.of("status", "OK"));
        } catch (SecurityException e) {
            return ResponseEntity.status(401).body(Map.of("error", "Invalid dashboard credentials"));
        }
    }

    @GetMapping("/me")
    public ResponseEntity<?> me(@RequestHeader(value = "X-API-Key", required = false) String apiKey) {
        try {
            var principal = security.authenticate(apiKey);
            if (principal.role() == SecurityContextHolder.Role.EMPLOYEE) return ResponseEntity.status(403).build();
            return ResponseEntity.ok(Map.of("role", principal.role().name()));
        } catch (SecurityException e) {
            return ResponseEntity.status(401).build();
        }
    }

    @PostMapping("/logout")
    public ResponseEntity<?> logout(HttpServletRequest request) {
        if (request.getCookies() != null) {
            for (Cookie cookie : request.getCookies()) {
                if ("mis_session".equals(cookie.getName())) security.revokeBrowserSession(cookie.getValue());
            }
        }
        return ResponseEntity.ok().header(HttpHeaders.SET_COOKIE,
                "mis_session=; Path=/; Max-Age=0; HttpOnly; SameSite=Strict").body(Map.of("status", "OK"));
    }
}
