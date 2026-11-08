package com.moveinsync.server.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.Cookie;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/** Accepts opaque Bearer credentials using the same RBAC rules as legacy API keys. */
@Component
public class BearerCredentialFilter extends OncePerRequestFilter {
    private final SecurityContextHolder security;

    public BearerCredentialFilter(SecurityContextHolder security) {
        this.security = security;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                  FilterChain chain) throws ServletException, IOException {
        String authorization = request.getHeader("Authorization");
        if (authorization == null) {
            if (request.getHeader("X-API-Key") != null) {
                chain.doFilter(request, response);
                return;
            }
            String token = null;
            if (request.getCookies() != null) {
                for (Cookie cookie : request.getCookies()) {
                    if ("mis_session".equals(cookie.getName())) token = cookie.getValue();
                }
            }
            String browserKey = security.browserSessionApiKey(token);
            if (browserKey == null) chain.doFilter(request, response);
            else chain.doFilter(withApiKey(request, browserKey), response);
            return;
        }
        if (!authorization.regionMatches(true, 0, "Bearer ", 0, 7)
                || authorization.substring(7).isBlank()) {
            response.sendError(401, "Invalid authorization credential");
            return;
        }
        String token = authorization.substring(7).trim();
        String apiKey = request.getHeader("X-API-Key");
        if (apiKey != null && !apiKey.equals(token)) {
            response.sendError(401, "Conflicting authorization credentials");
            return;
        }
        chain.doFilter(withApiKey(request, token), response);
    }

    private HttpServletRequestWrapper withApiKey(HttpServletRequest request, String token) {
        return new HttpServletRequestWrapper(request) {
            @Override
            public String getHeader(String name) {
                return "X-API-Key".equalsIgnoreCase(name) ? token : super.getHeader(name);
            }
            @Override
            public java.util.Enumeration<String> getHeaders(String name) {
                return "X-API-Key".equalsIgnoreCase(name)
                        ? java.util.Collections.enumeration(java.util.List.of(token))
                        : super.getHeaders(name);
            }
            @Override
            public java.util.Enumeration<String> getHeaderNames() {
                var names = new java.util.LinkedHashSet<>(java.util.Collections.list(super.getHeaderNames()));
                names.add("X-API-Key");
                return java.util.Collections.enumeration(names);
            }
        };
    }
}
