package com.moveinsync.server;

import jakarta.servlet.http.Cookie;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class BrowserSessionAuthenticationTest {
    @Autowired private MockMvc mvc;

    @Test
    void browserSessionAuthenticatesWithoutExposingTheApiKeyInRequests() throws Exception {
        mvc.perform(get("/api/v1/auth/me")).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/v1/config/stream?client_id=Google&api_key=admin-secret-key"))
                .andExpect(status().isUnauthorized());
        mvc.perform(post("/api/v1/auth/login").contentType("application/json")
                .content("{\"apiKey\":\"google-service-key\"}"))
                .andExpect(status().isUnauthorized());

        String setCookie = mvc.perform(post("/api/v1/auth/login").contentType("application/json")
                .content("{\"apiKey\":\"admin-secret-key\"}"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getHeader("Set-Cookie");
        org.assertj.core.api.Assertions.assertThat(setCookie).contains("HttpOnly", "SameSite=Strict");
        String token = setCookie.split(";", 2)[0].substring("mis_session=".length());
        Cookie cookie = new Cookie("mis_session", token);

        mvc.perform(get("/api/v1/auth/me").cookie(cookie)).andExpect(status().isOk());
        mvc.perform(get("/api/v1/config/snapshot?client_id=Google").cookie(cookie))
                .andExpect(status().isOk());
        mvc.perform(post("/api/v1/auth/logout").cookie(cookie)).andExpect(status().isOk());
        mvc.perform(get("/api/v1/config/snapshot?client_id=Google").cookie(cookie))
                .andExpect(status().isUnauthorized());
    }
}
