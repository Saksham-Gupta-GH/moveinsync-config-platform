package com.moveinsync.server;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class BearerAuthenticationTest {
    @Autowired private MockMvc mvc;

    @Test
    void bearerUsesTheSameAuthenticationAndTenantRules() throws Exception {
        mvc.perform(get("/api/v1/config/snapshot?client_id=Google")
                .header("Authorization", "Bearer google-service-key")).andExpect(status().isOk());
        mvc.perform(get("/api/v1/config/snapshot?client_id=Amazon")
                .header("Authorization", "Bearer google-service-key")).andExpect(status().isForbidden());
        mvc.perform(get("/api/v1/config/snapshot?client_id=Google")
                .header("Authorization", "Bearer invalid")).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/v1/config/snapshot?client_id=Google")
                .header("Authorization", "Bearer google-service-key")
                .header("X-API-Key", "admin-secret-key")).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/v1/config/snapshot?client_id=Google")
                .header("Authorization", "Basic invalid")).andExpect(status().isUnauthorized());
    }
}
