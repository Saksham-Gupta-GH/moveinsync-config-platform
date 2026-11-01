package com.moveinsync.server.controller;

import com.moveinsync.server.security.SecurityContextHolder;
import com.moveinsync.server.service.ConfigService;
import com.moveinsync.server.service.SseBroadcaster;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.lang.management.ManagementFactory;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Operational Monitoring & Metrics Controller.
 * Exposes core system health, queue depth, max lag seen, catch-up queries, and error counts.
 */
@RestController
@RequestMapping("/api/v1/metrics")
public class MetricsController {

    private final ConfigService configService;
    private final SseBroadcaster sseBroadcaster;
    private final SecurityContextHolder securityContext;

    public MetricsController(ConfigService configService, SseBroadcaster sseBroadcaster, SecurityContextHolder securityContext) {
        this.configService = configService;
        this.sseBroadcaster = sseBroadcaster;
        this.securityContext = securityContext;
    }

    @GetMapping
    public ResponseEntity<?> getMetrics(@RequestHeader(value = "X-API-Key", required = false) String apiKey) {
        var principal = securityContext.authenticate(apiKey);
        if (principal.role() == SecurityContextHolder.Role.EMPLOYEE) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(Map.of("error", "Forbidden: metrics require admin or operator privileges"));
        }

        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("config_current_version", configService.getLatestVersion());
        metrics.put("config_connected_subscribers", sseBroadcaster.getConnectedSubscribers());
        metrics.put("config_subscriber_queue_depth", sseBroadcaster.getTotalQueueDepth());
        metrics.put("config_rejections_total", configService.getRejectionCount());
        metrics.put("config_max_lag_seen", configService.getMaxVersionLagSeen());
        metrics.put("config_client_catchup_requests_total", configService.getClientCatchupRequestsTotal());
        metrics.put("config_retries_total", configService.getClientCatchupRequestsTotal());
        metrics.put("config_error_requests_total", configService.getErrorRequestsTotal());
        metrics.put("jvm_uptime_seconds", ManagementFactory.getRuntimeMXBean().getUptime() / 1000);
        metrics.put("jvm_memory_used_mb", (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024));
        metrics.put("jvm_memory_max_mb", Runtime.getRuntime().maxMemory() / (1024 * 1024));

        return ResponseEntity.ok(metrics);
    }

    @org.springframework.web.bind.annotation.ExceptionHandler(SecurityException.class)
    public ResponseEntity<?> handleSecurityException(SecurityException e) {
        configService.incrementErrorCount();
        HttpStatus status = e.getMessage().contains("Authentication failed") ? HttpStatus.UNAUTHORIZED : HttpStatus.FORBIDDEN;
        return ResponseEntity.status(status).body(Map.of("error", e.getMessage()));
    }
}
