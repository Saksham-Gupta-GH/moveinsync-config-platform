package com.moveinsync.server.controller;

import com.moveinsync.server.service.ConfigService;
import com.moveinsync.server.service.SseBroadcaster;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.lang.management.ManagementFactory;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Operational Monitoring & Metrics Controller for 3 AM On-Call visibility.
 */
@RestController
@RequestMapping("/api/v1/metrics")
public class MetricsController {

    private final ConfigService configService;
    private final SseBroadcaster sseBroadcaster;

    public MetricsController(ConfigService configService, SseBroadcaster sseBroadcaster) {
        this.configService = configService;
        this.sseBroadcaster = sseBroadcaster;
    }

    @GetMapping
    public ResponseEntity<Map<String, Object>> getMetrics() {
        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("config_current_version", configService.getLatestVersion());
        metrics.put("config_connected_subscribers", sseBroadcaster.getConnectedSubscribers());
        metrics.put("config_subscriber_queue_depth", sseBroadcaster.getTotalQueueDepth());
        metrics.put("config_rejections_total", configService.getRejectionCount());
        metrics.put("jvm_uptime_seconds", ManagementFactory.getRuntimeMXBean().getUptime() / 1000);
        metrics.put("jvm_memory_used_mb", (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / (1024 * 1024));
        metrics.put("jvm_memory_max_mb", Runtime.getRuntime().maxMemory() / (1024 * 1024));

        return ResponseEntity.ok(metrics);
    }
}

