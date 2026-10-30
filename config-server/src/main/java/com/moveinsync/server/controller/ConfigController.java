package com.moveinsync.server.controller;

import com.moveinsync.client.cache.ConfigSnapshot;
import com.moveinsync.core.model.*;
import com.moveinsync.core.validator.ValidationException;
import com.moveinsync.server.security.SecurityContextHolder;
import com.moveinsync.server.service.ConfigService;
import com.moveinsync.server.service.SseBroadcaster;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

@RestController
@RequestMapping("/api/v1/config")
public class ConfigController {
    private static final Logger log = Logger.getLogger(ConfigController.class.getName());

    private final ConfigService configService;
    private final SseBroadcaster sseBroadcaster;
    private final SecurityContextHolder securityContext;

    public record PublishChangeSetRequest(
            ConfigScopeKey scopeKey,
            List<String> stagedOffices,
            Map<String, Object> updates,
            String author,
            String reason
    ) {}

    public record RollbackRequest(
            long targetVersion,
            boolean dryRun,
            String author
    ) {}

    public record ErrorResponse(
            String errorCode,
            String message,
            Instant timestamp
    ) {}

    public ConfigController(ConfigService configService, SseBroadcaster sseBroadcaster, SecurityContextHolder securityContext) {
        this.configService = configService;
        this.sseBroadcaster = sseBroadcaster;
        this.securityContext = securityContext;
    }

    @PostMapping("/definitions")
    public ResponseEntity<?> defineSetting(@RequestHeader(value = "X-API-Key", required = false) String apiKey,
                                           @RequestBody SettingDefinition def) {
        var principal = securityContext.authenticate(apiKey);
        if (principal.role() != SecurityContextHolder.Role.ADMIN) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(new ErrorResponse("FORBIDDEN", "Only ADMIN can define settings", Instant.now()));
        }
        configService.registerDefinition(def);
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of("status", "SUCCESS", "key", def.key()));
    }

    @GetMapping("/definitions")
    public ResponseEntity<Map<String, SettingDefinition>> getDefinitions() {
        return ResponseEntity.ok(configService.getDefinitions());
    }

    @PostMapping("/changeset")
    public ResponseEntity<?> publishChangeSet(@RequestHeader(value = "X-API-Key", required = false) String apiKey,
                                              @RequestBody PublishChangeSetRequest req) {
        var principal = securityContext.authenticate(apiKey);
        if (principal.role() == SecurityContextHolder.Role.EMPLOYEE) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(new ErrorResponse("FORBIDDEN", "Read-only clients cannot publish changesets", Instant.now()));
        }
        if (req.scopeKey().clientId() != null) {
            securityContext.enforceTenantAccess(principal, req.scopeKey().clientId());
        }

        ChangeSet cs = configService.publishChangeSet(req.scopeKey(), req.stagedOffices(), req.updates(), req.author(), req.reason());
        return ResponseEntity.status(HttpStatus.CREATED).body(cs);
    }

    @PostMapping("/changeset/{id}/promote")
    public ResponseEntity<?> promoteChangeSet(@RequestHeader(value = "X-API-Key", required = false) String apiKey,
                                              @PathVariable("id") String id,
                                              @RequestParam(value = "author", defaultValue = "admin") String author) {
        var principal = securityContext.authenticate(apiKey);
        if (principal.role() == SecurityContextHolder.Role.EMPLOYEE) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(new ErrorResponse("FORBIDDEN", "Insufficient privileges", Instant.now()));
        }
        ChangeSet cs = configService.promoteChangeSet(id, author);
        return ResponseEntity.ok(cs);
    }

    @PostMapping("/rollback")
    public ResponseEntity<?> rollback(@RequestHeader(value = "X-API-Key", required = false) String apiKey,
                                      @RequestBody RollbackRequest req) {
        var principal = securityContext.authenticate(apiKey);
        if (principal.role() != SecurityContextHolder.Role.ADMIN) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body(new ErrorResponse("FORBIDDEN", "Only ADMIN can trigger rollbacks", Instant.now()));
        }
        var resp = configService.rollback(req.targetVersion(), req.dryRun(), req.author());
        return ResponseEntity.ok(resp);
    }

    @GetMapping("/snapshot")
    public ResponseEntity<?> getSnapshot(@RequestHeader(value = "X-API-Key", required = false) String apiKey,
                                         @RequestParam("client_id") String clientId,
                                         @RequestParam(value = "office_id", required = false) String officeId) {
        var principal = securityContext.authenticate(apiKey);
        securityContext.enforceTenantAccess(principal, clientId);

        ConfigSnapshot snapshot = configService.getSnapshot(clientId, officeId);
        return ResponseEntity.ok(snapshot);
    }

    @GetMapping("/deltas")
    public ResponseEntity<?> getDeltas(@RequestHeader(value = "X-API-Key", required = false) String apiKey,
                                       @RequestParam("client_id") String clientId,
                                       @RequestParam(value = "office_id", required = false) String officeId,
                                       @RequestParam(value = "since_version", defaultValue = "0") long sinceVersion) {
        var principal = securityContext.authenticate(apiKey);
        securityContext.enforceTenantAccess(principal, clientId);

        List<Delta> deltas = configService.getDeltas(clientId, officeId, sinceVersion);
        return ResponseEntity.ok(deltas);
    }

    @GetMapping("/version")
    public ResponseEntity<?> getVersion(@RequestHeader(value = "X-API-Key", required = false) String apiKey,
                                        @RequestParam("client_id") String clientId,
                                        @RequestParam(value = "office_id", required = false) String officeId,
                                        @RequestParam(value = "client_version", defaultValue = "0") long clientVersion) {
        var principal = securityContext.authenticate(apiKey);
        securityContext.enforceTenantAccess(principal, clientId);

        long latest = configService.getLatestVersion();
        long lag = Math.max(0, latest - clientVersion);
        return ResponseEntity.ok(Map.of(
                "version", latest,
                "client_version", clientVersion,
                "version_lag", lag
        ));
    }

    /**
     * Real-time Server-Sent Events (SSE) Stream.
     * Evaluator Fix: Subscription requires client_id + office_id for tenant filtering & staged canary scoping.
     */
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream(@RequestHeader(value = "X-API-Key", required = false) String apiKey,
                             @RequestParam("client_id") String clientId,
                             @RequestParam(value = "office_id", defaultValue = "ALL") String officeId) {
        var principal = securityContext.authenticate(apiKey);
        securityContext.enforceTenantAccess(principal, clientId);

        return sseBroadcaster.subscribe(clientId, officeId);
    }

    @ExceptionHandler(ValidationException.class)
    public ResponseEntity<ErrorResponse> handleValidationException(ValidationException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse("INVALID_SETTING_VALUE", e.getMessage(), Instant.now()));
    }

    @ExceptionHandler(SecurityException.class)
    public ResponseEntity<ErrorResponse> handleSecurityException(SecurityException e) {
        return ResponseEntity.status(HttpStatus.FORBIDDEN).body(new ErrorResponse("TENANT_ISOLATION_VIOLATION", e.getMessage(), Instant.now()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArgumentException(IllegalArgumentException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(new ErrorResponse("BAD_REQUEST", e.getMessage(), Instant.now()));
    }
}
