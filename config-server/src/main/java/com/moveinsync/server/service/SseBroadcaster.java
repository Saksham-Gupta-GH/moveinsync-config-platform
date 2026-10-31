package com.moveinsync.server.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.moveinsync.core.model.Delta;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages Server-Sent Events (SSE) subscriptions with bounded queues, slow-consumer isolation,
 * tenant filtering, contiguous version progression for staged rollouts, and serialized per-subscriber dispatch.
 */
@Service
public class SseBroadcaster {
    private static final Logger log = Logger.getLogger(SseBroadcaster.class.getName());

    public record SubscriberSession(
            String id,
            String clientId,
            String officeId,
            SseEmitter emitter,
            BlockingQueue<Delta> queue,
            AtomicInteger droppedEvents
    ) {}

    private final Map<String, SubscriberSession> subscribers = new ConcurrentHashMap<>();
    private final ExecutorService dispatchExecutor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "sse-dispatcher");
        t.setDaemon(true);
        return t;
    });

    private final ObjectMapper mapper;

    public SseBroadcaster() {
        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new JavaTimeModule());
    }

    public SseEmitter subscribe(String clientId, String officeId) {
        // 1-hour timeout for persistent connection
        SseEmitter emitter = new SseEmitter(3600_000L);
        String id = clientId + ":" + officeId + ":" + java.util.UUID.randomUUID();
        // Bounded queue: capacity 100 to protect server memory
        BlockingQueue<Delta> queue = new ArrayBlockingQueue<>(100);
        SubscriberSession session = new SubscriberSession(id, clientId, officeId, emitter, queue, new AtomicInteger(0));

        subscribers.put(id, session);

        emitter.onCompletion(() -> subscribers.remove(id));
        emitter.onTimeout(() -> {
            emitter.complete();
            subscribers.remove(id);
        });
        emitter.onError(e -> subscribers.remove(id));

        // Flush response headers even when no configuration has changed yet.
        try {
            emitter.send(SseEmitter.event().comment("connected"));
        } catch (IOException e) {
            subscribers.remove(id);
            emitter.completeWithError(e);
        }

        log.info("Registered SSE subscriber: " + id + " (Active subscribers: " + subscribers.size() + ")");
        return emitter;
    }

    /**
     * Broadcasts a delta to all subscribers in strict monotonic version order.
     * Option A: Out-of-scope subscribers receive an empty delta with the same version
     * so their local version counter advances contiguously without triggering false gaps.
     */
    public void broadcast(Delta delta) {
        for (SubscriberSession session : subscribers.values()) {
            boolean matchesTenant = isTenantInScope(delta, session.clientId());
            if (!matchesTenant) {
                // Tenant isolation: completely skip subscribers from different corporate clients
                continue;
            }

            boolean matchesOffice = isOfficeInScope(delta, session.officeId());
            Delta deltaToSend;
            if (matchesOffice) {
                deltaToSend = delta;
            } else {
                // Contiguous progression: send empty delta with the same version
                deltaToSend = Delta.emptyProgression(delta.version(), delta.scopeKey());
            }

            // Enqueue to bounded buffer
            boolean enqueued = session.queue().offer(deltaToSend);
            if (!enqueued) {
                // Slow consumer overflow -> drop client to protect server memory
                log.warning("Subscriber queue overflow for " + session.id() + ". Evicting slow consumer to force resync.");
                session.emitter().completeWithError(new RuntimeException("Buffer overflow: slow consumer"));
                subscribers.remove(session.id());
                continue;
            }

            // Asynchronously drain pending deltas to the client emitter with per-subscriber serialization
            dispatchExecutor.submit(() -> sendPending(session));
        }
    }

    private boolean isTenantInScope(Delta delta, String subscriberClientId) {
        String targetClient = delta.scopeKey().clientId();
        if (targetClient == null) {
            // Global scope change applies to all tenants
            return true;
        }
        return targetClient.equalsIgnoreCase(subscriberClientId);
    }

    private boolean isOfficeInScope(Delta delta, String subscriberOfficeId) {
        if (delta.stagedOffices() == null || delta.stagedOffices().isEmpty()) {
            // Not staged -> applies to all offices under the scope
            return true;
        }
        return delta.stagedOffices().contains(subscriberOfficeId);
    }

    /**
     * Serialized dispatch per subscriber: synchronizing on session ensures that multiple
     * concurrent dispatch tasks drain the queue in strict FIFO order without interleaving versions,
     * while guaranteeing thread safety on Spring's SseEmitter.send().
     */
    private void sendPending(SubscriberSession session) {
        synchronized (session) {
            Delta delta;
            while ((delta = session.queue().poll()) != null) {
                try {
                    String json = mapper.writeValueAsString(delta);
                    session.emitter().send(SseEmitter.event()
                            .name("delta")
                            .id(String.valueOf(delta.version()))
                            .data(json));
                } catch (Exception e) {
                    log.fine("Error sending SSE to " + session.id() + ": " + e.getMessage());
                    session.emitter().complete();
                    subscribers.remove(session.id());
                    break;
                }
            }
        }
    }

    public int getConnectedSubscribers() {
        return subscribers.size();
    }

    public int getTotalQueueDepth() {
        return subscribers.values().stream().mapToInt(s -> s.queue().size()).sum();
    }

    public Map<String, SubscriberSession> getSubscribers() {
        return Collections.unmodifiableMap(subscribers);
    }
}
