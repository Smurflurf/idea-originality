package de.simon.originality.magicquery.client_interact;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.simon.originality.magicquery.MagicNumbers;
import jakarta.annotation.PreDestroy;

/**
 * Manages SSE connections with heartbeats and an event cache for robust client communication.
 */
@Service
public class ServerSentEventService {
    private final Map<String, SseEmitter> emitters = new ConcurrentHashMap<>();
    private final ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
    private final Map<String, ScheduledFuture<?>> heartbeatTasks = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper;
    private record EventCacheEntry(String status, Object data) {}
    private final Map<String, List<EventCacheEntry>> eventCache = new ConcurrentHashMap<>();

    public ServerSentEventService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public void sendEvent(String jobId, String status, Object data) {
        // Event immer zuerst cachen, damit es bei Wiederverbindung verfügbar ist
        eventCache.computeIfAbsent(jobId, k -> new ArrayList<>()).add(new EventCacheEntry(status, data));
        
        SseEmitter emitter = emitters.get(jobId);
        if (emitter != null) {
            try {
                Map<String, Object> eventData = Map.of("status", status, "data", data);
                String jsonEventData = objectMapper.writeValueAsString(eventData);
                emitter.send(SseEmitter.event().name("update").data(jsonEventData));
            } catch (IOException e) {
                // Hier nichts tun. Der onError-Handler wird die Verbindung bereinigen, falls sie tot ist.
                // Das verhindert, dass eine Verbindung bei einem temporären Fehler sofort geschlossen wird.
            }
        }
    }

    private void sendHeartbeat(String jobId) {
        SseEmitter emitter = emitters.get(jobId);
        if (emitter != null) {
            try {
                emitter.send(SseEmitter.event().comment("keep-alive"));
            } catch (IOException e) {
                // Erwarteter Fehler, wenn der Client die Verbindung geschlossen hat.
            }
        }
    }

    private void startHeartbeat(String jobId) {
        ScheduledFuture<?> task = heartbeatExecutor.scheduleAtFixedRate(
            () -> 
            sendHeartbeat(jobId), 
            MagicNumbers.HEARTBEAT_INTERVALL_S.asInteger(), 
            MagicNumbers.HEARTBEAT_INTERVALL_S.asInteger(), 
            TimeUnit.SECONDS);
        heartbeatTasks.put(jobId, task);
    }

    private void stopHeartbeat(String jobId) {
        ScheduledFuture<?> task = heartbeatTasks.remove(jobId);
        if (task != null) {
            task.cancel(true);
        }
    }
    
    public void addEmitter(String jobId, SseEmitter emitter) {
        emitters.put(jobId, emitter);
        startHeartbeat(jobId);

        // Alle gecachten Events sofort an den (wieder-)verbundenen Client senden
        List<EventCacheEntry> cachedEvents = eventCache.get(jobId);
        if (cachedEvents != null) {
            for (EventCacheEntry entry : cachedEvents) {
                try {
                    Map<String, Object> eventData = Map.of("status", entry.status(), "data", entry.data());
                    String jsonEventData = objectMapper.writeValueAsString(eventData);
                    emitter.send(SseEmitter.event().name("update").data(jsonEventData));
                } catch (IOException e) {
                    // Wenn hier schon ein Fehler auftritt, wird der onError-Handler den Rest übernehmen
                    break; 
                }
            }
        }
        
        // Zentraler Callback für alle Arten von Verbindungsabbrüchen
        Runnable cleanupCallback = () -> {
            System.out.println("SSE connection for job " + jobId + " ended (timeout/error). Cleaning up.");
            emitters.remove(jobId);
            stopHeartbeat(jobId);
            cleanupJobData(jobId); 
        };
        
        // Handler für den erfolgreichen Abschluss
        emitter.onCompletion(() -> {
            System.out.println("SSE connection for job " + jobId + " completed successfully.");
            emitters.remove(jobId);
            stopHeartbeat(jobId);
            // HINWEIS: cleanupJobData wird hier NICHT aufgerufen,
            // da die Daten für die Ergebnisseite noch benötigt werden.
        });

        emitter.onTimeout(cleanupCallback);
        emitter.onError(e -> {
             System.err.println("SSE error for job " + jobId + ": " + e.getMessage());
             cleanupCallback.run();
        });
        
        try {
            emitter.send(SseEmitter.event().comment("SSE connection established."));
        } catch (IOException e) {
            emitter.complete(); 
        }
    }

    public void cleanupJobData(String jobId) {
        QueryProcessingService.finalResults.remove(jobId);
        QueryProcessingService.finalColorMaps.remove(jobId + "_" + MagicNumbers.OWN_IMAGE_PREFIX.asString());
        QueryProcessingService.finalColorMaps.remove(jobId + "_" + MagicNumbers.NEIGHBOR_CLUSTER_IMAGE_PREFIX.asString());
        QueryProcessingService.finalColorMaps.remove(jobId + "_" + MagicNumbers.SERENDIPITY_IMAGE_PREFIX.asString()); 
        QueryProcessingService.finalVisualizations.keySet().removeIf(key -> key.startsWith(jobId));
        eventCache.remove(jobId); 

        System.out.println("-> Job-Daten für " + jobId + " erfolgreich aus dem Speicher entfernt.");
        System.gc();
    }
    
    public void completeEmitter(String jobId) {
        SseEmitter emitter = emitters.get(jobId);
        if (emitter != null) {
            emitter.complete();
        }
    }
    
    @PreDestroy
    public void shutdown() {
        System.out.println("Shutting down ServerSentEventService...");
        emitters.values().forEach(SseEmitter::complete);
        heartbeatExecutor.shutdownNow(); 
    }
}