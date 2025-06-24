package de.simon.originality.magicquery.client_interact;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.annotation.PreDestroy;

/**
 * Manages sse, keeps track of the open connections and discards finished ones.
 * Communicates with the user client.
 */
@Service
public class ServerSentEventService {
	private final Map<String, SseEmitter> emitters = new ConcurrentHashMap<>();

	private final ObjectMapper objectMapper;

	public ServerSentEventService(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	public void sendEvent(String jobId, String status, Object data) {
		SseEmitter emitter = emitters.get(jobId);
		if (emitter != null) {
			try {
				Map<String, Object> eventData = Map.of("status", status, "data", data);
				String jsonEventData = objectMapper.writeValueAsString(eventData);

				emitter.send(SseEmitter.event().name("update").data(jsonEventData));
			} catch (IOException e) {
				emitter.complete();
				emitters.remove(jobId);
			}
		}
	}

	public void addEmitter(String jobId, SseEmitter emitter) {
		emitters.put(jobId, emitter);
		emitter.onCompletion(() -> emitters.remove(jobId));
		emitter.onTimeout(() -> emitters.remove(jobId));
	}

	public void completeEmitter(String jobId) {
		SseEmitter emitter = emitters.get(jobId);
		if (emitter != null) {
			emitter.complete();
			emitters.remove(jobId);
		}
	}
	
    @PreDestroy
    public void shutdown() {
        emitters.values().forEach(SseEmitter::complete);
    }
}