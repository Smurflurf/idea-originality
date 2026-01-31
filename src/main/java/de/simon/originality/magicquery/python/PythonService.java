// --- START OF FILE PythonService.java ---
package de.simon.originality.magicquery.python;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import de.simon.originality.magicquery.MagicNumbers;
import reactor.core.publisher.Mono;


/**
 * Interface to interact with the Python server.
 */
@Service
public class PythonService {
    private final WebClient webClient;
    private final ObjectMapper objectMapper;
    private final PythonServerManager serverManager;
    public record TranslationResponse(
        @JsonProperty("translated_text") String translatedText,
        @JsonProperty("detected_source_lang") String detectedSourceLang,
        @JsonProperty("target_lang") String targetLang
    ) {}
    private List<Map<String, String>> cachedLanguages = null;
    
    public PythonService(ObjectMapper objectMapper, PythonServerManager serverManager) {
        this.objectMapper = objectMapper;
        this.serverManager = serverManager;
        this.webClient = WebClient.builder()
                .baseUrl("http://127.0.0.1:5001")
                .build();
    }
    
    private <T> T performRequestWithRetry(String uri, Class<T> responseType) {
        return performRequestWithRetry(uri, null, responseType);
    }
    
    @SuppressWarnings("unchecked")
	private <T> T performRequestWithRetry(String uri, Object body, Class<T> responseType) {
        final int MAX_RETRIES = 1; 
        for (int i = 0; i <= MAX_RETRIES; i++) {
            try {
                if (!serverManager.isServerAlive()) {
                    throw new IOException("Python process is not alive.");
                }

                // UNTERSCHEIDUNG: GET oder POST
                var requestSpec = (body == null) 
                    ? webClient.get().uri(uri)
                    : webClient.post().uri(uri).bodyValue(body);

                String responseJson = requestSpec
                        .retrieve()
                        .bodyToMono(String.class)
                        .block(Duration.ofMillis(MagicNumbers.WAIT_FOR_VECTORIZING_SERVER_ANSWER_MS.asInteger()));

                if (responseJson == null) throw new RuntimeException("API endpoint " + uri + " didn't answer.");
                
                // Leere Antwort abfangen (außer bei Listen, da ist [] okay)
                if (responseType != null && responseType != List.class && responseJson.isEmpty()) {
                    return null;
                }

                JsonNode rootNode = objectMapper.readTree(responseJson);
                
                // UNTERSCHEIDUNG: Welches JSON-Feld nehmen wir?
                JsonNode dataNode;
                if ("/translate".equals(uri) || "/languages".equals(uri)) {
                    dataNode = rootNode;
                } else if (uri.startsWith("/reduce_dim")) {
                    dataNode = rootNode.path("reduced_vector");
                } else if (uri.startsWith("/cluster")) {
                    dataNode = rootNode;
                } else {
                    dataNode = rootNode.path("vector");
                }

                // Spezialfall: Wenn wir eine Liste erwarten (für Languages)
                if (uri.equals("/languages")) {
                    return (T) objectMapper.readerFor(new TypeReference<List<Map<String, String>>>(){}).readValue(dataNode);
                }

                return objectMapper.treeToValue(dataNode, responseType);

            } catch (Exception e) {
                if (i < MAX_RETRIES && (e instanceof IOException || (e.getCause() != null && e.getCause().getMessage().contains("Connection refused")))) {
                    System.err.println("Python server connection failed. Attempting restart... (Attempt " + (i + 1) + ")");
                    try {
                        serverManager.restartServer();
                    } catch (Exception restartException) {
                        throw new RuntimeException("Failed to restart the Python server.", restartException);
                    }
                } else {
                    throw new RuntimeException("Error communicating with Python server at " + uri, e);
                }
            }
        }
        throw new RuntimeException("Exited retry loop unexpectedly.");
    }
    
    /**
     * Prüft rekursiv, ob eine Exception durch einen Client-Abbruch verursacht wurde.
     */
    private boolean isClientAbort(Throwable t) {
        if (t == null) return false;

        // 1. Check auf spezifische Klassen
        if (t instanceof InterruptedException) return true;
        // Tomcat/Spring spezifische Client-Abbrüche (Namen als String prüfen, um Imports zu sparen)
        String clsName = t.getClass().getName();
        if (clsName.contains("ClientAbortException") || clsName.contains("AbortedException")) return true;

        // 2. Check auf spezifische Nachrichten
        String msg = t.getMessage();
        if (msg != null) {
            String lowerMsg = msg.toLowerCase();
            if (lowerMsg.contains("broken pipe") || 
                lowerMsg.contains("connection reset") || 
                lowerMsg.contains("client_abort") ||
                lowerMsg.contains("stream closed")) {
                return true;
            }
        }

        // 3. Rekursion: Ursache prüfen
        return isClientAbort(t.getCause());
    }

    /**
     * Streamt Audio-Daten direkt vom Python-Server in den OutputStream der HTTP-Antwort.
     */
    public void streamTts(String text, OutputStream responseOutputStream) {
        Map<String, String> requestBody = Map.of("text", text);
        final int MAX_RETRIES = 1;
        String uri = "/tts";

        for (int i = 0; i <= MAX_RETRIES; i++) {
            try {
                if (!serverManager.isServerAlive()) {
                    throw new IOException("Python process is not alive.");
                }

                webClient.post()
                        .uri(uri)
                        .bodyValue(requestBody)
                        .retrieve()
                        .bodyToFlux(DataBuffer.class)
                        .doOnNext(dataBuffer -> {
                            try {
                                byte[] bytes = new byte[dataBuffer.readableByteCount()];
                                dataBuffer.read(bytes);
                                responseOutputStream.write(bytes);
                                responseOutputStream.flush();
                            } catch (IOException e) {
                                // Wir werfen eine RuntimeException mit eindeutigem Namen, 
                                // die von isClientAbort erkannt wird.
                                throw new RuntimeException("CLIENT_ABORT: Write failed", e);
                            } finally {
                                DataBufferUtils.release(dataBuffer);
                            }
                        })
                        .blockLast(); // Hier fliegt die Exception raus

                return; // Alles gut gegangen

            } catch (Exception e) {
                // HIER IST DER FIX: Wir nutzen die rekursive Prüfung
                if (isClientAbort(e)) {
                    //System.out.println("TTS Stream vom Client abgebrochen (" + e.getClass().getSimpleName() + "). Kein Server-Restart.");
                    return; // Einfach aufhören, KEIN Restart.
                }

                // Echter Python-Fehler -> Restart Logik
                if (i < MAX_RETRIES) {
                    System.err.println("TTS stream failed due to server error (" + e.getClass().getSimpleName() + "). Restarting Python... (Attempt " + (i + 1) + ")");
                    try {
                        serverManager.restartServer();
                    } catch (Exception restartException) {
                        throw new RuntimeException("Failed to restart the Python server.", restartException);
                    }
                } else {
                    throw new RuntimeException("Error communicating with Python TTS endpoint.", e);
                }
            }
        }
    }
    
    public float[] vectorize(String text) {
        return performRequestWithRetry("/vectorize", text, float[].class);
    }

    @SuppressWarnings("unchecked")
	public List<Map<String, String>> getSupportedLanguages() {
        if (cachedLanguages != null && !cachedLanguages.isEmpty()) {
            return cachedLanguages;
        }
        try {
            cachedLanguages = performRequestWithRetry("/languages", List.class);
        } catch (Exception e) {
            System.err.println("Warnung: Konnte Sprachen nicht laden (Python nicht bereit?): " + e.getMessage());
            return Collections.emptyList();
        }
        
        return cachedLanguages != null ? cachedLanguages : Collections.emptyList();
    }

    /**
     * Flux-basierte Streaming-Methode.
     * Nutzt Reactive Streams, um Daten sofort weiterzuleiten, ohne zu puffern.
     */
    public void translateStream(String text, String targetLang, Consumer<String> chunkConsumer) {
        Map<String, String> requestBody = Map.of("text", text, "target_lang", targetLang);
        final int MAX_RETRIES = 1;

        for (int i = 0; i <= MAX_RETRIES; i++) {
            try {
                if (!serverManager.isServerAlive()) throw new IOException("Python process is not alive.");
                // Wir nutzen Flux, um den Stream "live" zu verarbeiten
                webClient.post()
                        .uri("/translate")
                        .bodyValue(requestBody)
                        .retrieve()
                        .bodyToFlux(String.class) // Holt die Daten stückchenweise als String
                        .doOnNext(line -> {
                            // Dieser Block wird für JEDEN Chunk sofort ausgeführt
                            try {
                                if (line.trim().isEmpty()) return; // Leere Keep-Alive Zeilen ignorieren

                                JsonNode node = objectMapper.readTree(line);
                                String type = node.path("type").asText();

                                if ("data".equals(type)) {
                                    // Chunk sofort an Frontend weitergeben
                                    String chunk = node.path("chunk").asText();
                                    chunkConsumer.accept(chunk);
                                }
                            } catch (Exception e) {
                                // Parsing-Fehler einzelner Zeilen ignorieren wir, um den Stream nicht abzubrechen
                            }
                        })
                        .blockLast(); // WICHTIG: Wir warten hier, bis der Stream fertig ist (da wir im Executor-Thread sind)
                
                return; // Erfolgreich fertig

            } catch (Exception e) {
                if (i < MAX_RETRIES) {
                    System.err.println("Translation stream interrupted. Retrying... " + e.getMessage());
                    try { serverManager.restartServer(); } catch (Exception ex) {}
                } else {
                    chunkConsumer.accept(" [Error: Connection lost] ");
                    System.err.println("Translation stream failed final: " + e.getMessage());
                }
            }
        }
    }
    
    public JsonNode cluster(float[] vectorClusterD, String cluster_id) {
         try {
            Map<String, float[]> requestBody = Map.of("vector", vectorClusterD);
            
            String responseJson = webClient.post()
                    .uri("/cluster/" + cluster_id)
                    .bodyValue(requestBody)
                    .retrieve()
                    .onStatus(status -> status.value() == 404, 
                              response -> Mono.empty()) 
                    .bodyToMono(String.class)
                    .block(Duration.ofMillis(MagicNumbers.WAIT_FOR_VECTORIZING_SERVER_ANSWER_MS.asInteger()));
            
            if (responseJson ==  null) {
                return objectMapper.createObjectNode()
                    .put("label", -1)
                    .put("probability", 0.0);
            }
            return objectMapper.readTree(responseJson);

        } catch (Exception e) {
            throw new RuntimeException("Error communicating with clustering-endpoint for " + cluster_id, e);
        }
    }

    public float[] reduceDimension(float[] vector, String modelType) {
        Map<String, float[]> requestBody = Map.of("vector", vector);
        return performRequestWithRetry("/reduce_dim/" + modelType, requestBody, float[].class);
    }
}