package de.simon.originality;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import de.simon.originality.magicquery.client_interact.JsonApiSearchService;
import jakarta.annotation.PreDestroy;
import jakarta.servlet.http.HttpServletRequest;

public class McpServerController {

    private final JsonApiSearchService jsonApiSearchService;
    private final ObjectMapper objectMapper;
    private final Set<SseEmitter> activeEmitters = ConcurrentHashMap.newKeySet();
    
    public McpServerController(JsonApiSearchService jsonApiSearchService, ObjectMapper objectMapper) {
        this.jsonApiSearchService = jsonApiSearchService;
        this.objectMapper = objectMapper;
    }

    @PreDestroy
    public void shutdown() {
        // Meldet dem Tomcat beim Beenden: "Alle MCP-Anfragen sind abgeschlossen",
        // wodurch der Graceful Shutdown sofort durchläuft.
        for (SseEmitter emitter : activeEmitters) {
            try {
                emitter.complete();
            } catch (Exception e) {
                // ignorieren
            }
        }
        activeEmitters.clear();
    }

    
    @GetMapping(produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter handleGet(HttpServletRequest request) {
        SseEmitter emitter = new SseEmitter(-1L);
        activeEmitters.add(emitter);

        emitter.onCompletion(() -> activeEmitters.remove(emitter));
        emitter.onTimeout(() -> {
            activeEmitters.remove(emitter);
            emitter.complete();
        });
        emitter.onError(e -> {
            activeEmitters.remove(emitter);
            emitter.completeWithError(e);
        });
        try {
            String endpointUri = request.getRequestURL().toString();
            emitter.send(SseEmitter.event().name("endpoint").data(endpointUri));
        } catch (Exception e) {
            emitter.completeWithError(e);
        }
        return emitter;
    }

    @PostMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<JsonNode> handlePost(@RequestBody JsonNode message) {
        String method = message.path("method").asText();
        JsonNode idNode = message.get("id");
        JsonNode params = message.get("params");

        System.out.println("MCP Anfrage: " + method);

        // 1. Notifications (keine ID) -> 200 OK
        if (idNode == null || idNode.isNull() || idNode.isMissingNode()) {
            if ("notifications/initialized".equals(method)) {
                System.out.println("-> Handshake abgeschlossen. Verbindung steht!");
            }
            return ResponseEntity.ok().build();
        }

        // 2. Antwort-Gerüst
        ObjectNode response = objectMapper.createObjectNode();
        response.put("jsonrpc", "2.0");
        response.set("id", idNode);

        try {
            // --- HANDSHAKE (INITIALIZE) ---
            if ("initialize".equals(method)) {
                ObjectNode result = objectMapper.createObjectNode();
                result.put("protocolVersion", "2024-11-05");
                
                ObjectNode capabilities = result.putObject("capabilities");
                
                // Wir kündigen NUR Tools an.
                capabilities.putObject("tools"); 
                
                // Resources & Prompts weglassen, da wir sie nicht nutzen.
                
                ObjectNode serverInfo = result.putObject("serverInfo");
                serverInfo.put("name", "Ideenatlas-MCP");
                serverInfo.put("version", "1.0.0");
                
                response.set("result", result);
                return ResponseEntity.ok(response);

            // --- TOOLS LIST ---
            } else if ("tools/list".equals(method)) {
                response.set("result", generateToolsList());
                return ResponseEntity.ok(response);

            // --- TOOL CALL ---
            } else if ("tools/call".equals(method)) {
                String toolName = params.path("name").asText();
                ObjectNode result = objectMapper.createObjectNode();
                ArrayNode contentArray = result.putArray("content");

                if ("search".equals(toolName)) {
                    String query = params.path("arguments").path("query").asText();
                    try {
                        var searchResult = jsonApiSearchService.performSearch(query);
                        // Ergebnis als JSON-String formatieren
                        String jsonString = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(searchResult);
                        contentArray.addObject().put("type", "text").put("text", jsonString);
                    } catch (Exception e) {
                        result.put("isError", true);
                        contentArray.addObject().put("type", "text").put("text", "Error: " + e.getMessage());
                    }
                } else {
                    result.put("isError", true);
                    contentArray.addObject().put("type", "text").put("text", "Unknown tool: " + toolName);
                }
                response.set("result", result);
                return ResponseEntity.ok(response);

            // --- RESOURCES LIST ---
            } else if ("resources/list".equals(method)) {
                ObjectNode result = objectMapper.createObjectNode();
                result.putArray("resources");
                // WICHTIG: KEIN "nextCursor" Feld hinzufügen, wenn es null ist!
                // Das Feld wegzulassen ist der korrekte Weg für "keine weiteren Seiten".
                response.set("result", result);
                return ResponseEntity.ok(response);

            // --- RESOURCE TEMPLATES LIST ---
            } else if ("resources/templates/list".equals(method)) {
                 ObjectNode result = objectMapper.createObjectNode();
                 result.putArray("resourceTemplates");
                 // WICHTIG: KEIN nextCursor
                 response.set("result", result);
                 return ResponseEntity.ok(response);

            // --- PROMPTS LIST ---
            } else if ("prompts/list".equals(method)) {
                ObjectNode result = objectMapper.createObjectNode();
                result.putArray("prompts");
                // WICHTIG: KEIN nextCursor
                response.set("result", result);
                return ResponseEntity.ok(response);

            // --- PING ---
            } else if ("ping".equals(method)) {
                response.set("result", objectMapper.createObjectNode());
                return ResponseEntity.ok(response);

            // --- UNBEKANNT ---
            } else {
                // Leeres Resultat senden, um Abstürze zu vermeiden
                response.set("result", objectMapper.createObjectNode());
                return ResponseEntity.ok(response);
            }

        } catch (Exception e) {
            e.printStackTrace();
            response.putObject("error").put("code", -32603).put("message", "Internal error: " + e.getMessage());
            return ResponseEntity.ok(response);
        }
    }

    private ObjectNode generateToolsList() {
        ObjectNode result = objectMapper.createObjectNode();
        ArrayNode tools = result.putArray("tools");
        
        ObjectNode tool = tools.addObject();
        tool.put("name", "search");
        tool.put("description", "Searches the ideenatlas.eu vector database for scientific papers, related topic fields, and serendipitous connections. INSTRUCTION: The 'query' argument MUST be a single, dense, factual paragraph extracting the core scientific idea. Use precise, objective scientific language.");
        
        ObjectNode inputSchema = tool.putObject("inputSchema");
        inputSchema.put("type", "object");
        ObjectNode properties = inputSchema.putObject("properties");
        ObjectNode queryProp = properties.putObject("query");
        queryProp.put("type", "string");
        queryProp.put("description", "Scientific query paragraph.");
        
        inputSchema.putArray("required").add("query");
        
        // WICHTIG: Hier auch KEIN nextCursor setzen!
        
        return result;
    }
}