package de.simon.originality;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.imageio.ImageIO;
import javax.sound.sampled.AudioFileFormat;
import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;

import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;
import org.thymeleaf.templatemode.TemplateMode;
import org.thymeleaf.templateresolver.FileTemplateResolver;
import org.w3c.dom.Document;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class FrontendTestServer {

    private static final int PORT = 8080;
    
    // Pfade bei Bedarf anpassen
    private static final String TEMPLATE_ROOT = "src/main/resources/templates/";
    private static final String STATIC_ROOT = "src/main/resources/static";
    private static final String STYLING_ROOT = "src/main/resources/static/styling";
    private static final String SCRIPT_ROOT = "src/main/resources/static/script";
    
    private static final ExecutorService executor = Executors.newCachedThreadPool();
    private static volatile OutputStream sseClientStream;
    private static TemplateEngine templateEngine;
    private static String APP_VERSION = "DEV-TEST"; 
    
    // Wichtig für JSON Generierung im Frontend
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws IOException {
        APP_VERSION = resolveAppVersion() + "." + System.currentTimeMillis();
        System.out.println("Frontend-Testserver initialized with version: " + APP_VERSION);

        initializeThymeleaf();
        HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", PORT), 0);

        server.createContext("/", new MainHandler());
        server.createContext("/query/init", exchange -> sendJson(exchange, "{\"jobId\": \"test-job-"+APP_VERSION+"\"}"));
        server.createContext("/query/status/", new SseHandler());
        server.createContext("/query/start/", new SimulationHandler());
        server.createContext("/query/filtered-results", new AjaxResultHandler());
        
        // Mock API: Translate
        server.createContext("/api/translate", exchange -> {
            String mockStream = "data: {\"type\": \"data\", \"chunk\": \"[Mock] Übersetzung... \"}\n\n";
            byte[] bytes = mockStream.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/event-stream");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(bytes); }
            exchange.close();
		});

		// Mock API: TTS
        server.createContext("/api/tts", exchange -> {
            try {
                exchange.getRequestBody().readAllBytes();

                Path mp3Path = Paths.get("src/test/resources/testaudio.mp3");
                if (!Files.exists(mp3Path)) {
                    System.err.println("testaudio.mp3 nicht gefunden!");
                    exchange.sendResponseHeaders(404, 0);
                    exchange.close();
                    return;
                }

                // --- SCHRITT 1: MP3 zu PCM dekodieren (ohne Kanaländerung) ---
                AudioInputStream mp3Stream = AudioSystem.getAudioInputStream(mp3Path.toFile());
                AudioFormat sourceFormat = mp3Stream.getFormat();
                
                // Zielformat für die reine Dekodierung (behält Kanalanzahl bei)
                AudioFormat pcmFormat = new AudioFormat(AudioFormat.Encoding.PCM_SIGNED,
                        sourceFormat.getSampleRate(), 16, sourceFormat.getChannels(),
                        sourceFormat.getChannels() * 2, sourceFormat.getSampleRate(), false);
                        
                AudioInputStream pcmStream = AudioSystem.getAudioInputStream(pcmFormat, mp3Stream);
                byte[] pcmBytes = pcmStream.readAllBytes();

                // --- SCHRITT 2: Manuelles Downmixing von Stereo zu Mono (falls nötig) ---
                byte[] monoPcmBytes;
                if (pcmFormat.getChannels() > 1) {
                    // ByteBuffer hilft uns, Bytes sicher in 16-bit Shorts umzuwandeln
                    ByteBuffer stereoBuffer = ByteBuffer.wrap(pcmBytes).order(ByteOrder.LITTLE_ENDIAN);
                    ByteBuffer monoBuffer = ByteBuffer.allocate(pcmBytes.length / 2).order(ByteOrder.LITTLE_ENDIAN);

                    while (stereoBuffer.hasRemaining()) {
                        short left = stereoBuffer.getShort();
                        short right = stereoBuffer.getShort();
                        // Einfacher Durchschnitt für den Mono-Wert
                        short mono = (short) ((left + right) / 2);
                        monoBuffer.putShort(mono);
                    }
                    monoPcmBytes = monoBuffer.array();
                } else {
                    // War bereits Mono, keine Änderung nötig
                    monoPcmBytes = pcmBytes;
                }
                
                // --- SCHRITT 3: Finalen Mono-WAV-Stream erstellen und senden ---
                AudioFormat finalMonoFormat = new AudioFormat(
                    AudioFormat.Encoding.PCM_SIGNED, 22050, 16, 1, 2, 22050, false
                );

                long frameCount = monoPcmBytes.length / finalMonoFormat.getFrameSize();
                AudioInputStream finalStream = new AudioInputStream(
                    new ByteArrayInputStream(monoPcmBytes), finalMonoFormat, frameCount
                );

                ByteArrayOutputStream out = new ByteArrayOutputStream();
                AudioSystem.write(finalStream, AudioFileFormat.Type.WAVE, out);
                byte[] wavData = out.toByteArray();

                // Senden
                exchange.getResponseHeaders().set("Content-Type", "testaudio/wav");
                exchange.sendResponseHeaders(200, wavData.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(wavData);
                }

                System.out.println("TTS Mock: audio.mp3 erfolgreich zu WAV konvertiert und gesendet.");

            } catch (Exception e) {
                e.printStackTrace();
                try { exchange.sendResponseHeaders(500, 0); } catch (Exception ignored) {}
            } finally {
                exchange.close();
            }
        });
        
        server.setExecutor(executor);
        server.start();
        System.out.println("Frontend-Testserver running: http://localhost:" + PORT);
    }

    private static String resolveAppVersion() {
        try {
            Path pomPath = Paths.get("pom.xml");
            if (Files.exists(pomPath)) {
                DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                Document doc = dBuilder.parse(pomPath.toFile());
                XPath xPath = XPathFactory.newInstance().newXPath();
                String version = (String) xPath.evaluate("/project/version", doc, XPathConstants.STRING);
                if (version != null && !version.isEmpty()) return version;
            }
        } catch (Exception e) { }
        return "DEV-SNAPSHOT";
    }

    private static void initializeThymeleaf() {
        FileTemplateResolver resolver = new FileTemplateResolver();
        resolver.setPrefix(TEMPLATE_ROOT);
        resolver.setSuffix(".html");
        resolver.setTemplateMode(TemplateMode.HTML);
        resolver.setCharacterEncoding("UTF-8");
        resolver.setCacheable(false);

        templateEngine = new TemplateEngine();
        templateEngine.setTemplateResolver(resolver);
        
        // LinkBuilder Mock, um Fehler zu vermeiden
        templateEngine.setLinkBuilder(new org.thymeleaf.linkbuilder.StandardLinkBuilder() {
            @Override
            protected String computeContextPath(final org.thymeleaf.context.IExpressionContext context, final String base, final java.util.Map<String, Object> parameters) {
                return "";
            }
        });
    }

    static class MainHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String reqPath = exchange.getRequestURI().getPath();

            if (reqPath.contains("/image/")) {
                sendGeneratedImage(exchange, reqPath);
                return;
            }

            if (reqPath.endsWith("index.css") || reqPath.endsWith("main.css") || reqPath.endsWith("legal.css")) {
                handleCssBundling(exchange, reqPath);
                return;
            }

            if (isStaticAsset(reqPath)) {
                handleStaticAsset(exchange, reqPath);
                return;
            }

            String templateName = resolveTemplateName(reqPath);
            renderTemplate(exchange, templateName, reqPath);
        }

        private boolean isStaticAsset(String path) {
            return path.endsWith(".css") || path.endsWith(".js") || path.endsWith(".png") || 
                   path.endsWith(".svg") || path.endsWith(".woff") || path.endsWith(".woff2") || 
                   path.endsWith(".ttf") || path.endsWith(".ico") || path.endsWith(".map") ||
                   path.endsWith(".json") || path.endsWith(".wasm");
        }

        private String resolveTemplateName(String path) {
            if (path.equals("/") || path.equals("/index.html")) return "index";
            if (path.equals("/impressum")) return "impressum";
            if (path.equals("/privacy")) return "privacy";
            if (path.equals("/licenses")) return "licenses";
            if (path.startsWith("/results/")) return "results";
            return "index";
        }
    }

    // --- RENDER METHODE (FIXED) ---
    private static void renderTemplate(HttpExchange exchange, String templateName, String reqPath) throws IOException {
        // FIX: Locale.US setzen! 
        // Thymeleaf benötigt zwingend eine Locale für Zahlenformatierung (#numbers).
        // Wenn new Context() ohne Parameter aufgerufen wird, kann Locale null sein -> Crash.
        Context context = new Context(Locale.US);
        
        context.setVariable("appVersion", APP_VERSION);
        context.setVariable("supportedLanguagesJson", "[{\"code\":\"de\",\"name\":\"Deutsch\"},{\"code\":\"en\",\"name\":\"English\"}]");

        if ("results".equals(templateName)) {
            setupResultsContext(context, reqPath);
        }

        try {
            String response = templateEngine.process(templateName, context);
            byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(bytes); }
        } catch (Exception e) {
            e.printStackTrace();
            String errorMsg = "Template Error: " + e.getMessage();
            exchange.sendResponseHeaders(500, errorMsg.length());
            try (OutputStream os = exchange.getResponseBody()) { os.write(errorMsg.getBytes()); }
        }
        exchange.close();
    }

    // --- DATEN-SETUP METHODE (ANGEREICHERT) ---
    private static void setupResultsContext(Context context, String reqPath) {
        String jobId = reqPath.replace("/results/", "");
        if (jobId.isEmpty() || jobId.equals("/results")) jobId = "test-job-"+APP_VERSION;

        // Wir nutzen den Generator für echte Datenstrukturen
        MockDataGenerator data = new MockDataGenerator();

        context.setVariable("isDataAvailable", true);
        context.setVariable("jobId", jobId);
        context.setVariable("jobTitle", "Neural Network Analysis (Test)");
        context.setVariable("query", "How do neural networks achieve intelligence?");
        
        // Listen füllen (für die Karten im HTML)
        context.setVariable("ownResults", data.getOwnResults());
        context.setVariable("neighborClusters", data.getNeighborClusters());
        context.setVariable("serendipityClusterResults", data.getSerendipityClusters());
        context.setVariable("clusterHierarchy", data.getHierarchy()); // Wichtig: Muss 'confidence' enthalten

        // JSONs füllen (für JavaScript Visualisierung)
        try {
            context.setVariable("ownResultsJson", objectMapper.writeValueAsString(data.getOwnResults()));
            context.setVariable("serendipityResultsJson", objectMapper.writeValueAsString(data.getSerendipityClusters()));
            
            context.setVariable("queryVectorJson", "[0.1, 0.2]");
            context.setVariable("crosshairCoordsJson", "{\"x\": 0.5, \"y\": 0.5}");
            context.setVariable("embeddingBoundsJson", "{\"xmin\":0, \"xmax\":1, \"ymin\":0, \"ymax\":1}");
            
            context.setVariable("ownColorMapJson", objectMapper.writeValueAsString(data.getOwnColorMap()));
            context.setVariable("neighborsclusterColorMapJson", objectMapper.writeValueAsString(data.getNeighborColorMap()));
            context.setVariable("serendipityColorMapJson", objectMapper.writeValueAsString(data.getSerendipityColorMap()));
            
            context.setVariable("ownLabelsJson", objectMapper.writeValueAsString(data.getOwnLabels()));
            context.setVariable("neighborLabelsJson", objectMapper.writeValueAsString(data.getNeighborLabels()));
            context.setVariable("serendipityLabelsJson", objectMapper.writeValueAsString(data.getSerendipityLabels()));
            
            // Leere Defaults um JS-Fehler zu vermeiden
            context.setVariable("contextLabelsJson", "[]");
            context.setVariable("outlinesJson", "{}");

        } catch (Exception e) { 
            e.printStackTrace(); 
        }

        context.setVariable("viz_own_available", true);
        context.setVariable("viz_own_aspect_ratio", 1);
        context.setVariable("viz_neighborscluster_available", true);
        context.setVariable("viz_neighborscluster_aspect_ratio", 1);
        context.setVariable("viz_serendipity_available", true);
        context.setVariable("viz_serendipity_aspect_ratio", 1);
        
        context.setVariable("contextPrefix", "own");
    }

    static class MockDataGenerator {
        
        /**
         * Erzeugt eine Fake-Hierarchie (Breadcrumbs) für die Anzeige auf den Karten.
         * Wichtig: Enthält 'score' für die Confidence-Anzeige.
         */
        private List<Map<String, Object>> createBreadcrumbs(String leafId, String leafName) {
            List<Map<String, Object>> list = new ArrayList<>();
            
            // Level 0 (Root)
            list.add(Map.of("id", "all_vectors", "name", "All Sciences", "score", 1.0d));
            
            String[] parts = leafId.split("-"); // z.B. ["all_vectors", "2", "1"]
            
            // Level 1 (Simuliert, falls vorhanden)
            if (parts.length > 2) {
                String l1Id = parts[0] + "-" + parts[1];
                // Wir erfinden Namen basierend auf der Nummer
                String l1Name = parts[1].equals("1") ? "Social Sciences" : "Computer Science";
                list.add(Map.of("id", l1Id, "name", l1Name, "score", 0.85d));
            }
            
            // Leaf (Das Element selbst)
            list.add(Map.of("id", leafId, "name", leafName, "score", 0.65d));
            
            return list;
        }

        public List<Map<String, Object>> getOwnResults() {
            List<Map<String, Object>> list = new ArrayList<>();
            // Wir übergeben jetzt auch einen Namen für das Blatt-Cluster
            list.add(createPaperResult("id-1", "Neural Networks Expressivity", 0.95, "all_vectors-2-1", "Deep Learning"));
            list.add(createPaperResult("id-2", "Deep Learning Generalization", 0.88, "all_vectors-2-1", "Deep Learning"));
            list.add(createPaperResult("id-3", "The Unreasonable Effectiveness", 0.82, "all_vectors-2-1", "Deep Learning"));
            return list;
        }

        public List<Map<String, Object>> getNeighborClusters() {
            List<Map<String, Object>> list = new ArrayList<>();
            list.add(createClusterResult("all_vectors-1", "Social Sciences", 0.57f, 2961234));
            list.add(createClusterResult("all_vectors-1-1", "Economics", 0.55f, 150000));
            list.add(createClusterResult("all_vectors-1-1-1", "Development Economics", 0.54f, 80000));
            list.add(createClusterResult("all_vectors-2-1", "Computational Statistics", 0.55f, 40000));
            list.add(createClusterResult("all_vectors-2-1-1", "Computational Statistics1", 0.55f, 40000));
            list.add(createClusterResult("all_vectors-2-1-2", "Computational Sta", 0.55f, 40000));
            list.add(createClusterResult("all_vectors-2-1-3", "COSTATI", 0.55f, 40000));
            return list;
        }

        public List<Map<String, Object>> getSerendipityClusters() {
            List<Map<String, Object>> list = new ArrayList<>();
            list.add(createClusterResult("all_vectors-4-2", "Quantum Physics", 0.55f, 12000));
            list.add(createClusterResult("all_vectors-4-2-1", "Math. Structures", 0.54f, 8000));
            return list;
        }

        // Hierarchie für die linke Seitenleiste (Own Idea View)
        public List<Map<String, Object>> getHierarchy() {
            List<Map<String, Object>> list = new ArrayList<>();
            // WICHTIG: Hier heißt der Key 'confidence' (für result_fragments.html), in den Cards 'score'.
            list.add(Map.of("id", "all_vectors", "name", "All Sciences", "confidence", 1.0d));
            list.add(Map.of("id", "all_vectors-2", "name", "Computer Science", "confidence", 0.85d));
            list.add(Map.of("id", "all_vectors-2-1", "name", "Deep Learning", "confidence", 0.65d));
            return list;
        }

        public Map<String, String> getOwnColorMap() { return Map.of("all_vectors-2-1", "#FF5733"); }
        public Map<String, String> getNeighborColorMap() { 
            return Map.of("all_vectors-1", "#E74C3C", "all_vectors-1-1", "#2ECC71", "all_vectors-2-1", "#F1C40F"); 
        }
        public Map<String, String> getSerendipityColorMap() { return Map.of("all_vectors-4-2", "#3498DB"); }

        public List<Map<String, Object>> getOwnLabels() { return List.of(createLabel("all_vectors-2-1", "Deep Learning", 0.5, 0.5, "#FF5733")); }
        public List<Map<String, Object>> getNeighborLabels() { 
            return List.of(createLabel("all_vectors-1", "Soc. Sci.", 0.2, 0.2, "#E74C3C"), createLabel("all_vectors-2-1", "Stats", 0.8, 0.3, "#F1C40F")); 
        }
        public List<Map<String, Object>> getSerendipityLabels() { return List.of(createLabel("all_vectors-4-2", "Physics", 0.7, 0.7, "#3498DB")); }

        private Map<String, Object> createLabel(String id, String text, double x, double y, String color) {
            Map<String, Object> label = new HashMap<>();
            label.put("clusterId", id); label.put("text", text); label.put("x_data", x); label.put("y_data", y);
            label.put("color", color); label.put("fontSize_base", 24); label.put("fontFamily", "Roboto"); label.put("fontWeight", "bold");
            return label;
        }

        // --- HELPER METHODS FOR RESULTS ---

        private Map<String, Object> createPaperResult(String id, String title, double score, String clusterId, String clusterName) {
            Map<String, Object> map = new HashMap<>();
            map.put("id", id);
            map.put("score", score);
            map.put("contentUrl", "#");
            map.put("relativeX", 0.5);
            map.put("relativeY", 0.5);
            map.put("clusterId", clusterId);
            
            Map<String, Object> payload = new HashMap<>();
            payload.put("type", "arXiv");
            payload.put("title", title);
            payload.put("abstract", "Simulated abstract for: " + title + ". Lorem ipsum dolor sit amet.");
            payload.put("prettyJson", "{\"mock\": true}");
            
            // WICHTIG: Das hier hat gefehlt!
            payload.put("namedClusterHierarchy", createBreadcrumbs(clusterId, clusterName));
            
            map.put("payload", payload);
            return map;
        }

        private Map<String, Object> createClusterResult(String id, String name, float score, int size) {
            Map<String, Object> map = new HashMap<>();
            map.put("id", id);
            map.put("score", score);
            map.put("contentUrl", "#");
            
            Map<String, Object> payload = new HashMap<>();
            payload.put("id", id);
            payload.put("cluster_name", name);
            payload.put("size", size);
            payload.put("cluster_description", "Description for " + name);
            payload.put("type", "Topic Cluster");
            
            // WICHTIG: Das hier hat gefehlt!
            payload.put("namedClusterHierarchy", createBreadcrumbs(id, name));
            
            List<Map<String, String>> dist = new ArrayList<>();
            dist.add(Map.of("name", "RePEc", "count", String.valueOf((int)(size * 0.8))));
            dist.add(Map.of("name", "arXiv", "count", String.valueOf((int)(size * 0.2))));
            payload.put("sorted_source_distribution", dist);
            payload.put("prettyJson", "{}");
            
            map.put("payload", payload);
            return map;
        }
    }

    private static Path findFileRecursively(Path root, String fileName) {
        try (var stream = Files.walk(root)) {
            return stream
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().equals(fileName))
                    .findFirst()
                    .orElse(null);
        } catch (IOException e) {
            return null;
        }
    }
    
    private static void handleStaticAsset(HttpExchange exchange, String reqPath) throws IOException {
    	String mime = getMimeType(reqPath);
        Path resourceBase;
        String fileName = Paths.get(reqPath).getFileName().toString();
        Path file = null;
        boolean isServiceWorker = false;

        if (reqPath.equals("/sw.js")) {
            file = Paths.get(STATIC_ROOT).resolve("sw.js");
            isServiceWorker = true;
        } 
        else if (reqPath.startsWith("/assets/")) {
            file = Paths.get(STATIC_ROOT).resolve(reqPath.substring(1));
        }
        else if (reqPath.contains("vendor/")) {
             // Vendor bleibt wie es ist
        } 
        else if (reqPath.endsWith(".css")) {
            file = findFileRecursively(Paths.get(STYLING_ROOT), fileName);
        } 
        else if (reqPath.endsWith(".js")) {
            file = findFileRecursively(Paths.get(SCRIPT_ROOT), fileName);
        }

        if (file == null) {
            file = Paths.get(STATIC_ROOT).resolve(reqPath.startsWith("/") ? reqPath.substring(1) : reqPath);
        }
        
        if (!Files.exists(file) && reqPath.startsWith("/assets/")) {
            sendJson(exchange, "{}");
            return;
        }

        if (Files.exists(file) && !Files.isDirectory(file)) {
            byte[] bytes;
            
            // WICHTIG: Hier wird JS bearbeitet, um CSS-Imports zu entfernen
            if (isServiceWorker || (reqPath.endsWith(".js") && !reqPath.contains("vendor/"))) {
                String content = Files.readString(file, StandardCharsets.UTF_8);
                
                // 1. CSS Imports entfernen (Verhindert den MIME-Type Fehler)
                if (!isServiceWorker) {
                    content = content.replaceAll("import\\s+['\"].*?\\.css['\"];?", "// CSS Import stripped by TestServer");
                }
                
                // 2. Version ersetzen
                if (content.contains("@project.version@")) {
                    content = content.replace("@project.version@", APP_VERSION);
                }

                // 3. Hotfix für Overlay Buttons (aus deinem Originalcode)
                if (reqPath.contains("handleQuery.js")) {
                    content = content.replace(
                        "const agreeBtn = document.getElementById('consent-agree');",
                        "const agreeBtn = overlay.querySelector('#consent-agree');"
                    );
                    content = content.replace(
                        "const privacyBtn = document.getElementById('consent-privacy');",
                        "const privacyBtn = overlay.querySelector('#consent-privacy');"
                    );
                    content = content.replace(
                        "document.body.appendChild(overlay);",
                        "document.querySelectorAll('.recorder-overlay').forEach(el => el.remove()); document.body.appendChild(overlay);"
                    );
                }

                bytes = content.getBytes(StandardCharsets.UTF_8);
            } else {
                // Bilder, CSS, Fonts direkt lesen
                bytes = Files.readAllBytes(file);
            }
            
            exchange.getResponseHeaders().set("Content-Type", mime);
            exchange.getResponseHeaders().set("Cache-Control", "no-cache, no-store, must-revalidate");
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(bytes); }
        } else {
            exchange.sendResponseHeaders(404, 0);
        }
        exchange.close();
    }

    private static void handleCssBundling(HttpExchange exchange, String reqPath) throws IOException {
        List<String> filesToBundle = new ArrayList<>();
        List<String> coreStyles = List.of("base.css", "colour.css", "light.css", "dark.css", "midnight.css");
        filesToBundle.addAll(coreStyles);

        if (reqPath.contains("index.css")) {
            filesToBundle.add("style.css");
            filesToBundle.add("recorder.css");
            filesToBundle.add("queryPopup.css");
            filesToBundle.add("tooltips.css");
            filesToBundle.add("menu.css");
            filesToBundle.add("translate.css");
        } else if (reqPath.contains("main.css")) {
            filesToBundle.add("style.css");
            filesToBundle.add("results.css");
            filesToBundle.add("recorder.css");
            filesToBundle.add("queryPopup.css");
            filesToBundle.add("tooltips.css");
            filesToBundle.add("visualize.css");
            filesToBundle.add("downloadPopup.css");
            filesToBundle.add("menu.css");
            filesToBundle.add("translate.css");
        } 
        
        filesToBundle.add("legal.css");
        
        StringBuilder sb = new StringBuilder("/* TEST BUNDLE */\n");
        Path stylingDir = Paths.get(STYLING_ROOT);
        
        for (String fileName : filesToBundle) {
            Path filePath = stylingDir.resolve(fileName);
            if (Files.exists(filePath)) {
                sb.append(Files.readString(filePath, StandardCharsets.UTF_8));
            }
        }
        // Force Testserver Fixes
        sb.append("\n.recorder-overlay { z-index: 99999 !important; pointer-events: auto !important; }");

        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/css");
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) { os.write(bytes); }
        exchange.close();
    }

    private static void sendJson(HttpExchange exchange, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) { os.write(bytes); }
        exchange.close();
    }

    // --- BILD GENERATOR (Bunter) ---
    private static void sendGeneratedImage(HttpExchange exchange, String path) throws IOException {
        BufferedImage img = new BufferedImage(1200, 1000, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g = img.createGraphics();
        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        
        if (path.contains("base")) { 
            g.setColor(new Color(20, 20, 25)); 
            g.fillRect(0, 0, 1200, 1000); 
        }
        else if (path.contains("points")) { 
            Random rand = new Random();
            for(int i=0; i<300; i++) {
                // Zufällige bunte Punkte simulieren Cluster
                g.setColor(new Color(rand.nextInt(255), rand.nextInt(255), rand.nextInt(255), 180));
                g.fillOval(rand.nextInt(1200), rand.nextInt(1000), 8, 8);
            }
            // Crosshair
            g.setColor(Color.WHITE);
            g.setStroke(new BasicStroke(2));
            g.drawOval(580, 480, 40, 40);
        }
        g.dispose();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(img, "png", baos);
        byte[] bytes = baos.toByteArray();
        exchange.getResponseHeaders().set("Content-Type", "image/png");
        exchange.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) { os.write(bytes); }
        exchange.close();
    }
    
    private static String getMimeType(String path) {
        if (path.endsWith(".css")) return "text/css";
        if (path.endsWith(".js")) return "application/javascript";
        if (path.endsWith(".json")) return "application/json";
        if (path.endsWith(".png")) return "image/png";
        if (path.endsWith(".svg")) return "image/svg+xml";
        return "application/octet-stream";
    }

    static class SseHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.getResponseHeaders().set("Content-Type", "text/event-stream; charset=utf-8");
            exchange.getResponseHeaders().set("Cache-Control", "no-cache");
            exchange.getResponseHeaders().set("Connection", "keep-alive");
            
            exchange.sendResponseHeaders(200, 0);
            sseClientStream = exchange.getResponseBody();

            try {
                sseClientStream.write(": ping\n\n".getBytes(StandardCharsets.UTF_8));
                sseClientStream.flush();
            } catch (IOException e) { sseClientStream = null; return; }
            
            while (sseClientStream != null) {
                try { Thread.sleep(1000); } catch (Exception e) { break; }
            }
            exchange.close();
        }
    }

    static class SimulationHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (sseClientStream != null) {
                executor.submit(FrontendTestServer::runSimulationSequence);
                sendJson(exchange, "{\"status\": \"started\"}");
            } else {
                sendJson(exchange, "{\"status\": \"error\"}");
            }
        }
    }
    
    static class AjaxResultHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String html = "<div class='result-card'>Test Result via Ajax</div>";
            String jsonResponse = "{\"html\": \"" + html + "\", \"pointsData\": []}";
            sendJson(exchange, jsonResponse);
        }
    }

    private static void runSimulationSequence() {
        try {
            Thread.sleep(200);
            sendEvent("EXTRACTING_COMPLETE", "Idee extrahiert.");
            Thread.sleep(200);
            sendEvent("CLUSTERING_COMPLETE", "Cluster zugeordnet.");
            Thread.sleep(200);
            sendEvent("CREATING_OWN_VISUALIZATIONS", "Rendering...");
            Thread.sleep(600);
            sendEvent("IMAGE_READY", "Bild fertig.");
            Thread.sleep(300);
            sendEvent("COMPLETE", "Fertig.");
        } catch (Exception e) { sseClientStream = null; }
    }

    private static void sendEvent(String status, String data) {
        if (sseClientStream == null) return;
        try {
            String msg = "event: update\ndata: {\"status\": \"" + status + "\", \"data\": \"" + data + "\"}\n\n";
            sseClientStream.write(msg.getBytes(StandardCharsets.UTF_8));
            sseClientStream.flush();
        } catch (IOException e) { sseClientStream = null; }
    }
}