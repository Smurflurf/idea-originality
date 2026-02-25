package de.simon.originality;

import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.CookieValue;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.springframework.web.servlet.view.RedirectView;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;
import org.thymeleaf.context.WebContext;
import org.thymeleaf.web.IWebExchange;
import org.thymeleaf.web.servlet.JakartaServletWebApplication;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Floats;

import de.simon.originality.comparators.ClusterIdComparator;
import de.simon.originality.dto.ApiManifestDto;
import de.simon.originality.dto.ApiSearchRequestDto;
import de.simon.originality.dto.JsonApiResponseDto;
import de.simon.originality.magicquery.DataPathService;
import de.simon.originality.magicquery.MagicNumbers;
import de.simon.originality.magicquery.client_interact.JsonApiSearchService;
import de.simon.originality.magicquery.client_interact.QueryProcessingService;
import de.simon.originality.magicquery.client_interact.ServerSentEventService;
import de.simon.originality.magicquery.cluster_analysis.graph.KnowledgeGraphService;
import de.simon.originality.magicquery.database_interact.DatabaseQuery;
import de.simon.originality.magicquery.database_interact.DatabaseQuery.QueryResult;
import de.simon.originality.magicquery.python.PythonService;
import de.simon.originality.magicquery.visualization.LabelData;
import de.simon.originality.magicquery.visualization.VisualizationLayer;
import de.simon.originality.magicquery.visualization.VisualizationManager;
import de.simon.originality.service.LegalPageRendererService;
import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@org.springframework.stereotype.Controller
public class Controller {
	private final QueryProcessingService queryProcessingService;
	private final ServerSentEventService sseService;
	private final DatabaseQuery databaseQuery;
	private final TemplateEngine templateEngine;
	private final HtmlSanitizerService htmlSanitizer; 
    private final PythonService pythonService;
    private final JsonApiSearchService jsonApiSearchService;
    private final ExecutorService translationExecutor;
	private final LegalPageRendererService legalPageRenderer;
    public record FilteredSearchRequest(List<Float> queryVector, String clusterId) {}
	public record FilteredResultsResponse(String html, List<Map<String, Object>> pointsData) {}
    public record TranslationRequest(String text, String target_lang) {}
    
	double dataXMin = KnowledgeGraphService.getXMin();
	double dataYMin = KnowledgeGraphService.getYMin();
	double dataWidth = KnowledgeGraphService.getXMax() - dataXMin;
	double dataHeight = KnowledgeGraphService.getYMax() - dataYMin;

	
    @Value("${app.version}")
    private String appVersion;
	
	public Controller(DatabaseQuery databaseQuery, 
			QueryProcessingService queryProcessingService, 
			ServerSentEventService sseService,
			TemplateEngine templateEngine,
			HtmlSanitizerService htmlSanitizer,
			PythonService pythonService,
			JsonApiSearchService jsonApiSearchService,
			LegalPageRendererService legalPageRenderer) {
		this.databaseQuery = databaseQuery;
		this.queryProcessingService = queryProcessingService;
		this.sseService = sseService;
		this.templateEngine = templateEngine;
		this.htmlSanitizer = htmlSanitizer;
		this.pythonService = pythonService;
        this.jsonApiSearchService = jsonApiSearchService;
        this.legalPageRenderer = legalPageRenderer;
		this.translationExecutor = Executors.newCachedThreadPool();
	}

	/**
	 * Der Endpoint für menschliche Besucher im Browser.
	 * Gibt die normale index Seite zurück.
	 */
    @GetMapping(value = "", headers = "Accept=text/html", produces = MediaType.TEXT_HTML_VALUE)
	public String index(Model model, HttpServletResponse response) {
        response.setHeader("X-Robots-Tag", "index, follow");
        model.addAttribute("appVersion", appVersion);
        return "index";
    }
	
	/**
     * API manifest endpoint for LLMs and other API clients.
     * Responds to requests expecting a JSON response.
     */
    @GetMapping(value = "", headers = "!Accept=text/html", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public ResponseEntity<ApiManifestDto> apiRoot() {
    	ApiManifestDto manifest = new ApiManifestDto(
            "Ideenatlas Semantic Search API",
            "This API provides tools to semantically analyze a text query (like an idea or question) against a large knowledge base of scientific documents. " +
            "The primary function is the '/api/search' endpoint. It returns a structured analysis of relevant topics, similar papers, and serendipitous connections. " +
            "The full technical specification is available at the openapiSpecUrl.",
            appVersion,
            "/v3/api-docs/public-search-api"
        );
    	
    	return ResponseEntity.ok()
    			.header("X-Robots-Tag", "index, follow")
                .body(manifest);
    }

	@GetMapping("favicon.ico")
	public String favicon() {
		return "forward:/assets/favicons/favicon.ico";
	}

	@GetMapping("/impressum")
	public String impressum(Model model, java.util.Locale locale, @RequestParam(required = false) String lang, 
			@CookieValue(value = "lang", required = false) String cookieLang) {
	    return prepareLegalPage(model, locale, lang, "impressum", cookieLang);
	}

	@GetMapping("/privacy")
	public String privacy(Model model, java.util.Locale locale, @RequestParam(required = false) String lang, 
			@CookieValue(value = "lang", required = false) String cookieLang) {
	    return prepareLegalPage(model, locale, lang, "privacy", cookieLang);
	}

	@GetMapping("/licenses")
	public String licenses(Model model, java.util.Locale locale, @RequestParam(required = false) String lang, 
			@CookieValue(value = "lang", required = false) String cookieLang) {
	    return prepareLegalPage(model, locale, lang, "licenses", cookieLang);
	}

	@GetMapping("/api")
	public String api(Model model, java.util.Locale locale, @RequestParam(required = false) String lang, 
			@CookieValue(value = "lang", required = false) String cookieLang) {
	    return prepareLegalPage(model, locale, lang, "api", cookieLang);
	}

	
	
	/* REDIRECTS */
	@GetMapping("/api/")
	@Hidden 
	public RedirectView redirectApiWithSlash() {
	    RedirectView redirectView = new RedirectView("/api");
	    redirectView.setStatusCode(HttpStatus.MOVED_PERMANENTLY);
	    return redirectView;
	}
	@GetMapping("/openapi.json")
	@Hidden 
	public RedirectView redirectOpenApiSpec() {
	    RedirectView redirectView = new RedirectView("/v3/api-docs/public-search-api");
	    redirectView.setStatusCode(HttpStatus.MOVED_PERMANENTLY);
	    return redirectView;
	}
	
	
	@RequestMapping("/version")
    @ResponseBody
	public String version() {
		return appVersion;
	}

	@GetMapping("/ai")
	public String aiSearchBridge(Model model) {
	    return "ai-search"; 
	}
	
	@PostMapping("/api/search")
    @ResponseBody
    @Operation(
            summary = "Search semantically for scientific papers", 
            operationId = "searchIdeenatlas", 
            description = "Searches ideenatlas.eu; the query gets converted to a vector, based on that scientific clusters and papers are found and returned."
            )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200", 
            description = "Search and analysis were successful.", 
            content = { @Content(mediaType = "application/json", schema = @Schema(implementation = JsonApiResponseDto.class)) }
        ),
        @ApiResponse(responseCode = "400", description = "Invalid request, e.g., the query was empty.", content = @Content),
        @ApiResponse(responseCode = "500", description = "An internal server error occurred during processing.", content = @Content)
	})
	public ResponseEntity<JsonApiResponseDto> getSearchResultsAsJson(@RequestBody ApiSearchRequestDto request,
			@RequestHeader(value = HttpHeaders.USER_AGENT, required = false) String userAgent) {

		if (request.query() == null || request.query().isBlank()) {
			return ResponseEntity.badRequest().build();
		}

		try {
			// User Agent logging, um zu prüfen ob es klappt
			String caller = (userAgent != null) ? userAgent : "Unknown Source";
			System.out.println("API-Call received from: " + caller);

			JsonApiResponseDto response = jsonApiSearchService.performSearch(request.query());
			return ResponseEntity.ok()
					.header("X-Robots-Tag", "noindex, follow")
					.body(response);
		} catch (Exception e) {
			e.printStackTrace();
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
		}
	}
	
	@GetMapping(value = "/api/search/view", produces = MediaType.TEXT_HTML_VALUE)
	@ResponseBody
	@Operation(
            summary = "Semantic Search for Browsing Bots (HTML Wrapped JSON)", 
            operationId = "searchIdeenatlasHTML", 
            description = "Searches ideenatlas.eu; the query gets converted to a vector, based on that scientific clusters and papers are found and returned."
            )
	public String getSearchResultsAsHtmlForBots(
	        @RequestParam("q") String query, // ?q=...
	        @RequestHeader(value = HttpHeaders.USER_AGENT, required = false) String userAgent) {
		try {
			// User Agent logging, um zu prüfen ob es klappt
			String caller = (userAgent != null) ? userAgent : "Unknown Source";
			System.out.println("View API-Call received from: " + caller);
			
			// 1. Die Suche ganz normal ausführen (holt das volle DTO)
			JsonApiResponseDto response = jsonApiSearchService.performSearch(query);

			// 2. Das DTO in einen schönen JSON-String verwandeln
			// Wir nutzen 'writerWithDefaultPrettyPrinter', damit es für ChatGPT leichter
			// lesbar ist
			ObjectMapper mapper = new ObjectMapper();
			String jsonString = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(response);

			// 3. Das JSON in ein minimales HTML-Gerüst packen
			// Der <pre>-Tag sorgt dafür, dass Zeilenumbrüche erhalten bleiben.
			// Die style-Attribute sorgen dafür, dass es auch auf kleinen Screens (Mobile)
			// lesbar bleibt (Wrapping).
			return """
				    <!DOCTYPE html>
				    <html lang="en">
				    <head>
				        <meta charset="UTF-8">
				        <title>Ideenatlas API Search Results</title>
				        <meta name="robots" content="noindex, follow">
				    </head>
				    <body style="font-family: sans-serif; background-color: #f5f5f5; padding: 20px;">
				        <h1>Search Results for: %s</h1>
				        <div id="results-container" style="background-color: white; border: 1px solid #ddd; padding: 15px; border-radius: 5px;">
				            <pre id="json-data" style="white-space: pre-wrap; word-wrap: break-word;">%s</pre>
				        </div>
				    </body>
				    </html>
				    """.formatted(htmlSanitizer.sanitize(query), jsonString);
		} catch (Exception e) {
			return "<html><body><h1>Error processing query</h1><p>" + e.getMessage() + "</p></body></html>";
		}
	}
	
	@PostMapping("/api/translate")
	@Hidden
    public SseEmitter translateText(@RequestBody TranslationRequest request) {
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);
        emitter.onTimeout(() -> {
            emitter.complete();
        });

        emitter.onError((e) -> {
            emitter.completeWithError(e);
        });
        
        if (request.text() == null || request.text().isBlank()) {
            emitter.completeWithError(new IllegalArgumentException("Empty text"));
            return emitter;
        }
        
        String targetLang = (request.target_lang() == null || request.target_lang().isBlank()) ? "en" : request.target_lang();

        translationExecutor.execute(() -> {
            try {
                pythonService.translateStream(request.text(), targetLang, (chunk) -> {
                    try {
                        emitter.send(chunk); 
                    } catch (IOException e) {
                        emitter.completeWithError(e);
                    }
                });
                emitter.complete();
            } catch (Exception e) {
                emitter.completeWithError(e);
            }
        });
        return emitter;
    }
	
	@PostMapping("/api/tts")
    @ResponseBody
    @Hidden
    public ResponseEntity<StreamingResponseBody> textToSpeech(@RequestBody Map<String, String> payload) {
        String text = payload.get("text");

        if (text == null || text.isBlank()) {
            return ResponseEntity.badRequest().build();
        }

        // Wir definieren den Stream. Dieser Lambda-Ausdruck wird ausgeführt,
        // sobald Spring die HTTP-Verbindung zum Browser hergestellt hat.
        StreamingResponseBody stream = outputStream -> {
            try {
                pythonService.streamTts(text, outputStream);
            } catch (Exception e) {
                System.err.println("Abbruch im TTS Stream: " + e.getMessage());
                // Hier können wir dem Client keinen HTTP-Fehlercode mehr senden, 
                // da der Header (200 OK) schon weg ist. Der Stream bricht einfach ab.
            }
        };

        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType("audio/wav"))
                // Optional: Pufferung im Browser/Netzwerk verhindern für niedrigste Latenz
                .header("X-Accel-Buffering", "no") 
                .header("Cache-Control", "no-cache, no-store, must-revalidate")
                .body(stream);
    }
	
	@PostMapping("/query/init") 
	@ResponseBody
	@Hidden
	public Map<String, String> initQuery( 
			@RequestParam(value = "idea-text", required = false) String ideaText,
			@RequestParam(value = "files", required = false) List<MultipartFile> files,
			@RequestParam("intent") String intent) {
		return queryProcessingService.initializeJob(ideaText, files, intent);
	}

	@PostMapping("/query/start/{jobId}")
	@ResponseBody
	@Hidden
	public ResponseEntity<Void> startProcessing(@PathVariable String jobId) {
		queryProcessingService.processQuery(jobId);
		System.out.println("New job started: " + jobId);
		return ResponseEntity.ok().build();
	}

	@GetMapping("/query/status/{jobId}")
	@Hidden
	public SseEmitter getQueryStatus(@PathVariable String jobId) {
		SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);
		sseService.addEmitter(jobId, emitter);
		return emitter;
	}

	@GetMapping("/results/{jobId}")
	@Hidden
	public ResponseEntity<String> getResults(@PathVariable String jobId, 
			HttpServletRequest request, 
			HttpServletResponse response) throws IOException { // IOException wird für den VisualizationManager benötigt

		// 1. Hole die Ergebnisdaten vom Server.
		QueryProcessingService.FinalResult resultsData = QueryProcessingService.finalResults.get(jobId);

		// 2. Erstelle den WebContext für Thymeleaf.
		JakartaServletWebApplication application = JakartaServletWebApplication.buildApplication(request.getServletContext());
		IWebExchange webExchange = application.buildExchange(request, response);
		final var context = new WebContext(webExchange);

        context.setVariable("appVersion", appVersion);
		
		// 3. Setze immer die jobId.
		context.setVariable("jobId", jobId);
        try {
             context.setVariable("supportedLanguagesJson", new ObjectMapper().writeValueAsString(pythonService.getSupportedLanguages()));
        } catch (Exception e) {
             context.setVariable("supportedLanguagesJson", "[]");
        }

		// 4. Prüfe, ob Daten verfügbar sind und fülle den Kontext entsprechend.
		if (resultsData != null) {
			context.setVariable("isDataAvailable", true);

			// --- Vorbereitung der Standard-Ergebnisdaten (unverändert) ---
			List<Map<String, Object>> preparedOwnResults = resultsData.ownIdeaResults().stream()
					.map(result -> {
						Map<String, Object> map = new java.util.HashMap<>();
						Map<String, Object> payload = result.payload();
						map.put("id", String.valueOf(result.id()));
						map.put("score", result.score());
						map.put("contentUrl", result.contentUrl());
						map.put("payload", databaseQuery.prepareOwnIdeaPayloadForView(payload));
						Object posObj = payload.get("pos_2d");
						if (posObj instanceof List && dataWidth > 0 && dataHeight > 0) {
							@SuppressWarnings("unchecked")
							List<Number> posRaw = (List<Number>) posObj;
							if (posRaw.size() == 2) {
								double dataX = posRaw.get(0).doubleValue();
								double dataY = posRaw.get(1).doubleValue();
								double relativeX = (dataX - dataXMin) / dataWidth;
								double relativeY = 1.0 - ((dataY - dataYMin) / dataHeight);
								map.put("relativeX", relativeX);
								map.put("relativeY", relativeY);
							}
						}
						Object hierarchyObj = payload.get("cluster_hierarchy");
						if (hierarchyObj instanceof List) {
							@SuppressWarnings("unchecked")
							List<String> hierarchy = (List<String>) hierarchyObj;
							if (!hierarchy.isEmpty()) {
								map.put("clusterId", hierarchy.get(hierarchy.size() - 1));
							}
						}
						return map;
					})
					.collect(Collectors.toList());

			context.setVariable("ownResults", preparedOwnResults);
			try {
				context.setVariable("ownResultsJson", new ObjectMapper().writeValueAsString(preparedOwnResults));
			} catch (JsonProcessingException e) {
				context.setVariable("ownResultsJson", "[]");
			}

			List<Map<String, Object>> preparedNeighborClusters = 
					resultsData.neighborClusterResults()
					.stream()
					.sorted(ClusterIdComparator.get())
					.map(result -> Map.of(
							"contentUrl", result.contentUrl(),
							"id", result.id(),
							"score", result.score(),
							"payload", databaseQuery.prepareCentroidPayloadForView(result.payload())
							))
					.collect(Collectors.toList());
			context.setVariable("neighborClusters", preparedNeighborClusters);

			List<Map<String, Object>> preparedSerendipityClusters = 
					resultsData.serendipityClusterResults()
					.stream()
					.sorted(ClusterIdComparator.get())
					.map(result -> Map.of(
							"contentUrl", result.contentUrl(),
							"id", result.id(),
							"score", result.score(),
							"payload", databaseQuery.prepareCentroidPayloadForView(result.payload())
							))
					.collect(Collectors.toList());
			context.setVariable("serendipityClusterResults", preparedSerendipityClusters);
			try {
				context.setVariable("serendipityResultsJson", new ObjectMapper().writeValueAsString(preparedSerendipityClusters));
			} catch (JsonProcessingException e) {
				context.setVariable("serendipityResultsJson", "[]");
			}

			context.setVariable("query", htmlSanitizer.sanitize(resultsData.query()));
			context.setVariable("jobTitle", 
					resultsData.title() == null || resultsData.title().isEmpty() ? 
							jobId : resultsData.title()); // htmlSanitizer.sanitize(resultsData.title()));
			context.setVariable("clusterHierarchy", resultsData.clusterHierarchy());
			try {
				context.setVariable("queryVectorJson", new ObjectMapper().writeValueAsString(resultsData.vector()));
			} catch (JsonProcessingException e) { e.printStackTrace(); }

			try {
				float[] crosshairVector2d = resultsData.vector2d();
				if (crosshairVector2d != null && crosshairVector2d.length == 2 && dataWidth > 0 && dataHeight > 0) {
					double relativeX = (crosshairVector2d[0] - dataXMin) / dataWidth;
					double relativeY = 1.0 - ((crosshairVector2d[1] - dataYMin) / dataHeight);
					Map<String, Double> crosshairCoords = Map.of("x", relativeX, "y", relativeY);
					context.setVariable("crosshairCoordsJson", new ObjectMapper().writeValueAsString(crosshairCoords));
				} else {
					context.setVariable("crosshairCoordsJson", "null");
				}
			} catch (JsonProcessingException e) {
				e.printStackTrace();
				context.setVariable("crosshairCoordsJson", "null");
			}

			// 1. Alle Cluster-IDs sammeln, die auf der Seite sichtbar sein könnten
			Set<String> ownClusterIds = resultsData.clusterHierarchy().stream()
					.map(map -> (String) map.get("id"))
					.collect(Collectors.toSet());

			Set<String> neighborClusterIds = resultsData.neighborClusterResults().stream()
					.map(result -> (String) result.payload().get("id"))
					.collect(Collectors.toSet());

			Set<String> serendipityClusterIds = resultsData.serendipityClusterResults().stream()
					.map(result -> (String) result.payload().get("id"))
					.collect(Collectors.toSet());

			// 2. Erzeuge eine Instanz des Managers, um auf seine Logik zuzugreifen
			VisualizationManager vizManager = new VisualizationManager();
			Set<String> contextClusterIds = vizManager.findContextClusterIds();

			// 3. Kombiniere alle IDs für die Outlines
			Set<String> allRequiredIds = new HashSet<>();
			allRequiredIds.addAll(ownClusterIds);
			allRequiredIds.addAll(neighborClusterIds);
			allRequiredIds.addAll(contextClusterIds);
			allRequiredIds.addAll(serendipityClusterIds); // ERWEITERT

			// 4. Lade die Master-Outline-Daten nur einmal
			Map<String, String> allOutlines = loadAllOutlines();

			// 5. Erstelle die dynamischen JSON-Objekte für das Frontend
			try {
				// Nur die benötigten Outlines filtern
				Map<String, String> requiredOutlines = allOutlines.entrySet().stream()
						.filter(entry -> allRequiredIds.contains(entry.getKey()))
						.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
				context.setVariable("outlinesJson", new ObjectMapper().writeValueAsString(requiredOutlines));

				// Label-Daten für "own view" generieren
				Map<String, Color> ownColorMap = vizManager.generateColorPalette(createSortedLayers(ownClusterIds));
				List<LabelData> ownLabels = vizManager.generateLabelData(ownClusterIds, ownColorMap);
				context.setVariable("ownLabelsJson", new ObjectMapper().writeValueAsString(ownLabels));

				// Label-Daten für "neighbor view" generieren
				Map<String, Color> neighborColorMap = vizManager.generateColorPalette(createSortedLayers(neighborClusterIds));
				List<LabelData> neighborLabels = vizManager.generateLabelData(neighborClusterIds, neighborColorMap);
				context.setVariable("neighborLabelsJson", new ObjectMapper().writeValueAsString(neighborLabels));

				// Label-Daten für "context view" generieren
				Map<String, Color> contextColorMap = new HashMap<>();
				contextClusterIds.forEach(id -> contextColorMap.put(id, new Color(128, 128, 128))); // Feste graue Farbe
				List<LabelData> contextLabels = vizManager.generateLabelData(contextClusterIds, contextColorMap);
				context.setVariable("contextLabelsJson", new ObjectMapper().writeValueAsString(contextLabels));

				Map<String, Color> serendipityColorMap = vizManager.generateColorPalette(createSortedLayers(serendipityClusterIds));
				List<LabelData> serendipityLabels = vizManager.generateLabelData(serendipityClusterIds, serendipityColorMap);
				context.setVariable("serendipityLabelsJson", new ObjectMapper().writeValueAsString(serendipityLabels));

			} catch (JsonProcessingException e) {
				System.err.println("WARNUNG: Konnte Visualisierungs-JSON nicht erstellen: " + e.getMessage());
				context.setVariable("outlinesJson", "{}");
				context.setVariable("ownLabelsJson", "[]");
				context.setVariable("neighborLabelsJson", "[]");
				context.setVariable("contextLabelsJson", "[]");
				context.setVariable("serendipityLabelsJson", "[]"); // NEU: Fallback
			}

			// --- Vorbereitung der restlichen Daten (Flags, Aspect Ratios, Color Maps für Punkte) ---
			String[] imagePrefixes = { 
					MagicNumbers.OWN_IMAGE_PREFIX.asString(), 
					MagicNumbers.NEIGHBOR_CLUSTER_IMAGE_PREFIX.asString(),
					MagicNumbers.SERENDIPITY_IMAGE_PREFIX.asString() 
			};
			for(String prefix : imagePrefixes) {
				String baseImageKey = jobId + "_" + prefix + "_base";
				if (QueryProcessingService.finalVisualizations.containsKey(baseImageKey)) {
					context.setVariable("viz_" + prefix + "_available", true);
					BufferedImage aspectRatioImage = QueryProcessingService.finalVisualizations.get(jobId + "_" + prefix + "_aspect_ratio");
					if (aspectRatioImage != null) {
						double aspectRatio = aspectRatioImage.getWidth() / 1000.0;
						context.setVariable("viz_" + prefix + "_aspect_ratio", aspectRatio);
					}
				}
				Map<String, String> colorMap = QueryProcessingService.finalColorMaps.get(jobId + "_" + prefix);
				try {
					context.setVariable(prefix + "ColorMapJson", new ObjectMapper().writeValueAsString(colorMap));
				} catch (JsonProcessingException e) { context.setVariable(prefix + "ColorMapJson", "{}"); }
			}

			try {
				Map<String, Double> embeddingBounds = Map.of(
						"xmin", KnowledgeGraphService.getXMin(),
						"xmax", KnowledgeGraphService.getXMax(),
						"ymin", KnowledgeGraphService.getYMin(),
						"ymax", KnowledgeGraphService.getYMax()
						);
				context.setVariable("embeddingBoundsJson", new ObjectMapper().writeValueAsString(embeddingBounds));
			} catch (JsonProcessingException e) {
				context.setVariable("embeddingBoundsJson", "null");
			}

		} else {
			// Fallback, wenn keine Daten gefunden wurden.
			context.setVariable("isDataAvailable", false);
			context.setVariable("ownResultsJson", "[]");
			context.setVariable("crosshairCoordsJson", "null");
			context.setVariable("queryVectorJson", "null");
			context.setVariable("ownColorMapJson", "{}");
			context.setVariable("neighborsclusterColorMapJson", "{}");
			context.setVariable("embeddingBoundsJson", "null");
			context.setVariable("outlinesJson", "{}");
			context.setVariable("ownLabelsJson", "[]");
			context.setVariable("neighborLabelsJson", "[]");
			context.setVariable("contextLabelsJson", "[]");
			context.setVariable("serendipityClusterResults", Collections.emptyList());
			context.setVariable("serendipityResultsJson", "[]");
			context.setVariable("serendipityLabelsJson", "[]");
			context.setVariable("serendipityColorMapJson", "{}");
		}

		// 5. Template manuell zu einem String rendern.
		final String htmlContent = templateEngine.process("results", context);

		// 6. Antwort senden.
		return ResponseEntity
				.ok()
			    .contentType(MediaType.TEXT_HTML) 
	            .header("X-Robots-Tag", "noindex, nofollow, noarchive") 
				.body(htmlContent);
	}

	/**
     * Neuer Endpunkt, der vom Client aufgerufen wird, nachdem alle
     * Ergebnis-Ressourcen geladen wurden, um ein sofortiges Cleanup anzustoßen.
     */
    @PostMapping("/results/{jobId}/cleanup")
    @ResponseBody
    @Hidden
    public ResponseEntity<Void> cleanupJob(@PathVariable String jobId) {
        queryProcessingService.triggerImmediateCleanup(jobId);
        return ResponseEntity.ok().build();
    }
	
	@GetMapping(value = "/results/{jobId}/image/{imageName}", produces = MediaType.IMAGE_PNG_VALUE)
	@ResponseBody
	@Hidden
	public ResponseEntity<byte[]> getVisualizationImage(@PathVariable String jobId, @PathVariable String imageName) {

	    final BufferedImage image = QueryProcessingService.finalVisualizations.get(jobId + "_" + imageName);

	    if (image == null) {
	        return new ResponseEntity<>(HttpStatus.NOT_FOUND);
	    }

	    try {
	        byte[] imageBytes = convertImageToBytes(image);

	        HttpHeaders headers = new HttpHeaders();
	        headers.setContentType(MediaType.IMAGE_PNG);
	        headers.setContentLength(imageBytes.length); 
	        headers.setCacheControl("max-age=3600, private, immutable");
	        return new ResponseEntity<>(imageBytes, headers, HttpStatus.OK);

	    } catch (IOException e) {
	        System.err.println("Fehler beim Konvertieren des Bildes '" + imageName + "' zu Bytes: " + e.getMessage());
	        return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
	    }
	}

	/**
	 * Konvertiert ein BufferedImage in ein Byte-Array im PNG-Format.
	 * @param image Das zu konvertierende Bild.
	 * @return Ein Byte-Array mit den PNG-Daten.
	 * @throws IOException wenn beim Schreiben in den Stream ein Fehler auftritt.
	 */
	private byte[] convertImageToBytes(BufferedImage image) throws IOException {
	    try (java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream()) {
	        
	        // 1. Hole einen PNG Writer
	        Iterator<ImageWriter> writers = ImageIO.getImageWritersByFormatName("png");
	        if (!writers.hasNext()) {
	            throw new IllegalStateException("Kein PNG Writer gefunden");
	        }
	        ImageWriter writer = writers.next();

	        // 2. Konfiguriere den Writer für Progressive (Interlaced) Output
	        ImageWriteParam param = writer.getDefaultWriteParam();
	        if (param.canWriteProgressive()) {
	            // MODE_DEFAULT aktiviert bei PNG das Adam7-Interlacing
	            param.setProgressiveMode(ImageWriteParam.MODE_DEFAULT);
	        }

	        // 3. Schreibe das Bild in den Stream
	        try (ImageOutputStream ios = ImageIO.createImageOutputStream(baos)) {
	            writer.setOutput(ios);
	            writer.write(null, new IIOImage(image, null, null), param);
	        } finally {
	            writer.dispose();
	        }
	        
	        return baos.toByteArray();
	    }
	}
	
	@PostMapping("/query/filtered-results")
	@ResponseBody
	@Hidden
	public ResponseEntity<FilteredResultsResponse> getFilteredResultsFragment(@RequestBody FilteredSearchRequest request) {
	    // --- Schritt 1: Datenbankabfrage ---
	    float[] vector = Floats.toArray(request.queryVector());
	    String clusterId = request.clusterId();
	    List<QueryResult> filteredResults = databaseQuery.searchWithFilter(
	            MagicNumbers.QDRANT_VECTOR_COLLECTION_NAME.asString(),
	            vector,
	            MagicNumbers.N_NEAREST_PAPERS.asInteger(),
	            clusterId);

	    // --- Schritt 2: Daten für Karten UND Visualisierungspunkte vorbereiten (KORRIGIERT) ---
	    List<Map<String, Object>> preparedResults = new ArrayList<>(); // Nur noch eine Liste!

	    for (QueryResult result : filteredResults) {
	        Map<String, Object> fullResultMap = new java.util.HashMap<>();
	        
	        fullResultMap.put("id", String.valueOf(result.id()));
	        fullResultMap.put("score", result.score());
	        fullResultMap.put("contentUrl", result.contentUrl());
	        
	        // Bereite die Payload vor, die "title", "abstract", "prettyJson" etc. enthält
	        fullResultMap.put("payload", databaseQuery.prepareOwnIdeaPayloadForView(result.payload()));
	        
	        // Füge die Positions- und Cluster-Daten für die Visualisierung hinzu
	        Object posObj = result.payload().get("pos_2d");
	        if (posObj instanceof List && dataWidth > 0 && dataHeight > 0) {
	            @SuppressWarnings("unchecked")
	            List<Number> posRaw = (List<Number>) posObj;
	            if (posRaw.size() == 2) {
	                double dataX = posRaw.get(0).doubleValue();
	                double dataY = posRaw.get(1).doubleValue();
	                fullResultMap.put("relativeX", (dataX - dataXMin) / dataWidth);
	                fullResultMap.put("relativeY", 1.0 - ((dataY - dataYMin) / dataHeight));
	            }
	        }

	        Object hierarchyObj = result.payload().get("cluster_hierarchy");
	        if (hierarchyObj instanceof List) {
	            @SuppressWarnings("unchecked")
	            List<String> hierarchy = (List<String>) hierarchyObj;
	            if (!hierarchy.isEmpty()) {
	                fullResultMap.put("clusterId", hierarchy.get(hierarchy.size() - 1));
	            }
	        }
	        
	        preparedResults.add(fullResultMap);
	    }

	    // --- Schritt 3: Thymeleaf-Fragment rendern ---
	    final Context context = new Context();
	    context.setVariable("filteredResults", preparedResults);
	    context.setVariable("contextPrefix", clusterId);

	    final String htmlContent = templateEngine.process("filtered_results_view", context);

	    // --- Schritt 4: Finale Antwort als JSON verpacken ---
	    FilteredResultsResponse response = new FilteredResultsResponse(htmlContent, preparedResults);
	    return ResponseEntity.ok(response);
	}


	private String prepareLegalPage(Model model, java.util.Locale requestLocale, String forcedLang, String pageName, @CookieValue(value = "lang", required = false) String cookieLang) {
	    String lang = "en"; // Default

	    // Priorität: 1. URL Parameter (?lang=de) -> 2. Cookie -> 3. Browser Header (requestLocale)
	    if (forcedLang != null) {
	        lang = forcedLang;
	    } else if (cookieLang != null) {
	        lang = cookieLang;
	    } else if (requestLocale != null) {
	        lang = requestLocale.getLanguage();
	    }

	    // Validierung (nur 'de' oder 'en' zulassen)
	    if (!"de".equalsIgnoreCase(lang)) {
	        lang = "en";
	    } else {
	        lang = "de";
	    }

	    model.addAttribute("appVersion", appVersion);
	    model.addAttribute("fallbackHtml", legalPageRenderer.renderFallbackHtml(pageName, lang));
	    return pageName;
	}

    /**
     * Loads the outlines.json into a map.
     */
    @SuppressWarnings("unchecked")
    private Map<String, String> loadAllOutlines() throws IOException {
        Path outlinesPath = DataPathService.getImagesPath().resolve("outlines.json");
        if (Files.exists(outlinesPath)) {
            String jsonContent = Files.readString(outlinesPath);
            return new ObjectMapper().readValue(jsonContent, Map.class);
        }
        System.err.println("WARNUNG: outlines.json wurde nicht gefunden unter: " + outlinesPath);
        return Collections.emptyMap();
    }
    
    /**
     * Creates a sorted List of Layers which are used for the colour generation.
     */
    private List<VisualizationLayer> createSortedLayers(Set<String> clusterIds) {
        List<VisualizationLayer> layers = new ArrayList<>();
        for (String id : clusterIds) {
            KnowledgeGraphService.getClusterPosition(id).ifPresent(pos -> {
                int level = id.split("-").length;
                layers.add(new VisualizationLayer(id, level, pos));
            });
        }
        Collections.sort(layers);
        return layers;
    }
	
	@ExceptionHandler(RuntimeException.class)
	public ResponseEntity<Map<String, String>> handleRuntimeException(RuntimeException ex) {
		String message = ex.getMessage();
		if (message == null && ex.getCause() != null) {
			message = ex.getCause().getMessage();
		}
		if (message == null) {
			message = "An unexpected error occurred: " + ex.getClass().getSimpleName();
		}

		return ResponseEntity
				.status(500)
				.body(Map.of("message", message));
	}
}