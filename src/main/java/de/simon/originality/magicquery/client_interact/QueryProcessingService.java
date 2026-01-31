package de.simon.originality.magicquery.client_interact;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.fasterxml.jackson.databind.JsonNode;

import de.simon.originality.magicquery.MagicNumbers;
import de.simon.originality.magicquery.cluster_analysis.ClusterAlgorithm;
import de.simon.originality.magicquery.cluster_analysis.graph.GraphNode;
import de.simon.originality.magicquery.cluster_analysis.graph.KnowledgeGraphService;
import de.simon.originality.magicquery.database_interact.DatabaseQuery;
import de.simon.originality.magicquery.database_interact.DatabaseQuery.QueryResult;
import de.simon.originality.magicquery.extract_idea.FileData;
import de.simon.originality.magicquery.extract_idea.QueryRequestHandler;
import de.simon.originality.magicquery.python.PythonService;
import de.simon.originality.magicquery.visualization.VisualizationManager;

@Service
public class QueryProcessingService {
	public record FinalResult(
			float[] vector, 
			float[] vector2d, 
			List<QueryResult> ownIdeaResults, 
			List<QueryResult> neighborClusterResults, 
			String query,
			List<Map<String, Object>> clusterHierarchy,
			List<QueryResult> serendipityClusterResults,
			String title) {}
	private record JobData(String ideaText, List<FileData> files, String intent) {}

	private final ServerSentEventService sseService;
	private final PythonService vectorizer;
	private final DatabaseQuery databaseQuery;

	public static final Map<String, FinalResult> finalResults = new ConcurrentHashMap<>();
	public static final Map<String, BufferedImage> finalVisualizations = new ConcurrentHashMap<>();
	private final Map<String, JobData> jobDataStore = new ConcurrentHashMap<>();
	public static final Map<String, Map<String, String>> finalColorMaps = new ConcurrentHashMap<>();

	public QueryProcessingService(ServerSentEventService sseService, PythonService vectorizer,
			DatabaseQuery databaseQuery, KnowledgeGraphService knowledgeGraph) {
		this.sseService = sseService;
		this.vectorizer = vectorizer;
		this.databaseQuery = databaseQuery;
	}

	/**
	 * Initializes a new job by creating a unique ID and temporarily storing the
	 * input data. This method does NOT start the heavy processing.
	 * 
	 * @param ideaText The user's text input.
	 * @param files    The user's file uploads.
	 * @param intent   The user's selected goal (question, idea, summarize).
	 * @return A map containing the unique jobId.
	 */
	public Map<String, String> initializeJob(String ideaText, List<MultipartFile> files, String intent) {
		List<FileData> fileDataList = new ArrayList<>();
		if (files != null && !files.isEmpty()) {
			for (MultipartFile file : files) {
				try {
					fileDataList.add(new FileData(file.getOriginalFilename(), file.getContentType(), file.getBytes()));
				} catch (IOException e) {
					throw new RuntimeException("Error reading bytes for file: " + file.getOriginalFilename(), e);
				}
			}
		}

		String jobId = UUID.randomUUID().toString();
		jobDataStore.put(jobId, new JobData(ideaText, fileDataList, intent));

		return Map.of("jobId", jobId);
	}

	/**
	 * Starts the asynchronous processing for a job that has already been
	 * initialized. This method is triggered by the client after the SSE connection
	 * is confirmed open.
	 * 
	 * @param jobId The ID of the job to process.
	 */
	@Async
	public void processQuery(String jobId) {
		// Fetch the job data for this ID and remove it from the temporary store.
		JobData jobData = jobDataStore.remove(jobId);
		if (jobData == null) {
			sseService.sendEvent(jobId, "ERROR", "Job data not found or already processed. Could not start.");
			sseService.completeEmitter(jobId);
			return;
		}

		try {
			sseService.sendEvent(jobId, "EXTRACTING_IDEA", "Extracting idea with LLM...");
			JsonNode geminiResponseNode = getGeminiResponse(jobData.ideaText(), jobData.intent(), jobData.files());
			sseService.sendEvent(jobId, "EXTRACTING_COMPLETE", geminiResponseNode.get("extracted_idea"));

			sseService.sendEvent(jobId, "EMBEDDING_IDEA", "Embedding idea into vectorspace...");
			float[] vector = vectorizer.vectorize(geminiResponseNode.get(MagicNumbers.EXTRACTED_IDEA_STRING.asString()).asText());
			sseService.sendEvent(jobId, "EMBEDDING_COMPLETE", "Embedding complete.");

			sseService.sendEvent(jobId, "REDUCING_GLOBAL", "Reducing dimension for global clustering...");
			float[] globalVector = vectorizer.reduceDimension(vector, "global");

			sseService.sendEvent(jobId, "REDUCING_LOCAL", "Reducing dimension for local clustering...");
			float[] localVector = vectorizer.reduceDimension(vector, "local");

			sseService.sendEvent(jobId, "REDUCING_2", "Reducing vector from 768 to 2 dimensions...");
			float[] vector2d = vectorizer.reduceDimension(vector, "2d");
			sseService.sendEvent(jobId, "REDUCING_COMPLETE", "All low dim reductions are complete.");

			sseService.sendEvent(jobId, "CLUSTERING_IDEA", "Clustering idea...");
			Set<String> clusterIdsForViz = new HashSet<>();
			List<Map<String, Object>> cluster_prob = getClusterHierarchie(globalVector, localVector, jobId, clusterIdsForViz);
			sseService.sendEvent(jobId, "CLUSTERING_COMPLETE", "Clustering complete.");

			sseService.sendEvent(jobId, "QUERYING_VECTOR_DATABASE", "Querying database for similar papers...");
			List<QueryResult> ownIdeaResults = databaseQuery
					.search(MagicNumbers.QDRANT_VECTOR_COLLECTION_NAME.asString(), vector, MagicNumbers.N_NEAREST_PAPERS.asInteger());
			sseService.sendEvent(jobId, "QUERYING_VECTORS_COMPLETE", "Database query for similar papers complete.");

			sseService.sendEvent(jobId, "QUERYING_CLUSTER_DATABASE", "Querying for similar topic clusters...");
			List<QueryResult> allNeighborClusterResults = databaseQuery
					.search(MagicNumbers.QDRANT_CENTROID_COLLECTION_NAME.asString(), vector, MagicNumbers.N_NEAREST_CLUSTERS_FOR_ANALYSIS.asInteger());
			
			List<QueryResult> neighborClusterResultsForFrontend = allNeighborClusterResults.stream()
					.limit(MagicNumbers.N_NEAREST_CLUSTERS_FOR_FRONTEND.asInteger())
					.collect(Collectors.toList());
			sseService.sendEvent(jobId, "QUERYING_CLUSTERS_COMPLETE", "Database queries complete.");
			
			// Führe die Serendipity-Analyse durch und erstelle die zugehörige Visualisierung.
			List<QueryResult> serendipityResults = performSerendipityAnalysisAndVisualization(
					jobId, allNeighborClusterResults, vector, sseService
					);

			// --- PARALLELE VISUALISIERUNG ---
		    
		    // 1. Daten vorbereiten (Sets extrahieren)
		    Set<String> serendipitousClusterIdsToHighlight = serendipityResults.stream()
		            .map(result -> (String) result.payload().get("id"))
		            .collect(Collectors.toSet());

		    Set<String> neighborClusterIdsToHighlight = neighborClusterResultsForFrontend.stream()
		            .map(result -> (String) result.payload().get("id"))
		            .collect(Collectors.toSet());

		    // 2. Parallele Verarbeitung starten (blockiert hier, bis alle Bilder fertig sind)
		    parallelVisualize(
		            jobId, 
		            vector2d, 
		            clusterIdsForViz, 
		            ownIdeaResults, 
		            serendipitousClusterIdsToHighlight, 
		            neighborClusterIdsToHighlight, 
		            neighborClusterResultsForFrontend
		    );

		    // --- FINALE ZUSAMMENSTELLUNG ---

			// Stelle das finale Ergebnis-Payload zusammen, inklusive der neuen Serendipity-Daten.
			FinalResult resultPayload = new FinalResult(
					vector, 
					vector2d, 
					ownIdeaResults, 
					neighborClusterResultsForFrontend, 
					geminiResponseNode.get(MagicNumbers.EXTRACTED_IDEA_STRING.asString()).asText(), 
					cluster_prob, 
					serendipityResults,
					geminiResponseNode.get(MagicNumbers.SHORT_SUMMARY_STRING.asString()).asText());
	        finalResults.put(jobId, resultPayload);

			sseService.sendEvent(jobId, "COMPLETE", "Process finished.");
			sseService.completeEmitter(jobId);

		} catch (Exception e) {
			e.printStackTrace();
			String errorMessage = e.getMessage();
			if (e.getCause() != null && e.getCause().getMessage() != null) {
				errorMessage = e.getCause().getMessage();
			}
			String clientFriendlyError = "Processing failed: " + errorMessage.replace("\"", "'"); 
			sseService.sendEvent(jobId, "ERROR", clientFriendlyError);
			sseService.completeEmitter(jobId);
		}
	}

	private List<QueryResult> performSerendipityAnalysisAndVisualization(
            String jobId,
            List<QueryResult> allNeighborClusters,
            float[] userQueryVector,
            ServerSentEventService sseService) {
        
        sseService.sendEvent(jobId, "ANALYZING_SERENDIPITY", "Finding and scoring surprising connections...");

        // Schritt 1: Rufe den Algorithmus auf, der die Cluster-IDs und ihre neuen Scores zurückgibt.
        Map<String, Float> winnerMap = ClusterAlgorithm.findSerendipitousClusters(
            allNeighborClusters,
            userQueryVector,
            MagicNumbers.N_NEAREST_CLUSTERS_FOR_FRONTEND.asInteger()
        );

        // Schritt 2: Erzeuge die finalen QueryResult-Objekte für das Frontend.
        List<QueryResult> finalWinnerResults = new ArrayList<>();
        for (Map.Entry<String, Float> entry : winnerMap.entrySet()) {
            String winnerId = entry.getKey();
            float relevanceScore = entry.getValue(); 

            Optional<GraphNode> nodeOpt = KnowledgeGraphService.getNodeAttributes(winnerId);
            if (nodeOpt.isPresent()) {
                GraphNode node = nodeOpt.get();
                Map<String, Object> payload = new HashMap<>();
                payload.put("id", node.getId());
                payload.put("cluster_name", node.getClusterName());
                payload.put("size", node.getSize());
                payload.put("cluster_description", node.getClusterDescription());
                payload.put("source_distribution", node.getSourceDistribution());
                
                QueryResult serendipityResult = new QueryResult(
                    winnerId.hashCode(),
                    relevanceScore, 
                    payload,
                    "#"
                );
                finalWinnerResults.add(serendipityResult);
            }
        }
        
        // NEUER, WICHTIGER SCHRITT: Sortiere die Serendipity-Ergebnisse nach dem neuen Score (absteigend).
        finalWinnerResults.sort((r1, r2) -> Float.compare(r2.score(), r1.score()));

        sseService.sendEvent(jobId, "SERENDIPITY_ANALYSIS_COMPLETE", "Ranked connections discovered.");
        return finalWinnerResults;
    }
	
	private void storeAndNotify(String jobId, String namePrefix, String layerName, BufferedImage image, ServerSentEventService sseService) {
		if (image != null) {
			String imageName = namePrefix + "_" + layerName;
			finalVisualizations.put(jobId + "_" + imageName, image);

			String imageUrl = "/results/" + jobId + "/image/" + imageName;
			sseService.sendEvent(jobId, "IMAGE_READY", imageUrl);
		}
	}

	/**
	 * Handles the parallel generation of all visualization layers using Virtual
	 * Threads. This method blocks until all visualizations are created or throws an
	 * exception if one fails.
	 */
	private void parallelVisualize(String jobId, float[] vector2d, Set<String> ownClusterIds,
			List<QueryResult> ownIdeaResults, Set<String> serendipityClusterIds, Set<String> neighborClusterIds,
			List<QueryResult> neighborResults) {
		try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {

			// Task 1: Own Idea Visualization
			var futureOwn = executor.submit(() -> {
				sseService.sendEvent(jobId, "CREATING_OWN_VISUALIZATIONS", "Creating visualization layers for idea...");
				createAndStoreVisualizations(jobId, MagicNumbers.OWN_IMAGE_PREFIX.asString(), vector2d, ownClusterIds,
						ownIdeaResults, sseService, "OWN_VISUALIZATIONS_COMPLETE", "Visualizations for idea created.");
			});

			// Task 2: Serendipity Visualization
			var futureSerendipity = executor.submit(() -> {
				// Serendipity läuft still im Hintergrund oder mit eigenen Events, falls gewünscht
				createAndStoreVisualizations(jobId, MagicNumbers.SERENDIPITY_IMAGE_PREFIX.asString(), vector2d,
						serendipityClusterIds, new ArrayList<>(), // Keine Marker für Serendipity
						sseService, null, // Kein explizites Success-Event nötig (oder nach Bedarf hinzufügen)
						null);
			});

			// Task 3: Neighbor Cluster Visualization
			var futureNeighbor = executor.submit(() -> {
				sseService.sendEvent(jobId, "CREATING_NEIGHBOR_VISUALIZATIONS",
						"Creating visualization layers for neighbors...");
				createAndStoreVisualizations(jobId, MagicNumbers.NEIGHBOR_CLUSTER_IMAGE_PREFIX.asString(), vector2d,
						neighborClusterIds, neighborResults, sseService, "NEIGHBOR_VISUALIZATIONS_COMPLETE",
						"Visualizations for neighbors created.");
			});

			// WICHTIG: Auf Ergebnisse warten und Exceptions prüfen!
			// Virtual Threads blockieren hier "billig", der Underlying OS-Thread wird
			// freigegeben.
			futureOwn.get();
			futureSerendipity.get();
			futureNeighbor.get();

		} catch (java.util.concurrent.ExecutionException e) {
			// Eine der Visualisierungen ist fehlgeschlagen -> Fehlerursache extrahieren und
			// werfen
			Throwable cause = e.getCause();
			throw new RuntimeException("Parallel visualization failed: " + cause.getMessage(), cause);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException("Visualization process was interrupted.", e);
		}
	}
	
	/**
	 * Reusable method to generate a full set of visualization layers for a given dataset.
	 * @param jobId                 The current job's ID.
	 * @param namePrefix            A prefix for storing the images (e.g., "own" or "neighborcluster").
	 * @param userIdeaVector2d      The 2D vector of the user's idea for the crosshair.
	 * @param clusterIdsToHighlight The set of cluster IDs to colorize.
	 * @param neighborsForMarkers   The list of results to use for the "neighbor" markers.
	 * @param sseService			sseService to send events
	 * @param successEventName		Title of the event
	 * @param successMessage		Body text of the event
	 */
	private void createAndStoreVisualizations(
	        String jobId, 
	        String namePrefix, 
	        float[] userIdeaVector2d,
	        Set<String> clusterIdsToHighlight, 
	        List<QueryResult> neighborsForMarkers,
	        ServerSentEventService sseService,
	        String successEventName,
	        String successMessage
	        ) {

	    try {
	        VisualizationManager manager = new VisualizationManager();

	        // 1. Color Map speichern
	        finalColorMaps.put(jobId + "_" + namePrefix, manager.getHexColorMap(clusterIdsToHighlight));
	        
	        // 2. Aspect Ratio Bild
	        double aspectRatio = manager.getAspectRatio();
	        storeAndNotify(jobId, namePrefix, "aspect_ratio", createAspectRatioImage(aspectRatio), sseService);

	        // 3. Layer generieren (Reihenfolge ist egal, da parallel im Manager verarbeitet, wenn möglich)
	        
	        // Base Layer nur für "Own Idea" nötig
	        if(namePrefix.equals(MagicNumbers.OWN_IMAGE_PREFIX.asString())) {
	            storeAndNotify(jobId, namePrefix, "base", manager.getBaseImage(), sseService);
	        }
	        
	        // Points Layer (immer)
	        storeAndNotify(jobId, namePrefix, "points", manager.drawPointsLayer(clusterIdsToHighlight), sseService);
	        
	        // 4. Success Event senden 
	        if (successEventName != null && successMessage != null) {
	            sseService.sendEvent(jobId, successEventName, successMessage);
	        }

	    } catch (Exception e) {
	        String errorMessage = "Visualization generation failed for job " + jobId + " with prefix " + namePrefix;
	        System.err.println("CRITICAL: " + errorMessage);
	        e.printStackTrace(); 
	        // WICHTIG: Exception weiterwerfen, damit parallelVisualize sie fängt!
	        throw new RuntimeException(errorMessage, e);
	    }
	}

	/**
	 * Workaround, um einen double-Wert in der BufferedImage-Map zu speichern.
	 * Die Breite des Bildes repräsentiert den Wert * 1000.
	 */
	private BufferedImage createAspectRatioImage(double aspectRatio) {
		int width = (int) (aspectRatio * 1000);
		return new BufferedImage(width, 1, BufferedImage.TYPE_INT_ARGB);
	}

	private List<Map<String, Object>> getClusterHierarchie(
			float[] globalVector, 
			float[] localVector, 
			String jobId, 
			Set<String> clusterIdsForViz) {

		String cluster_id = "all_vectors";
		List<Map<String, Object>> clusterHierarchyList = new ArrayList<>();

		for (int sub_cluster = 0; sub_cluster != -1;) {
			// 1. Hole die Größe des aktuellen Clusters.
			int clusterSize = KnowledgeGraphService.getNodeAttributes(cluster_id)
					.map(de.simon.originality.magicquery.cluster_analysis.graph.GraphNode::getSize)
					.orElse(Integer.MAX_VALUE);

			// 2. Wähle den Vektor basierend auf dem Schwellenwert.
			float[] vectorForClustering;
			if (clusterSize >= MagicNumbers.GLOBAL_VECTOR_THRESHOLD.asInteger()) {
				vectorForClustering = globalVector;
			} else {
				vectorForClustering = localVector;
			}

			// 3. Führe das Clustering durch. 
			JsonNode node = vectorizer.cluster(vectorForClustering, cluster_id);
			sub_cluster = node.path("label").asInt(-1);

			if (sub_cluster != -1) {
				cluster_id += "-" + sub_cluster;
			} else {
				break;
			}

			clusterIdsForViz.add(cluster_id);
			String clusterName = KnowledgeGraphService.getClusterName(cluster_id).orElse(cluster_id);
			String probability = node.path("probability").asText("0.0");

			clusterHierarchyList.add(Map.of(
					"id", cluster_id,
					"name", clusterName,
					"confidence", probability
					));

			sseService.sendEvent(jobId, "STILL_CLUSTERING", clusterHierarchyList);
		}

		return clusterHierarchyList;
	}

	private JsonNode getGeminiResponse(String ideaText, String intend, List<FileData> files) {
		boolean hasText = ideaText != null && !ideaText.trim().isEmpty();
		boolean hasFiles = files != null && !files.isEmpty();

		if (!hasText && !hasFiles) {
			throw new RuntimeException("Neither text nor files were submitted.");
		}

		if (hasText && hasFiles) {
			return QueryRequestHandler.handleTextAndFiles(ideaText, intend, files);
		} else if (hasText) {
			return QueryRequestHandler.handleText(ideaText, intend);
		} else {
			return QueryRequestHandler.handleFiles(files, intend);
		}
	}
}