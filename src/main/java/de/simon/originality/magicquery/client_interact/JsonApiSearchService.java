package de.simon.originality.magicquery.client_interact;

import com.fasterxml.jackson.databind.JsonNode;
import de.simon.originality.dto.JsonApiResponseDto; // Haupt-DTO importieren
import de.simon.originality.magicquery.MagicNumbers;
import de.simon.originality.magicquery.cluster_analysis.ClusterAlgorithm;
import de.simon.originality.magicquery.cluster_analysis.graph.GraphNode;
import de.simon.originality.magicquery.cluster_analysis.graph.KnowledgeGraphService;
import de.simon.originality.magicquery.database_interact.DatabaseQuery;
import de.simon.originality.magicquery.database_interact.DatabaseQuery.QueryResult;
import de.simon.originality.magicquery.python.PythonService;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Service to handle pure JSON search requests. 
 * Uses Method A (nested DTOs) for structured communication.
 */
@Service
public class JsonApiSearchService {

    private final PythonService vectorizer;
    private final DatabaseQuery databaseQuery;

    public JsonApiSearchService(PythonService vectorizer, DatabaseQuery databaseQuery) {
        this.vectorizer = vectorizer;
        this.databaseQuery = databaseQuery;
    }

    /**
     * Performs a full semantic analysis and returns a nested JSON response.
     */
    public JsonApiResponseDto performSearch(String queryText) {
        // 1. Vectorization (1:1 from raw input)
        float[] vector = vectorizer.vectorize(queryText);
        float[] globalVector = vectorizer.reduceDimension(vector, "global");
        float[] localVector = vectorizer.reduceDimension(vector, "local");

        // 2. Hierarchical Clustering (Internal Path)
        List<JsonApiResponseDto.ClusterHierarchyItem> clusterHierarchy = performClustering(globalVector, localVector);

        // 3. Direct Similarity Search (Top Papers)
        List<QueryResult> rawOwnResults = databaseQuery.search(
                MagicNumbers.QDRANT_VECTOR_COLLECTION_NAME.asString(), 
                vector, 
                MagicNumbers.N_NEAREST_PAPERS.asInteger());
        
        List<JsonApiResponseDto.DetailedResult> detailedSimilarResults = rawOwnResults.stream()
                .map(this::mapToDetailedResult)
                .collect(Collectors.toList());

        // 4. Discover Neighboring Clusters & Serendipity
        List<QueryResult> allNeighborClusterResults = databaseQuery.search(
                MagicNumbers.QDRANT_CENTROID_COLLECTION_NAME.asString(), 
                vector, 
                MagicNumbers.N_NEAREST_CLUSTERS_FOR_ANALYSIS.asInteger());
        
        List<QueryResult> topNeighborClusters = allNeighborClusterResults.stream()
                .limit(MagicNumbers.N_NEAREST_CLUSTERS_FOR_FRONTEND.asInteger())
                .collect(Collectors.toList());

        Map<String, Float> serendipityWinnerMap = ClusterAlgorithm.findSerendipitousClusters(
                allNeighborClusterResults, 
                vector, 
                MagicNumbers.N_NEAREST_CLUSTERS_FOR_FRONTEND.asInteger());

        // 5. Deep-Dive Cluster Analysis (Fetching papers for each cluster)
        List<JsonApiResponseDto.TopicField> similarTopicFields = topNeighborClusters.stream()
                .map(qr -> fetchClusterDetails(qr.payload(), qr.score(), vector))
                .collect(Collectors.toList());

        List<JsonApiResponseDto.TopicField> serendipitousConnections = serendipityWinnerMap.entrySet().stream()
                .sorted(Map.Entry.<String, Float>comparingByValue().reversed())
                .map(entry -> {
                    String clusterId = entry.getKey();
                    float score = entry.getValue();
                    return KnowledgeGraphService.getNodeAttributes(clusterId)
                        .map(node -> {
                            Map<String, Object> payload = Map.of("id", node.getId(), "cluster_name", node.getClusterName());
                            return fetchClusterDetails(payload, score, vector);
                        })
                        .orElse(null);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        // 6. Build Summaries
        List<String> mainTopics = clusterHierarchy.stream()
                .map(JsonApiResponseDto.ClusterHierarchyItem::name)
                .collect(Collectors.toList());
        
        List<JsonApiResponseDto.SummaryTopic> summarySimilar = similarTopicFields.stream()
                .map(tf -> new JsonApiResponseDto.SummaryTopic(tf.clusterName(), tf.summary(), tf.relevanceScore()))
                .collect(Collectors.toList());

        List<JsonApiResponseDto.SummaryTopic> summarySerendipitous = serendipitousConnections.stream()
                 .map(tf -> new JsonApiResponseDto.SummaryTopic(tf.clusterName(), tf.summary(), tf.relevanceScore()))
                .collect(Collectors.toList());
        
        // 7. Assemble final nested structure
        JsonApiResponseDto.Summary summary = new JsonApiResponseDto.Summary(
                "Direct input via ideenatlas.eu search API", 
                queryText, 
                mainTopics, 
                summarySimilar, 
                summarySerendipitous
        );

        JsonApiResponseDto.OwnIdeaAnalysis ownIdeaAnalysis = new JsonApiResponseDto.OwnIdeaAnalysis(clusterHierarchy);

        return new JsonApiResponseDto(
                summary, 
                ownIdeaAnalysis, 
                similarTopicFields, 
                serendipitousConnections, 
                detailedSimilarResults
        );
    }
    
    /**
     * Helper: Fetch description and top 5 papers for a specific cluster.
     */
    private JsonApiResponseDto.TopicField fetchClusterDetails(Map<String, Object> clusterPayload, float relevanceScore, float[] queryVector) {
        String clusterId = (String) clusterPayload.get("id");
        String clusterName = (String) clusterPayload.getOrDefault("cluster_name", "Unknown Topic");
        
        String description = KnowledgeGraphService.getNodeAttributes(clusterId)
                                .map(GraphNode::getClusterDescription)
                                .orElse("No description available for this research area.");

        List<QueryResult> filteredResults = databaseQuery.searchWithFilter(
                MagicNumbers.QDRANT_VECTOR_COLLECTION_NAME.asString(),
                queryVector,
                5, 
                clusterId);
        
        List<JsonApiResponseDto.DetailedResult> detailedResults = filteredResults.stream()
                .map(this::mapToDetailedResult)
                .collect(Collectors.toList());

        return new JsonApiResponseDto.TopicField(clusterId, clusterName, relevanceScore, description, detailedResults);
    }

    /**
     * Helper: Hierarchical Graph Traversal.
     */
    private List<JsonApiResponseDto.ClusterHierarchyItem> performClustering(float[] globalVector, float[] localVector) {
        String currentClusterId = "all_vectors";
        List<JsonApiResponseDto.ClusterHierarchyItem> clusterHierarchyList = new ArrayList<>();

        for (int subClusterId = 0; subClusterId != -1; ) {
            int clusterSize = KnowledgeGraphService.getNodeAttributes(currentClusterId)
                    .map(GraphNode::getSize)
                    .orElse(Integer.MAX_VALUE);

            float[] vectorForClustering = (clusterSize >= MagicNumbers.GLOBAL_VECTOR_THRESHOLD.asInteger())
                    ? globalVector
                    : localVector;

            JsonNode node = vectorizer.cluster(vectorForClustering, currentClusterId);
            subClusterId = node.path("label").asInt(-1);

            if (subClusterId == -1) break;

            currentClusterId += "-" + subClusterId;
            String clusterName = KnowledgeGraphService.getClusterName(currentClusterId).orElse(currentClusterId);
            double probability = node.path("probability").asDouble(0.0);

            clusterHierarchyList.add(new JsonApiResponseDto.ClusterHierarchyItem(currentClusterId, clusterName, probability));
        }
        return clusterHierarchyList;
    }

    /**
     * Helper: Map Database Results to API DTO.
     */
    private JsonApiResponseDto.DetailedResult mapToDetailedResult(QueryResult queryResult) {
        Map<String, Object> viewPayload = databaseQuery.prepareOwnIdeaPayloadForView(queryResult.payload());
        String title = (String) viewPayload.getOrDefault("title", "Unknown Title");
        String summary = (String) viewPayload.getOrDefault("abstract", "Abstract not available.");
        
        return new JsonApiResponseDto.DetailedResult(
                String.valueOf(queryResult.id()), 
                title, 
                summary, 
                queryResult.score(), 
                queryResult.contentUrl()
        );
    }
}