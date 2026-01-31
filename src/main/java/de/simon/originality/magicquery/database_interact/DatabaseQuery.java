package de.simon.originality.magicquery.database_interact;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Floats;

import de.simon.originality.HtmlSanitizerService;
import de.simon.originality.magicquery.MagicNumbers;
import de.simon.originality.magicquery.cluster_analysis.graph.KnowledgeGraphService;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.Common.Condition;
import io.qdrant.client.grpc.Common.FieldCondition;
import io.qdrant.client.grpc.Common.Filter;
import io.qdrant.client.grpc.Common.Match;
import io.qdrant.client.grpc.JsonWithInt.Value;
import io.qdrant.client.grpc.Points.ScoredPoint;
import io.qdrant.client.grpc.Points.SearchPoints;
import io.qdrant.client.grpc.Points.WithPayloadSelector;
import io.qdrant.client.grpc.Points.WithVectorsSelector;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
 
/**
 * Manages the connection to the Qdrant vector database and cleans the results.
 * TODO if new datasets get added, update {@link #preparePayloadForView(Map)}
 */
@Service
public class DatabaseQuery {
	public record QueryResult(long id, float score, Map<String, Object> payload, String contentUrl) {}
	private QdrantClient client;
    private final LatexConverter latexToUnicodeService;
    private final HtmlSanitizerService htmlSanitizer;
    
    @Autowired
    public DatabaseQuery(LatexConverter latexToUnicodeService, HtmlSanitizerService htmlSanitizer, KnowledgeGraphService knowledgeGraphService) {
		this.latexToUnicodeService = latexToUnicodeService;
		this.htmlSanitizer = htmlSanitizer;
    }
    
	@PostConstruct
	public void init() {
		System.out.println("Initializing QdrantService and connecting to Qdrant on " + MagicNumbers.QDRANT_IP.asString() + ":" + MagicNumbers.QDRANT_PORT.asInteger());
		this.client = new QdrantClient(
				QdrantGrpcClient.newBuilder(MagicNumbers.QDRANT_IP.asString(), MagicNumbers.QDRANT_PORT.asInteger() , true)
				.withApiKey(MagicNumbers.QDRANT_API_KEY.asString())
				.build()
				);
		System.out.println("Successfully connected to Qdrant.");
	}

	public List<QueryResult> search(String collectionName, float[] vector, int limit) {
	    try {
	        SearchPoints request = SearchPoints.newBuilder()
	                .setCollectionName(collectionName) 
	                .addAllVector(Floats.asList(vector))
	                .setLimit(limit)
	                .setWithPayload(WithPayloadSelector.newBuilder().setEnable(true).build())
	                .setWithVectors(WithVectorsSelector.newBuilder().setEnable(true).build())
	                .build();

	        List<ScoredPoint> searchResult = client.searchAsync(request).get();

	        return searchResult.stream()
	                .map(this::convertScoredPointToQueryResult)
	                .collect(Collectors.toList());
	    } catch (InterruptedException | ExecutionException e) {
	        Thread.currentThread().interrupt();
	        throw new RuntimeException("Error with Qdrant-Search on collection " + collectionName, e);
	    }
	}
	
	/**
	 * Searches the vector database with an additional filter to limit results
	 * to a specific cluster hierarchy. This version is compatible with qdrant-client
	 * versions that expect a 'Condition' object in the filter.
	 * 
	 * @param collectionName The name of the collection to search in.
	 * @param vector The query vector.
	 * @param limit The maximum number of results to return.
	 * @param clusterId The ID of the cluster to filter by (e.g., "all_vectors-0-1").
	 * @return A list of filtered query results.
	 */
	public List<QueryResult> searchWithFilter(String collectionName, float[] vector, int limit, String clusterId) {
	    try {
	        // Step 1: Create the FieldCondition for the exact match on the keyword.
	        FieldCondition fieldCondition = FieldCondition.newBuilder()
	            .setKey("cluster_hierarchy")
	            .setMatch(Match.newBuilder().setKeyword(clusterId).build())
	            .build();

	        // Step 2: Wrap the FieldCondition inside a Condition object.
	        Condition condition = Condition.newBuilder()
	            .setField(fieldCondition)
	            .build();

	        // Step 3: Build the main filter and add the Condition to the 'must' list.
	        Filter filter = Filter.newBuilder()
	            .addMust(condition)
	            .build();

	        // The rest of the request remains the same.
	        SearchPoints request = SearchPoints.newBuilder()
	                .setCollectionName(collectionName)
	                .addAllVector(Floats.asList(vector))
	                .setLimit(limit)
	                .setWithPayload(WithPayloadSelector.newBuilder().setEnable(true).build())
	                .setFilter(filter) // Apply the correctly built filter.
	                .build();

	        List<ScoredPoint> searchResult = client.searchAsync(request).get();

	        return searchResult.stream()
	                .map(this::convertScoredPointToQueryResult)
	                .collect(Collectors.toList());
	                
	    } catch (InterruptedException | ExecutionException e) {
	        Thread.currentThread().interrupt();
	        throw new RuntimeException("Error during filtered Qdrant search on collection " + collectionName, e);
	    }
	}
	
	/**
	 * Converts a raw Qdrant ScoredPoint to a clean QueryResult-Object.
	 * @param scoredPoint
	 * @return
	 */
	private QueryResult convertScoredPointToQueryResult(ScoredPoint scoredPoint) {
		long id = scoredPoint.getId().getNum();
		float score = scoredPoint.getScore();
		Map<String, Object> payload = convertPayloadMap(scoredPoint.getPayloadMap());

		if (scoredPoint.getVectors() != null && scoredPoint.getVectors().getVector() != null) {
            payload.put("vector", scoredPoint.getVectors().getVector().getDataList());
        }
		
		return new QueryResult(id, score, payload, TypeSpecificOperations.getContentUrl(payload));
	}
	
	/**
	 * Converts the raw Qdrant payload-map into a LinkedHashMap.
	 * @param payloadMap
	 * @return
	 */
	private Map<String, Object> convertPayloadMap(Map<String, Value> payloadMap) {
		Map<String, Object> finalPayload = new LinkedHashMap<>();
		for (Map.Entry<String, Value> entry : payloadMap.entrySet()) {
			finalPayload.put(entry.getKey(), convertQdrantValueToJavaObject(entry.getValue()));
		}
		return finalPayload;
	}
	
	/**
	 * Converts Qdrant-Values to the according Java-Object.
	 * @param qdrantValue
	 * @return
	 */
	private Object convertQdrantValueToJavaObject(Value qdrantValue) {
		return switch (qdrantValue.getKindCase()) {
		case NULL_VALUE -> null;
		case DOUBLE_VALUE -> qdrantValue.getDoubleValue();
		case INTEGER_VALUE -> qdrantValue.getIntegerValue();
		case BOOL_VALUE -> qdrantValue.getBoolValue();
		case STRING_VALUE -> qdrantValue.getStringValue();
		case STRUCT_VALUE -> convertPayloadMap(qdrantValue.getStructValue().getFieldsMap());
		case LIST_VALUE -> qdrantValue.getListValue().getValuesList().stream()
		.map(this::convertQdrantValueToJavaObject)
		.collect(Collectors.toList());
		default -> "UNSUPPORTED_TYPE";
		};
	}
	
	/**
	 * Formats any object to a pretty-printed JSON-String
	 * @param object
	 * @return
	 */
	private String toPrettyJson(Object object) {
		try {
			return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(object);
		} catch (Exception e) {
			return "[Error formatting to JSON]";
		}
	}
	
	/**
	 * Call from Controller to prepare the raw JSON String for view.
	 * Sorts the JSON entries and puts title, abstract on the very top.
	 * This version is robust against malformed or missing payload data.
	 * @param payload
	 * @return
	 */
	public Map<String, Object> prepareOwnIdeaPayloadForView(Map<String, Object> payload) {
	    Map<String, Object> viewPayload = new LinkedHashMap<>();
	    
	    if (payload == null) {
	        viewPayload.put("title", "Error: Payload was null");
	        viewPayload.put("abstract", "Could not retrieve document details.");
	        viewPayload.put("prettyJson", "{}");
	        viewPayload.put("type", "Unknown");
	        return viewPayload;
	    }

		String type = (String) payload.getOrDefault("type", "Unknown");
		viewPayload.put("type", type);

		if (payload.get("cluster_hierarchy") instanceof List) {
			@SuppressWarnings("unchecked")
			List<String> hierarchyIds = (List<String>) payload.get("cluster_hierarchy");

			@SuppressWarnings("unchecked")
			Map<String, Number> probs = (Map<String, Number>) payload.get("cluster_probabilities");

			List<Map<String, Object>> namedHierarchy = new ArrayList<>();
			for (String id : hierarchyIds) {
				String name = KnowledgeGraphService.getClusterName(id).orElse(id);
				Double score = 0.0;
				if (probs != null && probs.containsKey(id)) {
					score = probs.get(id).doubleValue();
				}
				Map<String, Object> entry = new java.util.HashMap<>();
				entry.put("id", id);
				entry.put("name", name);
				entry.put("score", score); 

				namedHierarchy.add(entry);
			}
			viewPayload.put("namedClusterHierarchy", namedHierarchy);
		}
	    
	    if (payload.get("original_json") instanceof Map) {
	        @SuppressWarnings("unchecked")
	        Map<String, Object> originalJsonMap = (Map<String, Object>) payload.get("original_json");
	        
	        viewPayload.put("title", cleanupTextForView(
	                        TypeSpecificOperations
	                        .getTitleFromPayload(originalJsonMap, type)));
	        
	        viewPayload.put("abstract", 
	                cleanupTextForView(
	                        TypeSpecificOperations
	                        .getAbstractFromPayload(originalJsonMap, type)));

	        viewPayload.put("prettyJson", 
	                toPrettyJson(
	                        TypeSpecificOperations
	                        .getSortedJson(payload, originalJsonMap)));

	    } else {
	        viewPayload.put("title", "Payload format error");
	        viewPayload.put("abstract", "The original data for this document could not be read.");
	        viewPayload.put("prettyJson", toPrettyJson(payload)); 
	    }

	    return viewPayload;
	}
	
	/**
	 * Call from Controller to prepare the raw JSON String for view.
	 * Sorts the JSON entries and puts title and a summary on the very top.
	 * TODO make more unspecific, in case of other sources get added.
	 * @param payload
	 * @return
	 */
	public Map<String, Object> prepareCentroidPayloadForView(Map<String, Object> payload) {
	    Map<String, Object> viewPayload = new LinkedHashMap<>();

	    String clusterId = (String) payload.get("id");
	    viewPayload.put("id", clusterId);

	    viewPayload.put("cluster_name", payload.getOrDefault("cluster_name", "Unnamed Cluster"));

	    Integer size = ((Number) payload.getOrDefault("size", 0)).intValue();
	    viewPayload.put("size", size);

	    KnowledgeGraphService.getNodeAttributes(clusterId).ifPresent(node -> {
	        String description = node.getClusterDescription();
	        if (description != null && !description.isBlank()) {
	            viewPayload.put("cluster_description", description);
	        }
	    });
	    
	    if (size > 0 && payload.get("source_distribution") instanceof Map) {
	        @SuppressWarnings("unchecked")
	        Map<String, Object> sources = (Map<String, Object>) payload.get("source_distribution");
	        
	        if (!sources.isEmpty()) {
	            List<Map<String, String>> sortedSources = sources.entrySet().stream()
	                .sorted(Map.Entry.<String, Object>comparingByValue(Comparator.comparingInt(v -> ((Number) v).intValue())).reversed())
	                .map(entry -> Map.of(
	                    "name", entry.getKey(),
	                    "count", String.format("%,d", ((Number) entry.getValue()).intValue())
	                ))
	                .collect(Collectors.toList());
	            
	            viewPayload.put("sorted_source_distribution", sortedSources);
	        }
	    }

	    if (clusterId != null) {
	        Map<String, String> namedHierarchy = KnowledgeGraphService.getAncestors(clusterId).stream()
	            .collect(Collectors.toMap(
	                de.simon.originality.magicquery.cluster_analysis.graph.GraphNode::getId,
	                node -> node.getClusterName() != null ? node.getClusterName() : node.getId(),
	                (u, v) -> u,
	                LinkedHashMap::new
	            ));
	        viewPayload.put("namedClusterHierarchy", namedHierarchy);
	    }
	    
	    viewPayload.put("prettyJson", toPrettyJson(payload));
	    viewPayload.put("type", "Topic Cluster");

	    return viewPayload;
	}
	
	/**
	 * Sorts a given JSON by keywords in a predefined order.
	 * Most important keywords are defined in keyOrder, the first defined one is on top.
	 * Not explicitly defined keywords are sorted alphabetically.
	 * @param unsortedMap
	 * @param keyOrder most important keys which will be on top of the JSON. They keep their indexes from the list in the JSON.
	 * @return
	 */
	static Map<String, Object> sortOriginalJson(Map<String, Object> unsortedMap, List<String> keyOrder) {
	    Comparator<String> keyComparator = (key1, key2) -> {
	        int index1 = keyOrder.indexOf(key1);
	        int index2 = keyOrder.indexOf(key2);

	        if (index1 != -1 && index2 != -1) {
	            return Integer.compare(index1, index2);
	        }
	        if (index1 != -1) {
	            return -1;
	        }
	        if (index2 != -1) {
	            return 1;
	        }
	        return key1.compareTo(key2);
	    };

	    Map<String, Object> sortedMap = new LinkedHashMap<>();
	    
	    unsortedMap.entrySet().stream()
	        .sorted(Map.Entry.comparingByKey(keyComparator))
	        .forEach(entry -> sortedMap.put(entry.getKey(), entry.getValue()));
	        
	    return sortedMap;
	}

	/**
	 * Helper method to clean text from weird whitespaces and newlines.
	 */
	private String cleanupTextForView(String text) {
        if (text == null) return "";
        text.replace("\\\"", "\\\\\"");
        String unicodeText = latexToUnicodeService.convert(text);
        String cleanedString = unicodeText.replace("\\n", " ").replaceAll("\\s+", " ").trim();
        
        return htmlSanitizer.sanitize(cleanedString);
    }
	
	public boolean ping() {
	    try {
	        client.listCollectionsAsync().get();
	        return true;
	    } catch (Exception e) {
	        System.err.println("Qdrant Ping fehlgeschlagen: " + e.getMessage());
	        return false;
	    }
	}
	
	@PreDestroy
	public void cleanup() {
		System.out.println("Closing Qdrant-Client...");
		if (client != null) {
			client.close();
		}
	}
}