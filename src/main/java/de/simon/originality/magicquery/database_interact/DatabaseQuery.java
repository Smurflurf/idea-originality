package de.simon.originality.magicquery.database_interact;

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

import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import io.qdrant.client.grpc.JsonWithInt.Value;
import io.qdrant.client.grpc.Points.ScoredPoint;
import io.qdrant.client.grpc.Points.SearchPoints;
import io.qdrant.client.grpc.Points.WithPayloadSelector;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

/**
 * Manages the connection to the Qdrant vector database and cleans the results.
 * TODO if new datasets get added, update {@link #preparePayloadForView(Map)}
 */
@Service
public class DatabaseQuery {
	public record QueryResult(long id, float score, Map<String, Object> payload, String contentUrl) {}
	private static String SERVER_IP;
	private static int QDRANT_PORT;
	private static String COLLECTION_NAME;
	private QdrantClient client;
    private final LatexConverter latexToUnicodeService;
    
    @Autowired
    public DatabaseQuery(LatexConverter latexToUnicodeService) {
		this.latexToUnicodeService = new LatexConverter();
		SERVER_IP = System.getenv("QDRANT_HOST");
		QDRANT_PORT = System.getenv("QDRANT_PORT") == null ? 6334 : Integer.parseInt(System.getenv("QDRANT_PORT"));
		COLLECTION_NAME = System.getenv("QDRANT_COLLECTION_NAME") == null ? "idea-db" : System.getenv("QDRANT_COLLECTION_NAME");
    }
    
	@PostConstruct
	public void init() {
		System.out.println("Initializing QdrantService and connecting to Qdrant on " + SERVER_IP + ":" + QDRANT_PORT);
		this.client = new QdrantClient(
				QdrantGrpcClient.newBuilder(SERVER_IP, QDRANT_PORT, false)
				.build()
				);
		System.out.println("Successfully connected to Qdrant.");
	}


	public List<QueryResult> search(float[] vector, int limit) {
		try {
			SearchPoints request = SearchPoints.newBuilder()
					.setCollectionName(COLLECTION_NAME)
					.addAllVector(Floats.asList(vector))
					.setLimit(limit)
					.setWithPayload(WithPayloadSelector.newBuilder().setEnable(true).build())
					.build();

			List<ScoredPoint> searchResult = client.searchAsync(request).get();

			return searchResult.stream()
					.map(this::convertScoredPointToQueryResult)
					.collect(Collectors.toList());
		} catch (InterruptedException | ExecutionException e) {
			Thread.currentThread().interrupt();
			throw new RuntimeException("Error with Qdrant-Search", e);
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
	 * TODO make more unspecific, in case of other sources get added.
	 * @param payload
	 * @return
	 */
	public Map<String, Object> preparePayloadForView(Map<String, Object> payload) {
	    Map<String, Object> viewPayload = new LinkedHashMap<>();
	    
	    viewPayload.put("type", payload.get("type"));

	    if (payload.get("original_json") instanceof Map) {
	        @SuppressWarnings("unchecked")
	        Map<String, Object> originalJsonMap = (Map<String, Object>) payload.get("original_json");
	        
	        String title = (String) originalJsonMap.getOrDefault("title", "No title available");
	        String abstractText = (String) originalJsonMap.getOrDefault("abstract", "No abstract available");

	        viewPayload.put("title", cleanupTextForView(title));
	        viewPayload.put("abstract", cleanupTextForView(abstractText));

	        viewPayload.put("prettyJson", 
	        		toPrettyJson(
	        				TypeSpecificOperations
	        				.getSortedJson(payload, originalJsonMap)));

	    } else {
	        viewPayload.put("title", "Payload format error");
	        viewPayload.put("abstract", "");
	        viewPayload.put("prettyJson", toPrettyJson(payload));
	    }

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
        return unicodeText.replace("\\n", " ").replaceAll("\\s+", " ").trim();
    }
	
	@PreDestroy
	public void cleanup() {
		System.out.println("Closing Qdrant-Client...");
		if (client != null) {
			client.close();
		}
	}
}