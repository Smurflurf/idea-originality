package de.simon.originality.magicquery.database_interact;

import java.util.List;
import java.util.Map;

/**
 * Contains source specific JSON analysis methods in one place.
 * Makes scaling easier, as all are contained here.
 */
public class TypeSpecificOperations {
	/**
	 * Used in DatabaseQuery.java
	 * Returns a URL to the paper/resource in question by analyzing the payload.
	 * This depends on the resource source.
	 * @param payload 
	 * @return URL to the resource or # if there is none.
	 */
	public static String getContentUrl(Map<String, Object> payload) {
	    String type = (String) payload.get("type");
	    if (type == null) {
	        return "#";
	    }

	    switch(type) {
	        case "arXiv":
	            Object originalJsonObj = payload.get("original_json");
	            if (originalJsonObj instanceof Map) {
	                @SuppressWarnings("unchecked")
	                Map<String, Object> originalJsonMap = (Map<String, Object>) originalJsonObj;
	                Object idObj = originalJsonMap.get("id");
	                if (idObj instanceof String) {
	                    return "https://arxiv.org/abs/" + idObj;
	                }
	            }
	            
	        // case "DBLP":
	        //     return "https://dblp.org/rec/" + payload.get("id");

	        default:
	            return "#";
	    }
	}
	
	/**
	 * Used in DatabaseQuery.java
	 * Sorts a JSON String by putting predefined entries on top, everything else gets sorted alphabetically.
	 * @param payload
	 * @param originalJsonMap
	 * @return Sorted JSON.
	 */
	public static Map<String, Object> getSortedJson(Map<String, Object> payload, Map<String, Object> originalJsonMap) {
		switch ((String)payload.get("type")) {
        	case "arXiv": 
        		return DatabaseQuery.sortOriginalJson(originalJsonMap, 
        				List.of("title", "abstract"));
        	default: 
        		return DatabaseQuery.sortOriginalJson(originalJsonMap, 
        				List.of(""));
        }
	}
}
