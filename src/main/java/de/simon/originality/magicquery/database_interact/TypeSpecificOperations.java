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

		Object originalJsonObj = payload.get("original_json");
		if (!(originalJsonObj instanceof Map)) {
			// Early exit if original_json is missing or not a Map
			return "#";
		}
		@SuppressWarnings("unchecked")
		Map<String, Object> originalJsonMap = (Map<String, Object>) originalJsonObj;

		switch(type) {
		case "arXiv":
			Object idObj = originalJsonMap.get("id");
			if (idObj instanceof String) {
				return "https://arxiv.org/abs/" + idObj;
			}
			break;
			
		case "medRxiv":
			Object doiO_med = originalJsonMap.get("doi");
			Object dateO_med = originalJsonMap.get("date");
			if (doiO_med instanceof String && dateO_med instanceof String) {
				String doi = (String) doiO_med;
				String date = (String) dateO_med;
				return "https://www.medrxiv.org/content/early/" 
						+ date.replace("-", "/") 
						+ "/" 
						+ doi.substring(doi.indexOf("/") +1);
			}
			break;
			
		case "bioRxiv":
			Object doiO_bio = originalJsonMap.get("doi");
			Object dateO_bio = originalJsonMap.get("date");
			if (doiO_bio instanceof String && dateO_bio instanceof String) {
				String doi = (String) doiO_bio;
				String date = (String) dateO_bio;
				return "https://www.biorxiv.org/content/early/" 
						+ date.replace("-", "/") 
						+ "/" 
						+ doi.substring(doi.indexOf("/") +1);
			}
			break;
			
		case "PhilPapers":
			return getFirstStringFromList(originalJsonMap.get("identifier"));

		case "RePEc":
			String identifier = getFirstStringFromList(originalJsonMap.get("identifier"));
			if (!identifier.isEmpty()) {
				return "https://econpapers.repec.org/" + identifier;
			}
			break;
			
		case "PMC":
		    Object identifiersObj = originalJsonMap.get("identifiers");
		    if (identifiersObj instanceof Map) {
		        @SuppressWarnings("unchecked")
		        Map<String, Object> identifiersMap = (Map<String, Object>) identifiersObj;
		        Object pmidObj = identifiersMap.get("pmid");
		        if (pmidObj instanceof String) {
		            String pmid = (String) pmidObj;
		            if (!pmid.trim().isEmpty()) {
		                return "https://pubmed.ncbi.nlm.nih.gov/" + pmid;
		            }
		        }
		    }
		    break;
		}
		
		return "#"; // Default case if logic inside switch fails
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
		case "arXiv", "bioRxiv", "medRxiv", "PMC": 
			return DatabaseQuery.sortOriginalJson(originalJsonMap, 
					List.of("title", "abstract"));
			
		case "PhilPapers", "RePEc": 
			return DatabaseQuery.sortOriginalJson(originalJsonMap, 
					List.of("title", "description"));

		default: 
			return DatabaseQuery.sortOriginalJson(originalJsonMap, 
					List.of(""));
		}
	}

	/**
	 * Returns the title from the json payload.
	 * @param originalJsonMap
	 * @return the entries title
	 */
	public static String getTitleFromPayload(Map<String, Object> originalJsonMap, String type) {
		switch(type) {
		case "arXiv", "bioRxiv", "medRxiv", "PhilPapers", "RePEc", "PMC":
			return getFirstStringFromList(originalJsonMap.getOrDefault("title", "No title in original JSON."));
		default:
			return "No title in original payload.";
		}
	}

	/**
	 * Checks the entries for all Abstract keys, returns the one which is not null, i.e. contains a value.
	 * @param originalJsonMap
	 * @return the Abstract, or "No abstract available".
	 */
	public static String getAbstractFromPayload(Map<String, Object> originalJsonMap, String type) {
		String abstractText = null;
		switch(type) {
		case "arXiv", "bioRxiv", "medRxiv", "PMC":
			abstractText = getFirstStringFromList(originalJsonMap.getOrDefault("abstract", ""));
			break;
			
		case "RePEc":
			abstractText = getFirstStringFromList(originalJsonMap.getOrDefault("description", ""));
			break;

		case "PhilPapers":
			abstractText =  getFirstStringFromList(originalJsonMap.getOrDefault("description", ""));
		}

		return !abstractText.equals("") ? abstractText : "No abstract available in original JSON.";
	}


	private static String getFirstStringFromList(Object obj) {
		if (obj instanceof String) {
			return (String) obj;
		}
		if (obj instanceof List) {
			@SuppressWarnings("unchecked")
			List<Object> list = (List<Object>) obj;
			if (!list.isEmpty()) {
				return list.stream()
						.filter(item -> item instanceof String && !((String) item).trim().isEmpty())
						.map(item -> (String) item)
						.findFirst()
						.orElse("");
			}
		}
		return "";
	}
}
