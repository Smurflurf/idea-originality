package de.simon.originality;

import org.springaicommunity.mcp.annotation.McpTool;
import org.springaicommunity.mcp.annotation.McpToolParam;
import org.springframework.stereotype.Service;

import de.simon.originality.magicquery.client_interact.JsonApiSearchService;

@Service
public class McpService {

    private final JsonApiSearchService jsonApiSearchService;

    public McpService(JsonApiSearchService jsonApiSearchService) {
        this.jsonApiSearchService = jsonApiSearchService;
    }

    @McpTool(
        name = "search",
        description = "Searches the ideenatlas.eu vector database for scientific papers, related topic fields, and serendipitous connections. INSTRUCTION: The 'query' argument MUST be a single, dense, factual paragraph extracting the core scientific idea. Use precise, objective scientific language."
    )
    public Object performScientificSearch(
        @McpToolParam(description = "Scientific query paragraph.", required = true) String query
    ) {
        // Einfacher, synchroner Aufruf. 
        // Spring AI WebMVC übernimmt das Threading via Tomcat.
        return jsonApiSearchService.performSearch(query);
    }
}