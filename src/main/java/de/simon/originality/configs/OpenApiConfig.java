package de.simon.originality.configs;

import org.springdoc.core.customizers.OpenApiCustomizer;
import org.springdoc.core.models.GroupedOpenApi;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;

@Configuration
public class OpenApiConfig {

    @Bean
    public GroupedOpenApi publicApi() {
        return GroupedOpenApi.builder()
                .group("public-search-api")
                // Wir listen alles auf, was wir drin haben wollen
                .pathsToMatch("/", "/ai", "/api", "/api/search", "/api/search/view", "/impressum", "/privacy", "/licenses")
                .addOpenApiCustomizer(dynamicPromptCustomizer())
                .build();
	}

	private OpenApiCustomizer dynamicPromptCustomizer() {
		return openApi -> {
			// 1. Manuelles Hinzufügen der HTML-Seiten
			addHtmlEndpoint(openApi, "/ai", "AI Gateway", "Minimalist HTML entry point for AI agents.");
			addHtmlEndpoint(openApi, "/impressum", "Legal Notice", "Legal information about the provider.");
			addHtmlEndpoint(openApi, "/privacy", "Privacy Policy", "Information about data processing.");
			addHtmlEndpoint(openApi, "/licenses", "Licenses", "List of used open source licenses.");
			addHtmlEndpoint(openApi, "/", "Homepage", "Main entry point for human users.");
			addHtmlEndpoint(openApi, "/api", "API Info", "General information and documentation about the API.");

			// 2. EXTREM VERDICHTETE DESCRIPTION (< 700 Zeichen)
			// Wir sagen dem LLM genau, WAS es tun soll und WIE der String formatiert sein
			// muss.
			String finalDescription = """
					Searches the ideenatlas.eu vector database for scientific papers, related topic fields, and serendipitous connections.
					INSTRUCTION FOR THE 'query' PARAMETER: It MUST be a single, dense, factual paragraph extracting the core scientific idea. Use precise, objective scientific language (no conversational filler like 'The user wants...'), like a paper's abstract.
					Always analyze the returned 'similarTopicFields' and 'serendipitousConnections' for deep research.
					""";

			// 3. Beschreibung und das "Magic Flag" anwenden
			applyDescriptionToPath(openApi, "/api/search", PathItem.HttpMethod.POST, finalDescription);
			applyDescriptionToPath(openApi, "/api/search/view", PathItem.HttpMethod.GET, finalDescription);
			applyDescriptionToPath(openApi, "/ai", PathItem.HttpMethod.GET, finalDescription);
		};
	}

    /**
     * Fügt einen Pfad manuell hinzu, falls SpringDoc ihn übersehen hat (z.B. Thymeleaf Views).
     */
    private void addHtmlEndpoint(OpenAPI openApi, String pathUrl, String summary, String description) {
        if (openApi.getPaths() == null) {
            openApi.setPaths(new io.swagger.v3.oas.models.Paths());
        }

        // Nur hinzufügen, wenn noch nicht da
        if (!openApi.getPaths().containsKey(pathUrl)) {
            PathItem pathItem = new PathItem();
            Operation operation = new Operation();
            operation.setSummary(summary);
            operation.setDescription(description);
            
            // Wir definieren, dass hier HTML zurückkommt
            ApiResponses responses = new ApiResponses();
            ApiResponse response200 = new ApiResponse().description("HTML Page");
            Content content = new Content();
            MediaType mediaType = new MediaType();
            mediaType.setSchema(new Schema<String>().type("string").format("html"));
            content.addMediaType("text/html", mediaType);
            response200.setContent(content);
            
            responses.addApiResponse("200", response200);
            operation.setResponses(responses);

            pathItem.setGet(operation);
            openApi.getPaths().addPathItem(pathUrl, pathItem);
        }
    }

    private void applyDescriptionToPath(OpenAPI openApi, String path, PathItem.HttpMethod method, String description) {
        if (openApi.getPaths() == null) return;
        PathItem pathItem = openApi.getPaths().get(path);
        if (pathItem == null) return;

        Operation operation = null;
        switch (method) {
            case POST: operation = pathItem.getPost(); break;
            case GET: operation = pathItem.getGet(); break;
            default: break;
        }

        if (operation != null) {
            operation.setDescription(description);
            operation.addExtension("x-openai-isConsequential", false);
        }
    }
}