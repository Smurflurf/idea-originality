package de.simon.originality.magicquery.client_interact;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import de.simon.originality.magicquery.database_interact.DatabaseQuery;
import de.simon.originality.magicquery.extract_idea.FileData;
import de.simon.originality.magicquery.extract_idea.QueryRequestHandler;
import de.simon.originality.magicquery.vectorizer.Vectorizer;

/**
 * Manages the information pipeline and informs the user client with sse.
 * Calls gemini to extract the idea from the given text and files,
 * embeds the idea into the vector space by calling the vectorizing server,
 * queries the database for similar results, 
 * processes the results into a final payload,
 * sends the payload back to the user client.
 */
@Service
public class QueryProcessingService {
	public record FinalResult(List<DatabaseQuery.QueryResult> results, String query) {}
    private final ServerSentEventService sseService;
    private final Vectorizer vectorizer;
    private final DatabaseQuery databaseQuery;
    public static final Map<String, FinalResult> finalResults = new ConcurrentHashMap<>();

    public QueryProcessingService(ServerSentEventService sseService, Vectorizer vectorizer, DatabaseQuery databaseQuery) {
        this.sseService = sseService;
        this.vectorizer = vectorizer;
        this.databaseQuery = databaseQuery;
    }

    @Async
    public void processQuery(String jobId, String ideaText, List<FileData> files) {
        try {
            sseService.sendEvent(jobId, "EXTRACTING_IDEA", "Extracting idea with LLM...");
//            System.out.println("Extracting idea with LLM...");
            String geminiResponseText = getGeminiResponse(ideaText, files);
            sseService.sendEvent(jobId, "EXTRACTING_COMPLETE", geminiResponseText);

//            System.out.println("Embedding idea into vectorspace...");
            sseService.sendEvent(jobId, "EMBEDDING_IDEA", "Embedding idea into vectorspace...");
            float[] vector = vectorizer.vectorize(geminiResponseText);
            sseService.sendEvent(jobId, "EMBEDDING_COMPLETE", "Embedding complete.");

//            System.out.println("Querying database...");
            sseService.sendEvent(jobId, "QUERYING_DATABASE", "Querying database...");
            List<DatabaseQuery.QueryResult> searchResults = databaseQuery.search(vector, 10);
            sseService.sendEvent(jobId, "QUERYING_COMPLETE", "Database query complete.");

//            System.out.println("Done...");
            FinalResult resultPayload = new FinalResult(searchResults, geminiResponseText);
            finalResults.put(jobId, resultPayload);

            sseService.sendEvent(jobId, "COMPLETE", "Process finished.");
            sseService.completeEmitter(jobId);

        } catch (Exception e) {
            e.printStackTrace();
            sseService.sendEvent(jobId, "ERROR", e.getMessage());
            sseService.completeEmitter(jobId);
        }
    }
    
    private String getGeminiResponse(String ideaText, List<FileData> files) {
    	boolean hasText = ideaText != null && !ideaText.trim().isEmpty();
        boolean hasFiles = files != null && !files.isEmpty();
        
        if (!hasText && !hasFiles) {
            throw new RuntimeException("Neither text nor files were submitted.");
        }
        
        if (hasText && hasFiles) {
            return QueryRequestHandler.handleTextAndFiles(ideaText, files);
        } else if (hasText) {
        	return  QueryRequestHandler.handleText(ideaText);
        } else {
        	return  QueryRequestHandler.handleFiles(files);
        }
    }
}