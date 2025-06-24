package de.simon.originality;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import de.simon.originality.magicquery.client_interact.QueryProcessingService;
import de.simon.originality.magicquery.client_interact.ServerSentEventService;
import de.simon.originality.magicquery.database_interact.DatabaseQuery;
import de.simon.originality.magicquery.extract_idea.FileData;

@org.springframework.stereotype.Controller
public class Controller {
	private final QueryProcessingService queryProcessingService;
	private final ServerSentEventService sseService;
	private final DatabaseQuery databaseQuery;

	public Controller(DatabaseQuery databaseQuery, 
			QueryProcessingService queryProcessingService, 
			ServerSentEventService sseService) {
		this.databaseQuery = databaseQuery;
		this.queryProcessingService = queryProcessingService;
		this.sseService = sseService;
	}

	@GetMapping("")
	public String index() {
		return "index";
	}

	@PostMapping("/query/start")
	@ResponseBody
	public Map<String, String> startQuery(
			@RequestParam(value = "idea-text", required = false) String ideaText,
			@RequestParam(value = "files", required = false) List<MultipartFile> files) {
		List<FileData> fileDataList = new ArrayList<>();
	    if (files != null && !files.isEmpty()) {
	        for (MultipartFile file : files) {
	            try {
					fileDataList.add(new FileData(
					    file.getOriginalFilename(),
					    file.getContentType(),
					    file.getBytes()
					));
				} catch (IOException e) {
					e.printStackTrace();
					return Map.of("Error", "Error reading bytes of " + file.getOriginalFilename());
				}
	        }
	    }		
		
		String jobId = UUID.randomUUID().toString();
		queryProcessingService.processQuery(jobId, ideaText, fileDataList);
		return Map.of("jobId", jobId);
	}

	@GetMapping("/query/status/{jobId}")
	public SseEmitter getQueryStatus(@PathVariable String jobId) {
		SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);
		sseService.addEmitter(jobId, emitter);
		return emitter;
	}

	@GetMapping("/results/{jobId}")
	public ModelAndView getResults(@PathVariable String jobId) {
		ModelAndView mav = new ModelAndView("results");

		QueryProcessingService.FinalResult resultsData = QueryProcessingService.finalResults.get(jobId);

		if (resultsData != null) {
			List<DatabaseQuery.QueryResult> searchResults = resultsData.results();
			List<Map<String, Object>> preparedResults = searchResults.stream()
					.map(result -> Map.of(
							"contentUrl", result.contentUrl(),
							"id", result.id(),
							"score", result.score(),
							"payload", databaseQuery.preparePayloadForView(result.payload())
							))
					.collect(Collectors.toList());

			mav.addObject("results", preparedResults);
	        mav.addObject("query", resultsData.query());
		}

		QueryProcessingService.finalResults.remove(jobId);
		return mav;
	}

	@ExceptionHandler(RuntimeException.class)
	public ResponseEntity<Map<String, String>> handleRuntimeException(RuntimeException ex) {
		String message = ex.getCause() != null ? ex.getCause().getMessage() : ex.getMessage();
		return ResponseEntity
				.status(500)
				.body(Map.of("message", message));
	}
}
