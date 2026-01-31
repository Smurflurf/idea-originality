package de.simon.originality;

import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ModelAttribute;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.simon.originality.magicquery.python.PythonService;

@ControllerAdvice(assignableTypes = Controller.class)
public class GlobalControllerAdvice {

	private final PythonService pythonService;
	private final ObjectMapper objectMapper;

	public GlobalControllerAdvice(PythonService pythonService, ObjectMapper objectMapper) {
		this.pythonService = pythonService;
		this.objectMapper = objectMapper;
	}

	@ModelAttribute("supportedLanguagesJson")
	public String populateLanguages() {
		List<Map<String, String>> langs = pythonService.getSupportedLanguages();
		try {
			return objectMapper.writeValueAsString(langs);
		} catch (Exception e) {
			return "[]";
		}
	}
}