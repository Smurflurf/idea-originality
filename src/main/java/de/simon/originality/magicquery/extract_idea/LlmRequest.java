package de.simon.originality.magicquery.extract_idea;

import java.util.ArrayList;
import java.util.List;

import com.google.genai.Client;
import com.google.genai.types.Content;
import com.google.genai.types.GenerateContentResponse;
import com.google.genai.types.Part;

/**
 * Picks an LLM from a predefined list, asks it the given prompt and returns the answer.
 * TODO fully implement retry with different models if one does not work.
 */
public class LlmRequest {
	private static final String GEMINI_API_KEY = System.getenv("GEMINI_API_KEY");
	public static final String[] multimodal_models = {"gemini-2.5-flash-lite-preview-06-17", "gemini-2.0-flash-lite"};
	public static final String[] basic_models = {"gemma-3n-e4b-it"};

	/**
	 * Sends a multimodal query to the Gemma/Gemini-model
	 * @param prompt text part of the query
	 * @param files a list of files to be embedded
	 * @return the models text answer
	 */
	public static String askGemma(String prompt, List<FileData> files) {
		int retries = 0;
		try (Client client = Client.builder().apiKey(GEMINI_API_KEY).build()) {
			List<Part> parts = new ArrayList<>();

            if (prompt != null && !prompt.isEmpty()) {
                parts.add(Part.fromText(prompt));
            }

            if (files != null) {
                for (FileData file : files) {
                    parts.add(Part.fromBytes(file.content(), file.contentType()));
                }
            }
            
            if (parts.isEmpty()) {
                return "Error: There were no initial parts to send.";
            }

            Content content = Content.fromParts(parts.toArray(Part[]::new));
            
            GenerateContentResponse response =
    				client
    				.models
    				.generateContent(getModel(files == null, retries++), content, null);

			return response.text();
            
		} catch (Exception e) {
            e.printStackTrace();
            return "Error communicating with Gemini API.";
        }
	}
	
	private static String getModel(boolean onlyForText, int retries) {
		if(onlyForText)
			return basic_models[retries];
		else 
			return multimodal_models[retries];
	}
}