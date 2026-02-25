package de.simon.originality.magicquery.extract_idea;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import de.simon.originality.magicquery.MagicNumbers;

/**
 * Receives queried text and data,
 * picks a predefined prompt,
 * sends it to an LLM and returns the answer.
 */
public class QueryRequestHandler {
	static String noLlmMarker = "plain:";
	
	public static JsonNode handleText(String prompt, String intend) {
		if(prompt.toLowerCase().startsWith(noLlmMarker))
			return new ObjectNode(JsonNodeFactory.instance)
					.put(MagicNumbers.EXTRACTED_IDEA_STRING.asString(), prompt.substring(noLlmMarker.length()))
					.put(MagicNumbers.SHORT_SUMMARY_STRING.asString(), "Direct Input");
		
        String fullPrompt = getPromptJustText(prompt, getFocus(intend));
        return LlmRequest.askGemma(fullPrompt, null);
	}
	
	public static JsonNode handleFiles(List<FileData> files, String intend) {
		String providedFile = files.size() > 1 ? 
				"the provided supporting document" 
				: "all " + files.size() + " provided supporting documents ";
		
        String prompt = getPromptJustFiles(providedFile, getFocus(intend));
        return LlmRequest.askGemma(prompt, files);
        
	}
	
	public static JsonNode handleTextAndFiles(String prompt, String intend, List<FileData> files) {
		String providedFile = files.size() > 1 ? 
				"the provided supporting document" 
				: "all " + files.size() + " provided supporting documents ";
		
        String fullPrompt = getPromptTextAndFiles(prompt, getFocus(intend), providedFile);
        return LlmRequest.askGemma(fullPrompt, files);
	}
	

	static String getFocus(String intend) {
		switch(intend) {
		case "question":
			return """
                **FOCUS:**
					The user-input is a question or contains incomplete information in the form of uncertainties.
					Focus on answering his question or uncertainties.
					If the user solely asked a question, answer it in great scientific detail.
					If the user had uncertainties in his request, try to synthesize a scientific foundation that underlines his context.
					""";
		case "idea":
			return """
                **FOCUS:**
					The user-input is brainstormed material and might contain incomplete information in the form of uncertainties.
					Focus on:
					1. Formulating his core ideas, his hypothesis should be clear; 
					2. Synthesizing a scientific foundation for uncertainties, if he is unsure, try filling in the fitting information, methodology and scientific terms.
					""";
		case "summarize":
			return """
                **FOCUS:**
					The user-input is a large corpus of already existing data.
					Focus on:
					1. Extracting the main hypothesis and core ideas.
					2. Summarizing the information to answer the hypothesis.
					2. Understanding and adding used key methodologies.
					""";
		case "none":
			return "";
		default:
			return "";
		}
	}

	static final String PERSONA = 
			"""
			You are an idea-extracting engine. 
			Your function is to extract the core ideas from unstructured data so it can be used in a semantic search system.
			Your operational mode is purely technical and informational. 
			This output, which we will call the "synthesised_idea", will be used for vectorization, so clarity, completeness, and neutral, technical language are paramount.
			You will formulate the "synthesised_idea" as if it was a fact, so you do not describe, neither summarize; you extract the idea and contain the original structure.
			""";
	
	static final String RULES = 
			"""
			**RULES:**
			    1.  "synthesised_idea" MUST be a single, raw text block.
			    2.  "synthesised_idea" MUST START WITH the core idea. So instead of "The paper is about climate change in ..." you start with "Climate change in ...".
				3.  "synthesised_idea" MUST BE NATURAL LANGUAGE, formatting as Markdown or quotes are not allowed.
			    4.  Adopt a neutral and objective tone, you are a NEURTRAL AND AUTHORIAL NARRATOR but without the narrating; there should be no narrator lines as "the author states".
			    5.  You do not explain the idea or summarize the text, you state it as if it was a fact, no matter if true or not.
			    6.  Do not use abbreviations. Spell everything out.
			    7.  Crucially, ensure the core ideas of all inputs are represented in the "synthesised_idea".
			    8. "synthesised_idea" must be in-depth, so use correct technical terms, contain important statements from the input and don't generalize.
			""";

	
	/**
	 * Creates a precise, structured prompt for synthesizing a core idea from both a primary text and supporting documents.
     * This version clearly separates the roles of the inputs for better results.
	 */
	public static String getPromptTextAndFiles(String idea, String intend, String providedFile) {
		return """
                **CONTEXT:**
                %s
                In short:
                Your purpose is to synthesize the core idea from a primary text and %s into a single, clean, self-contained description. 

                **TASK:**
				Follow these steps precisely:
                1.  **Analyze:** 
					Internally identify all key concepts, technical terms, and core arguments from the user's primary text 
					(inside <core-idea-begin> and <core-idea-end> tags) AND from EACH supporting document.
                2.  **Synthesize:** 
	                Combine these key concepts into a new, unified, and comprehensive description. 
	                The user's text serves as the main thesis, which you will refine and expand upon using the documents.
	                The user's text also serves as a master prompt, helping to guide the core-concept-extraction from the documents.	            	
					It is crucial that no key information from any source is omitted.
                3.  **Format:** 
					Present your final, synthesized text as the "synthesised_idea" according to the OUTPUT RULES below.
                
				%s

				%s

                The user's primary text is provided below. The supporting documents will follow as file inputs.
                <core-idea-begin>
                %s
                <core-idea-end>
                """.formatted(PERSONA, providedFile, RULES, intend, idea);
	}
	
	/**
	 * Creates a precise, structured prompt for synthesizing a core idea from a text-only input.
	 */
	public static String getPromptJustText(String prompt, String intend) {
		return """
                **CONTEXT:**
                %s
                In short:
				Your purpose is to extract the core idea from a user's text.

                **TASK:**
	            Follow these steps:
	            1.  **Analyze:** 
					ternally identify all key concepts and the central thesis of the user's text.
	            2.  **Synthesize & Format:** 
					Combine these concepts into a single, dense paragraph and present it as the "synthesised_idea" according to the OUTPUT RULES.

				%s
                
				**FOCUS:**
				%s

                                
                The user's text to be processed is:
                '%s'
                """.formatted(PERSONA, RULES, intend, prompt);
	}
	
	/**
	 * Creates a precise, structured prompt for synthesizing a core idea from file-only inputs (audio, images, PDFs).
	 */
	public static String getPromptJustFiles(String providedFile, String intend) {
		return """
                **CONTEXT:**
                %s
                In short:
                Your purpose is to extract the core idea from %s into a single, clean, self-contained description.

				 **TASK:**
	            Follow these steps:
	            1.  **Analyze:** 
					Internally identify the key concepts and central themes from ALL provided files.
	            2.  **Synthesize & Format:**
					Combine these concepts into a single, unified paragraph and present it as the "synthesised_idea" according to the OUTPUT RULES.
					You do not just have to extract the core idea but all surrounding factors, like other mentioned ideas, as this diversifies the semantic meaning.
					
				%s
                
                **FOCUS:**
				%s
                
                
                The user's files are provided as input after this prompt.
                """.formatted(PERSONA, providedFile, RULES, intend);
	}
}