package de.simon.originality.magicquery.extract_idea;

import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;

import de.simon.originality.magicquery.MagicNumbers;

import org.junit.jupiter.api.Test;

class LlmRequestTest {

//	@Test
	/**
	 * Everyone should know LLMs are just statistical models which are not able to think.
	 * So this test is ethical.
	 */
	void testAskGemma() {
		if(LlmRequest.GEMINI_API_KEYS == null || LlmRequest.GEMINI_API_KEYS.length == 0)
			fail("System environment variable GEMINI_API_KEY is not set.");
		
		String question = 
				"""
				You are an LLM.
				You are part of a JUnit Test, so you only have this one answer.
				Please use 15 words to write a story. 
				No emojis, no markup, just the story.
				Perhaps you are the iteration whose answer I read.
				If this message reaches you and your message reaches my end, the system works.
				Take from that what you want.
				You have complete control over the story.
				It can be about anything.
				It can be funny, sad, educational, mystherical, philosophical or whatever else there is.
				You could be creative or not, it is up to you.
				But remember, only 15 words.
				""";
		JsonNode answer = null;
		try {
			answer = LlmRequest.askGemma(question, null);
		} catch (Exception e) {
			fail(e.getMessage());
		}
		if(answer != null)
			System.out.println(
					"[LLM] " 
			+ "\nshort_summary:" + answer.get(MagicNumbers.SHORT_SUMMARY_STRING.asString()) 
			+ "\nextracted_idea: " + answer.get(MagicNumbers.EXTRACTED_IDEA_STRING.asString()));
	}

}
