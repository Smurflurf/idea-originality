package de.simon.originality.magicquery.extract_idea;

import java.util.List;

/**
 * Receives queried text and data,
 * picks a predefined prompt,
 * sends it to an LLM and returns the answer.
 */
public class QueryRequestHandler {

	public static String handleText(String prompt) {
        String fullPrompt = getPromptJustText() + prompt;
        return LlmRequest.askGemma(fullPrompt, null);
	}
	
	public static String handleFiles(List<FileData> files) {
        String prompt = getPromptJustFiles();
        return LlmRequest.askGemma(prompt, files);
        
	}
	
	public static String handleTextAndFiles(String prompt, List<FileData> files) {
        String fullPrompt = getPromptTextAndFiles(prompt);
        return LlmRequest.askGemma(fullPrompt, files);
	}
	

	/**
	 * Erstellt einen Prompt, der das Modell anweist, den Text zu bereinigen UND die angehängten Dateien zu berücksichtigen.
	 */
	public static String getPromptTextAndFiles(String idea) {
		return "Deine Aufgabe ist es, die Idee aus dem Folgendem Input zu extrahieren. "
				+ "Du sollst die enthaltene Idee, so wie sie beschrieben wurde, zurückgeben. "
				+ "Nenne die Kernidee bündig und vollständig. "
				+ "Achte dabei darauf, dass du den kompletten Input wiedergibst um die komplette Idee aufzuzeigen. "
				+ "Ich werde sie in einen Vektorraum embedden, darum sollte sie keine unnötigen Artefakte natürlicher Sprache enthalten. "
				+ "Deine Sprache soll deskriptiv sein und nur den Kern der beschriebenen Idee nennen. "
				+ "Du sollst ausdrücklich keine beschreibenden Worte hinzufügen, nur so gut es geht die Idee beschreiben. "
				+ "Schreibe dabei so, als wäre es deine Idee. "
				+ "Deine Antwort sollte jede vorhandene Information enthalten und in die Idee gepackt haben. "
				+ "Die Idee soll nur kühl genannt werden, ohne Anhängsel wie 'Kernidee:', 'Die Idee ist...', 'Man könnte...' oder 'Ich ...'"
				+ "Gib AUSSCHLIESSLICH den finalen, reinen Text der extrahierten Idee zurück, "
	            + "ohne jegliche Formatierung, Einleitungen, Anführungszeichen oder besagte Phrasen. "
				+ "Also nutze keine Einleitungen sondern beschreibe direkt ohne Umwege die Idee. "
	            + "Das Ergebnis muss ein einzelner, sauberer Textblock sein, der für die Vektorisierung geeignet ist."
				+ "Das wichtigste, und die Zusammenfassung der Kernidee ist folgender Text: <core-indea-begin> " 
				+ idea 
				+ "<core-idea-end. Diese Kernidee wird jedoch noch von weiteren Dokumenten untermauert. "
				+ "Verbinde die Dokumente mit der Kernidee, um diese klar zu extrahieren. "
				+ "Behalte bei deiner Formulierung der Idee die Ursprungssprache bei. "
				+ "Nutze die weiteren Dokumente um die Idee zu spezifizieren. "
				+ "Jedes Dokument beinhaltet wichtiges Wissen, welches relevant ist. "
				+ "Verbinde das Wissen der Dokumente mit der Kernidee, um diese besser und genauer darzustellen. "
				+ "Nutze keine Abkürzungen. "
				+ "Folgendes sind besagte Dokumente: "
				;
	}

	/**
	 * @return einen Prompt, um nur Text zu bereinigen und zusammenzufassen.
	 */
	public static String getPromptJustText() {
		return "Deine Aufgabe ist es, die Idee aus dem Folgendem Input zu extrahieren. "
				+ "Du sollst die enthaltene Idee, so wie sie beschrieben wurde, zurückgeben. "
				+ "Nenne die Kernidee bündig und vollständig. "
				+ "Achte dabei darauf, dass du den kompletten Input wiedergibst um die komplette Idee aufzuzeigen. "
				+ "Ich werde sie in einen Vektorraum embedden, darum sollte sie keine unnötigen Artefakte natürlicher Sprache enthalten. "
				+ "Deine Sprache soll deskriptiv sein und nur den Kern der beschriebenen Idee nennen. "
				+ "Du sollst ausdrücklich keine beschreibenden Worte hinzufügen, nur so gut es geht die Idee beschreiben. "
				+ "Schreibe dabei so, als wäre es deine Idee. "
				+ "Deine Antwort sollte jede vorhandene Information enthalten und in die Idee gepackt haben. "
				+ "Die Idee soll nur kühl genannt werden, ohne Anhängsel wie 'Kernidee:', 'Die Idee ist...', 'Man könnte...' oder 'Ich ...'"
				+ "Gib AUSSCHLIESSLICH den finalen, reinen Text der extrahierten Idee zurück, "
	            + "ohne jegliche Formatierung, Einleitungen, Anführungszeichen oder besagte Phrasen. "
				+ "Also nutze keine Einleitungen sondern beschreibe direkt ohne Umwege die Idee. "
	            + "Das Ergebnis muss ein einzelner, sauberer Textblock sein, der für die Vektorisierung geeignet ist."
				+ "Behalte bei deiner Formulierung der Idee die Ursprungssprache bei."
				+ "Nutze keine Abkürzungen. "
	            + "Der Text aus dem du die Idee extrahieren sollst lautet: "
				;
	}

	/**
	 * @return einen Prompt, um eine oder mehrere Dateien zu analysieren/transkribieren.
	 */
	public static String getPromptJustFiles() {
		return "Deine Aufgabe ist es, die Idee aus dem Folgendem Input zu extrahieren. "
				+ "Du sollst die enthaltene Idee, so wie sie beschrieben wurde, zurückgeben. "
				+ "Nenne die Kernidee bündig und vollständig. "
				+ "Achte dabei darauf, dass du den kompletten Input wiedergibst um die komplette Idee aufzuzeigen. "
				+ "Ich werde sie in einen Vektorraum embedden, darum sollte sie keine unnötigen Artefakte natürlicher Sprache enthalten. "
				+ "Deine Sprache soll deskriptiv sein und nur den Kern der beschriebenen Idee nennen. "
				+ "Du sollst ausdrücklich keine beschreibenden Worte hinzufügen, nur so gut es geht die Idee beschreiben. "
				+ "Schreibe dabei so, als wäre es deine Idee. "
				+ "Deine Antwort sollte jede vorhandene Information enthalten und in die Idee gepackt haben. "
				+ "Die Idee soll nur kühl genannt werden, ohne Anhängsel wie 'Kernidee:', 'Die Idee ist...', 'Man könnte...' oder 'Ich ...'"
				+ "Gib AUSSCHLIESSLICH den finalen, reinen Text der extrahierten Idee zurück, "
	            + "ohne jegliche Formatierung, Einleitungen, Anführungszeichen oder besagte Phrasen. "
				+ "Also nutze keine Einleitungen sondern beschreibe direkt ohne Umwege die Idee. "
	            + "Das Ergebnis muss ein einzelner, sauberer Textblock sein, der für die Vektorisierung geeignet ist. "
				+ "Behalte bei deiner Formulierung der Idee die Ursprungssprache bei."
				+ "Nutze keine Abkürzungen. "
	            + "Du sollst aus folgenden Fragmenten die Idee extrahieren: "
	            ;
	}
}