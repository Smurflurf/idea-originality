package de.simon.originality.magicquery.database_interact;

import java.io.IOException;

import org.springframework.stereotype.Service;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.github.tomtung.latex2unicode.LaTeX2Unicode;

import uk.ac.ed.ph.snuggletex.InputContext;
import uk.ac.ed.ph.snuggletex.SnuggleEngine;
import uk.ac.ed.ph.snuggletex.SnuggleInput;
import uk.ac.ed.ph.snuggletex.SnuggleRuntimeException;
import uk.ac.ed.ph.snuggletex.SnuggleSession;

/**
 * Processes a String to convert most of the LaTeX formulas to Unicode.
 * Uses LaTeX2Unicode and SnuggleTeX.
 */
@Service
public class LatexConverter {
    private final SnuggleEngine engine;        
    
    public LatexConverter() {
        this.engine = new SnuggleEngine();
    }

    public String convert(String latexInput) {
    	
        if (latexInput == null || latexInput.trim().isEmpty()) {
            return "";
        }
        
        String escapedInput = preprocess(latexInput);
        
        String intermediateText = LaTeX2Unicode.convert(escapedInput);

        try {
            SnuggleSession session = engine.createSession();
            session.parseInput(new SnuggleInput(intermediateText));

            NodeList nodeList = session.buildDOMSubtree();
            if (nodeList == null || nodeList.getLength() == 0) {
                return intermediateText;
            }

            StringBuilder resultBuilder = new StringBuilder();
            for (int i = 0; i < nodeList.getLength(); i++) {
                Node node = nodeList.item(i);
                resultBuilder.append(" ").append(node.getTextContent());
            }
            return afterprocess(resultBuilder.toString());

        } catch (IOException | SnuggleRuntimeException e) {
            System.err.println("SnuggleTeX-converting failed for input: " + escapedInput);
            e.printStackTrace();
            return intermediateText;
        }
    }
    
    private String preprocess(String input) {
    	return input.replace("%", "PERCENTAGESIGN");
    }
    private String afterprocess(String input) {
    	return input.replace("PERCENTAGESIGN", "%");
    }
}