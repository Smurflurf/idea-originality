package de.simon.originality;

import org.owasp.html.HtmlPolicyBuilder;
import org.owasp.html.PolicyFactory;
import org.springframework.stereotype.Service;

@Service
public class HtmlSanitizerService {
    private final PolicyFactory policy = new HtmlPolicyBuilder()
            .allowCommonBlockElements() // <p>, <div>, <h1>...
            .allowCommonInlineFormattingElements() // <b>, <i>, <strong>...
            .allowStandardUrlProtocols() // http, https, mailto
            .allowElements("a") // Links erlauben
            .allowAttributes("href").onElements("a") // href Attribut erlauben
            .requireRelNofollowOnLinks() // SEO/Security Best Practice
            .toFactory();

    public String sanitize(String input) {
        if (input == null) return "";
        return policy.sanitize(input);
    }
}