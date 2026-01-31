package de.simon.originality;

import org.springframework.boot.web.server.MimeMappings;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.boot.web.servlet.server.ConfigurableServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WebConfig {

    @Bean
    public WebServerFactoryCustomizer<ConfigurableServletWebServerFactory> mimeTypeCustomizer() {
        return factory -> {
            MimeMappings mappings = new MimeMappings(MimeMappings.DEFAULT);
            // Erzwinge die korrekten Typen für Schriften
            mappings.add("woff2", "font/woff2");
            mappings.add("woff", "font/woff");
            mappings.add("ttf", "font/ttf");
            mappings.add("eot", "application/vnd.ms-fontobject");
            factory.setMimeMappings(mappings);
        };
    }
}