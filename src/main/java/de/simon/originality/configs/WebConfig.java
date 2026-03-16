package de.simon.originality.configs;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.server.MimeMappings;
import org.springframework.boot.web.server.WebServerFactoryCustomizer;
import org.springframework.boot.web.server.servlet.ConfigurableServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import de.simon.originality.security.RateLimitInterceptor;

@Configuration
public class WebConfig implements WebMvcConfigurer {

	@Autowired
    private RateLimitInterceptor rateLimitInterceptor;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
		// Hier tragen wir alle Routen ein, die 1 Request pro Sekunde haben sollen:
		registry
		.addInterceptor(rateLimitInterceptor)
		.addPathPatterns(
				"/api/search", 
				"/api/search/view"
		);
	}
	
	@Bean
    public WebClient.Builder webClientBuilder() {
        return WebClient.builder();
    }

	
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
    
    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/mcp/**")
                .allowedOrigins("*") 
				.allowedMethods("GET", "POST", "OPTIONS").allowedHeaders("*");
	}
}
