package de.simon.originality.security;

import java.io.IOException;
import java.util.List;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.www.BasicAuthenticationFilter;
import org.springframework.security.web.csrf.CookieCsrfTokenRepository;
import org.springframework.security.web.csrf.CsrfToken;
import org.springframework.security.web.csrf.CsrfTokenRequestAttributeHandler;
import org.springframework.security.web.header.writers.ReferrerPolicyHeaderWriter;
import org.springframework.security.web.header.writers.StaticHeadersWriter;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@Configuration
@EnableWebSecurity
public class SecurityConfig {

	@Bean
	public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
		http
		
	    .cors(cors -> cors.configurationSource(corsConfigurationSource()))
	    
		// 1. CSRF
		.csrf(csrf -> csrf
	            .ignoringRequestMatchers("/api/**", "/v3/**", "/mcp/**", "/sse/**", "/ai/**")
				.csrfTokenRepository(CookieCsrfTokenRepository.withHttpOnlyFalse())
				.csrfTokenRequestHandler(new CsrfTokenRequestAttributeHandler())
				)
		.addFilterAfter(new CsrfCookieFilter(), BasicAuthenticationFilter.class)
		
		// ServiceWorker NIEMALS cachen
		.addFilterAfter(new ServiceWorkerCacheFilter(), CsrfCookieFilter.class)

		// 2. Security Headers
		.headers(headers -> headers
				// A. HSTS
				.httpStrictTransportSecurity(hsts -> hsts
						.includeSubDomains(true)
						.preload(true)
						.maxAgeInSeconds(31536000)
						)
				// B. Clickjacking
				.frameOptions(frame -> frame.deny())

				// C. CSP
				.contentSecurityPolicy(csp -> csp
						.policyDirectives(
								"default-src 'self'; " +

                		        "script-src 'self' 'unsafe-inline' 'unsafe-eval' blob: https://ideenatlas.eu; " +

                		        "style-src 'self' 'unsafe-inline'; " +
                		        "font-src 'self' data:; " +
                		        "img-src 'self' data: blob:; " +

                                "connect-src 'self' https://ideenatlas.eu http://localhost:* ws://localhost:*; " +
                                
                		        "media-src 'self' blob:; " +
                		        "worker-src 'self' blob:; " +
                		        "object-src 'none'; " +
                		        "frame-ancestors 'none';"
								)
						)

				// D. Permissions Policy (Manuell mit HeaderWriter)
				.addHeaderWriter(new StaticHeadersWriter("Permissions-Policy", 
						"microphone=(self), " +
								"camera=(), " +
								"geolocation=(), " +
								"payment=(), " +
								"usb=()"
						))

				// E. Referrer Policy 
				.referrerPolicy(referrer -> referrer
						.policy(ReferrerPolicyHeaderWriter.ReferrerPolicy.STRICT_ORIGIN_WHEN_CROSS_ORIGIN)
						)
				)

		// 3. Autorisierung
		.authorizeHttpRequests(authz -> authz
			    .requestMatchers(org.springframework.http.HttpMethod.OPTIONS, "/**").permitAll()
				
				.requestMatchers("/dist/**", "/vendor/**", "/assets/**").permitAll()
				.requestMatchers("/query/**", "/results/**", "/").permitAll()
				.requestMatchers("/favicon.ico", "/sw.js", "/robots.txt", "/llms.txt", "/sitemap.xml", "/openapi.json").permitAll()
				.requestMatchers("/impressum", "/privacy", "/licenses", "/faq", "/about").permitAll()
				.requestMatchers("/ai/**", "/api/**", "/mcp/**", "/sse/**").permitAll()
			    
			    .requestMatchers("/v3/api-docs/**").permitAll()    // Die JSON-Spezifikation
			    .requestMatchers("/swagger-ui/**").permitAll()     // Die Swagger-UI Dateien
			    .requestMatchers("/swagger-ui.html").permitAll()   // Der Einstieg zur Swagger-UI
			    .requestMatchers("/.well-known/**").permitAll()

			    .requestMatchers("/version").permitAll()
				.requestMatchers("/error").permitAll()
				.anyRequest().denyAll() 
				);

		return http.build();
	}

	@Bean
	CorsConfigurationSource corsConfigurationSource() {
		CorsConfiguration configuration = new CorsConfiguration();
		configuration.setAllowedOriginPatterns(List.of("*"));
		configuration.setAllowedMethods(List.of("GET", "POST", "OPTIONS"));
		configuration.setAllowedHeaders(List.of("*"));
	    configuration.setAllowCredentials(true); 
	    configuration.setAllowedHeaders(List.of("Authorization", "Cache-Control", "Content-Type", "Connection", "Last-Event-ID"));
	    
		UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
		source.registerCorsConfiguration("/mcp/**", configuration);
		source.registerCorsConfiguration("/sse/**", configuration);
		return source;
	}
	private static class CsrfCookieFilter extends OncePerRequestFilter {
		@Override
		protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
				throws ServletException, IOException {
			CsrfToken csrfToken = (CsrfToken) request.getAttribute("_csrf");
			if (csrfToken != null) {
				csrfToken.getToken();
			}
			filterChain.doFilter(request, response);
		}
	}

	private static class ServiceWorkerCacheFilter extends OncePerRequestFilter {
		@Override
		protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
				FilterChain filterChain) throws ServletException, IOException {
			// Prüfen, ob die Anfrage für den Service Worker ist
			if (request.getRequestURI().endsWith("/sw.js")) {
				// Caching komplett verbieten
				response.setHeader("Cache-Control", "no-cache, no-store, must-revalidate");
				response.setHeader("Pragma", "no-cache");
				response.setHeader("Expires", "0");
			}

			filterChain.doFilter(request, response);
		}
	}
}