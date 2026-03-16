package de.simon.originality.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.io.InputStream;

@Service
public class LegalPageRendererService {

    private final ObjectMapper objectMapper;

    public LegalPageRendererService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String renderFallbackHtml(String pageName, String langCode) {
        try {
            // Pfad zur JSON Datei
            ClassPathResource resource = new ClassPathResource("static/assets/i18n/" + langCode + "/" + pageName + ".json");
            
            // Fallback auf Englisch, falls Deutsch nicht existiert (optional, aber sicher)
            if (!resource.exists() && "de".equals(langCode)) {
                resource = new ClassPathResource("static/assets/i18n/en/" + pageName + ".json");
            }

            if (!resource.exists()) {
                return "<p>Content currently unavailable.</p>";
            }

            try (InputStream is = resource.getInputStream()) {
                JsonNode root = objectMapper.readTree(is);
                return buildHtmlFromJson(root);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return "<p>Error loading content.</p>";
        }
    }

    private String buildHtmlFromJson(JsonNode root) {
        StringBuilder html = new StringBuilder();

     // 0. Globale Labels laden (für License Section benötigt)
		JsonNode labels = root.has("labels") ? root.get("labels") : null;

		// 1. Titel & Intro
		if (root.has("page_title")) {
			html.append("<h1>").append(escapeHtml(root.get("page_title").asText())).append("</h1>\n");
		}
		if (root.has("intro")) {
			html.append("<p>").append(renderParagraphContent(root.get("intro"))).append("</p>\n");
		}

        // 2. Struktur-Blöcke iterieren
        if (root.has("structure") && root.get("structure").isArray()) {
            for (JsonNode block : root.get("structure")) {
                String type = block.has("type") ? block.get("type").asText() : "";
                
                switch (type) {
                    case "section_title":
                        html.append("<h2>").append(escapeHtml(block.get("text").asText())).append("</h2>\n");
                        break;
                    case "subsection_title":
                        html.append("<h3>").append(escapeHtml(block.get("text").asText())).append("</h3>\n");
                        break;
                    case "heading":
                        String level = block.has("level") ? block.get("level").asText() : "h2";
                        html.append("<").append(level).append(">")
                            .append(escapeHtml(block.get("text").asText()))
                            .append("</").append(level).append(">\n");
                        break;
                    case "paragraph":
                        if (block.has("content")) {
                            html.append("<p>").append(renderParagraphContent(block.get("content"))).append("</p>\n");
                        }
                        break;
                    case "list":
                        if (block.has("items")) {
                            boolean ordered = block.has("ordered") && block.get("ordered").asBoolean();
                            html.append(ordered ? "<ol>\n" : "<ul>\n");
                            for (JsonNode item : block.get("items")) {
                                html.append("<li>").append(renderSmartContent(item)).append("</li>\n");
                            }
                            html.append(ordered ? "</ol>\n" : "</ul>\n");
                        }
                        break;
                    case "lines_block":
                    case "address_block": 
                        if (block.has("headline")) {
                            html.append("<h2>").append(escapeHtml(block.get("headline").asText())).append("</h2>\n");
                        }
                        if (block.has("lines")) {
                            html.append("<p>");
                            JsonNode lines = block.get("lines");
                            for (int i = 0; i < lines.size(); i++) {
                                JsonNode line = lines.get(i);
                                if (line.isArray()) {
                                    for (JsonNode segment : line) {
                                        html.append(renderSmartContent(segment));
                                    }
                                } else {
                                    html.append(renderSmartContent(line));
                                }
                                if (i < lines.size() - 1) {
                                    html.append("<br>\n");
                                }
                            }
                            html.append("</p>\n");
                        }
                        break;
                    case "warning_box":
                        html.append("<p class=\"legal-warning-box\">");
                        html.append(renderSmartContent(block)); 
                        html.append("</p>\n");
                        break;
                    case "license_section":
                        html.append(renderLicenseSection(block, labels));
                        break;
                    // --- NEUE FAQ BLÖCKE ---
                    case "faq_search":
                        html.append(renderFaqSearch(block));
                        break;
                    case "cascade_block":
                        html.append(renderCascadeBlock(block, 2));
                        break;
                    default:
                        // Fallback, falls ein FAQ Item direkt auf oberster Ebene liegt
                        if (block.has("question") && block.has("answer")) {
                            html.append(renderFaqItem(block));
                        }
                        break;
                }
            }
        }
        
        // 3. Warnbox (Globales Feld am Anfang, falls vorhanden)
        if (root.has("warning_box")) {
            html.append("<br>");
            html.append("<p class=\"legal-warning-box\">");
            html.append(escapeHtml(root.get("warning_box").asText()));
            html.append("</p>\n");
        }
        
        return html.toString();
    }

    // =========================================================================
    // FAQ RENDERER METHODS
    // =========================================================================

    private String renderFaqSearch(JsonNode block) {
        String placeholder = block.has("placeholder") ? escapeHtml(block.get("placeholder").asText()) : "Search...";
        return "<div class=\"faq-search-container\" id=\"static-faq-search-box\">\n" +
               "  <div class=\"faq-input-wrapper\">\n" +
               "    <input type=\"text\" class=\"faq-search-input\" placeholder=\"" + placeholder + "\" readonly style=\"cursor: pointer; pointer-events: none;\">\n" +
               "  </div>\n" +
               "</div>\n";
    }

    private String renderCascadeBlock(JsonNode block, int level) {
        StringBuilder sb = new StringBuilder();
        boolean hasTitle = block.has("title") && !block.get("title").asText().isEmpty();

        if (!hasTitle) {
            sb.append("<div class=\"cascade-wrapper\">\n");
            if (block.has("items") && block.get("items").isArray()) {
                for (JsonNode item : block.get("items")) {
                    String type = item.has("type") ? item.get("type").asText() : "";
                    if ("cascade_block".equals(type)) {
                        sb.append(renderCascadeBlock(item, level + 1));
                    } else {
                        sb.append(renderFaqItem(item));
                    }
                }
            }
            sb.append("</div>\n");
            return sb.toString();
        }

        sb.append("<div class=\"faq-category-card\">\n");
        sb.append("  <button class=\"faq-category-header\">\n");
        sb.append("    <h3>").append(escapeHtml(block.get("title").asText())).append("</h3>\n");
        sb.append("  </button>\n");
        sb.append("  <div class=\"faq-category-body-wrapper\">\n");
        sb.append("    <div class=\"faq-category-body-inner\">\n");

        if (block.has("items") && block.get("items").isArray()) {
            for (JsonNode item : block.get("items")) {
                String type = item.has("type") ? item.get("type").asText() : "";
                if ("cascade_block".equals(type)) {
                    sb.append(renderCascadeBlock(item, level + 1));
                } else {
                    sb.append(renderFaqItem(item));
                }
            }
        }

        sb.append("    </div>\n");
        sb.append("  </div>\n");
        sb.append("</div>\n");

        return sb.toString();
    }

    private String renderFaqItem(JsonNode item) {
        if (!item.has("question")) return "";

        StringBuilder sb = new StringBuilder();
        String idAttr = item.has("id") ? " id=\"" + escapeHtml(item.get("id").asText()) + "\"" : "";

        sb.append("<div class=\"faq-item\"").append(idAttr).append(">\n");
        sb.append("  <button class=\"faq-question\">\n");
        sb.append("    <span>").append(escapeHtml(item.get("question").asText())).append("</span>\n");
        sb.append("  </button>\n");
        sb.append("  <div class=\"faq-answer-wrapper\">\n");
        sb.append("    <div class=\"faq-answer-inner\">\n");
        sb.append("      <div class=\"faq-answer-content\">\n");

        // Answer parsen (kann String oder Array sein)
        if (item.has("answer")) {
            JsonNode answerNode = item.get("answer");
            if (answerNode.isArray()) {
                for (JsonNode ans : answerNode) {
                    sb.append("<p>").append(renderParagraphContent(ans)).append("</p>\n");
                }
            } else {
                sb.append("<p>").append(renderParagraphContent(answerNode)).append("</p>\n");
            }
        }

        sb.append("      </div>\n");
        sb.append("    </div>\n");
        sb.append("  </div>\n");
        sb.append("</div>\n");

        return sb.toString();
    }

    // =========================================================================
    // GENERAL RENDERER METHODS
    // =========================================================================

    private String renderLicenseSection(JsonNode block, JsonNode labels) {
        StringBuilder sb = new StringBuilder();
        
        if (block.has("title")) {
            sb.append("<h2>").append(escapeHtml(block.get("title").asText())).append("</h2>\n");
        }
        
        if (block.has("description")) {
            sb.append("<p>").append(escapeHtml(block.get("description").asText())).append("</p>\n");
        }
        
        if (block.has("items") && block.get("items").isArray()) {
            sb.append("<ul>\n");
            for (JsonNode item : block.get("items")) {
                sb.append("<li>");
                
                String url = item.has("url") ? item.get("url").asText() : "#";
                String name = item.has("name") ? item.get("name").asText() : "";
                
                sb.append("<strong><a href=\"").append(url).append("\" target=\"_blank\" rel=\"noopener noreferrer\" class=\"item-link\">")
                  .append(escapeHtml(name))
                  .append("</a></strong><br>");
                
                if (item.has("purpose")) {
                    sb.append(escapeHtml(item.get("purpose").asText())).append("<br>");
                }
                
                appendLicenseLine(sb, item, "license", labels);
                appendLicenseLine(sb, item, "metadata_license", labels);
                
                if (item.has("citation")) {
                    sb.append("<div class=\"citation-block\">")
                      .append(escapeHtml(item.get("citation").asText()).replace("\n", "<br>"))
                      .append("</div>");
                }
                
                sb.append("</li>\n");
            }
            sb.append("</ul>\n");
        }
        
        return sb.toString();
    }

    private void appendLicenseLine(StringBuilder sb, JsonNode item, String key, JsonNode labels) {
        if (item.has(key)) {
            String value = item.get(key).asText();
            String labelTxt = (labels != null && labels.has(key)) ? labels.get(key).asText() : key;
            
            sb.append("<em><small><strong>")
              .append(escapeHtml(labelTxt)).append(": </strong>")
              .append(escapeHtml(value))
              .append("</small></em> ");
        }
    }

    private String renderParagraphContent(JsonNode contentNode) {
        StringBuilder sb = new StringBuilder();
        if (contentNode.isArray()) {
            for (JsonNode segment : contentNode) {
                sb.append(renderSmartContent(segment));
            }
        } else {
            sb.append(renderSmartContent(contentNode));
        }
        return sb.toString();
    }

    private String renderSmartContent(JsonNode item) {
        if (item.isTextual()) {
            return escapeHtml(item.asText());
        }
        
        StringBuilder out = new StringBuilder();
        
        if (item.has("label")) {
            out.append("<strong>").append(escapeHtml(item.get("label").asText())).append(": </strong> ");
        }
        
        String text = item.has("text") ? item.get("text").asText() : "";
        if (text.isEmpty() && item.has("url")) {
            text = item.get("url").asText();
        }
        
        if (item.has("url")) {
            String url = item.get("url").asText();
            out.append("<a href=\"").append(url).append("\"");
            
            if (!url.startsWith("mailto:")) {
                out.append(" target=\"_blank\" rel=\"noopener noreferrer\"");
            }
            out.append(">");
            
            if (item.has("bold") && item.get("bold").asBoolean()) {
                out.append("<strong>").append(escapeHtml(text)).append("</strong>");
            } else if (item.has("italic") && item.get("italic").asBoolean()) {
                out.append("<cite>").append(escapeHtml(text)).append("</cite>");
            } else {
                out.append(escapeHtml(text));
            }
            out.append("</a>");
        } else {
            if (item.has("bold") && item.get("bold").asBoolean()) {
                out.append("<strong>").append(escapeHtml(text)).append("</strong>");
            } else if (item.has("italic") && item.get("italic").asBoolean()) {
                out.append("<cite>").append(escapeHtml(text)).append("</cite>");
            } else {
                out.append(escapeHtml(text));
            }
        }
        
        return out.toString();
    }

    private String escapeHtml(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;")
                   .replace("\"", "&quot;")
                   .replace("'", "&#39;");
    }
}