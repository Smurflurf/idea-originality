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
            html.append(renderParagraphContent(root.get("intro"))).append("\n");
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
                    case "address_block": // Beides gleich behandeln
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
                                // Zeilenumbruch, außer beim letzten Element
                                if (i < lines.size() - 1) {
                                    html.append("<br>\n");
                                }
                            }
                            html.append("</p>\n");
                        }
                        break;
                    case "warning_box":
                        html.append("<p class=\"legal-warning-box\">");
                        // Wir bauen ein temporäres Objekt für smart content
                        // Hier wird implizit angenommen, dass der Block 'text' und optional 'label' hat
                        html.append(renderSmartContent(block)); 
                        html.append("</p>\n");
                        break;
                    case "license_section":
                        html.append(renderLicenseSection(block, labels));
                        break;
                }
            }
        }
        
        // 3. Warnbox (Globales Feld am Anfang, falls vorhanden)
        if (root.has("warning_box")) {
            // Um es kompatibel mit renderSmartContent zu machen, bauen wir ein Dummy-Objekt oder nutzen direkt HTML
            html.append("<br>");
            html.append("<p class=\"legal-warning-box\">");
            html.append(escapeHtml(root.get("warning_box").asText()));
            html.append("</p>\n");
        }
        
        return html.toString();
    }

    /**
     * Entspricht der Logik in JS: renderLicenseSection
     */
    private String renderLicenseSection(JsonNode block, JsonNode labels) {
        StringBuilder sb = new StringBuilder();
        
        // 1. Titel
        if (block.has("title")) {
            sb.append("<h2>").append(escapeHtml(block.get("title").asText())).append("</h2>\n");
        }
        
        // 2. Beschreibung
        if (block.has("description")) {
            sb.append("<p>").append(escapeHtml(block.get("description").asText())).append("</p>\n");
        }
        
        // 3. Items
        if (block.has("items") && block.get("items").isArray()) {
            sb.append("<ul>\n");
            for (JsonNode item : block.get("items")) {
                sb.append("<li>");
                
                // A) Name & Link
                String url = item.has("url") ? item.get("url").asText() : "#";
                String name = item.has("name") ? item.get("name").asText() : "";
                
                sb.append("<strong><a href=\"").append(url).append("\" target=\"_blank\" rel=\"noopener noreferrer\" class=\"item-link\">")
                  .append(escapeHtml(name))
                  .append("</a></strong><br>");
                
                // B) Purpose
                if (item.has("purpose")) {
                    sb.append(escapeHtml(item.get("purpose").asText())).append("<br>");
                }
                
                // C) Helper für Lizenzen (addLicenseLine)
                appendLicenseLine(sb, item, "license", labels);
                appendLicenseLine(sb, item, "metadata_license", labels);
                
                // D) Citation Block
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
              .append("</small></em> "); // Leerzeichen am Ende wie im JS
        }
    }

    /**
     * Wrapper für renderParagraph (String oder Array von SmartContent)
     */
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

    /**
     * Entspricht der Logik in JS: appendSmartContent
     */
    private String renderSmartContent(JsonNode item) {
        if (item.isTextual()) {
            return escapeHtml(item.asText());
        }
        
        StringBuilder out = new StringBuilder();
        
        // A: Label (z.B. "E-Mail:")
        if (item.has("label")) {
            out.append("<strong>").append(escapeHtml(item.get("label").asText())).append(": </strong> ");
        }
        
        String text = item.has("text") ? item.get("text").asText() : "";
        // Fallback: Wenn kein Text, aber URL da ist, nimm URL als Text
        if (text.isEmpty() && item.has("url")) {
            text = item.get("url").asText();
        }
        
        // B: Link oder Text
        if (item.has("url")) {
            String url = item.get("url").asText();
            out.append("<a href=\"").append(url).append("\"");
            
            // Mailto Check wie im JS
            if (!url.startsWith("mailto:")) {
                out.append(" target=\"_blank\" rel=\"noopener noreferrer\"");
            }
            out.append(">");
            
            if (item.has("bold") && item.get("bold").asBoolean()) {
                out.append("<strong>").append(escapeHtml(text)).append("</strong>");
            } else {
                out.append(escapeHtml(text));
            }
            out.append("</a>");
        } else {
            // Nur Text (ggf. fett)
            if (item.has("bold") && item.get("bold").asBoolean()) {
                out.append("<strong>").append(escapeHtml(text)).append("</strong>");
            } else {
                out.append(escapeHtml(text));
            }
        }
        
        return out.toString();
    }

    /**
     * Einfache HTML-Escaping Methode, um XSS im Fallback zu verhindern und Darstellung zu sichern.
     */
    private String escapeHtml(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;")
                   .replace("<", "&lt;")
                   .replace(">", "&gt;")
                   .replace("\"", "&quot;")
                   .replace("'", "&#39;");
    }
}