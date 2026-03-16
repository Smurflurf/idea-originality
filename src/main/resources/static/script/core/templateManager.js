import { applyGeneralTranslations } from '/script/core/localization.js';

/**
 * Holt ein Template basierend auf einer Konvention, klont es, fügt es ein und übersetzt es.
 * Konvention: Template-ID ist "tpl-" + wrapperId.
 * Arbeitet als Singleton: Wenn das Element (wrapperId) schon existiert, wird es zurückgegeben.
 * 
 * @param {string} wrapperId - ID des Root-Elements im Template (z.B. 'download-popup-overlay')
 * @param {HTMLElement} [container=document.body] - Ziel-Container.
 * @returns {HTMLElement|null} - Das fertig eingefügte und übersetzte Element.
 */
export function renderTemplate(wrapperId, container = document.body) {
    // 1. Singleton-Check: Existiert es schon?
    let element = document.getElementById(wrapperId);
    if (element) {
        return element;
    }

    // 2. Template-ID aus Konvention ableiten
    const templateId = `tpl-${wrapperId}`;
    const fragment = getTemplate(templateId); // Nutzt weiterhin die schnelle, alte Funktion
    if (!fragment) {
        console.warn(`[TemplateManager] Template mit ID #${templateId} für Wrapper #${wrapperId} nicht gefunden.`);
        return null;
    }

    // 3. Ins DOM einfügen
    container.appendChild(fragment);

    // 4. Referenz auf das neue Element holen
    element = document.getElementById(wrapperId);
    if (!element) {
        console.warn(`[TemplateManager] Element #${wrapperId} wurde im Template #${templateId} nicht gefunden.`);
        return null;
    }

    // 5. Übersetzen
    applyGeneralTranslations(element);

    return element;
}

// Die Basis-Funktion bleibt für Flexibilität erhalten
export function getTemplate(templateId) {
    const tpl = document.getElementById(templateId);
    if (!tpl) {
        console.warn(`Template with ID '${templateId}' not found in the DOM.`);
        return null;
    }
    return tpl.content.cloneNode(true);
}