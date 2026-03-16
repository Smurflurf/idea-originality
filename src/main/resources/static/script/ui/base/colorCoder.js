import { getContext } from '/script/core/context.js';

/**
 * Dieses Skript ist für das kontextsensitive Einfärben von UI-Elementen verantwortlich.
 * Es passt die vom Backend kommenden Farben an, stellt sicher, dass Elemente in den richtigen
 * Abschnitten korrekt gefärbt werden, und wendet erweiterte Stile für Hierarchie-Verbinder an.
 */

// --- HILFSFUNKTIONEN ZUR FARBMANIPULATION ---

function hexToHSL(H) {
    let r = 0, g = 0, b = 0;
    if (H.length == 4) { r = "0x" + H[1] + H[1]; g = "0x" + H[2] + H[2]; b = "0x" + H[3] + H[3]; } 
    else if (H.length == 7) { r = "0x" + H[1] + H[2]; g = "0x" + H[3] + H[4]; b = "0x" + H[5] + H[6]; }
    r /= 255; g /= 255; b /= 255;
    let cmin = Math.min(r, g, b), cmax = Math.max(r, g, b), delta = cmax - cmin, h = 0, s = 0, l = 0;
    if (delta == 0) h = 0;
    else if (cmax == r) h = ((g - b) / delta) % 6;
    else if (cmax == g) h = (b - r) / delta + 2;
    else h = (r - g) / delta + 4;
    h = Math.round(h * 60);
    if (h < 0) h += 360;
    l = (cmax + cmin) / 2;
    s = delta == 0 ? 0 : delta / (1 - Math.abs(2 * l - 1));
    s = +(s * 100).toFixed(1);
    l = +(l * 100).toFixed(1);
    return { h, s, l };
}

function hslToHEX(h, s, l) {
    s /= 100; l /= 100;
    let c = (1 - Math.abs(2 * l - 1)) * s, x = c * (1 - Math.abs((h / 60) % 2 - 1)), m = l - c / 2, r = 0, g = 0, b = 0;
    if (0 <= h && h < 60) { r = c; g = x; b = 0; }
    else if (60 <= h && h < 120) { r = x; g = c; b = 0; }
    else if (120 <= h && h < 180) { r = 0; g = c; b = x; }
    else if (180 <= h && h < 240) { r = 0; g = x; b = c; }
    else if (240 <= h && h < 300) { r = x; g = 0; b = c; }
    else if (300 <= h && h < 360) { r = c; g = 0; b = x; }
    r = Math.round((r + m) * 255).toString(16);
    g = Math.round((g + m) * 255).toString(16);
    b = Math.round((b + m) * 255).toString(16);
    if (r.length == 1) r = "0" + r;
    if (g.length == 1) g = "0" + g;
    if (b.length == 1) b = "0" + b;
    return "#" + r + g + b;
}

export function getUiColor(hexColor) {
    if (!hexColor || !hexColor.startsWith('#')) return '#8ab4f8';
    const hsl = hexToHSL(hexColor);
    hsl.l = Math.max(40, hsl.l * 0.70);
    hsl.s = Math.max(50, hsl.s * 0.90);
    return hslToHEX(hsl.h, hsl.s, hsl.l);
}

export function getContrastingTextColor(hexColor) {
    if (!hexColor) return "#202124";
    let hex = hexColor.replace(/^#/, "");
    if (hex.length === 3) hex = hex.split("").map(c => c + c).join("");
    const r = parseInt(hex.substring(0, 2), 16);
    const g = parseInt(hex.substring(2, 4), 16);
    const b = parseInt(hex.substring(4, 6), 16);
    const brightness = (r * 299 + g * 587 + b * 114) / 1000;
    return brightness > 125 ? "#202124" : "#e8eaed";
}

// --- KERNLOGIK ZUM FÄRBEN ---

/**
 * Färbt die Rahmen von Hierarchie-Boxen innerhalb eines Containers.
 * @param {HTMLElement} container - Das Elternelement, das die Boxen enthält.
 * @param {object} colorMap - Die Farbzuordnung { clusterId: hexColor }.
 */
export function colorizeElementsInContainer(container, colorMap) {
    if (!container || !colorMap) return;
    container.querySelectorAll('.color-coded-box').forEach(element => {
        const clusterId = element.dataset.clusterId;
        const color = colorMap[clusterId];
        if (color) {
            element.style.setProperty('--cluster-border-color', color);
            element.style.setProperty('--cluster-border-color-hover', color);
        } else {
            element.style.removeProperty('--cluster-border-color');
            element.style.removeProperty('--cluster-border-color-hover');
        }
    });
}

/**
 * Färbt die '>' Indikatoren in Hierarchien (wird von Tooltips benötigt).
 * @param {HTMLElement} container - Der Container mit der Hierarchie (z.B. der Tooltip-Inhalt).
 * @param {object} colorMap - Die Farbkarte.
 */
export function colorizeIndicators(container, colorMap) {
    if (!container || !colorMap) return;
    container.querySelectorAll('.hierarchy-list > li').forEach((li) => {
        const box = li.querySelector('.color-coded-box');
        if (!box) return;
        let currentId = box.dataset.clusterId;
        let foundColor = false;
        while (currentId.includes('-') && !foundColor) {
            const parentId = getParentId(currentId);
            if (colorMap[parentId]) {
                li.style.setProperty('--indicator-color', colorMap[parentId]);
                foundColor = true;
            }
            currentId = parentId;
        }
        if (!foundColor) {
            li.style.removeProperty('--indicator-color');
        }
    });
}


/**
 * Extrahiert die ID des direkten Eltern-Clusters. Gibt null zurück für Top-Level-Elemente.
 * @param {string} clusterId - z.B. "1-2-3"
 * @returns {string|null} - z.B. "1-2"
 */
function getParentId(clusterId) {
    if (!clusterId || !clusterId.includes('-')) {
        return null;
    }
    return clusterId.substring(0, clusterId.lastIndexOf('-'));
}

/**
 * Wendet erweiterte Hierarchie-Stile an: Farben, Verbinder und Separatoren.
 * Diese Funktion berechnet die Einrückungsebene des Elternteils für korrekte Verbinder.
 * @param {HTMLElement} listElement - Das UL-Element der Hierarchie.
 * @param {object} colorMap - Die Farbkarte für die Cluster.
 */
export function applyAdvancedHierarchyStyling(listElement, colorMap) {
    if (!listElement || !colorMap) return;
    const listItems = listElement.querySelectorAll(':scope > li');

    listItems.forEach((li, index) => {
        li.style.removeProperty('--indicator-color');
        li.style.removeProperty('--parent-indent-level');
        li.classList.remove('needs-separator');

        const box = li.querySelector('.color-coded-box');
        if (!box) return;

        let currentId = box.dataset.clusterId;
        let foundParent = false;

        // --- KORRIGIERTE LOGIK ---
        // Finde den nächsten Vorfahren, um Layout-Informationen zu setzen,
        // und DANN prüfe, ob er eingefärbt werden soll.
        while (currentId.includes('-') && !foundParent) {
            const parentId = getParentId(currentId);
            if (!parentId) break;

            const parentBox = listElement.querySelector(`.hierarchy-item-box[data-cluster-id="${parentId}"]`);
            
            // Haben wir einen gültigen Elternteil im DOM gefunden?
            if (parentBox) {
                const parentLi = parentBox.closest('li');
                const parentIndentLevel = parseInt(parentLi.style.getPropertyValue('--indent-level'), 10);

                // **SCHRITT 1: IMMER die Layout-Variable für die Positionierung setzen.**
                li.style.setProperty('--parent-indent-level', parentIndentLevel);

                // **SCHRITT 2: NUR DANN die Farbe setzen, wenn der Elternteil eine hat.**
                if (colorMap[parentId]) {
                    li.style.setProperty('--indicator-color', colorMap[parentId]);
                }
                
                foundParent = true; // Wir haben den nächsten Elternteil gefunden, die Suche ist beendet.
            }
            
            currentId = parentId; // Falls Elternteil nicht im DOM, gehe eine Ebene höher.
        }
        
        // Logik für Separatoren (unverändert)
        if (index > 0) {
            const previousLi = listItems[index - 1];
            const previousBox = previousLi.querySelector('.color-coded-box');
            if (previousBox) {
                const currentId = box.dataset.clusterId;
                const previousId = previousBox.dataset.clusterId;
                const isDescendant = currentId.startsWith(previousId + '-');
                const areSiblings = getParentId(currentId) === getParentId(previousId);
                if (!isDescendant && !areSiblings) {
                    li.classList.add('needs-separator');
                }
            }
        }
    });
}


/**
 * Die zentrale Funktion, die alle Färbe- und Styling-Operationen auf der Seite steuert.
 */
export function applyColorCoding() {
	const ctx = getContext();
    const ownVizContainer = document.getElementById('own-viz-content');
    const neighborVizContainer = document.getElementById('neighbor-viz-content');
    const serendipityVizContainer = document.getElementById('serendipity-viz-content');

    // FIX: Prüfen auf window.VAR (das fängt null und undefined ab)
    
    if (ownVizContainer && ctx.ownColorMap) {
        colorizeElementsInContainer(ownVizContainer, ctx.ownColorMap);
    }
    
    if (neighborVizContainer && ctx.neighborColorMap) {
        colorizeElementsInContainer(neighborVizContainer, ctx.neighborColorMap);
    }
    
    if (serendipityVizContainer && ctx.serendipityColorMap) {
        colorizeElementsInContainer(serendipityVizContainer, ctx.serendipityColorMap);
    }

    // Erweiterte Hierarchie-Stile (Gleicher Fix: window.VAR prüfen)
    const neighborMainHierarchyList = document.querySelector('#neighbor-viz-content > .hierarchy-container > .hierarchy-list');
    if (neighborMainHierarchyList && ctx.neighborColorMap) {
        applyAdvancedHierarchyStyling(neighborMainHierarchyList, ctx.neighborColorMap);
    }
    
    const serendipityMainHierarchyList = document.querySelector('#serendipity-viz-content > .hierarchy-container > .hierarchy-list');
    if (serendipityMainHierarchyList && ctx.serendipityColorMap) {
        applyAdvancedHierarchyStyling(serendipityMainHierarchyList, ctx.serendipityColorMap);
    }

    // Topic Tabs
    const neighborTopicTabs = document.querySelectorAll('#neighbor-viz-content .topic-tab');
    if (neighborTopicTabs.length > 0 && ctx.neighborColorMap) {
        neighborTopicTabs.forEach(tab => {
            const clusterId = tab.dataset.clusterId;
            if (clusterId && ctx.neighborColorMap[clusterId]) {
                const baseColor = ctx.neighborColorMap[clusterId];
                const uiColor = getUiColor(baseColor);
                tab.style.setProperty('--tab-border-color', uiColor);
            }
        });
    }

    const serendipityTopicTabs = document.querySelectorAll('#serendipity-viz-content .topic-tab');
    if (serendipityTopicTabs.length > 0 && ctx.serendipityColorMap) {
        serendipityTopicTabs.forEach(tab => {
            const clusterId = tab.dataset.clusterId;
            if (clusterId && ctx.serendipityColorMap[clusterId]) {
                const baseColor = ctx.serendipityColorMap[clusterId];
                const uiColor = getUiColor(baseColor);
                tab.style.setProperty('--tab-border-color', uiColor);
            }
        });
    }
}

// Global verfügbar machen für ältere Skripte, falls nötig
window.applyColorCoding = applyColorCoding;

/**
 * Initialisiert die Färbelogik und stellt sicher, dass sie auch für dynamisch
 * nachgeladene Inhalte erneut ausgeführt wird.
 */
export function initializeColorCodingTriggers() {
    applyColorCoding();
    document.addEventListener('dynamicContentLoaded', () => {
        applyColorCoding();
    });
}