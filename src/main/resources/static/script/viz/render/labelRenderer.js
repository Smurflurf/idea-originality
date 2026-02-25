import { getContext } from '/script/core/context.js';
import { registerCleanup } from '/script/core/lifecycleManager.js';
import { on, off } from '/script/core/eventBus.js';

// ==========================================
// LABEL EINSTELLUNGEN
// ==========================================
export const LABEL_CONFIG = {
    // Gibt als Prozentwert an (z.B. 1.0 = 1%), wie weit der simulierte Rahmen nach innen rückt.
    // Bei 0.0 schließen die Labels absolut bündig (ohne Lücke) mit dem physischen Rand ab.
    EDGE_PADDING_PERCENT: 1.0,  
    MAX_CHARS_PER_LINE: 18        // Begrenzt Textlänge bevor umgebrochen wird
};

// --- Automatischer Zeilenumbruch (Word Wrap) ---
function wrapText(text, maxChars = LABEL_CONFIG.MAX_CHARS_PER_LINE) {
    if (!text) return [];
    const words = text.split(' ');
    const lines = [];
    let currentLine = '';
    for (let i = 0; i < words.length; i++) {
        const word = words[i];
        if ((currentLine + word).length > maxChars && currentLine.length > 0) {
            lines.push(currentLine.trim());
            currentLine = word + ' ';
        } else {
            currentLine += word + ' ';
        }
    }
    if (currentLine) lines.push(currentLine.trim());
    return lines;
}

function drawLabels(gContainer, labelData, baseScale, bounds) {
    if (!gContainer || !labelData || !bounds) return;
    gContainer.innerHTML = '';
	
    const dataWidth = bounds.xmax - bounds.xmin;
    const dataHeight = bounds.ymax - bounds.ymin;
    
    // Berechne den %-Puffer in absoluten Daten-Koordinaten (1.0 = 1%)
    const padX = dataWidth * (LABEL_CONFIG.EDGE_PADDING_PERCENT / 100);
    const padY = dataHeight * (LABEL_CONFIG.EDGE_PADDING_PERCENT / 100);

    // Der absolut harte, konfigurierte Rand (Simulierte Bounding Box)
    const safeMinX = bounds.xmin + padX;
    const safeMaxX = bounds.xmax - padX;
    const safeMinY = bounds.ymin + padY;
    const safeMaxY = bounds.ymax - padY;

    labelData.forEach(label => {
        const group = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        group.classList.add('label-bundle');
        
        let textAnchor = 'middle';
        let renderX = label.x_data;
        let renderY = label.y_data;

        // --- SMART BORDER SNAP ---
        // Wenn das Label in den äußeren 15% liegt, pinnen wir es EXAKT an die simulierte Box!
        // Bei 0% Padding klebt es dadurch genau ohne Spalt an der Wand.
        if (label.x_data > bounds.xmax - (dataWidth * 0.15)) {
            textAnchor = 'end';
            // Zwingt das rechte Text-Ende haargenau auf die Begrenzungslinie
            renderX = safeMaxX; 
        } else if (label.x_data < bounds.xmin + (dataWidth * 0.15)) {
            textAnchor = 'start';
            // Zwingt den linken Text-Anfang haargenau auf die Begrenzungslinie
            renderX = safeMinX; 
        } else {
            // Mitten im Bild bleibt es am Datenpunkt, bricht aber notfalls nicht aus
            renderX = Math.max(safeMinX, Math.min(safeMaxX, renderX));
        }

        renderY = Math.max(safeMinY, Math.min(safeMaxY, renderY));

        const transformValue = `translate(${renderX}, ${renderY}) scale(1, -1)`;
        group.setAttribute('transform', transformValue);
        
        const fontSizeInDataCoords = label.fontSize_base * baseScale;
        group.dataset.baseSize = fontSizeInDataCoords;

        const lines = wrapText(label.text);

        const createTextBase = () => {
            const el = document.createElementNS('http://www.w3.org/2000/svg', 'text');
            el.setAttribute('font-family', label.fontFamily);
            el.setAttribute('font-weight', label.fontWeight);
            el.setAttribute('text-anchor', textAnchor);
            el.setAttribute('dominant-baseline', 'central');
            
            const verticalOffset = (lines.length - 1) * 0.5;
            lines.forEach((line, index) => {
                const tspan = document.createElementNS('http://www.w3.org/2000/svg', 'tspan');
                tspan.setAttribute('x', 0);
                tspan.setAttribute('dy', index === 0 ? `-${verticalOffset}em` : '1.1em');
                tspan.textContent = line;
                el.appendChild(tspan);
            });
            return el;
        };

        const whiteLayer = createTextBase();
        whiteLayer.setAttribute('fill', 'white');
        whiteLayer.setAttribute('stroke', 'white');
        whiteLayer.setAttribute('stroke-width', '0.3em'); 
        whiteLayer.setAttribute('stroke-linejoin', 'round');

        const blackLayer = createTextBase();
        blackLayer.setAttribute('fill', 'black');
        blackLayer.setAttribute('stroke', 'black');
        blackLayer.setAttribute('stroke-width', '0.20em');
        blackLayer.setAttribute('stroke-linejoin', 'round');

        const colorLayer = createTextBase();
        colorLayer.setAttribute('fill', label.color);

        group.appendChild(whiteLayer);
        group.appendChild(blackLayer);
        group.appendChild(colorLayer);
        
        gContainer.appendChild(group);
    });
}

export function initializeLabelRenderer(prefix) {
	const ctx = getContext();
    const svg = document.getElementById(`viz-layer-${prefix}-labels-svg`);
    const container = document.getElementById(`viz-stack-container-${prefix}`);
	
    if (!svg || !container || !ctx.embeddingBounds) {
        console.warn(`LabelRenderer für '${prefix}': Kritische Elemente oder Daten fehlen.`);
        return;
    }

    const bounds = ctx.embeddingBounds;
    const dataWidth = bounds.xmax - bounds.xmin;
    const dataHeight = bounds.ymax - bounds.ymin;
    
    if (dataWidth <= 0 || dataHeight <= 0) return;

    const aspectRatio = dataHeight / dataWidth;
    const imageWidth = 3000; 
    const imageHeight = imageWidth * aspectRatio;

    svg.setAttribute('viewBox', `0 0 ${imageWidth} ${imageHeight}`);
    
    const gMain = document.getElementById(`g-labels-${prefix}-main`);
    const gContext = document.getElementById(`g-labels-${prefix}-context`);
    const transformString = `scale(${imageWidth / dataWidth} ${-imageHeight / dataHeight}) translate(${-bounds.xmin} ${-bounds.ymax})`;

    gMain.setAttribute('transform', transformString);
    gContext.setAttribute('transform', transformString);
    
    const baseScale = dataWidth / imageWidth;
    
    let mainLabels = [];
	switch (prefix) {
		case 'own': mainLabels = ctx.ownLabels; break;
		case 'nc': mainLabels = ctx.neighborLabels; break;
		case 'serendipity': mainLabels = ctx.serendipityLabels; break;
    }
	const contextLabels = ctx.contextLabels;
	
    // Alle Labels werden ausnahmslos gezeichnet
    drawLabels(gMain, mainLabels, baseScale, bounds);
    drawLabels(gContext, contextLabels, baseScale, bounds);

    const cachedLabels = [];
    const labelGroups = svg.querySelectorAll('.label-bundle');
    labelGroups.forEach(group => {
        const baseSize = parseFloat(group.dataset.baseSize);
        if (isNaN(baseSize)) return;
        cachedLabels.push({
            baseSize: baseSize,
            textNodes: Array.from(group.querySelectorAll('text'))
        });
    });

    const updateLabelSizes = (detail) => {
        if (!detail || detail.prefix !== prefix) return;

        const currentZoom = detail.zoom;
        const containerWidth = detail.containerWidth;

        if (containerWidth === 0) return;

        // --- SCHRIFTGRÖßEN LIMITS ---
        const TARGET_SCREEN_SIZE_PX = 12; // Feineres Schriftbild
        const MIN_SCREEN_SIZE_PX = 6;     // Darf weiter schrumpfen um Platz zu machen
        const MAX_SCREEN_SIZE_PX = 24;    // Deckelung nach oben
        const CONVERGENCE_SPEED = 0.2;

        const targetBaseSize = (TARGET_SCREEN_SIZE_PX / containerWidth) * dataWidth;
        const convergenceFactor = 1 - Math.exp(-(currentZoom - 1) * CONVERGENCE_SPEED);

        const dataToPxFactor = (containerWidth * currentZoom) / dataWidth;
        const pxToDataFactor = dataWidth / (containerWidth * currentZoom);

        for (let i = 0; i < cachedLabels.length; i++) {
            const cache = cachedLabels[i];
            
            const targetSizeNow = targetBaseSize / currentZoom;
            let finalSizeData = cache.baseSize * (1 - convergenceFactor) + targetSizeNow * convergenceFactor;
            
            // Rechnen in Pixel um und kappen extreme Größen, damit nichts das halbe Bild verdeckt
            let finalPx = finalSizeData * dataToPxFactor;
            finalPx = Math.max(MIN_SCREEN_SIZE_PX, Math.min(MAX_SCREEN_SIZE_PX, finalPx));
            finalSizeData = finalPx * pxToDataFactor; 

            const texts = cache.textNodes;
            for (let j = 0; j < texts.length; j++) {
                texts[j].setAttribute('font-size', finalSizeData);
            }
        }
    };

    on('viz-transform', updateLabelSizes);

	registerCleanup(() => {
		off('viz-transform', updateLabelSizes);
	});
}