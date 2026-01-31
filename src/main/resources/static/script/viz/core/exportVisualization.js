/**
 * Hilfsfunktion, um den Wert einer CSS-Variable aus dem :root-Element zu lesen.
 * @param {string} varName -- Name der Variable (z.B. '--bg-deep')
 * @returns {string | null} Der Wert der Variable oder null.
 */
function getCssVariableValue(varName) {
    if (typeof getComputedStyle === 'function') {
        const value = getComputedStyle(document.documentElement).getPropertyValue(varName);
        return value ? value.trim() : null;
    }
    return null;
}

/**
 * Zeichnet ein sichtbares SVG-Element auf einen Canvas.
 * @param {CanvasRenderingContext2D} ctx - Der 2D-Kontext des Ziel-Canvas.
 * @param {SVGElement} svgElement - Das zu zeichnende SVG-Element.
 * @param {number} width - Die Zielbreite des Canvas.
 * @param {number} height - Die Zielhöhe des Canvas.
 * @param {number} onScreenWidth - Die Breite des Visualisierungs-Containers auf dem Bildschirm in Pixeln.
 * @returns {Promise<void>}
 */
function drawCleanedSvgToCanvas(ctx, svgElement, width, height, onScreenWidth) {
    return new Promise((resolve) => {
        const svgClone = svgElement.cloneNode(true);

        // Unsichtbare Gruppen entfernen (wie bisher)
        svgClone.querySelectorAll(':scope > g').forEach(clonedGroup => {
            const originalGroup = document.getElementById(clonedGroup.id);
            if (originalGroup && originalGroup.style.display === 'none') {
                clonedGroup.remove();
            }
        });

        // --- KORREKTUR FÜR OUTLINES (Diese Logik ist korrekt und bleibt) ---
        const originalPaths = svgElement.querySelectorAll('.outline-path');
        const clonedPaths = svgClone.querySelectorAll('.outline-path');
        if (originalPaths.length > 0 && clonedPaths.length > 0 && window.EMBEDDING_BOUNDS && onScreenWidth > 0) {
            const onScreenPixelWidth = parseFloat(window.getComputedStyle(originalPaths[0]).strokeWidth);
            const bounds = window.EMBEDDING_BOUNDS;
            const dataWidth = bounds.xmax - bounds.xmin;
            const finalStrokeWidthInDataCoords = (onScreenPixelWidth * dataWidth) / onScreenWidth;

            clonedPaths.forEach(path => {
                path.setAttribute('stroke-width', finalStrokeWidthInDataCoords);
                path.style.strokeWidth = '';
                path.removeAttribute('vector-effect');
            });
        }

        // --- FIX FÜR LABELS ---
        // Der vorherige Block, der die Schriftgröße skaliert hat, wurde entfernt.
        // Die `font-size` ist bereits als Attribut im richtigen Daten-Koordinatensystem
        // im SVG gespeichert. Das Klonen (`cloneNode`) übernimmt diesen Wert.
        // Die korrekte Skalierung geschieht automatisch beim Rendern des SVGs in das Bild.

        const svgString = new XMLSerializer().serializeToString(svgClone);
        const svgBlob = new Blob([svgString], { type: 'image/svg+xml;charset=utf-8' });
        const url = URL.createObjectURL(svgBlob);

        const img = new Image();
        img.onload = () => {
            ctx.drawImage(img, 0, 0, width, height);
            URL.revokeObjectURL(url);
            resolve();
        };
        img.onerror = (e) => {
            console.error("Konnte bereinigtes SVG nicht als Bild für den Export laden.", e);
            URL.revokeObjectURL(url);
            resolve();
        };
        img.src = url;
    });
}


/**
 * Kernfunktion: Erstellt ein Canvas-Bild aus den sichtbaren Layern.
 */
export async function generateVisualizationImage(layerIdPrefix, pointsContainerId, pointsData, colorMap) {
    const canvas = document.createElement('canvas');
    const ctx = canvas.getContext('2d');

    const baseLayer = document.getElementById(`${layerIdPrefix}-base`);
    if (!baseLayer || !baseLayer.complete || baseLayer.naturalWidth === 0) {
        console.error("Basis-Visualisierung für Export nicht gefunden oder nicht geladen!");
        return null;
    }
    
    const vizContainer = baseLayer.closest('.viz-stack-container');
    const onScreenWidth = vizContainer ? vizContainer.clientWidth : baseLayer.naturalWidth;
    const resolutionScaleFactor = onScreenWidth > 0 ? baseLayer.naturalWidth / onScreenWidth : 1;

    canvas.width = baseLayer.naturalWidth;
    canvas.height = baseLayer.naturalHeight;
    
    const bgColor = getCssVariableValue('--bg-deep') || '#0e0e0f';
    ctx.fillStyle = bgColor;
    ctx.fillRect(0, 0, canvas.width, canvas.height);

    const imageLayerIds = [`${layerIdPrefix}-base`, `${layerIdPrefix}-points`];
    imageLayerIds.forEach(id => {
        const layer = document.getElementById(id);
        if (layer && layer.style.display !== 'none' && layer.complete) {
            ctx.drawImage(layer, 0, 0, canvas.width, canvas.height);
        }
    });

    const svgLayerIds = [`${layerIdPrefix}-outlines-svg`, `${layerIdPrefix}-labels-svg`];
    for (const id of svgLayerIds) {
        const svgLayer = document.getElementById(id);
        if (svgLayer) {
            await drawCleanedSvgToCanvas(ctx, svgLayer, canvas.width, canvas.height, onScreenWidth);
        }
    }
    
	const crosshairCanvasElement = document.getElementById(`${layerIdPrefix}-crosshair-canvas`);
	if (crosshairCanvasElement && crosshairCanvasElement.style.display !== 'none' && window.CROSSHAIR_COORDS) {
        const coords = window.CROSSHAIR_COORDS;
        const finalX = coords.x * canvas.width;
        const finalY = coords.y * canvas.height;
        const crosshairColor = getCssVariableValue('--color-crosshair') || 'rgba(255, 255, 0, 0.7)';
        ctx.strokeStyle = crosshairColor;
        ctx.lineWidth = 2 * resolutionScaleFactor; 
        ctx.setLineDash([10 * resolutionScaleFactor, 8 * resolutionScaleFactor]);
        ctx.beginPath();
        ctx.moveTo(0, finalY);
        ctx.lineTo(canvas.width, finalY);
        ctx.stroke();
        ctx.beginPath();
        ctx.moveTo(finalX, 0);
        ctx.lineTo(finalX, canvas.height);
        ctx.stroke();
        ctx.setLineDash([]);
    }

    const pointsContainer = document.getElementById(pointsContainerId);
    if (pointsContainer && pointsContainer.style.display !== 'none' && pointsData) {
        const basePointSize = 12;
        const baseStrokeWidth = 2.0;
        const scaledSize = basePointSize * resolutionScaleFactor;
        const scaledStrokeWidth = baseStrokeWidth * resolutionScaleFactor;

        pointsData.forEach(paper => {
            if (paper.relativeX === undefined || paper.relativeY === undefined) return;
            const x = paper.relativeX * canvas.width;
            const y = paper.relativeY * canvas.height;
            const color = colorMap[paper.clusterId] || '#bdc1c6';
            ctx.fillStyle = color;
            ctx.strokeStyle = 'rgba(0, 0, 0, 0.9)';
            ctx.lineWidth = scaledStrokeWidth;
            ctx.beginPath();
            ctx.arc(x, y, scaledSize / 2, 0, Math.PI * 2);
            ctx.fill();
            ctx.stroke();
        });
    }

    return canvas.toDataURL('image/png');
}

// Der Rest der Datei (sanitizeForFilename, setupExportButton) bleibt unverändert
function sanitizeForFilename(title) {
	if (!title || title.trim() === '') return t('global.untitled_analysis'); 
    return title
        .trim()
        .toLowerCase()
        .replace(/\s+/g, '-') 
        .replace(/[^\w-]/g, '') 
        .substring(0, 50);
}

document.addEventListener('DOMContentLoaded', () => {
    
    function setupExportButton(buttonId, layerIdPrefix, pointsContainerId, pointsData, colorMap) {
        const exportBtn = document.getElementById(buttonId);
        if (!exportBtn) return;

        exportBtn.addEventListener('click', async () => {
            const finalImageURL = await generateVisualizationImage(
                layerIdPrefix, pointsContainerId, pointsData, colorMap
            );
            
            if (finalImageURL) {
                const link = document.createElement('a');
                link.href = finalImageURL;
				const sanitizedTitle = sanitizeForFilename(window.JOB_TITLE);
				const suffix = layerIdPrefix.replace('viz-layer-', '');
				link.download = `visualization-${sanitizedTitle}-${suffix}.png`;

                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);
            }
        });
    }

    setupExportButton(
        'export-viz-btn-own', 
        'viz-layer-own',
        'dynamic-points-own-neighbors',
        typeof OWN_RESULTS_DATA !== 'undefined' ? OWN_RESULTS_DATA : [],
        typeof OWN_IDEA_COLOR_MAP !== 'undefined' ? OWN_IDEA_COLOR_MAP : {}
    );
    
    setupExportButton(
        'export-viz-btn-nc', 
        'viz-layer-nc',
        'dynamic-points-nc-neighbors',
        [], {}
    );
	
	setupExportButton(
	    'export-viz-btn-serendipity', 
	    'viz-layer-serendipity',
	    'dynamic-points-serendipity-neighbors', 
	    [],
	    typeof SERENDIPITY_COLOR_MAP !== 'undefined' ? SERENDIPITY_COLOR_MAP : {}
	);
});