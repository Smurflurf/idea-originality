/**
 * Zeichnet die SVG-Pfade für die Outlines in einen <g>-Container.
 * Liest die Pfaddaten dynamisch aus window.OUTLINE_DATA.
 * @param {SVGElement} gContainer - Der Ziel-Container.
 * @param {Set<string>} clusterIds - Die IDs der zu zeichnenden Cluster.
 * @param {object} colorMap - Die Farbkarte.
 * @param {boolean} isContext - Ob es sich um Kontext-Outlines handelt (grau).
 */
function drawOutlines(gContainer, clusterIds, colorMap, isContext = false) {
    if (!gContainer || !clusterIds || !colorMap) return;
    gContainer.innerHTML = '';
    
    const outlinePathData = window.OUTLINE_DATA || {};
    const contextColor = '#808080';
    const baseStrokeWidth = 0.015;

    for (const clusterId of clusterIds) {
        const pathString = outlinePathData[clusterId];
        if (pathString) {
            const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
            path.setAttribute('d', pathString);
            path.setAttribute('stroke', isContext ? contextColor : colorMap[clusterId] || '#FFFFFF');
            path.setAttribute('fill', 'none');
            
            path.setAttribute('stroke-width', baseStrokeWidth);
            path.style.strokeWidth = 'var(--stroke-width-px, 1.5px)';
            
            path.setAttribute('vector-effect', 'non-scaling-stroke');
            path.classList.add('outline-path');
            gContainer.appendChild(path);
        }
    }
}

/**
 * Initialisiert den Outline-Renderer.
 * @param {string} prefix - Das Präfix der Element-IDs ('own', 'nc', 'serendipity').
 */
export function initializeOutlineRenderer(prefix) {
    const svg = document.getElementById(`viz-layer-${prefix}-outlines-svg`);
    const wrapper = document.getElementById(`zoom-pan-wrapper-${prefix}`);

    if (!svg || !wrapper || !window.EMBEDDING_BOUNDS || !window.OUTLINE_DATA) {
        console.warn(`OutlineRenderer für '${prefix}': Kritische Elemente oder Daten fehlen.`);
        return;
    }

    // --- PRERENDERING-LOGIK ---
    const bounds = window.EMBEDDING_BOUNDS;
    const dataWidth = bounds.xmax - bounds.xmin;
    const dataHeight = bounds.ymax - bounds.ymin;
    const aspectRatio = dataHeight / dataWidth;
    
    const imageWidth = 2000;
    const imageHeight = imageWidth * aspectRatio;

    svg.setAttribute('viewBox', `0 0 ${imageWidth} ${imageHeight}`);
    
    const gMain = document.getElementById(`g-outlines-${prefix}-main`);
    const gContext = document.getElementById(`g-outlines-${prefix}-context`);
	const transformString = `scale(${imageWidth / dataWidth} ${-imageHeight / dataHeight}) translate(${-bounds.xmin} ${-bounds.ymax})`;

	gMain.setAttribute('transform', transformString);
	gContext.setAttribute('transform', transformString);

	let colorMap = {};
	switch (prefix) {
		case 'own':
			colorMap = window.OWN_IDEA_COLOR_MAP || {};
			break;
		case 'nc':
			colorMap = window.NEIGHBOR_CLUSTER_COLOR_MAP || {};
			break;
		case 'serendipity':
			colorMap = window.SERENDIPITY_COLOR_MAP || {};
			break;
	}

	// Prüfen ob Kontextdaten da sind, sonst leeres Array
	const contextLabels = window.CONTEXT_LABELS_DATA || [];
	const contextClusterIds = new Set(contextLabels.map(label => label.clusterId));

	const mainClusterIds = new Set(Object.keys(colorMap));

	drawOutlines(gMain, mainClusterIds, colorMap, false);
	drawOutlines(gContext, contextClusterIds, {}, true);

    const observer = new MutationObserver(() => {
        const containerWidth = wrapper.parentElement.clientWidth;
        if (containerWidth === 0) return;
        const wrapperWidth = wrapper.getBoundingClientRect().width;
        const currentZoom = wrapperWidth / containerWidth;
        const dynamicStrokeInPixels = 1.0 * Math.sqrt(currentZoom); 
        svg.style.setProperty('--stroke-width-px', `${dynamicStrokeInPixels}px`);
    });
    observer.observe(wrapper, { attributes: true, attributeFilter: ['style'] });
}