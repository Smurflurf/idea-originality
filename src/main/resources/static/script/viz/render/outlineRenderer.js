import { getContext } from '/script/core/context.js';
import { registerCleanup } from '/script/core/lifecycleManager.js';
import { on, off } from '/script/core/eventBus.js';

function drawOutlines(gContainer, clusterIds, colorMap, isContext = false) {
	if (!gContainer || !clusterIds || !colorMap) return;
	gContainer.innerHTML = '';
	const ctx = getContext();

	const outlinePathData = ctx.outlineData;
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

export function initializeOutlineRenderer(prefix) {
	const svg = document.getElementById(`viz-layer-${prefix}-outlines-svg`);
	const container = document.getElementById(`viz-stack-container-${prefix}`);
	const ctx = getContext();

	if (!svg || !container || !ctx.embeddingBounds || !ctx.outlineData) {
		console.warn(`OutlineRenderer für '${prefix}': Kritische Elemente oder Daten fehlen.`);
		return;
	}

	const bounds = ctx.embeddingBounds;

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
		case 'own': colorMap = ctx.ownColorMap; break;
		case 'nc': colorMap = ctx.neighborColorMap || {}; break;
		case 'serendipity': colorMap = ctx.serendipityColorMap || {}; break;
	}

	const contextLabels = ctx.contextLabels;
	const contextClusterIds = new Set(contextLabels.map(label => label.clusterId));
	const mainClusterIds = new Set(Object.keys(colorMap));

	drawOutlines(gMain, mainClusterIds, colorMap, false);
	drawOutlines(gContext, contextClusterIds, {}, true);

    let lastCalculatedStroke = null;

    const updateOutlineStroke = (detail) => {
        if (!detail || detail.prefix !== prefix) return;

        const currentZoom = detail.zoom;
        const dynamicStrokeInPixels = 1.0 * Math.sqrt(currentZoom);
        
        // PERFORMANCE FIX: Setzt den Style nur, wenn er sich signifikant ändert.
        if (lastCalculatedStroke === null || Math.abs(lastCalculatedStroke - dynamicStrokeInPixels) > 0.01) {
            svg.style.setProperty('--stroke-width-px', `${dynamicStrokeInPixels}px`);
            lastCalculatedStroke = dynamicStrokeInPixels;
        }
    };

    on('viz-transform', updateOutlineStroke);
	
	registerCleanup(() => {
	    off('viz-transform', updateOutlineStroke);
	});
}