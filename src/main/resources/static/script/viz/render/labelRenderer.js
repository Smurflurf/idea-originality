function drawLabels(gContainer, labelData, baseScale) {
    if (!gContainer || !labelData) return;
    gContainer.innerHTML = '';

    labelData.forEach(label => {
        const group = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        group.classList.add('label-bundle');
        const transformValue = `translate(${label.x_data}, ${label.y_data}) scale(1, -1)`;
        group.setAttribute('transform', transformValue);
        const fontSizeInDataCoords = label.fontSize_base * baseScale;
        group.dataset.baseSize = fontSizeInDataCoords;

        const createTextBase = () => {
            const el = document.createElementNS('http://www.w3.org/2000/svg', 'text');
            el.setAttribute('x', 0);
            el.setAttribute('y', 0);
            el.setAttribute('font-family', label.fontFamily);
            el.setAttribute('font-weight', label.fontWeight);
            el.setAttribute('text-anchor', 'middle');
            el.setAttribute('dominant-baseline', 'central');
            el.setAttribute('font-size', fontSizeInDataCoords);
            el.textContent = label.text;
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
    const svg = document.getElementById(`viz-layer-${prefix}-labels-svg`);
    const wrapper = document.getElementById(`zoom-pan-wrapper-${prefix}`);

    if (!svg || !wrapper || !window.EMBEDDING_BOUNDS) {
        console.warn(`LabelRenderer für '${prefix}': Kritische Elemente oder Daten fehlen.`);
        return;
    }

    const bounds = window.EMBEDDING_BOUNDS;
    const dataWidth = bounds.xmax - bounds.xmin;
    const dataHeight = bounds.ymax - bounds.ymin;
    
    if (dataWidth <= 0 || dataHeight <= 0) {
        console.error(`LabelRenderer für '${prefix}': Ungültige Embedding-Bounds.`);
        return;
    }

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
    
    // KORREKTUR: Robuste Auswahl der Label-Daten basierend auf dem Präfix
    let mainLabels = [];
    switch(prefix) {
        case 'own':
            mainLabels = window.OWN_LABELS_DATA || [];
            break;
        case 'nc':
            mainLabels = window.NEIGHBOR_LABELS_DATA || [];
            break;
        case 'serendipity':
            mainLabels = window.SERENDIPITY_LABELS_DATA || [];
            break;
    }
    const contextLabels = window.CONTEXT_LABELS_DATA || [];

    drawLabels(gMain, mainLabels, baseScale);
    drawLabels(gContext, contextLabels, baseScale);

    const updateLabelSizes = () => {
        const container = wrapper.parentElement;
        if (!container || container.clientWidth === 0 || !window.EMBEDDING_BOUNDS) {
            return;
        }

        const TARGET_SCREEN_SIZE_PX = 17; 
        const CONVERGENCE_SPEED = 0.2;

        const containerWidth = container.clientWidth;
        const wrapperWidth = wrapper.getBoundingClientRect().width;
        const currentZoom = wrapperWidth / containerWidth;

        const targetBaseSize = (TARGET_SCREEN_SIZE_PX / containerWidth) * dataWidth;
        const convergenceFactor = 1 - Math.exp(-(currentZoom - 1) * CONVERGENCE_SPEED);

        const labelGroups = svg.querySelectorAll('.label-bundle');
        labelGroups.forEach(group => {
            const originalBaseSize = parseFloat(group.dataset.baseSize);
            if (isNaN(originalBaseSize)) return;

            const targetSizeNow = targetBaseSize / currentZoom;
            const finalSize = originalBaseSize * (1 - convergenceFactor) + targetSizeNow * convergenceFactor;
            
            group.querySelectorAll('text').forEach(textElement => {
                textElement.setAttribute('font-size', finalSize);
            });
        });
    };

    const zoomObserver = new MutationObserver(updateLabelSizes);
    zoomObserver.observe(wrapper, { attributes: true, attributeFilter: ['style'] });
    
    setTimeout(updateLabelSizes, 0);
}