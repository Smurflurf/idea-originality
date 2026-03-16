import { getContext, getJobTitle } from '/script/core/context.js';

/**
 * Hilfsfunktion, um den Wert einer CSS-Variable aus dem :root-Element zu lesen.
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
 */
function drawCleanedSvgToCanvas(ctx, svgElement, width, height, onScreenWidth, pixelRatio = 1) {
    return new Promise((resolve) => {
        const svgClone = svgElement.cloneNode(true);
        const renderWidth = width * pixelRatio;
        const renderHeight = height * pixelRatio;
        
        const resolutionRatio = (onScreenWidth > 0) ? (width / onScreenWidth) : 1;

        const viewBox = svgClone.getAttribute('viewBox');
        if (viewBox) {
            const vbVals = viewBox.split(/[\s,]+/).map(parseFloat);
            if (vbVals.length === 4) {
                svgClone.setAttribute('width', Math.ceil(vbVals[2]));
                svgClone.setAttribute('height', Math.ceil(vbVals[3]));
            } else {
                svgClone.setAttribute('width', renderWidth);
                svgClone.setAttribute('height', renderHeight);
            }
        } else {
            svgClone.setAttribute('width', renderWidth);
            svgClone.setAttribute('height', renderHeight);
        }
        
		svgClone.setAttribute('preserveAspectRatio', 'none');

		svgClone.querySelectorAll(':scope > g').forEach(clonedGroup => {
			const originalGroup = document.getElementById(clonedGroup.id);
			if (originalGroup && originalGroup.style.display === 'none') {
				clonedGroup.remove();
			}
		});

		const ctxData = getContext();
		const originalPaths = svgElement.querySelectorAll('.outline-path');
		const clonedPaths = svgClone.querySelectorAll('.outline-path');

		if (originalPaths.length > 0 && clonedPaths.length > 0 && ctxData.embeddingBounds && onScreenWidth > 0) {
			const currentZoom = width / onScreenWidth;
			const TARGET_PIXEL_WIDTH = 1.0 * Math.sqrt(currentZoom);

			const bounds = ctxData.embeddingBounds;
			const dataWidth = bounds.xmax - bounds.xmin;

			const finalStrokeWidthInDataCoords = (TARGET_PIXEL_WIDTH * dataWidth) / width;

			clonedPaths.forEach(path => {
				path.setAttribute('stroke-width', finalStrokeWidthInDataCoords);
				path.style.strokeWidth = '';
				path.removeAttribute('vector-effect');
				path.setAttribute('stroke-linejoin', 'round');
				path.setAttribute('stroke-linecap', 'round');
			});
		}
        
        const svgString = new XMLSerializer().serializeToString(svgClone);
        const svgBlob = new Blob([svgString], { type: 'image/svg+xml;charset=utf-8' });
        const url = URL.createObjectURL(svgBlob);
        const img = new Image();
        img.onload = () => { ctx.drawImage(img, 0, 0, width, height); URL.revokeObjectURL(url); resolve(); };
        img.onerror = () => { URL.revokeObjectURL(url); resolve(); };
        img.src = url;
    });
}

/**
 * NATIVE CANVAS TEXT RENDERER
 */
function drawLabelsNativeToCanvas(ctx, layerIdPrefix, drawWidth, drawHeight, forceBaseFontSize = false) {
    const suffix = layerIdPrefix.replace('viz-layer-', '');
    const svgLayer = document.getElementById(`viz-layer-${suffix}-labels-svg`);
	if (!svgLayer || svgLayer.style.display === 'none') return;

    const bodyFontFamily = window.getComputedStyle(document.body).fontFamily || "sans-serif";

    ctx.save();

    const viewBox = svgLayer.getAttribute('viewBox');
    if (viewBox) {
        const vb = viewBox.split(/[\s,]+/).map(parseFloat);
        if(vb[2] > 0 && vb[3] > 0) {
            const scaleX = drawWidth / vb[2];
            const scaleY = drawHeight / vb[3];
            ctx.scale(scaleX, scaleY);
        }
    }

    const applyTransform = (transformStr) => {
        if (!transformStr) return;
        const regex = /([a-zA-Z]+)\(([^)]+)\)/g;
        let match;
        while ((match = regex.exec(transformStr)) !== null) {
            const type = match[1];
            const args = match[2].split(/[\s,]+/).map(parseFloat);
            if (type === 'scale') {
                ctx.scale(args[0], args.length > 1 ? args[1] : args[0]);
            } else if (type === 'translate') {
                ctx.translate(args[0], args.length > 1 ? args[1] : 0);
            }
        }
    };

    const drawGroup = (groupId) => {
        const groupEl = document.getElementById(groupId);
        if (!groupEl || groupEl.style.display === 'none') return;
		        
        ctx.save();
        applyTransform(groupEl.getAttribute('transform'));

        const bundles = groupEl.querySelectorAll('.label-bundle');
        bundles.forEach(bundle => {
            ctx.save();
            applyTransform(bundle.getAttribute('transform'));

            const currentMatrix = ctx.getTransform();
            const scaleX = Math.abs(currentMatrix.a);
            const scaleY = Math.abs(currentMatrix.d);
            
            if (scaleX <= 0 || scaleY <= 0) {
                ctx.restore();
                return;
            }

            const texts = bundle.querySelectorAll('text');
            texts.forEach(textEl => {
                const fill = textEl.getAttribute('fill') || '#000000';
                const stroke = textEl.getAttribute('stroke');
                const strokeWidth = textEl.getAttribute('stroke-width');
                const textAnchor = textEl.getAttribute('text-anchor');
                
                const bundleBaseSize = parseFloat(bundle.dataset.baseSize);
				let fontSize;
				if (forceBaseFontSize) {
					fontSize = bundleBaseSize || 12;
				} else {
					fontSize = parseFloat(textEl.getAttribute('font-size')) || bundleBaseSize || 12;
				}
                if (fontSize <= 0) return;
                
                let rawFont = textEl.getAttribute('font-family');
                if (!rawFont || rawFont === 'null' || rawFont === 'undefined') {
                    rawFont = bodyFontFamily;
                }
                let cleanFont = rawFont.replace(/['"]/g, '');
                if (!cleanFont.includes('sans-serif')) {
                    cleanFont += ', sans-serif';
                }

                const fontWeight = textEl.getAttribute('font-weight') || 'normal';

                const screenPxSize = fontSize * scaleY;
                let renderPxSize = screenPxSize;
                let scaleCorrection = 1;
                
                if (renderPxSize < 1) {
                    scaleCorrection = renderPxSize;
                    renderPxSize = 1;
                } else if (renderPxSize > 250) {
                    scaleCorrection = renderPxSize / 250;
                    renderPxSize = 250;
                }

                const TEXT_MULT_X = scaleX / scaleCorrection;
                const TEXT_MULT_Y = scaleY / scaleCorrection;

                ctx.save();
                ctx.scale(1 / TEXT_MULT_X, 1 / TEXT_MULT_Y);

                ctx.font = `${fontWeight} ${renderPxSize}px ${cleanFont}`;

                if (textAnchor === 'middle') ctx.textAlign = 'center';
                else if (textAnchor === 'start') ctx.textAlign = 'left';
                else if (textAnchor === 'end') ctx.textAlign = 'right';

                ctx.textBaseline = 'middle';

                let strokeW = 0;
                if (strokeWidth && strokeWidth.endsWith('em')) {
                    strokeW = parseFloat(strokeWidth) * fontSize;
                } else if (strokeWidth) {
                    strokeW = parseFloat(strokeWidth);
                }

                let currentYOffset = 0;
                const tspans = textEl.querySelectorAll('tspan');
                
                tspans.forEach((tspan) => {
                    const lineText = tspan.textContent;
                    const xStr = tspan.getAttribute('x');
                    const x = xStr !== null ? parseFloat(xStr) : 0;
                    const dyStr = tspan.getAttribute('dy');
                    
                    let dy = 0;
                    if (dyStr && dyStr.endsWith('em')) dy = parseFloat(dyStr) * fontSize;
                    else if (dyStr) dy = parseFloat(dyStr);
                    
					currentYOffset += dy;

					const targetX = x * TEXT_MULT_X;
					const yCorrection = fontSize * 0.04;
					let targetY = (currentYOffset + yCorrection) * TEXT_MULT_Y;

					const strokeMult = (TEXT_MULT_X + TEXT_MULT_Y) / 2;
					const finalStrokeW = strokeW * strokeMult * 0.95;

					if (fill && fill !== 'none' && fill !== 'transparent') {
                        ctx.fillStyle = fill;
                        ctx.fillText(lineText, targetX, targetY);
                    }
                    
                    if (stroke && finalStrokeW > 0) {
                        ctx.lineJoin = 'round';
                        ctx.lineCap = 'round'; 
                        ctx.miterLimit = 2; 
                        ctx.strokeStyle = stroke;
                        ctx.lineWidth = finalStrokeW;
                        ctx.strokeText(lineText, targetX, targetY);
                    }
                });
                
                ctx.restore();
            });
            ctx.restore();
        });
        ctx.restore();
    };

    drawGroup(`g-labels-${suffix}-main`);
    drawGroup(`g-labels-${suffix}-context`);

    ctx.restore();
}

/**
 * Kernfunktion: Erstellt ein Canvas-Bild aus den sichtbaren Layern.
 * PERFORMANCE FIX: Liefert für Snapshots direkt den Canvas zurück (ohne CPU-heavy WebP Encoding).
 */
export async function generateVisualizationImage(layerIdPrefix, pointsContainerId, pointsData, colorMap, captureVisibleOnly = false) {
	const baseLayer = document.getElementById(`${layerIdPrefix}-base`);
	if (!baseLayer || !baseLayer.complete || baseLayer.naturalWidth === 0) {
		return new Promise((resolve, reject) => {
			if (!baseLayer) {
				// Wenn das Element gar nicht im DOM ist, sofort abbrechen.
				console.error("Basis-Visualisierung für Export nicht gefunden oder nicht geladen!");
				return resolve(null); // Gibt null zurück, anstatt einen Fehler zu werfen.
			}

			// Wenn es da ist, aber noch lädt, hängen wir uns an das 'load'-Event.
			const onLoad = () => {
				// Sobald es geladen ist, rufen wir die Funktion einfach nochmal auf.
				// Dieses Mal wird sie erfolgreich sein.
				generateVisualizationImage(layerIdPrefix, pointsContainerId, pointsData, colorMap, captureVisibleOnly)
					.then(resolve)
					.catch(reject);
			};
			const onError = () => {
				console.error(`Bild-Layer ${layerIdPrefix}-base konnte nicht geladen werden.`);
				resolve(null);
			};

			baseLayer.addEventListener('load', onLoad, { once: true });
			baseLayer.addEventListener('error', onError, { once: true });
		});
	}
    
	// Falls wir kein WebP mehr machen, können wir für Snapshots den Canvas-Hintergrund deckend machen
	// Das erlaubt uns den context mit 'alpha: false' zu erstellen (Hardware-Boost in vielen Browsern)
	const canvasOptions = captureVisibleOnly ? { alpha: false } : {};
	const canvas = document.createElement('canvas');
	const ctx = canvas.getContext('2d', canvasOptions);
	
    const vizContainer = baseLayer.closest('.viz-stack-container');
    const wrapper = vizContainer ? vizContainer.querySelector('.zoom-pan-wrapper') : null;

    let outputWidth = baseLayer.naturalWidth;
    let outputHeight = baseLayer.naturalHeight;
    let tx = 0, ty = 0;
    let wWidth = outputWidth, wHeight = outputHeight;
    let pixelRatio = 1;
    let screenCw = outputWidth; 

    if (captureVisibleOnly && vizContainer && wrapper) {
        const exactCw = parseFloat(vizContainer.dataset.exactCw) || vizContainer.clientWidth;
        const exactCh = parseFloat(vizContainer.dataset.exactCh) || vizContainer.clientHeight;

		if (exactCw === 0 || isNaN(exactCw)) {
			const activeContainer = document.querySelector('.viz-content-pane.active .viz-stack-container');
			if (activeContainer) {
				exactCw = parseFloat(activeContainer.dataset.exactCw) || activeContainer.clientWidth;
				exactCh = parseFloat(activeContainer.dataset.exactCh) || activeContainer.clientHeight;
			}
		}
		
        outputWidth = exactCw;
        outputHeight = exactCh;
        
        tx = parseFloat(wrapper.dataset.exactLeft) || 0;
		ty = parseFloat(wrapper.dataset.exactTop) || 0;
		wWidth = parseFloat(wrapper.dataset.exactWidth) || exactCw;
		wHeight = parseFloat(wrapper.dataset.exactHeight) || exactCh;

		const dpr = window.devicePixelRatio || 1;
		pixelRatio = Math.min(dpr, 2.0); 

		// BUGFIX 2: Exakt wie der Render -> Native Bildschirmauflösung (DPR) nutzen statt komprimiertem 1.25x
		//pixelRatio = window.devicePixelRatio || 1;
				
		screenCw = exactCw;
    } else if (vizContainer) {
        // Full Export (Download)
        screenCw = parseFloat(vizContainer.dataset.exactCw) || vizContainer.clientWidth;
		// BUGFIX 1: Normale x1 Bildgröße nutzen (statt x2.5), um gigantische 24MB-Dateien und Lags zu vermeiden
		pixelRatio = 1;
    }

    canvas.width = Math.round(outputWidth * pixelRatio);
    canvas.height = Math.round(outputHeight * pixelRatio);
    
	ctx.imageSmoothingEnabled = true;
	//ctx.imageSmoothingQuality = 'high'; 
	
    const bgColor = getCssVariableValue('--bg-deep') || '#0e0e0f';
    ctx.fillStyle = bgColor;
    ctx.fillRect(0, 0, canvas.width, canvas.height);

	ctx.save();

	if (captureVisibleOnly) {
		ctx.scale(pixelRatio, pixelRatio);
		ctx.translate(tx, ty);
	} else {
		ctx.scale(pixelRatio, pixelRatio);
	}
	
	const imageLayerIds = [`${layerIdPrefix}-base`, `${layerIdPrefix}-points`];

	const bitmapPromises = imageLayerIds.map(async id => {
		const layer = document.getElementById(id);
        if (layer && layer.complete && layer.style.display !== 'none') {
			try {
				return await createImageBitmap(layer);
			} catch (err) {
				return layer; 
			}
		}
		return null;
	});

	const bitmaps = await Promise.all(bitmapPromises);
	bitmaps.forEach(bmp => {
		if (bmp) {
			ctx.drawImage(bmp, 0, 0, wWidth, wHeight);
			if (bmp.close) bmp.close(); 
		}
	});

	if (captureVisibleOnly && ctx.outlineData) {
		const mainClusterIds = Object.keys(colorMap);
		const dataWidth = ctx.embeddingBounds.xmax - ctx.embeddingBounds.xmin;
		const currentZoom = wWidth / screenCw;
		const TARGET_PIXEL_WIDTH = 1.0 * Math.sqrt(currentZoom);
		const strokeW = (TARGET_PIXEL_WIDTH * dataWidth) / wWidth;

		ctx.save();
		const aspect = (ctx.embeddingBounds.ymax - ctx.embeddingBounds.ymin) / dataWidth;
		const imgW = 2000;
		ctx.scale(wWidth / imgW, wHeight / (imgW * aspect));
		ctx.scale(imgW / dataWidth, -(imgW * aspect) / (ctx.embeddingBounds.ymax - ctx.embeddingBounds.ymin));
		ctx.translate(-ctx.embeddingBounds.xmin, -ctx.embeddingBounds.ymax);

		ctx.lineWidth = strokeW;
		ctx.lineJoin = 'round';
		ctx.lineCap = 'round';

		for (const id of mainClusterIds) {
			if (ctx.outlineData[id]) {
				ctx.strokeStyle = colorMap[id] || '#FFFFFF';
				const path = new Path2D(ctx.outlineData[id]);
				ctx.stroke(path);
			}
		}
		ctx.restore();
	} else {
		const outlinesSvg = document.getElementById(`${layerIdPrefix}-outlines-svg`);
		await drawCleanedSvgToCanvas(ctx, outlinesSvg, wWidth, wHeight, screenCw, pixelRatio);
	}

	drawLabelsNativeToCanvas(ctx, layerIdPrefix, wWidth, wHeight, !captureVisibleOnly);

    const pointsContainer = document.getElementById(pointsContainerId);
    const isPointsContainerVisible = pointsContainer && pointsContainer.style.display !== 'none';
	
    if (isPointsContainerVisible && pointsData && pointsData.length > 0) {
        const basePointSize = 14; 
        const baseStrokeWidth = 2.0;

        const blackRgb = getCssVariableValue('--bg-black-rgb') || '0, 0, 0';
        const textPrimaryRgb = getCssVariableValue('--text-primary-rgb') || '255, 255, 255';
        
        let resolutionScaleFactor = captureVisibleOnly ? 1 : (wWidth / screenCw);

        pointsData.forEach(paper => {
            if (paper.relativeX === undefined || paper.relativeY === undefined) return;
            
            let screenX = paper.relativeX * wWidth;
            let screenY = paper.relativeY * wHeight;

            if (captureVisibleOnly) {
                let absX = tx + screenX;
                let absY = ty + screenY;
                if (absX < -20 || absX > outputWidth + 20 || absY < -20 || absY > outputHeight + 20) return;
            }

            const finalSize = basePointSize * resolutionScaleFactor;
            const finalStroke = baseStrokeWidth * resolutionScaleFactor;
            
            const outerBorderRadius = finalSize / 2;
            const strokeW = finalStroke;
            const coreRadius = Math.max(0, outerBorderRadius - strokeW);
            const shadowRadius = outerBorderRadius + strokeW;

            const color = colorMap[paper.clusterId] || '#bdc1c6';
            
            ctx.fillStyle = `rgba(${textPrimaryRgb}, 0.8)`;
            ctx.beginPath(); ctx.arc(screenX, screenY, shadowRadius, 0, Math.PI * 2); ctx.fill();

            ctx.fillStyle = `rgba(${blackRgb}, 0.9)`;
            ctx.beginPath(); ctx.arc(screenX, screenY, outerBorderRadius, 0, Math.PI * 2); ctx.fill();

            ctx.fillStyle = color;
            ctx.beginPath(); ctx.arc(screenX, screenY, coreRadius, 0, Math.PI * 2); ctx.fill();
        });
    }

	ctx.restore(); 

	ctx.save();
	ctx.scale(pixelRatio, pixelRatio);

	const ctxData = getContext();
	const crosshairCanvasElement = document.getElementById(`${layerIdPrefix}-crosshair-canvas`);

	if (crosshairCanvasElement && crosshairCanvasElement.style.display !== 'none' && ctxData.crosshairCoords) {
		const coords = ctxData.crosshairCoords;
		let leftOffset = captureVisibleOnly ? tx : 0;
		let topOffset = captureVisibleOnly ? ty : 0;

		let logicalWidth = captureVisibleOnly ? wWidth : outputWidth;
		let logicalHeight = captureVisibleOnly ? wHeight : outputHeight;

		let finalX = leftOffset + (coords.x * logicalWidth);
		let finalY = topOffset + (coords.y * logicalHeight);

		let resolutionScaleFactor = captureVisibleOnly ? 1 : (outputWidth / screenCw);
		let cLineWidth = 2 * resolutionScaleFactor;

		const crosshairColor = getCssVariableValue('--color-crosshair') || 'rgba(255, 255, 0, 0.7)';
		ctx.strokeStyle = crosshairColor;
		ctx.lineWidth = cLineWidth;

		const dashLen = 10 * resolutionScaleFactor;
		const gapLen = 8 * resolutionScaleFactor;
		const patternLength = dashLen + gapLen;
		ctx.setLineDash([dashLen, gapLen]);

		ctx.beginPath();
		ctx.lineDashOffset = -(finalX % patternLength);
		ctx.moveTo(0, finalY);
		ctx.lineTo(logicalWidth, finalY); 
		ctx.stroke();

		ctx.beginPath();
		ctx.lineDashOffset = -(finalY % patternLength);
		ctx.moveTo(finalX, 0);
		ctx.lineTo(finalX, logicalHeight); 
		ctx.stroke();

		ctx.setLineDash([]);
		ctx.lineDashOffset = 0;
	}

	ctx.restore();

	if (captureVisibleOnly) {
        // PERFORMANCE FIX 2: KEIN WebP KOMPRIMIEREN!
        // Wir geben einfach das fertige Canvas-Element direkt zurück. Null CPU Overhead!
		return Promise.resolve(canvas);
	}

	return canvas.toDataURL('image/png');
}

function sanitizeForFilename(title) {
    const untitled = (typeof t === 'function') ? t('global.untitled_analysis') : 'untitled_analysis';
    if (!title || title.trim() === '') return untitled; 
    return title
        .trim()
        .toLowerCase()
        .replace(/\s+/g, '-') 
        .replace(/[^\w-]/g, '') 
        .substring(0, 50);
}

export function initializeExportButtons() {
    function setupExportButton(buttonId, layerIdPrefix, pointsContainerId, contextPrefix) {
        const exportBtn = document.getElementById(buttonId);
        if (!exportBtn) return;

        exportBtn.addEventListener('click', async () => {
            const ctxData = getContext();
            let pointsData = [];
            let colorMap = {};

            const pContainer = document.getElementById(pointsContainerId);
            if (pContainer && pContainer.__pointsDataCache) {
                pointsData = pContainer.__pointsDataCache;
            } else if (contextPrefix === 'own') {
                pointsData = ctxData.ownResults || [];
            } else if (contextPrefix === 'serendipity') {
                pointsData = ctxData.serendipityResults || [];
            }

            if (contextPrefix === 'own') {
                colorMap = ctxData.ownColorMap || {};
            } else if (contextPrefix === 'serendipity') {
                colorMap = ctxData.serendipityColorMap || {};
            } else if (contextPrefix === 'nc') {
                colorMap = ctxData.neighborColorMap || {};
            }

            const finalImageURL = await generateVisualizationImage(
                layerIdPrefix, pointsContainerId, pointsData, colorMap, false
            );
            
            if (finalImageURL && typeof finalImageURL === 'string') {
                const link = document.createElement('a');
                link.href = finalImageURL;
                const sanitizedTitle = sanitizeForFilename(getJobTitle());
                const suffix = layerIdPrefix.replace('viz-layer-', '');
                link.download = `visualization-${sanitizedTitle}-${suffix}.png`;

                document.body.appendChild(link);
                link.click();
                document.body.removeChild(link);
            }
        });
    }

    setupExportButton('export-viz-btn-own', 'viz-layer-own', 'dynamic-points-own-neighbors', 'own');
    setupExportButton('export-viz-btn-nc', 'viz-layer-nc', 'dynamic-points-nc-neighbors', 'nc');
    setupExportButton('export-viz-btn-serendipity', 'viz-layer-serendipity', 'dynamic-points-serendipity-neighbors', 'serendipity');
}