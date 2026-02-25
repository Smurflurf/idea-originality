import { getContext } from '/script/core/context.js';
import { registerCleanup } from '/script/core/lifecycleManager.js';
import { on, off } from '/script/core/eventBus.js';

function drawCrosshairFast(canvas, left, top, width, height, crosshairCoords) {
    const ctx = canvas.getContext('2d');
    
    const finalX = left + (crosshairCoords.x * width);
    const finalY = top + (crosshairCoords.y * height);
    
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    const crosshairColor = getComputedStyle(document.documentElement).getPropertyValue('--color-crosshair').trim() || 'rgba(255, 255, 0, 0.7)';
    
    ctx.strokeStyle = crosshairColor;
    ctx.lineWidth = 2;
    
    const dashPattern = [10, 8];
    const patternLength = dashPattern[0] + dashPattern[1];
    ctx.setLineDash(dashPattern);
    
    const offsetX = finalX % patternLength;
    ctx.lineDashOffset = -offsetX;
    ctx.beginPath();
    ctx.moveTo(0, finalY);
    ctx.lineTo(canvas.width, finalY);
    ctx.stroke();

    const offsetY = finalY % patternLength;
    ctx.lineDashOffset = -offsetY;
    ctx.beginPath();
    ctx.moveTo(finalX, 0);
    ctx.lineTo(finalX, canvas.height);
    ctx.stroke();
}

function setupSingleCrosshair(prefix, crosshairCoords) {
    const canvasId = `viz-layer-${prefix}-crosshair-canvas`;
    const containerId = `viz-stack-container-${prefix}`;
    
    const canvas = document.getElementById(canvasId);
    const container = document.getElementById(containerId);
    const vizPane = canvas ? canvas.closest('.viz-content-pane') : null;

    if (!canvas || !container || !crosshairCoords) return;

    const onTransformUpdate = (detail) => {
        if (!detail || detail.prefix !== prefix) return;

        if (canvas.style.display !== 'none') {
            const { left, top, width, height, containerWidth, containerHeight } = detail;
            
            if (canvas.width !== containerWidth || canvas.height !== containerHeight) {
                canvas.width = containerWidth;
                canvas.height = containerHeight;
            }
            
            drawCrosshairFast(canvas, left, top, width, height, crosshairCoords);
        }
    };

    on('viz-transform', onTransformUpdate);

    const canvasDisplayObserver = new MutationObserver((mutations) => {
        mutations.forEach((mutation) => {
            if (mutation.attributeName === 'style' && canvas.style.display !== 'none') {
                const event = new Event('resize');
                window.dispatchEvent(event);
            }
        });
    });
    canvasDisplayObserver.observe(canvas, { attributes: true, attributeFilter: ['style'] });
    
    if (vizPane) {
        const paneObserver = new MutationObserver(() => {
            if (vizPane.offsetParent !== null) {
                const event = new Event('resize');
                window.dispatchEvent(event);
            }
        });
        paneObserver.observe(vizPane, { attributes: true, attributeFilter: ['class', 'style'] });
    }

	registerCleanup(() => {
        off('viz-transform', onTransformUpdate);
		canvasDisplayObserver.disconnect();
		if (typeof paneObserver !== 'undefined') paneObserver.disconnect();
	});
}

export function initializeAllCrosshairs() {
    const ctx = getContext();
    const crosshairCoords = ctx.crosshairCoords;

    if (crosshairCoords) {
        setupSingleCrosshair('own', crosshairCoords);
        setupSingleCrosshair('nc', crosshairCoords);
        setupSingleCrosshair('serendipity', crosshairCoords);
    } else {
        ['own', 'nc', 'serendipity'].forEach(prefix => {
            const btn = document.getElementById(`viz-toggle-${prefix}-crosshair`);
            if (btn) btn.style.display = 'none';
        });
    }
}