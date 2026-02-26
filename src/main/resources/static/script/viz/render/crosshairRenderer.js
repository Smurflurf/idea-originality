import { getContext } from '/script/core/context.js';
import { registerCleanup } from '/script/core/lifecycleManager.js';
import { on, off } from '/script/core/eventBus.js';

// Cache für die Farbe, um getComputedStyle im Loop zu vermeiden
let cachedCrosshairColor = null;

function drawCrosshairFast(canvas, left, top, width, height, crosshairCoords) {
    const ctx = canvas.getContext('2d');
    const finalX = left + (crosshairCoords.x * width);
    const finalY = top + (crosshairCoords.y * height);
    
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    // PERFORMANCE: Farbe nur einmalig lesen, nicht 60x pro Sekunde
    if (!cachedCrosshairColor) {
        cachedCrosshairColor = getComputedStyle(document.documentElement).getPropertyValue('--color-crosshair').trim() || 'rgba(255, 255, 0, 0.7)';
    }
    
    ctx.strokeStyle = cachedCrosshairColor;
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

    if (vizPane) {
        // PERFORMANCE FIX: Wir ignorieren 'transform'-Änderungen (Scrollen) komplett!
        let lastVisibleState = vizPane.style.display;
        let lastClassState = vizPane.className;

        const paneObserver = new MutationObserver(() => {
            const currentVisible = vizPane.style.display;
            const currentClass = vizPane.className;

            // NUR feuern, wenn sich die Sichtbarkeit oder die aktive Klasse ändert,
            // NICHT wenn sich nur das transform (Scroll-Position) ändert.
            if (currentVisible !== lastVisibleState || currentClass !== lastClassState) {
                lastVisibleState = currentVisible;
                lastClassState = currentClass;
                
                if (vizPane.offsetParent !== null) {
                    // Cache löschen falls Theme gewechselt wurde
                    cachedCrosshairColor = null; 
                    window.dispatchEvent(new Event('resize'));
                }
            }
        });
        // WICHTIG: attributeFilter schränkt ein, aber wir prüfen oben manuell auf 'transform'
        paneObserver.observe(vizPane, { attributes: true, attributeFilter: ['class', 'style'] });
        
        registerCleanup(() => paneObserver.disconnect());
    }

    registerCleanup(() => {
        off('viz-transform', onTransformUpdate);
    });
}

export function initializeAllCrosshairs() {
    const ctx = getContext();
    const crosshairCoords = ctx.crosshairCoords;
    cachedCrosshairColor = null;

    if (crosshairCoords) {
        setupSingleCrosshair('own', crosshairCoords);
        setupSingleCrosshair('nc', crosshairCoords);
        setupSingleCrosshair('serendipity', crosshairCoords);
    }
}