import { getContext } from '/script/core/context.js';

/**
 * Zeichnet eine einzelne Fadenkreuz-Instanz.
 */
function drawCrosshair(canvasId, wrapperId, crosshairCoords) {
    const canvas = document.getElementById(canvasId);
    const wrapper = document.getElementById(wrapperId);

    if (!canvas || !wrapper || !crosshairCoords) return;

    const ctx = canvas.getContext('2d');
    
    const container = canvas.parentElement;
    if (container.clientWidth === 0) return;
    
    canvas.width = container.clientWidth;
    canvas.height = container.clientHeight;

    const wrapperRect = wrapper.getBoundingClientRect();
    const containerRect = container.getBoundingClientRect();

    const crosshairX_on_image = crosshairCoords.x * wrapperRect.width;
    const crosshairY_on_image = crosshairCoords.y * wrapperRect.height;
    
    const finalX = (wrapperRect.left - containerRect.left) + crosshairX_on_image;
    const finalY = (wrapperRect.top - containerRect.top) + crosshairY_on_image;
    
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    // WICHTIG: Wir holen die Farbe vom <html> Element (documentElement), da dort das Theme-Attribut sitzt.
    // Das ist zuverlässiger als document.body.
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


/**
 * Initialisiert eine einzelne Fadenkreuz-Instanz mit allen Beobachtern.
 */
function setupSingleCrosshair(prefix, crosshairCoords) {
    const canvasId = `viz-layer-${prefix}-crosshair-canvas`;
    const toggleButtonId = `viz-toggle-${prefix}-crosshair`;
    const wrapperId = `zoom-pan-wrapper-${prefix}`;
    
    const canvas = document.getElementById(canvasId);
    const toggleButton = document.getElementById(toggleButtonId);
    const wrapper = document.getElementById(wrapperId);
    const vizPane = canvas ? canvas.closest('.viz-content-pane') : null;

    if (!canvas || !toggleButton || !wrapper || !crosshairCoords || !vizPane) {
        if (toggleButton) toggleButton.style.display = 'none';
        return;
	}

	const redraw = () => {
		// FIX: Nicht nur auf .active prüfen, sondern generell auf Sichtbarkeit.
		// offsetParent ist null, wenn das Element display:none hat.
		// Beim Swipen ist display:block gesetzt, also ist offsetParent vorhanden.
		const isVisible = vizPane.offsetParent !== null;

		if (isVisible && canvas.style.display !== 'none') {
			drawCrosshair(canvasId, wrapperId, crosshairCoords);
		}
	};
    
    redraw();

    toggleButton.addEventListener('click', () => {
        setTimeout(() => {
            canvas.style.display = toggleButton.classList.contains('active') ? 'block' : 'none';
            redraw(); // Sofort neu zeichnen beim Einschalten
        }, 0);
    });
    
    // Observer 1: Zoom/Pan Änderungen
	const wrapperObserver = new MutationObserver(redraw);
	wrapperObserver.observe(wrapper, { attributes: true, attributeFilter: ['style'] });

	// Observer 2: Tab-Wechsel (z.B. von Own -> Neighbor)
	const paneObserver = new MutationObserver(() => {
		redraw();
	});
	paneObserver.observe(vizPane, { attributes: true, attributeFilter: ['class', 'style'] });

    // Observer 3: THEME WECHSEL (Das hat gefehlt!)
    // Wir beobachten das <html> Element auf Attribut-Änderungen (data-theme)
    const themeObserver = new MutationObserver(() => {
        // Kurze Verzögerung, damit CSS-Variablen sicher aktualisiert sind
        requestAnimationFrame(redraw);
    });
    themeObserver.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
}


export function initializeAllCrosshairs() {
	const ctx = getContext();
	const crosshairCoords = ctx.crosshairCoords;

	if (crosshairCoords) {
		setupSingleCrosshair('own', crosshairCoords);
		setupSingleCrosshair('nc', crosshairCoords);
		setupSingleCrosshair('serendipity', crosshairCoords);
	} else {
		const btnOwn = document.getElementById('viz-toggle-own-crosshair');
		if (btnOwn) btnOwn.style.display = 'none';

		const btnNc = document.getElementById('viz-toggle-nc-crosshair');
		if (btnNc) btnNc.style.display = 'none';

		const btnSerendipity = document.getElementById('viz-toggle-serendipity-crosshair');
		if (btnSerendipity) btnSerendipity.style.display = 'none';
	}
}