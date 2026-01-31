import { generateVisualizationImage } from '/script/viz/core/exportVisualization.js';

const ZOOM_FACTOR = 1.4;
const MIN_ZOOM = 1.0;
const MAX_ZOOM = 10.0;

const vizInstances = {};
let activeVizId = null; 

// WICHTIG: Cleanup-Funktionen
let activeCleanupFunctions = [];

// Globale Listener
window.addEventListener('click', (e) => {
    if (!e.target.closest('.viz-container-managed')) {
        Object.values(vizInstances).forEach(instance => {
            if (instance.setScrollZoomActive) instance.setScrollZoomActive(false);
        });
    }
});

window.addEventListener('keydown', (e) => {
    if (!activeVizId || !vizInstances[activeVizId]) return;
    const activeInstance = vizInstances[activeVizId];
    if (!activeInstance) return;
    if (e.key === '+' || e.key === '=') { e.preventDefault(); activeInstance.zoom(true); } 
    else if (e.key === '-') { e.preventDefault(); activeInstance.zoom(false); }
});

export function triggerPositionUpdateForViz(containerId) {
    if (vizInstances[containerId] && typeof vizInstances[containerId].update === 'function') {
        vizInstances[containerId].update();
    }
}

// NEU: Explizite Funktion zum Aufräumen aller Listener
export function disposeAllVisualizations() {
    if (activeCleanupFunctions.length > 0) {
        // console.log("[Zoom] Disposing all visualizations...");
        activeCleanupFunctions.forEach(fn => fn());
        activeCleanupFunctions = [];
    }
    // Instanzen auch leeren
    for (const key in vizInstances) delete vizInstances[key];
    activeVizId = null;
}

export function initializeZoomAndPan(containerId, wrapperId, zoomInId, zoomOutId, zoomTarget, pointsContainerId) {
    // HIER KEIN CLEANUP MEHR! 
    // Wir vertrauen darauf, dass main.js `disposeAllVisualizations` aufruft, BEVOR es anfängt zu initialisieren.
    // Das verhindert, dass init('own') die Listener von 'nc' löscht.

    const container = document.getElementById(containerId);
    const wrapper = document.getElementById(wrapperId);
    const zoomInBtn = document.getElementById(zoomInId);
    const zoomOutBtn = document.getElementById(zoomOutId);
    
    if (!container || !wrapper || !zoomInBtn || !zoomOutBtn) return;
    
    container.classList.add('viz-container-managed');

    let isScrollZoomActive = false;
    let longPressTimer = null;

    const idSuffix = containerId.includes('own') ? 'own' : (containerId.includes('nc') ? 'nc' : 'serendipity');
    const debugOverlay = document.getElementById(`debug-overlay-${idSuffix}`);
    const debugContainerSpan = document.getElementById(`debug-container-coords-${idSuffix}`);
    const debugWrapperSpan = document.getElementById(`debug-wrapper-coords-${idSuffix}`);
    const debugImageSpan = document.getElementById(`debug-image-coords-${idSuffix}`);
    if (debugOverlay) debugOverlay.style.display = 'none';

    let state = { zoom: MIN_ZOOM, centerX: zoomTarget ? zoomTarget.x : 0.5, centerY: zoomTarget ? zoomTarget.y : 0.5 };
    let hasUserPanned = false;
    let isPanning = false;
    let startPanX, startPanY, startTranslateX, startTranslateY;
    let lastMouseEvent = null;
    let initialPinchDistance = null;
    let zoomOnPinchStart = null;
    let initialPinchCenter = null; 
    let animation = { id: null, startTime: 0, duration: 0, startState: { ...state }, targetState: { ...state } };
    
    function getDistance(touches) {
        const [touch1, touch2] = touches;
        return Math.sqrt(Math.pow(touch2.clientX - touch1.clientX, 2) + Math.pow(touch2.clientY - touch1.clientY, 2));
    }
    function getMidpoint(touches) {
        const [touch1, touch2] = touches;
        return { x: (touch1.clientX + touch2.clientX) / 2, y: (touch1.clientY + touch2.clientY) / 2 };
    }

    function updateView() {
        const rect = container.getBoundingClientRect();
        if (rect.width === 0 || rect.height === 0) return;
        const { zoom, centerX, centerY } = state;
        const newWidth = rect.width * zoom;
        const newHeight = rect.height * zoom;
        let newLeft = (rect.width / 2) - (centerX * newWidth);
        let newTop = (rect.height / 2) - (centerY * newHeight);
        const minLeft = rect.width - newWidth, maxLeft = 0;
        const minTop = rect.height - newHeight, maxTop = 0;
        newLeft = Math.max(minLeft, Math.min(maxLeft, newLeft));
        newTop = Math.max(minTop, Math.min(maxTop, newTop));
        wrapper.style.width = `${newWidth}px`;
        wrapper.style.height = `${newHeight}px`;
        wrapper.style.left = `${newLeft}px`;
        wrapper.style.top = `${newTop}px`;
        updateDynamicPositions(newLeft, newTop, newWidth, newHeight);
        updateButtonStates();
        updateDebugInfo();
    }

    function updateDebugInfo(event = lastMouseEvent) {
        if (!debugOverlay || debugOverlay.style.display === 'none') return;
        const containerRect = container.getBoundingClientRect();
        const wrapperRect = wrapper.getBoundingClientRect();
        if (!event) {
            if (debugContainerSpan) debugContainerSpan.textContent = 'N/A';
            if (debugWrapperSpan) debugWrapperSpan.textContent = 'N/A';
            if (debugImageSpan) debugImageSpan.textContent = 'N/A';
            return;
        }
        if (debugContainerSpan) {
            const containerX = event.clientX - containerRect.left;
            const containerY = event.clientY - containerRect.top;
            debugContainerSpan.textContent = `x:${containerX.toFixed(0)}, y:${containerY.toFixed(0)}`;
        }
        if (debugWrapperSpan) {
            const wrapperX = event.clientX - wrapperRect.left;
            const wrapperY = event.clientY - wrapperRect.top;
            debugWrapperSpan.textContent = `x:${wrapperX.toFixed(0)}, y:${wrapperY.toFixed(0)}`;
        }
        if (debugImageSpan) {
            const imageX = (event.clientX - wrapperRect.left) / wrapperRect.width;
            const imageY = (event.clientY - wrapperRect.top) / wrapperRect.height;
            if (imageX >= 0 && imageX <= 1 && imageY >= 0 && imageY <= 1) debugImageSpan.textContent = `x:${imageX.toFixed(3)}, y:${imageY.toFixed(3)}`;
            else debugImageSpan.textContent = 'Outside';
        }
    }
    
    function updateDynamicPositions(wrapperLeft, wrapperTop, wrapperWidth, wrapperHeight) {
        const pointsContainer = document.getElementById(pointsContainerId);
        if (!pointsContainer || wrapperWidth <= 0) return;
        const hitboxes = pointsContainer.querySelectorAll('.point-hitbox');
        const hitboxSize = 30;
        hitboxes.forEach(hitbox => {
            const relX = parseFloat(hitbox.dataset.relativeX);
            const relY = parseFloat(hitbox.dataset.relativeY);
            hitbox.style.left = `${wrapperLeft + (relX * wrapperWidth) - (hitboxSize / 2)}px`;
            hitbox.style.top = `${wrapperTop + (relY * wrapperHeight) - (hitboxSize / 2)}px`;
        });
    }

    function animationLoop(currentTime) {
        if (animation.id === null) return;
        const elapsedTime = currentTime - animation.startTime;
        const progress = Math.min(elapsedTime / animation.duration, 1);
        const ease = 0.5 - 0.5 * Math.cos(progress * Math.PI);
        state.zoom = animation.startState.zoom + (animation.targetState.zoom - animation.startState.zoom) * ease;
        state.centerX = animation.startState.centerX + (animation.targetState.centerX - animation.startState.centerX) * ease;
        state.centerY = animation.startState.centerY + (animation.targetState.centerY - animation.startState.centerY) * ease;
        updateView();
        if (progress < 1) animation.id = requestAnimationFrame(animationLoop);
        else { state = { ...animation.targetState }; animation.id = null; updateView(); }
    }

	function startAnimation(targetState, duration) {
	    if (animation.id) cancelAnimationFrame(animation.id);
	    animation.startState = { ...state }; 
	    animation.targetState = targetState;
	    animation.duration = duration;
	    animation.startTime = performance.now();
	    if (duration === 0) { state = { ...targetState }; updateView(); animation.id = null; }
        else animation.id = requestAnimationFrame(animationLoop);
	}
    
    function zoom(isZoomingIn, clientX = null, clientY = null) {
	    const oldZoom = state.zoom;
	    const newZoom = Math.max(MIN_ZOOM, Math.min(MAX_ZOOM, oldZoom * (isZoomingIn ? ZOOM_FACTOR : 1 / ZOOM_FACTOR)));
	    if (newZoom === oldZoom) return;
        let zoomAnchor;
	    if (clientX !== null && clientY !== null) {
	        hasUserPanned = true;
	        const wrapperRect = wrapper.getBoundingClientRect();
	        zoomAnchor = wrapperRect.width > 0 ? { x: (clientX - wrapperRect.left) / wrapperRect.width, y: (clientY - wrapperRect.top) / wrapperRect.height } : { x: 0.5, y: 0.5 };
	    } else zoomAnchor = { x: state.centerX, y: state.centerY };
	    let targetCenterX = zoomAnchor.x - (zoomAnchor.x - state.centerX) * (oldZoom / newZoom);
	    let targetCenterY = zoomAnchor.y - (zoomAnchor.y - state.centerY) * (oldZoom / newZoom);
        if (newZoom <= MIN_ZOOM) {
	        hasUserPanned = false;
	        targetCenterX = zoomTarget ? zoomTarget.x : 0.5;
	        targetCenterY = zoomTarget ? zoomTarget.y : 0.5;
	    }
        startAnimation({ zoom: newZoom, centerX: targetCenterX, centerY: targetCenterY }, 150);
	}

    function centerOnIdea() { hasUserPanned = false; startAnimation({ zoom: state.zoom, centerX: zoomTarget?.x||0.5, centerY: zoomTarget?.y||0.5 }, 500); }
    function zoomToMin() { hasUserPanned = false; startAnimation({ zoom: MIN_ZOOM, centerX: zoomTarget?.x||0.5, centerY: zoomTarget?.y||0.5 }, 500); }
    function zoomToMax() { startAnimation({ zoom: MAX_ZOOM, centerX: state.centerX, centerY: state.centerY }, 500); }
    function setScrollZoomActive(isActive) {
        if (isScrollZoomActive === isActive) return;
        isScrollZoomActive = isActive;
        container.classList.toggle('is-scroll-zoom-active', isActive);
    }
    
    vizInstances[containerId] = { update: updateView, zoom: zoom, setScrollZoomActive: setScrollZoomActive };
    
    function updateButtonStates() {
        zoomInBtn.disabled = state.zoom >= MAX_ZOOM;
        zoomOutBtn.disabled = state.zoom <= MIN_ZOOM;
		if (!isPanning) container.style.cursor = state.zoom > MIN_ZOOM ? 'grab' : 'default';
	}

	async function saveImage() {
		// 1. Layer-Prefix bestimmen
		let layerPrefix = 'viz-layer-nc';
		if (idSuffix === 'own') layerPrefix = 'viz-layer-own';
		else if (idSuffix === 'serendipity') layerPrefix = 'viz-layer-serendipity';

		// 2. Datenpunkte bestimmen (Nur bei Own haben wir derzeit Result-Dots)
		const pointsData = (idSuffix === 'own' && typeof OWN_RESULTS_DATA !== 'undefined')
			? OWN_RESULTS_DATA
			: [];

		// 3. Farbkarte bestimmen
		let colorMap = {};
		if (idSuffix === 'own') {
			colorMap = typeof OWN_IDEA_COLOR_MAP !== 'undefined' ? OWN_IDEA_COLOR_MAP : {};
		} else if (idSuffix === 'serendipity') {
			colorMap = typeof SERENDIPITY_COLOR_MAP !== 'undefined' ? SERENDIPITY_COLOR_MAP : {};
		} else {
			// Fallback auf Neighbor Cluster (nc)
			colorMap = typeof NEIGHBOR_CLUSTER_COLOR_MAP !== 'undefined' ? NEIGHBOR_CLUSTER_COLOR_MAP : {};
		}

		const finalImageURL = await generateVisualizationImage(
			layerPrefix,
			pointsContainerId,
			pointsData,
			colorMap
		);

		if (finalImageURL) {
			const link = document.createElement('a');
			link.href = finalImageURL;

			let title = t('global.analysis');
			if (window.JOB_TITLE) title = window.JOB_TITLE.trim().toLowerCase().replace(/[^a-z0-9]/g, '-');
			link.download = `${t('global.visualization_prefix')}${title}-${idSuffix}.png`; 
			
			document.body.appendChild(link);
			link.click();
			document.body.removeChild(link);
		}
	}

    function showContextMenu(e) {
        e.preventDefault();
        const existingMenu = document.getElementById('custom-context-menu');
        if (existingMenu) existingMenu.remove();
        const menu = document.createElement('div');
        menu.id = 'custom-context-menu'; menu.className = 'custom-context-menu';
        const clientX = e.touches ? e.touches[0].clientX : e.clientX;
        const clientY = e.touches ? e.touches[0].clientY : e.clientY;
        menu.style.top = `${clientY}px`; menu.style.left = `${clientX}px`;
		const menuItems = [ { label: 'Center on Idea', action: centerOnIdea }, { label: 'Zoom out', action: zoomToMin }, { label: 'Zoom in', action: zoomToMax }, { type: 'separator' }, { label: 'Save Image as PNG', action: saveImage }, { type: 'separator' }, { label: 'Toggle Debug Info', action: () => { if (debugOverlay) { debugOverlay.style.display = debugOverlay.style.display === 'none' ? 'block' : 'none'; updateDebugInfo(); } } }];
        menuItems.forEach(item => {
            if (item.type === 'separator') { const separator = document.createElement('div'); separator.className = 'context-menu-separator'; menu.appendChild(separator); } 
            else { const menuItem = document.createElement('div'); menuItem.className = 'context-menu-item'; menuItem.textContent = item.label; menuItem.addEventListener('click', () => { item.action(); menu.remove(); }); menu.appendChild(menuItem); }
        });
        document.body.appendChild(menu);
        setTimeout(() => window.addEventListener('click', () => menu.remove(), { once: true }), 0);
    }
    
    container.addEventListener('contextmenu', showContextMenu);
    
	function handlePanning(e) {
	    if (isPanning) {
	        hasUserPanned = true;
	        const dx = e.clientX - startPanX; const dy = e.clientY - startPanY;
	        const newLeft = startTranslateX + dx; const newTop = startTranslateY + dy;
	        const rect = container.getBoundingClientRect();
	        const newWidth = rect.width * state.zoom; const newHeight = rect.height * state.zoom;
	        const newCenterX = ((rect.width / 2) - newLeft) / newWidth; const newCenterY = ((rect.height / 2) - newTop) / newHeight;
	        startAnimation({ zoom: state.zoom, centerX: newCenterX, centerY: newCenterY }, 0);
	    }
	}
    
    function handleActivation() {
        Object.entries(vizInstances).forEach(([id, instance]) => { if (id !== containerId && instance.setScrollZoomActive) instance.setScrollZoomActive(false); });
        setScrollZoomActive(true);
    }

    container.addEventListener('mousemove', (e) => { lastMouseEvent = e; updateDebugInfo(e); });
    container.addEventListener('mouseleave', () => { lastMouseEvent = null; updateDebugInfo(null); });

    const onWindowMouseMove = (e) => handlePanning(e);
    const onWindowMouseUp = () => { if (!isPanning) return; isPanning = false; container.style.cursor = 'grab'; };
    const onWindowTouchMove = (e) => {
		if (document.body.classList.contains('is-swiping-active')) return;
        if (!isPanning && !initialPinchDistance) return;
	    e.preventDefault();
	    if (isPanning && e.touches.length === 1) {
	        hasUserPanned = true;
	        const dx = e.touches[0].clientX - startPanX; const dy = e.touches[0].clientY - startPanY;
	        const newLeft = startTranslateX + dx; const newTop = startTranslateY + dy;
	        const rect = container.getBoundingClientRect();
	        const newWidth = rect.width * state.zoom; const newHeight = rect.height * state.zoom;
            const newCenterX = ((rect.width / 2) - newLeft) / newWidth; const newCenterY = ((rect.height / 2) - newTop) / newHeight;
            startAnimation({ zoom: state.zoom, centerX: newCenterX, centerY: newCenterY }, 0);
	    } else if (initialPinchDistance && e.touches.length === 2) {
	        hasUserPanned = true;
	        const currentPinchDistance = getDistance(e.touches);
	        const pinchRatio = currentPinchDistance / initialPinchDistance;
	        const newZoom = Math.max(MIN_ZOOM, Math.min(MAX_ZOOM, zoomOnPinchStart * pinchRatio));
            const wrapperRect = wrapper.getBoundingClientRect();
            const zoomAnchor = { x: (initialPinchCenter.x - wrapperRect.left) / wrapperRect.width, y: (initialPinchCenter.y - wrapperRect.top) / wrapperRect.height, };
            const targetCenterX = zoomAnchor.x - (zoomAnchor.x - state.centerX) * (state.zoom / newZoom);
            const targetCenterY = zoomAnchor.y - (zoomAnchor.y - state.centerY) * (state.zoom / newZoom);
            let newTargetState = { zoom: newZoom, centerX: targetCenterX, centerY: targetCenterY };
	        if (newZoom <= MIN_ZOOM) { hasUserPanned = false; newTargetState.centerX = zoomTarget ? zoomTarget.x : 0.5; newTargetState.centerY = zoomTarget ? zoomTarget.y : 0.5; }
	        startAnimation(newTargetState, 0);
	    }
    };

    window.addEventListener('mousemove', onWindowMouseMove);
    window.addEventListener('mouseup', onWindowMouseUp);
    window.addEventListener('touchmove', onWindowTouchMove, { passive: false });

    // Nur window listener aufräumen, wenn dispose() gerufen wird
    activeCleanupFunctions.push(() => {
        window.removeEventListener('mousemove', onWindowMouseMove);
        window.removeEventListener('mouseup', onWindowMouseUp);
        window.removeEventListener('touchmove', onWindowTouchMove);
        if (vizInstances[containerId]) delete vizInstances[containerId];
    });

    container.addEventListener('wheel', (e) => { if (!isScrollZoomActive) return; e.preventDefault(); zoom(e.deltaY < 0, e.clientX, e.clientY); }, { passive: false });
    container.addEventListener('mousedown', (e) => {
        handleActivation(); e.preventDefault(); if (state.zoom <= MIN_ZOOM) return;
        isPanning = true; startPanX = e.clientX; startPanY = e.clientY;
        const style = window.getComputedStyle(wrapper); startTranslateX = parseFloat(style.left) || 0; startTranslateY = parseFloat(style.top) || 0;
        container.style.cursor = 'grabbing';
    });

	container.addEventListener('mouseenter', () => { activeVizId = containerId; });
	container.addEventListener('mouseleave', () => { if (activeVizId === containerId) { activeVizId = null; } });

	container.addEventListener('touchstart', (e) => {
		// NEU: Wenn global geswipet wird -> Zoom ignorieren!
		if (document.body.classList.contains('is-swiping-active')) return;

		// Buttons und Punkte sollen immer funktionieren
		if (e.target.closest('.zoom-controls button') || e.target.closest('.point-hitbox')) return;

		const isMultiTouch = e.touches.length > 1;
		// Kleiner Puffer für Float-Ungenauigkeiten
		const isZoomedIn = state.zoom > MIN_ZOOM + 0.01;

		// LOGIK-ÄNDERUNG:
		// Wenn wir ausgezoomt sind (Standardansicht) UND nur einen Finger nutzen:
		// -> Abbrechen! Wir machen NICHTS.
		// -> Kein preventDefault(), kein handleActivation().
		// -> Das Event darf zum Browser (Scrollen) oder zur swipeNavigation.js durchdringen.
		if (!isMultiTouch && !isZoomedIn) {
			return;
		}

		// Ab hier übernehmen wir die Kontrolle (Zoomed in oder Pinch)
		handleActivation();

		// Verhindert, dass der erste Finger eines Pinches das Swipe-Skript am Body triggert.
		e.stopPropagation(); 
		
		// Browser-Scrollen verhindern, da wir jetzt pannen oder zoomen
		if (e.cancelable) e.preventDefault();

		if (!isMultiTouch && isZoomedIn) {
			// Panning starten (nur wenn eingezoomt)
			isPanning = true;
			startPanX = e.touches[0].clientX;
			startPanY = e.touches[0].clientY;
			const style = window.getComputedStyle(wrapper);
			startTranslateX = parseFloat(style.left) || 0;
			startTranslateY = parseFloat(style.top) || 0;
		} else if (isMultiTouch) {
			// Pinch-Zoom starten
			isPanning = false;
			hasUserPanned = true;
			initialPinchDistance = getDistance(e.touches);
			initialPinchCenter = getMidpoint(e.touches);
			zoomOnPinchStart = state.zoom;
		}

		clearTimeout(longPressTimer);
		longPressTimer = setTimeout(() => { if (e.touches.length === 1) showContextMenu(e); }, 800);
	}, { passive: false });

	container.addEventListener('touchend', () => { clearTimeout(longPressTimer); isPanning = false; initialPinchDistance = null; });
	container.addEventListener('touchmove', () => { clearTimeout(longPressTimer); });

	const setupButton = (button, isZoomingIn) => {
		let startX = 0;
		let startY = 0;
		let isValidTap = false;
		// 1. TOUCH START: Position merken, aber Event durchlassen
		button.addEventListener('touchstart', (e) => {
			if (e.touches.length > 1) return;
			isValidTap = true;
			startX = e.touches[0].clientX;
			startY = e.touches[0].clientY;
			// WICHTIG: 
			// 1. Kein e.stopPropagation() -> Damit swipeNavigation.js den Start mitbekommt.
			// 2. Kein e.preventDefault() -> Damit der Browser weiß, dass hier Interaktion startet.
			// 3. KEIN handleActivation() hier -> Sonst blockiert die Klasse 'is-scroll-zoom-active'
			//    sofort das Swipe-Skript (falls du das im ignoreSelector hast).
		}, { passive: false });
		// 2. TOUCH END: Distanz prüfen
		button.addEventListener('touchend', (e) => {
			if (!isValidTap) return;
			const touch = e.changedTouches[0];
			// Euklidische Distanz berechnen
			const dist = Math.sqrt(
				Math.pow(touch.clientX - startX, 2) +
				Math.pow(touch.clientY - startY, 2)
			);
			// Toleranz für "Tap": 25 Pixel. 
			// Alles darüber werten wir als Swipe-Versuch und ignorieren den Zoom.
			if (dist < 25) {
				e.preventDefault(); // Verhindert, dass danach noch ein Maus-Klick gefeuert wird
				e.stopPropagation(); // Jetzt stoppen wir es, da wir gezoomt haben
				handleActivation(); // Jetzt aktivieren wir den View
				zoom(isZoomingIn);
			}
			isValidTap = false;
		});
		// 3. MAUS KLICK (Desktop Fallback)
		button.addEventListener('click', (e) => {
			// Auf Touch-Geräten wird dieses Event oft 300ms nach touchend gefeuert.
			// Da wir oben im touchend bei Erfolg e.preventDefault() rufen, 
			// kommt dieses Event bei einem echten Tap gar nicht mehr an.
			// Es feuert also nur noch bei echter Mausbedienung.
			e.preventDefault();
			e.stopPropagation();
			handleActivation();
			zoom(isZoomingIn);
		});
		// Verhindert, dass der Button beim Klicken den Fokus behält (optisch schöner)
		button.addEventListener('mousedown', (e) => e.preventDefault());
	};
    setupButton(zoomInBtn, true);
    setupButton(zoomOutBtn, false);
    updateView();
}