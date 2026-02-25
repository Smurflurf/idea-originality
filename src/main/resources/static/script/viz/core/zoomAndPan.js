import { generateVisualizationImage } from '/script/viz/core/exportVisualization.js';
import { registerCleanup } from '/script/core/lifecycleManager.js';
import { getContext, getJobTitle } from '/script/core/context.js';
import { emit } from '/script/core/eventBus.js';

// ==========================================
// ⚙️ ZOOM EINSTELLUNGEN
// ==========================================
export const ZOOM_CONFIG = {
	ANIMATION_SPEED: 0.32,  
	BUTTON_ZOOM_STEP: 1.6, 
	WHEEL_ZOOM_STEP: 1.15
};

const MIN_ZOOM = 1.0;
const MAX_ZOOM = 10.0;

const vizInstances = {};
let globallyActiveVizId = null;

let globalMouseX = 0;
let globalMouseY = 0;

window.addEventListener('mousemove', (e) => {
	globalMouseX = e.clientX;
	globalMouseY = e.clientY;
});

window.addEventListener('mousedown', (e) => {
	const container = e.target.closest('.viz-container-managed');
	if (container) {
		globallyActiveVizId = container.id;
	} else if (!e.target.closest('.zoom-controls')) {
		globallyActiveVizId = null;
		Object.values(vizInstances).forEach(inst => inst.setScrollZoomActive(false));
	}
}, { capture: true });

window.addEventListener('keydown', (e) => {
	if (!globallyActiveVizId || !vizInstances[globallyActiveVizId]) return;
	if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;

	const activeInstance = vizInstances[globallyActiveVizId];

	if (e.key === '+' || e.key === '=') {
		e.preventDefault();
		activeInstance.zoomToTarget(true, true);
	}
	else if (e.key === '-') {
		e.preventDefault();
		activeInstance.zoomToTarget(false, true);
	}
});

export function triggerPositionUpdateForViz(containerId) {
	if (vizInstances[containerId] && typeof vizInstances[containerId].update === 'function') {
		vizInstances[containerId].update();
	}
}

export function disposeAllVisualizations() {
	for (const key in vizInstances) delete vizInstances[key];
	globallyActiveVizId = null;
}

export function initializeZoomAndPan(containerId, wrapperId, zoomInId, zoomOutId, zoomTarget, pointsContainerId) {
	const container = document.getElementById(containerId);
	const wrapper = document.getElementById(wrapperId);
	const zoomInBtn = document.getElementById(zoomInId);
	const zoomOutBtn = document.getElementById(zoomOutId);

	if (!container || !wrapper || !zoomInBtn || !zoomOutBtn) return;

	container.classList.add('viz-container-managed');
	const idSuffix = containerId.includes('own') ? 'own' : (containerId.includes('nc') ? 'nc' : 'serendipity');

	let isScrollZoomActive = false;
	let longPressTimer = null;

	const debugOverlay = document.getElementById(`debug-overlay-${idSuffix}`);
	const debugContainerSpan = document.getElementById(`debug-container-coords-${idSuffix}`);
	const debugWrapperSpan = document.getElementById(`debug-wrapper-coords-${idSuffix}`);
	const debugImageSpan = document.getElementById(`debug-image-coords-${idSuffix}`);
	if (debugOverlay) debugOverlay.style.display = 'none';

	let cachedCw = container.clientWidth;
	let cachedCh = container.clientHeight;

	const resizeObserver = new ResizeObserver(() => {
		if (!container) return;
		cachedCw = container.clientWidth;
		cachedCh = container.clientHeight;
		updateDOM();
	});
	resizeObserver.observe(container);

	wrapper.style.left = '0px';
	wrapper.style.top = '0px';

	function getCenterAnchor() {
		const ctx = getContext();
		if (ctx && ctx.crosshairCoords && Array.isArray(ctx.crosshairCoords) && ctx.crosshairCoords.length >= 2) {
			return { x: ctx.crosshairCoords[0], y: ctx.crosshairCoords[1] };
		} else if (zoomTarget) {
			return { x: zoomTarget.x, y: zoomTarget.y };
		}
		return { x: 0.5, y: 0.5 };
	}

	const initialCenter = getCenterAnchor();
	let state = { zoom: MIN_ZOOM, centerX: 0.5, centerY: 0.5 };
	let targetState = { ...state };
	let animFrameId = null;

	let isPanning = false;
	let hasPannedFromCenter = false;
	let panAnchorImageX = 0;
	let panAnchorImageY = 0;
	let isPanFramePending = false;
	let latestPanX = 0;
	let latestPanY = 0;
	let lastMouseEvent = null;

	let initialPinchDistance = null;
	let initialPinchZoom = null;

	const lerp = (start, end, factor) => start + (end - start) * factor;

	container.addEventListener('mousemove', (e) => {
		lastMouseEvent = e;
		if (debugOverlay && debugOverlay.style.display !== 'none') updateDebugInfo();
	});
	container.addEventListener('mouseleave', () => {
		lastMouseEvent = null;
		if (debugOverlay && debugOverlay.style.display !== 'none') updateDebugInfo();
	});

	function getClampBounds(z) {
		if (z <= MIN_ZOOM) return { minX: 0.5, maxX: 0.5, minY: 0.5, maxY: 0.5 };
		return {
			minX: 1 / (2 * z),
			maxX: 1 - 1 / (2 * z),
			minY: 1 / (2 * z),
			maxY: 1 - 1 / (2 * z)
		};
	}

	function clampTargetState() {
		// Fix für den Button-Bug (Floating Point Ungenauigkeiten abfangen)
		if (targetState.zoom <= MIN_ZOOM + 0.001) targetState.zoom = MIN_ZOOM;
		if (targetState.zoom >= MAX_ZOOM - 0.001) targetState.zoom = MAX_ZOOM;

		targetState.zoom = Math.max(MIN_ZOOM, Math.min(MAX_ZOOM, targetState.zoom));

		// Wenn wir wieder in der Ausgangsansicht sind, Reset des Pan-Status
		if (targetState.zoom <= MIN_ZOOM) {
			hasPannedFromCenter = false;
		}

		const bounds = getClampBounds(targetState.zoom);
		targetState.centerX = Math.max(bounds.minX, Math.min(bounds.maxX, targetState.centerX));
		targetState.centerY = Math.max(bounds.minY, Math.min(bounds.maxY, targetState.centerY));
	}

	function renderLoop() {
		let needsMoreFrames = false;

		const prevZoom = state.zoom;
		// Smooth wie früher, aber durch Speed 0.35 viel crisper
		state.zoom = lerp(state.zoom, targetState.zoom, ZOOM_CONFIG.ANIMATION_SPEED);

		// Höherer Threshold (0.002 statt 0.001) kappt die mikroskopischen letzten Reste sofort ab
		if (Math.abs(state.zoom - targetState.zoom) > 0.002) {
			needsMoreFrames = true;
		} else {
			state.zoom = targetState.zoom;
		}

		if (isPanning) {
			applyPanAnchor();
		} else {
			// MATHE-MAGIC FÜR DIE GRÜNE LINIE: Wir lerpen im Screen-Space!
			const currentScreenX = (0.5 - state.centerX) * prevZoom;
			const currentScreenY = (0.5 - state.centerY) * prevZoom;

			const targetScreenX = (0.5 - targetState.centerX) * targetState.zoom;
			const targetScreenY = (0.5 - targetState.centerY) * targetState.zoom;

			const nextScreenX = lerp(currentScreenX, targetScreenX, ZOOM_CONFIG.ANIMATION_SPEED);
			const nextScreenY = lerp(currentScreenY, targetScreenY, ZOOM_CONFIG.ANIMATION_SPEED);

			// Zurück in Bildkoordinaten übersetzen
			state.centerX = 0.5 - (nextScreenX / state.zoom);
			state.centerY = 0.5 - (nextScreenY / state.zoom);

			if (Math.abs(state.centerX - targetState.centerX) > 0.0001 || Math.abs(state.centerY - targetState.centerY) > 0.0001) {
				needsMoreFrames = true;
			} else {
				state.centerX = targetState.centerX;
				state.centerY = targetState.centerY;
			}
		}

		const currentBounds = getClampBounds(state.zoom);
		state.centerX = Math.max(currentBounds.minX, Math.min(currentBounds.maxX, state.centerX));
		state.centerY = Math.max(currentBounds.minY, Math.min(currentBounds.maxY, state.centerY));

		updateDOM();

		if (needsMoreFrames) {
			animFrameId = requestAnimationFrame(renderLoop);
		} else {
			animFrameId = null;
		}
	}

	function requestPhysicsUpdate() {
		if (!animFrameId) animFrameId = requestAnimationFrame(renderLoop);
	}

	function updateDOM() {
		const rectW = cachedCw;
		const rectH = cachedCh;
		if (rectW === 0 || rectH === 0) return;

		const newWidth = rectW * state.zoom;
		const newHeight = rectH * state.zoom;

		let newLeft = (rectW / 2) - (state.centerX * newWidth);
		let newTop = (rectH / 2) - (state.centerY * newHeight);

		if (state.zoom <= MIN_ZOOM) {
			newLeft = (rectW - newWidth) / 2;
			newTop = (rectH - newHeight) / 2;
			state.centerX = 0.5;
			state.centerY = 0.5;
		}

		wrapper.style.width = `${newWidth}px`;
		wrapper.style.height = `${newHeight}px`;
		wrapper.style.transform = `translate(${newLeft}px, ${newTop}px)`;

		updateDynamicPositions(newLeft, newTop, newWidth, newHeight);
		updateButtonStates();

		emit('viz-transform', {
			prefix: idSuffix,
			zoom: state.zoom,
			left: newLeft,
			top: newTop,
			width: newWidth,
			height: newHeight,
			containerWidth: rectW,
			containerHeight: rectH
		});

		if (debugOverlay && debugOverlay.style.display !== 'none') updateDebugInfo();
	}

	function updateDynamicPositions(wrapperLeft, wrapperTop, wrapperWidth, wrapperHeight) {
		const pointsContainer = document.getElementById(pointsContainerId);
		if (!pointsContainer || wrapperWidth <= 0) return;

		const hitboxes = pointsContainer.getElementsByClassName('point-hitbox');
		const hitboxSize = 30;

		for (let i = 0; i < hitboxes.length; i++) {
			const hitbox = hitboxes[i];
			const relX = parseFloat(hitbox.dataset.relativeX);
			const relY = parseFloat(hitbox.dataset.relativeY);

			hitbox.style.left = '0px';
			hitbox.style.top = '0px';
			hitbox.style.transform = `translate(${wrapperLeft + (relX * wrapperWidth) - (hitboxSize / 2)}px, ${wrapperTop + (relY * wrapperHeight) - (hitboxSize / 2)}px)`;
		}
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

	function zoomToTarget(isZoomingIn, forceCrosshair = false, pointerX = null, pointerY = null, isWheel = false) {
		const oldZoom = targetState.zoom;
		const stepFactor = isWheel ? ZOOM_CONFIG.WHEEL_ZOOM_STEP : ZOOM_CONFIG.BUTTON_ZOOM_STEP;
		let newZoom = oldZoom * (isZoomingIn ? stepFactor : 1 / stepFactor);
		newZoom = Math.max(MIN_ZOOM, Math.min(MAX_ZOOM, newZoom));

		if (newZoom === oldZoom) return;

		if (forceCrosshair) {
			// Wenn NICHT gepannt wurde, zentrieren wir aufs Crosshair
			if (!hasPannedFromCenter) {
				const crosshair = getCenterAnchor();
				targetState.centerX = crosshair.x;
				targetState.centerY = crosshair.y;
			}
			// Wenn gepannt wurde, bleibt das aktuelle centerX/Y bestehen (Zoom in die Mitte)
			targetState.zoom = newZoom;
		} else {
			// Mausrad / Pinch-to-Zoom verändert den Fokus und gilt daher als Panning
			hasPannedFromCenter = true;

			let anchorX = 0.5;
			let anchorY = 0.5;
			let px = pointerX !== null ? pointerX : globalMouseX;
			let py = pointerY !== null ? pointerY : globalMouseY;

			const rect = container.getBoundingClientRect();
			if (px >= rect.left && px <= rect.right && py >= rect.top && py <= rect.bottom) {
				anchorX = (px - rect.left) / rect.width;
				anchorY = (py - rect.top) / rect.height;
			}

			targetState.centerX += (anchorX - 0.5) * (1 / oldZoom - 1 / newZoom);
			targetState.centerY += (anchorY - 0.5) * (1 / oldZoom - 1 / newZoom);
			targetState.zoom = newZoom;
		}

		clampTargetState();
		requestPhysicsUpdate();
	}

	function applyPanAnchor() {
		hasPannedFromCenter = true;
		if (!container) return;
		const cRect = container.getBoundingClientRect();
		const mouseXInContainer = latestPanX - cRect.left;
		const mouseYInContainer = latestPanY - cRect.top;

		const cw = cachedCw;
		const ch = cachedCh;
		const currentWidth = cw * state.zoom;
		const currentHeight = ch * state.zoom;
		if (currentWidth <= 0 || currentHeight <= 0) return;

		let newLeft = mouseXInContainer - (panAnchorImageX * currentWidth);
		let newTop = mouseYInContainer - (panAnchorImageY * currentHeight);

		if (state.zoom <= MIN_ZOOM) {
			newLeft = (cw - currentWidth) / 2;
			newTop = (ch - currentHeight) / 2;
		} else {
			const maxLeft = 0;
			const minLeft = cw - currentWidth;
			newLeft = Math.max(minLeft, Math.min(maxLeft, newLeft));
			const maxTop = 0;
			const minTop = ch - currentHeight;
			newTop = Math.max(minTop, Math.min(maxTop, newTop));
		}

		state.centerX = ((cw / 2) - newLeft) / currentWidth;
		state.centerY = ((ch / 2) - newTop) / currentHeight;
		targetState.centerX = state.centerX;
		targetState.centerY = state.centerY;
	}

	function performPan() {
		applyPanAnchor();
		updateDOM();
		isPanFramePending = false;
	}

	function getDistance(touches) {
		return Math.sqrt(Math.pow(touches[1].clientX - touches[0].clientX, 2) + Math.pow(touches[1].clientY - touches[0].clientY, 2));
	}

	container.addEventListener('wheel', (e) => {
		if (!isScrollZoomActive) return;
		e.preventDefault();
		const intensity = Math.abs(e.deltaY) > 50 ? 2 : 1;
		for (let i = 0; i < intensity; i++) {
			zoomToTarget(e.deltaY < 0, false, e.clientX, e.clientY, true);
		}
	}, { passive: false });

	container.addEventListener('mousedown', (e) => {
		if (e.target.closest('.point-hitbox') || e.target.closest('.zoom-controls')) return;
		setScrollZoomActive(true);
		globallyActiveVizId = containerId;
		e.preventDefault();
		if (targetState.zoom <= MIN_ZOOM) return;
		isPanning = true;
		latestPanX = e.clientX;
		latestPanY = e.clientY;
		const cw = cachedCw;
		const ch = cachedCh;
		const currentWidth = cw * state.zoom;
		const currentHeight = ch * state.zoom;
		const startTranslateX = (cw / 2) - (state.centerX * currentWidth);
		const startTranslateY = (ch / 2) - (state.centerY * currentHeight);
		const cRect = container.getBoundingClientRect();
		panAnchorImageX = (e.clientX - cRect.left - startTranslateX) / currentWidth;
		panAnchorImageY = (e.clientY - cRect.top - startTranslateY) / currentHeight;
		container.style.cursor = 'grabbing';
	});

	const onWindowMouseMove = (e) => {
		if (isPanning) {
			latestPanX = e.clientX;
			latestPanY = e.clientY;
			if (!isPanFramePending) {
				isPanFramePending = true;
				requestAnimationFrame(performPan);
			}
		}
	};

	const onWindowMouseUp = () => {
		if (isPanning) {
			isPanning = false;
			container.style.cursor = targetState.zoom > MIN_ZOOM ? 'grab' : 'default';
		}
	};

	const onWindowTouchMove = (e) => {
		if (document.body.classList.contains('is-swiping-active')) return;
		if (!isPanning && !initialPinchDistance) return;

		if (isPanning && e.touches.length === 1) {
			e.preventDefault();
			latestPanX = e.touches[0].clientX;
			latestPanY = e.touches[0].clientY;
			if (!isPanFramePending) {
				isPanFramePending = true;
				requestAnimationFrame(performPan);
			}
		} else if (initialPinchDistance && e.touches.length === 2) {
			e.preventDefault();
			const currentPinchDistance = getDistance(e.touches);
			const pinchRatio = currentPinchDistance / initialPinchDistance;
			const newZoom = Math.max(MIN_ZOOM, Math.min(MAX_ZOOM, initialPinchZoom * pinchRatio));
			latestPanX = (e.touches[0].clientX + e.touches[1].clientX) / 2;
			latestPanY = (e.touches[0].clientY + e.touches[1].clientY) / 2;
			targetState.zoom = newZoom;
			state.zoom = newZoom;
			applyPanAnchor();
			updateDOM();
		}
	};

	window.addEventListener('mousemove', onWindowMouseMove);
	window.addEventListener('mouseup', onWindowMouseUp);
	window.addEventListener('touchmove', onWindowTouchMove, { passive: false });

	container.addEventListener('touchstart', (e) => {
		if (document.body.classList.contains('is-swiping-active')) return;
		if (e.target.closest('.zoom-controls button') || e.target.closest('.point-hitbox')) return;
		const isMultiTouch = e.touches.length > 1;
		const isZoomedIn = targetState.zoom > MIN_ZOOM + 0.01;
		if (!isMultiTouch && !isZoomedIn) return;
		setScrollZoomActive(true);
		globallyActiveVizId = containerId;
		e.stopPropagation();
		if (e.cancelable) e.preventDefault();
		if (!isMultiTouch && isZoomedIn) {
			isPanning = true;
			latestPanX = e.touches[0].clientX;
			latestPanY = e.touches[0].clientY;
			const cw = cachedCw;
			const ch = cachedCh;
			const currentWidth = cw * state.zoom;
			const currentHeight = ch * state.zoom;
			const startTranslateX = (cw / 2) - (state.centerX * currentWidth);
			const startTranslateY = (ch / 2) - (state.centerY * currentHeight);
			const cRect = container.getBoundingClientRect();
			panAnchorImageX = (latestPanX - cRect.left - startTranslateX) / currentWidth;
			panAnchorImageY = (latestPanY - cRect.top - startTranslateY) / currentHeight;
		} else if (isMultiTouch) {
			isPanning = false;
			initialPinchDistance = getDistance(e.touches);
			initialPinchZoom = targetState.zoom;
			latestPanX = (e.touches[0].clientX + e.touches[1].clientX) / 2;
			latestPanY = (e.touches[0].clientY + e.touches[1].clientY) / 2;
			const cw = cachedCw;
			const ch = cachedCh;
			const currentWidth = cw * state.zoom;
			const currentHeight = ch * state.zoom;
			const startTranslateX = (cw / 2) - (state.centerX * currentWidth);
			const startTranslateY = (ch / 2) - (state.centerY * currentHeight);
			const cRect = container.getBoundingClientRect();
			panAnchorImageX = (latestPanX - cRect.left - startTranslateX) / currentWidth;
			panAnchorImageY = (latestPanY - cRect.top - startTranslateY) / currentHeight;
		}
		clearTimeout(longPressTimer);
		longPressTimer = setTimeout(() => { if (e.touches.length === 1) showContextMenu(e); }, 800);
	}, { passive: false });

	container.addEventListener('touchend', () => {
		clearTimeout(longPressTimer);
		isPanning = false;
		initialPinchDistance = null;
	});

	container.addEventListener('touchmove', () => {
		clearTimeout(longPressTimer);
	});

	const setupButton = (button, isZoomingIn) => {
		let startX = 0;
		let startY = 0;
		let isValidTap = false;
		button.addEventListener('touchstart', (e) => {
			if (e.touches.length > 1) return;
			isValidTap = true;
			startX = e.touches[0].clientX;
			startY = e.touches[0].clientY;
		}, { passive: false });
		button.addEventListener('touchend', (e) => {
			if (!isValidTap) return;
			const touch = e.changedTouches[0];
			const dist = Math.sqrt(Math.pow(touch.clientX - startX, 2) + Math.pow(touch.clientY - startY, 2));
			if (dist < 25) {
				e.preventDefault();
				e.stopPropagation();
				zoomToTarget(isZoomingIn, true);
			}
			isValidTap = false;
		});
		button.addEventListener('click', (e) => {
			e.preventDefault(); e.stopPropagation();
			zoomToTarget(isZoomingIn, true);
		});
		button.addEventListener('mousedown', (e) => { e.preventDefault(); e.stopPropagation(); });
	};

	setupButton(zoomInBtn, true);
	setupButton(zoomOutBtn, false);

	function showContextMenu(e) {
		e.preventDefault();
		const existingMenu = document.getElementById('custom-context-menu');
		if (existingMenu) existingMenu.remove();

		const menu = document.createElement('div');
		menu.className = 'custom-context-menu';
		menu.id = 'custom-context-menu';

		const clientX = e.touches ? e.touches[0].clientX : e.clientX;
		const clientY = e.touches ? e.touches[0].clientY : e.clientY;

		menu.style.top = `${clientY}px`;
		menu.style.left = `${clientX}px`;

		const menuItems = [
			{
				label: 'Center on Idea', action: () => {
					hasPannedFromCenter = false;
					const center = getCenterAnchor();
					targetState.centerX = center.x;
					targetState.centerY = center.y;
					clampTargetState(); requestPhysicsUpdate();
				}
			},
			{
				label: 'Zoom in', action: () => {
					const center = getCenterAnchor();
					targetState.centerX = center.x;
					targetState.centerY = center.y;
					targetState.zoom = MAX_ZOOM;
					clampTargetState();
					requestPhysicsUpdate();
				}
			},
			{
				label: 'Zoom out', action: () => {
					targetState.zoom = MIN_ZOOM;
					const center = getCenterAnchor();
					targetState.centerX = center.x;
					targetState.centerY = center.y;
					clampTargetState(); requestPhysicsUpdate();
				}
			},
			{ type: 'separator' },
			{ label: 'Save Image as PNG', action: saveImage },
			{ type: 'separator' },
			{
				label: 'Toggle Debug Info', action: () => {
					if (debugOverlay) {
						debugOverlay.style.display = debugOverlay.style.display === 'none' ? 'block' : 'none';
						updateDOM();
					}
				}
			}
		];

		menuItems.forEach(item => {
			if (item.type === 'separator') {
				const sep = document.createElement('div'); sep.className = 'context-menu-separator'; menu.appendChild(sep);
			} else {
				const menuItem = document.createElement('div'); menuItem.className = 'context-menu-item'; menuItem.textContent = item.label;
				menuItem.addEventListener('click', () => { item.action(); menu.remove(); });
				menu.appendChild(menuItem);
			}
		});

		document.body.appendChild(menu);
		setTimeout(() => window.addEventListener('click', () => menu.remove(), { once: true }), 0);
	}

	container.addEventListener('contextmenu', showContextMenu);

	async function saveImage() {
		let layerPrefix = 'viz-layer-nc';
		if (idSuffix === 'own') layerPrefix = 'viz-layer-own';
		else if (idSuffix === 'serendipity') layerPrefix = 'viz-layer-serendipity';
		const ctx = getContext();
		const pointsData = (idSuffix === 'own' && ctx.ownResults) ? ctx.ownResults : [];
		let colorMap = {};
		if (idSuffix === 'own') colorMap = ctx.ownColorMap || {};
		else if (idSuffix === 'serendipity') colorMap = ctx.serendipityColorMap || {};
		else colorMap = ctx.neighborColorMap || {};
		const finalImageURL = await generateVisualizationImage(layerPrefix, pointsContainerId, pointsData, colorMap);
		if (finalImageURL) {
			const link = document.createElement('a');
			link.href = finalImageURL;
			let title = getJobTitle() ? getJobTitle().trim().toLowerCase().replace(/[^a-z0-9]/g, '-') : 'analysis';
			link.download = `viz-${title}-${idSuffix}.png`;
			document.body.appendChild(link);
			link.click();
			document.body.removeChild(link);
		}
	}

	function setScrollZoomActive(isActive) {
		if (isScrollZoomActive === isActive) return;
		isScrollZoomActive = isActive;
		container.classList.toggle('is-scroll-zoom-active', isActive);
		if (!isActive) container.style.cursor = 'default';
		else updateButtonStates();
	}

	function updateButtonStates() {
		zoomInBtn.disabled = targetState.zoom >= MAX_ZOOM;
		zoomOutBtn.disabled = targetState.zoom <= MIN_ZOOM;
		if (!isPanning && isScrollZoomActive) container.style.cursor = targetState.zoom > MIN_ZOOM ? 'grab' : 'default';
	}

	vizInstances[containerId] = { update: () => { updateDOM(); }, zoomToTarget: zoomToTarget, setScrollZoomActive: setScrollZoomActive };
	updateDOM();

	registerCleanup(() => {
		resizeObserver.disconnect();
		window.removeEventListener('mousemove', onWindowMouseMove);
		window.removeEventListener('mouseup', onWindowMouseUp);
		window.removeEventListener('touchmove', onWindowTouchMove);
		if (animFrameId) cancelAnimationFrame(animFrameId);
	});
}