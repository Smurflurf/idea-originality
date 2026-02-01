import { getContext } from '/script/core/context.js';
import { initializeZoomAndPan, triggerPositionUpdateForViz } from '/script/viz/core/zoomAndPan.js';
import { colorizeElementsInContainer, colorizeIndicators } from '/script/ui/base/colorCoder.js';

const flashObserver = new IntersectionObserver((entries) => {
	entries.forEach(entry => {
		if (entry.isIntersecting) {
			const element = entry.target;
			element.classList.add('highlight');
			setTimeout(() => { element.classList.remove('highlight'); }, 1500);
			flashObserver.unobserve(element);
		}
	});
}, { threshold: 0.2 });

/**
 * Findet den scrollbaren Container (Pane).
 */
function getScrollParent(node) {
	if (node == null) return null;
	if (node.classList.contains('viz-content-pane')) return node;
	return getScrollParent(node.parentElement);
}

/**
 * Findet die Zielkarte robust, auch wenn Präfixe abweichen.
 * Strategie: 
 * 1. Exakte ID (z.B. nc-result-card-123)
 * 2. Generische ID (z.B. result-card-123)
 * 3. Suche im aktiven Tab nach Endung (z.B. ...-123)
 */
function findTargetCard(paperId, contextPrefix) {
	if (!paperId) return null;

	// 1. Versuch: Exakte ID (wie vom Skript erwartet)
	const exactId = `${contextPrefix}-result-card-${paperId}`;
	let card = document.getElementById(exactId);
	if (card) return card;

	// 2. Versuch: Ohne Context-Präfix (falls Loader generische IDs nutzt)
	const genericId = `result-card-${paperId}`;
	card = document.getElementById(genericId);

	// Check: Ist diese Karte im richtigen Container? 
	// Wenn wir 'nc' suchen, aber 'own' finden (weil gleiche ID), ist das falsch.
	if (card) {
		const pane = card.closest('.viz-content-pane');
		// Wenn Karte in einem sichtbaren Pane ist oder kein Pane gefunden wurde, nehmen wir sie.
		// Falls sie im falschen (versteckten) Pane ist, suchen wir weiter.
		if (pane && pane.style.display !== 'none') {
			return card;
		}
	}

	// 3. Versuch: Suffix-Suche im AKTIVEN Container
	// Wir suchen den Container, der zum Prefix gehört
	let containerSelector = '';
	if (contextPrefix === 'own') containerSelector = '#own-viz-content';
	else if (contextPrefix === 'nc') containerSelector = '#neighbor-viz-content';
	else if (contextPrefix === 'serendipity') containerSelector = '#serendipity-viz-content';

	const activeContainer = document.querySelector(containerSelector);
	if (activeContainer) {
		// Suche irgendeine Karte, die auf die ID endet
		return activeContainer.querySelector(`[id$="result-card-${paperId}"]`);
	}

	return null;
}

/**
 * Scrollt das Element präzise in die Mitte des Viewports / Containers.
 * Nutzt getBoundingClientRect für maximale Genauigkeit.
 */
function smoothScrollToElement(targetElement, duration = 600) {
	return new Promise(resolve => {
		if (!targetElement) return resolve();

		const pane = getScrollParent(targetElement);
		if (!pane) return resolve();

		// Positionen relativ zum Viewport holen
		const paneRect = pane.getBoundingClientRect();
		const elementRect = targetElement.getBoundingClientRect();

		// Aktueller Scroll-Status
		const currentScrollTop = pane.scrollTop;

		// Berechnung:
		// Wir wollen: Element-Mitte == Pane-Mitte
		// Differenz = (ElementTop - PaneTop) + (ElementHeight/2) - (PaneHeight/2)
		const relativeTop = elementRect.top - paneRect.top;
		const offsetToCenter = relativeTop + (elementRect.height / 2) - (paneRect.height / 2);

		const targetScrollTop = currentScrollTop + offsetToCenter;
		const startScrollTop = currentScrollTop;
		const distance = targetScrollTop - startScrollTop;

		let startTime = null;
		function animationLoop(currentTime) {
			if (startTime === null) startTime = currentTime;
			const elapsedTime = currentTime - startTime;
			const progress = Math.min(elapsedTime / duration, 1);

			// Easing: Ease-In-Out
			const easedProgress = progress < .5 ? 2 * progress * progress : -1 + (4 - 2 * progress) * progress;

			pane.scrollTop = startScrollTop + (distance * easedProgress);

			if (elapsedTime < duration) {
				requestAnimationFrame(animationLoop);
			} else {
				pane.scrollTop = targetScrollTop;
				resolve();
			}
		}

		// Nur animieren, wenn der Weg lohnenswert ist (> 2px)
		if (Math.abs(distance) > 2) {
			requestAnimationFrame(animationLoop);
		} else {
			pane.scrollTop = targetScrollTop;
			resolve();
		}
	});
}

function scrollToAndFlash(paperId, contextPrefix) {
	const targetElement = findTargetCard(paperId, contextPrefix);

	if (!targetElement) {
		console.warn(`[ClickPoints] Karte für ID ${paperId} (${contextPrefix}) nicht gefunden.`);
		return;
	}
	smoothScrollToElement(targetElement).then(() => {
		flashObserver.observe(targetElement);
	});
}

function initializeInteractiveTooltips(selector, colorMap) {
	const isTouchDevice = ('ontouchstart' in window) || (navigator.maxTouchPoints > 0);
	const isSmallScreen = window.innerWidth <= 768;

	const tippyConfig = {
		allowHTML: true,
		interactive: true,
		appendTo: () => document.body,
		theme: 'custom-card',
		animation: 'shift-away',
		placement: isSmallScreen ? 'bottom' : 'right-start',

		onShow(instance) {
			tippy.hideAll({ exclude: instance });
		},

		content(reference) {
			const paperId = reference.dataset.paperId;
			const contextPrefix = reference.dataset.contextPrefix;

			const originalCard = findTargetCard(paperId, contextPrefix);

			if (!originalCard) return '<div style="padding:10px;">Information not found (ID mismatch).</div>';

			const clonedCard = originalCard.cloneNode(true);
			clonedCard.removeAttribute('id');
			clonedCard.style.margin = '0';
			clonedCard.querySelector('.toggle-json-btn')?.remove();
			clonedCard.querySelector('.result-payload')?.remove();

			const abstractWrapper = clonedCard.querySelector('.abstract-wrapper');
			if (abstractWrapper) abstractWrapper.classList.add('expanded');

			return clonedCard;
		},
		onMount(instance) {
			const pointHitbox = instance.reference;
			const tooltipContent = instance.popper;

			tooltipContent.addEventListener('mouseenter', () => pointHitbox.classList.add('is-hovered-from-tooltip'));
			tooltipContent.addEventListener('mouseleave', () => pointHitbox.classList.remove('is-hovered-from-tooltip'));

			if (tooltipContent && colorMap) {
				colorizeElementsInContainer(tooltipContent, colorMap);
				colorizeIndicators(tooltipContent, colorMap);
			}

			const clickableArea = tooltipContent.querySelector('.result-summary');
			if (clickableArea) {
				clickableArea.addEventListener('click', () => {
					const paperId = instance.reference.dataset.paperId;
					const contextPrefix = instance.reference.dataset.contextPrefix;
					scrollToAndFlash(paperId, contextPrefix);
					instance.hide();
				});
			}
		}
	};

	tippy(selector, {
		...tippyConfig,
		trigger: isTouchDevice ? 'click' : 'mouseenter focus',
		delay: isTouchDevice ? [0, 100] : [250, 200],
	});
}

export function initializeDynamicPoints(containerId, resultsData, colorMap, contextPrefix) {
	if (!resultsData || !colorMap) return;
	const container = document.getElementById(containerId);
	if (!container) return;

	container.innerHTML = '';

	resultsData.forEach(paper => {
		if (paper.relativeX === undefined || paper.relativeY === undefined) return;
		const hitbox = document.createElement('div');
		hitbox.className = 'point-hitbox';
		hitbox.dataset.relativeX = paper.relativeX;
		hitbox.dataset.relativeY = paper.relativeY;

		// NEU: Wir speichern die Rohdaten, um flexibel zu suchen
		hitbox.dataset.paperId = paper.id;
		hitbox.dataset.contextPrefix = contextPrefix;

		// Legacy ID für Debugging
		hitbox.dataset.targetCardId = `${contextPrefix}-result-card-${paper.id}`;

		const visiblePoint = document.createElement('div');
		visiblePoint.className = 'neighbor-point';
		visiblePoint.style.backgroundColor = colorMap[paper.clusterId] || '#bdc1c6';
		hitbox.appendChild(visiblePoint);
		container.appendChild(hitbox);
	});

	initializeInteractiveTooltips(`#${containerId} .point-hitbox`, colorMap);

	const parentVizContainerId = container.closest('.viz-stack-container')?.id;
	if (parentVizContainerId) {
		const observer = new MutationObserver((mutations) => {
			for (const mutation of mutations) {
				if (mutation.attributeName === 'style') {
					if (container.style.display !== 'none') {
						triggerPositionUpdateForViz(parentVizContainerId);
					}
				}
			}
		});
		observer.observe(container, { attributes: true });
	}

	// Click Handler für Desktop (Touch macht Tippy)
	const isTouchDevice = ('ontouchstart' in window) || (navigator.maxTouchPoints > 0);
	if (!isTouchDevice) {
		container.addEventListener('click', (event) => {
			const clickedHitbox = event.target.closest('.point-hitbox');
			if (!clickedHitbox) return;
			event.stopPropagation();

			const paperId = clickedHitbox.dataset.paperId;
			const prefix = clickedHitbox.dataset.contextPrefix;
			scrollToAndFlash(paperId, prefix);
		});
	}
}

export function initializeAllVisualizations() {
	const ctx = getContext();

	// OWN
	initializeDynamicPoints(
		'dynamic-points-own-neighbors',
		ctx.ownResults,
		ctx.ownColorMap,
		'own'
	);

	initializeZoomAndPan(
		'viz-stack-container-own', 'zoom-pan-wrapper-own',
		'zoom-in-btn-own', 'zoom-out-btn-own',
		ctx.crosshairCoords,
		'dynamic-points-own-neighbors'
	);

	// NEIGHBOR (nc)
	initializeDynamicPoints(
		'dynamic-points-nc-neighbors',
		[],
		ctx.neighborColorMap,
		'nc'
	);

	initializeZoomAndPan(
		'viz-stack-container-nc', 'zoom-pan-wrapper-nc',
		'zoom-in-btn-nc', 'zoom-out-btn-nc',
		ctx.crosshairCoords,
		'dynamic-points-nc-neighbors'
	);

	// SERENDIPITY
	initializeDynamicPoints(
		'dynamic-points-serendipity-neighbors',
		[],
		ctx.serendipityColorMap,
		'serendipity'
	);

	initializeZoomAndPan(
		'viz-stack-container-serendipity', 'zoom-pan-wrapper-serendipity',
		'zoom-in-btn-serendipity', 'zoom-out-btn-serendipity',
		ctx.crosshairCoords,
		'dynamic-points-serendipity-neighbors'
	);
}