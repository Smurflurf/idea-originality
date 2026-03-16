import { getContext } from '/script/core/context.js';
import { initializeZoomAndPan, triggerPositionUpdateForViz } from '/script/viz/core/zoomAndPan.js';
import { colorizeElementsInContainer, colorizeIndicators } from '/script/ui/base/colorCoder.js';
import { getTemplate, renderTemplate } from '/script/core/templateManager.js';

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
 * Findet die Zielkarte robust, auch wenn Präfixe abweichen.
 */
function findTargetCard(paperId, contextPrefix) {
	if (!paperId) return null;

	const exactId = `${contextPrefix}-result-card-${paperId}`;
	let card = document.getElementById(exactId);
	if (card) return card;

	const genericId = `result-card-${paperId}`;
	card = document.getElementById(genericId);

	if (card) {
		const pane = card.closest('.viz-content-pane');
		if (pane && pane.style.display !== 'none') {
			return card;
		}
	}

	let containerSelector = '';
	if (contextPrefix === 'own') containerSelector = '#own-viz-content';
	else if (contextPrefix === 'nc') containerSelector = '#neighbor-viz-content';
	else if (contextPrefix === 'serendipity') containerSelector = '#serendipity-viz-content';

	const activeContainer = document.querySelector(containerSelector);
	if (activeContainer) {
		return activeContainer.querySelector(`[id$="result-card-${paperId}"]`);
	}

	return null;
}

/**
 * Scrollt das Element präzise in die Mitte des Viewports / Containers.
 */
function smoothScrollToElement(targetElement) {
	return new Promise(resolve => {
		if (!targetElement) return resolve();

		const rect = targetElement.getBoundingClientRect();
		const offsetToCenter = rect.top - (window.innerHeight / 2) + (rect.height / 2);
		
		window.scrollBy({
			top: offsetToCenter,
			behavior: 'smooth'
		});

		setTimeout(resolve, 400);
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

function initializeInteractiveTooltips(containerId, colorMap) {
	const container = document.getElementById(containerId);
	if (!container) return;

	const isTouchDevice = ('ontouchstart' in window) || (navigator.maxTouchPoints > 0);
	const isSmallScreen = window.innerWidth <= 768;

	const tippyConfig = {
		allowHTML: true,
		interactive: true,
		appendTo: () => document.body,
		theme: 'custom-card',
		animation: 'shift-away',
		placement: isSmallScreen ? 'bottom' : 'right-start',
        // Die Delay/Trigger Konfigurationen wandern in die Instanz
		trigger: isTouchDevice ? 'click' : 'mouseenter focus',
		delay: isTouchDevice ? [0, 100] : [250, 200],

		onShow(instance) {
			if (document.body.classList.contains('faq-selection-active')) return false;

			tippy.hideAll({ exclude: instance });
			if (instance.reference) instance.reference.classList.add('is-hovered-from-tooltip');
		},
		onHide(instance) {
			if (instance.reference) instance.reference.classList.remove('is-hovered-from-tooltip');
		},
		content(reference) {
			const paperId = reference.dataset.paperId;
			const contextPrefix = reference.dataset.contextPrefix;
			const originalCard = findTargetCard(paperId, contextPrefix);

			if (!originalCard) {
				const fallback = document.createElement('div');
				fallback.style.padding = '10px';
				fallback.textContent = 'Information not found (ID mismatch).';
				return fallback;
			}

			const clonedCard = originalCard.cloneNode(true);
			clonedCard.removeAttribute('id');
			clonedCard.style.margin = '0';
			clonedCard.querySelector('.toggle-json-btn')?.remove();
			clonedCard.querySelector('.result-payload')?.remove();

			// Entfernt die Umrandung, falls das Original gerade markiert war.
			clonedCard.classList.remove('faq-context-highlight', 'is-highlighted');
			clonedCard.querySelectorAll('.faq-context-highlight, .is-highlighted').forEach(el => {
				el.classList.remove('faq-context-highlight', 'is-highlighted');
				delete el.dataset.faqId;
			});

			const abstractWrapper = clonedCard.querySelector('.abstract-wrapper');
			if (abstractWrapper) abstractWrapper.classList.add('expanded');

			return clonedCard;
		},
		onMount(instance) {
			const tooltipContent = instance.popper;
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

	// NEU: Ein einziger globaler Listener für den GANZEN Container
	const initEvent = isTouchDevice ? 'touchstart' : 'mouseover';
	
	container.addEventListener(initEvent, (e) => {
		const hitbox = e.target.closest('.point-hitbox');
		if (!hitbox || hitbox._tippy) return; // Falls schon initialisiert, nichts tun
		
		// 1. Instanz erstellen (ohne sie sofort zu zeigen)
		tippy(hitbox, tippyConfig);
		
		// 2. Ersten Aufruf mit Verzögerung simulieren
		if (!isTouchDevice) {
			const enterDelay = tippyConfig.delay[0]; // Das sind die 250ms
			
			setTimeout(() => {
				// WICHTIG: Nur zeigen, wenn die Maus nach 250ms noch immer über der Hitbox ist!
				if (hitbox.matches(':hover')) {
					hitbox._tippy.show();
				}
			}, enterDelay);
		} else {
			// Auf Touch-Geräten (Click) darf es sofort kommen
			hitbox._tippy.show();
		}
	}, { passive: true });
}

export function initializeDynamicPoints(containerId, resultsData, colorMap, contextPrefix) {
	if (!resultsData || !colorMap) return;
	const container = document.getElementById(containerId);
	if (!container) return;

	container.__pointsDataCache = resultsData;
	container.innerHTML = '';

	//  Ein Fragment im Arbeitsspeicher erstellen
	const fragment = document.createDocumentFragment();

	resultsData.forEach(paper => {
	    if (paper.relativeX === undefined || paper.relativeY === undefined) return;
	    const hitbox = document.createElement('div');
	    hitbox.className = 'point-hitbox';
	    hitbox.style.left = `${paper.relativeX * 100}%`;
	    hitbox.style.top = `${paper.relativeY * 100}%`;
	    hitbox.style.transform = 'translate(-50%, -50%)'; 

	    hitbox.dataset.paperId = paper.id;
	    hitbox.dataset.contextPrefix = contextPrefix;
		hitbox.dataset.targetCardId = `${contextPrefix}-result-card-${paper.id}`;

		const visiblePoint = document.createElement('div');
		visiblePoint.className = 'neighbor-point';
		visiblePoint.style.backgroundColor = colorMap[paper.clusterId] || '#bdc1c6';
		hitbox.appendChild(visiblePoint);
		
		// Ans Fragment hängen (NICHT ans live DOM!)
		fragment.appendChild(hitbox);
	});

	// Alles auf einmal ins DOM feuern
	container.appendChild(fragment);

	initializeInteractiveTooltips(containerId, colorMap);


	const parentVizContainerId = container.closest('.viz-stack-container')?.id;
	if (parentVizContainerId) {
		const observer = new MutationObserver((mutations) => {
			for (const mutation of mutations) {
				if (window.isSwiping) return;
				if (mutation.attributeName === 'style') {
					if (container.style.display !== 'none') {
						triggerPositionUpdateForViz(parentVizContainerId);
					}
				}
			}
		});
		observer.observe(container, { attributes: true });
	}

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