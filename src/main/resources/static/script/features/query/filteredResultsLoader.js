import { getContext } from '/script/core/context.js';
import { applyColorCoding, getUiColor, getContrastingTextColor } from "/script/ui/base/colorCoder.js";
import { initializeDynamicPoints } from '/script/viz/render/clickablePoints.js';
import { triggerPositionUpdateForViz, requestSnapshotUpdate } from '/script/viz/core/zoomAndPan.js';
import { getCsrfToken } from '/script/core/security.js';
import { applyGeneralTranslations, t } from '/script/core/localization.js';
import { emit, EVENTS } from '/script/core/eventBus.js';
import { initializeAbstractButtonsFor } from '/script/ui/interaction/toggleAbstractButton.js';
import { registerCleanup } from '/script/core/lifecycleManager.js';
import { getTemplate } from '/script/core/templateManager.js';

// Liefert jetzt ein echtes DocumentFragment mit 3 Karten
function createSkeletonCards() {
	const frag = document.createDocumentFragment();
	for (let i = 0; i < 3; i++) {
		const tpl = getTemplate('tpl-skeleton-card');
		if (tpl) frag.appendChild(tpl);
	}
	return frag;
}

function initializeTopicTabs(paneId, colorMap, queryVector) {
	const contentPane = document.getElementById(paneId);
	if (!contentPane) return;

	if (contentPane.dataset.loaderInitialized === 'true') return;

	if (!colorMap || !queryVector) {
		console.warn(`[FilteredLoader] Missing data for ${paneId}.`);
		return;
	}

	contentPane.dataset.loaderInitialized = 'true';

	const resultsCache = {};
	let isFirstLoad = true;
	let isLoading = false;

	const tabsContainer = contentPane.querySelector('.topic-tabs-scroller');
	const contextContainer = contentPane.querySelector('.active-cluster-card-container');
	const dataContainerRoot = contentPane.querySelector('[id^="context-card-data"]');
	const filteredResultsContainer = contentPane.querySelector('.filtered-results-list');
	const hierarchyContainer = contentPane.querySelector('.hierarchy-container.is-nested');

	const vizPrefix = paneId.includes('neighbor') ? 'nc' : 'serendipity';
	const dynamicPointsContainerId = `dynamic-points-${vizPrefix}-neighbors`;

	const activateTabAndUpdateView = (tabToActivate, isUserAction = true) => {
		if (!tabToActivate || isLoading) return;

		contentPane.querySelector('.topic-tab.active')?.classList.remove('active');
		tabToActivate.classList.add('active');

		if (tabsContainer) {
			const containerCenter = tabsContainer.clientWidth / 2;
			const tabCenter = tabToActivate.offsetLeft + (tabToActivate.clientWidth / 2);
			tabsContainer.scrollTo({
				left: tabCenter - containerCenter,
				behavior: 'smooth'
			});
		}

		const clusterId = tabToActivate.dataset.clusterId;
		const targetId = tabToActivate.dataset.targetId;
		const baseColor = colorMap[clusterId] || '#8ab4f8';

		const uiColor = getUiColor(baseColor);
		const textColor = getContrastingTextColor(uiColor);

		contentPane.style.setProperty('--active-cluster-color', uiColor);
		contentPane.style.setProperty('--active-cluster-text-color', textColor);

		const dataContent = dataContainerRoot.querySelector('#' + targetId);
		if (contextContainer && dataContent) {
			contextContainer.innerHTML = dataContent.innerHTML;
		}

		loadFilteredResults(clusterId, isUserAction); // <--- WICHTIG: Parameter weitergeben
	};

	const initializeView = () => {
		if (!isFirstLoad) return;
		isFirstLoad = false;
		const firstTab = contentPane.querySelector('.topic-tab');
		if (firstTab) {
			activateTabAndUpdateView(firstTab, false); // <--- FALSE: Das ist keine User-Aktion!
		}
	};

	// PERFORMANCE FIX: Speichere die letzten Werte
	let lastDisplayState = contentPane.style.display;
	let lastActiveState = contentPane.classList.contains('active');

	const observer = new MutationObserver(() => {
		const currentDisplay = contentPane.style.display;
		const currentActive = contentPane.classList.contains('active');

		// ABBRUCH: Wenn sich weder display noch die active-Klasse geändert haben,
		// war es eine reine 'transform' Änderung durch das Scrollen.
		// Das MUSS ignoriert werden, sonst triggern wir Layout-Berechnungen im Loop.
		if (currentDisplay === lastDisplayState && currentActive === lastActiveState) {
			return;
		}

		lastDisplayState = currentDisplay;
		lastActiveState = currentActive;

		const isVisible = currentActive || currentDisplay !== 'none';

		if (isVisible) {
			if (isFirstLoad) {
				initializeView();
			} else {
				const activeTab = tabsContainer?.querySelector('.topic-tab.active');
				if (activeTab) {
					const clusterId = activeTab.dataset.clusterId;
					const baseColor = colorMap[clusterId] || '#8ab4f8';
					const uiColor = getUiColor(baseColor);
					const textColor = getContrastingTextColor(uiColor);
					contentPane.style.setProperty('--active-cluster-color', uiColor);
					contentPane.style.setProperty('--active-cluster-text-color', textColor);
				}
			}
			initializeAbstractButtonsFor(contentPane);
		}
	});

	// Wir beobachten weiterhin style/class, aber die Logik oben filtert das Scrollen raus.
	observer.observe(contentPane, { attributes: true, attributeFilter: ['class', 'style'] });

	if (contentPane.classList.contains('active')) initializeView();
	
	if (tabsContainer) {
		tabsContainer.addEventListener('click', (event) => {
			const clickedTab = event.target.closest('.topic-tab');
			if (clickedTab && !clickedTab.classList.contains('active')) {
				activateTabAndUpdateView(clickedTab, true); // <--- TRUE
			}
		});
    }

    if (hierarchyContainer && tabsContainer) {
		hierarchyContainer.addEventListener('click', (event) => {
			const clickedBox = event.target.closest('.hierarchy-item-box');
			if (!clickedBox) return;
			const clusterId = clickedBox.dataset.clusterId;
			const correspondingTab = contentPane.querySelector(`.topic-tab[data-cluster-id="${clusterId}"]`);
			if (correspondingTab) {
				scrollIntoViewSafe(tabsContainer);
				if (!correspondingTab.classList.contains('active')) {
					activateTabAndUpdateView(correspondingTab, true); // <--- TRUE
				}
			}
        });
	}

	function scrollIntoViewSafe(element) {
		if (!element) return;
		const rect = element.getBoundingClientRect();
		const offsetToCenter = rect.top - (window.innerHeight / 2) + (rect.height / 2);

		window.scrollBy({
			top: offsetToCenter,
			behavior: 'smooth'
		});
	}

	async function loadFilteredResults(clusterId, isUserAction = true) {
		if (!filteredResultsContainer) return;

		const contextPrefix = paneId.includes('neighbor') ? 'nc' : 'serendipity';

		function updateUiWithResults(container, results, clusterId, isUserActionCall) {
			container.innerHTML = results.html;

			applyGeneralTranslations(container);
			applyColorCoding();

			initializeDynamicPoints(
				dynamicPointsContainerId,
				results.pointsData,
				colorMap,
				contextPrefix
			);

			initializeAbstractButtonsFor(container);

			setTimeout(() => {
				const vizStackContainer = contentPane.querySelector('.viz-stack-container');
				if (vizStackContainer) {
					triggerPositionUpdateForViz(vizStackContainer.id);

					// DER FIX: Snapshot NUR anfordern, wenn es ein User-Klick war, 
					// ODER wenn der Container wirklich noch keinen IDB-Snapshot hat!
					if (isUserActionCall || !vizStackContainer.classList.contains('has-snapshot')) {
						if ('requestIdleCallback' in window) {
							requestIdleCallback(() => requestSnapshotUpdate(vizStackContainer.id), { timeout: 1500 });
						} else {
							setTimeout(() => requestSnapshotUpdate(vizStackContainer.id), 600);
						}
					}
				}
			}, 50);

			emit(EVENTS.FILTERED_RESULTS_RENDERED, {
				containerId: container.id,
				clusterId: clusterId
			});
		}
        
        if (resultsCache[clusterId]) {
            updateUiWithResults(filteredResultsContainer, resultsCache[clusterId], clusterId, isUserAction);
            return;
        }
        
        const cacheIsland = document.getElementById('prefetched-data-cache');
        if (cacheIsland) {
            try {
                const snapshotCache = JSON.parse(cacheIsland.textContent);
                const cachedResults = snapshotCache[clusterId];
                if (cachedResults) {
                    resultsCache[clusterId] = cachedResults;
                    updateUiWithResults(filteredResultsContainer, cachedResults, clusterId, isUserAction);
                    return;
                }
            } catch (e) {
                console.warn("Could not parse prefetched data cache.", e);
            }
        }

        isLoading = true;
		filteredResultsContainer.innerHTML = '';
		filteredResultsContainer.appendChild(createSkeletonCards());
        
        try {
            const finalQueryVector = (typeof queryVector === 'string') ? JSON.parse(queryVector) : queryVector;
            const response = await fetch('/query/filtered-results', {
                method: 'POST',
                headers: { 
                    'Content-Type': 'application/json',
                    'X-XSRF-TOKEN': getCsrfToken() 
                },
                body: JSON.stringify({ queryVector: finalQueryVector, clusterId: clusterId })
            });
            
            if (!response.ok) throw new Error(`Server error: ${response.status}`);
			const results = await response.json();
			resultsCache[clusterId] = results;
			updateUiWithResults(filteredResultsContainer, results, clusterId, isUserAction);

		} catch (error) {
			console.error(`Error loading filtered results for ${paneId}:`, error);

			// Error Template einfügen und übersetzen
			filteredResultsContainer.innerHTML = '';
			const errTpl = getTemplate('tpl-results-error');
			if (errTpl) {
				applyGeneralTranslations(errTpl); 
				filteredResultsContainer.appendChild(errTpl);
			}
		} finally {
			isLoading = false;
		}
    }

	registerCleanup(() => {
		observer.disconnect();
		// Flag zurücksetzen, damit es beim Rückwärts-Navigieren neu initiiert werden kann
		if (contentPane) contentPane.dataset.loaderInitialized = 'false';
	});
}

export function initFilteredLoader() {
	const ctx = getContext();
	const neighborMap = ctx.neighborColorMap;
	const serendipityMap = ctx.serendipityColorMap;
	const queryVec = ctx.queryVector;
	if (!queryVec) return;
	initializeTopicTabs('neighbor-viz-content', neighborMap, queryVec);
	initializeTopicTabs('serendipity-viz-content', serendipityMap, queryVec);
}