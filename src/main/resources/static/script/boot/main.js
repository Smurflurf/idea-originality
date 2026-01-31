import '/styling/style.css';
import '/styling/recorder.css';
import '/styling/queryPopup.css';
import '/styling/results.css';
import '/styling/tooltips.css';
import '/styling/visualize.css';
import '/styling/downloadPopup.css';

import { initializeTippy } from '/script/ui/base/tooltips.js';
import { initializePageCache } from '/script/data/pageCache.js';
import { initializeJsonToggles } from '/script/ui/interaction/toggleButton.js';
import { initializeAbstractButtonsFor } from '/script/ui/interaction/toggleAbstractButton.js';
import { initializeHierarchyToggles } from '/script/ui/interaction/hierarchyToggle.js';
import { initializeCardOptimizer } from '/script/ui/base/cardOptimizer.js';
import { initializeAllVisualizations } from '/script/viz/render/clickablePoints.js';
import { initializeVisualizationToggles, clearAvailableTabs } from '/script/viz/core/visualizationToggle.js';
import { initializeColorCodingTriggers, applyColorCoding } from '/script/ui/base/colorCoder.js';
import { initializeDownloadButton } from '/script/features/download/downloadManager.js';
import { initializeAllCrosshairs } from '/script/viz/render/crosshairRenderer.js';
import { initializeOutlineRenderer } from '/script/viz/render/outlineRenderer.js';
import { initializeLabelRenderer } from '/script/viz/render/labelRenderer.js';
import { saveJobToHistory } from '/script/data/idb-helper.js';
import { setupMenuInteractions, renderHistoryList } from '/script/ui/navigation/menu.js';
import { disposeAllVisualizations } from '/script/viz/core/zoomAndPan.js';
import { initializeQueryButtonLogic } from '/script/features/query/queryButtonManager.js';
import { t } from '/script/core/localization.js';
import { initFPSMonitor } from '/script/core/fps.js';

import '/script/features/media/mediaActions.js';
import '/script/features/media/dragAndDrop.js';
import '/script/features/media/attachmentManager.js';
import { attachQueryListener } from '/script/features/query/handleQuery.js';

import '/script/features/query/filteredResultsLoader.js';
import '/script/viz/core/exportVisualization.js';

/**
 * ZENTRALE ROUTING-LOGIK
 */
function handlePageChange(container) {
	const body = document.body;

	// ===================================================================
	// 0. ZUSTAND AUFRÄUMEN
	// ===================================================================
	clearAvailableTabs();

	// ===================================================================
	// 1. GLOBALE FUNKTIONEN (werden auf JEDER Seite ausgeführt)
	// ===================================================================
	initializeTippy();
	setupMenuInteractions(); // WICHTIG: Initialisiert Menü & Lokalisierung
	initializeJsonToggles(body);
	initializeHierarchyToggles();
	if (container) initializeAbstractButtonsFor(container);

	// KaTeX für mathematische Formeln rendern, falls vorhanden
	if (typeof renderMathInElement === 'function') {
		try {
			renderMathInElement(container || document.body, {
				delimiters: [
					{ left: '$$', right: '$$', display: true },
					{ left: '$', right: '$', display: false }
				],
				throwOnError: false
			});
		} catch (e) { console.warn("KaTeX render issue:", e); }
	}

	// ===================================================================
	// 2. SEITEN-SPEZIFISCHE LOGIK (Routing)
	// ===================================================================

	// FALL A: Startseite (index.html)
	if (document.querySelector('.idea-form')) {
		console.log("Spezifische Logik für Index-Seite wird ausgeführt.");
		initializeQueryButtonLogic();
		attachQueryListener();
		initFPSMonitor();
	}

	// FALL B: Ergebnisseite (results.html)
	const vizContainer = document.querySelector('.viz-stack-container');
	const dataObj = window.INITIAL_DATA;
	const hasData = dataObj && dataObj.IS_DATA_AVAILABLE;

	if (vizContainer || hasData) {
		console.log("Spezifische Logik für Ergebnisseite wird ausgeführt.");
		disposeAllVisualizations();

		if (hasData && dataObj.JOB_ID) {
			initializePageCache(dataObj);

			if (dataObj.JOB_TITLE) {
				document.title = `${dataObj.JOB_TITLE} | Ideenatlas`;
			}
			const titleToSave = dataObj.JOB_TITLE || t('global.analysis') || 'Analysis';
			saveJobToHistory(dataObj.JOB_ID, titleToSave).then(() => {
				// Menü sofort aktualisieren, damit der Eintrag erscheint
				renderHistoryList();
			});
		}

		initializeDownloadButton();
		initializeVisualizationToggles();
		initializeCardOptimizer();
		initializeAllVisualizations();
		initializeColorCodingTriggers();

		if (window.IS_DATA_AVAILABLE && vizContainer) {
			if (window.CROSSHAIR_COORDS) initializeAllCrosshairs();
			if (window.EMBEDDING_BOUNDS) {
				initializeOutlineRenderer('own');
				initializeOutlineRenderer('nc');
				initializeOutlineRenderer('serendipity');
				initializeLabelRenderer('own');
				initializeLabelRenderer('nc');
				initializeLabelRenderer('serendipity');
			}
		}
	}
	
	// Fallback für Seiten ohne spezifische Logik, die aber trotzdem Farben brauchen könnten
	if (!document.querySelector('.idea-form') && !(vizContainer || hasData)) {
		if (typeof applyColorCoding === 'function') {
			applyColorCoding();
		}
	}

	document.body.classList.remove('is-loading');
	window.appInitialized = true;
}

// ------------------------------------------------------------------
// EVENT HANDLERS
// ------------------------------------------------------------------

document.addEventListener('dynamicContentLoaded', (e) => {
	const container = e.detail?.container || document.body;

	const isPartialUpdate = container.classList.contains('filtered-results-list') ||
		container.closest('.viz-content-pane');

	if (isPartialUpdate) {
		console.log("[Main] Partial update detected. Skipping heavy initialization.");

		// Führe NUR leichte Aufgaben aus, die für neue Elemente nötig sind
		initializeTippy();

		// Farben anwenden (wichtig für die neuen Cards)
		if (typeof applyColorCoding === 'function') {
			applyColorCoding();
		}

		// "Abstract expand" Buttons für den neuen Container
		initializeAbstractButtonsFor(container);

		return; // HIER ABBRECHEN, damit Zoom/Pan nicht gekillt werden!
	}

	// Nur bei echtem Seitenwechsel oder Body-Update alles neu laden
	handlePageChange(container);

});

if (document.readyState === 'loading') {
	document.addEventListener('DOMContentLoaded', () => {
		handlePageChange(document.body);
	});
} else {
	setTimeout(() => {
		handlePageChange(document.body);
	}, 0);
}
