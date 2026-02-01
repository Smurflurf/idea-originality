// ./boot/main.js
import { initializeContext, getContext, isDataAvailable, getJobTitle } from '/script/core/context.js';
import { initializeTippy } from '/script/ui/base/tooltips.js';
import { initThemeListener } from '/script/ui/base/themeSwitch.js'; // Umbenannt!
import { initializeJsonToggles } from '/script/ui/interaction/toggleButton.js';
import { initializeAbstractButtonsFor } from '/script/ui/interaction/toggleAbstractButton.js';
import { initializeHierarchyToggles } from '/script/ui/interaction/hierarchyToggle.js';
import { initializeCardOptimizer } from '/script/ui/base/cardOptimizer.js';
import { initializeVisualizationToggles, clearAvailableTabs } from '/script/viz/core/visualizationToggle.js';
import { initializeColorCodingTriggers, applyColorCoding } from '/script/ui/base/colorCoder.js';
import { initializeDownloadButton } from '/script/features/download/downloadManager.js';
import { initializeAllCrosshairs } from '/script/viz/render/crosshairRenderer.js';
import { initializeOutlineRenderer } from '/script/viz/render/outlineRenderer.js';
import { initializeLabelRenderer } from '/script/viz/render/labelRenderer.js';
import { saveJobToHistory } from '/script/data/idb-helper.js';
import { setupMenuInteractions, renderHistoryList, initGlobalMenuListeners } from '/script/ui/navigation/menu.js';
import { disposeAllVisualizations } from '/script/viz/core/zoomAndPan.js';
import { initializeQueryButtonLogic } from '/script/features/query/queryButtonManager.js';
import { t, initializeLocalization } from '/script/core/localization.js';
import { initFPSMonitor } from '/script/core/fps.js'; // Falls exportiert
import { initializeMediaButtons } from '/script/features/media/mediaActions.js';
import { initializeDragAndDrop } from '/script/features/media/dragAndDrop.js';
import { attachQueryListener } from '/script/features/query/handleQuery.js';
import { initFilteredLoader } from '/script/features/query/filteredResultsLoader.js';
import { initializeExportButtons } from '/script/viz/core/exportVisualization.js';
import { initializeTranslator } from '/script/features/accessibility/translate.js';
import { initializeAllVisualizations } from '/script/viz/render/clickablePoints.js';
import { initializePageCache } from '/script/data/pageCache.js';
import { initTTS } from '/script/features/accessibility/tts.js';

// NEU: Hilfsfunktion für Math (KaTeX)
function initMath(container) {
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
}

/**
 * ZENTRALE ROUTING-LOGIK
 */
async function handlePageChange(container) {
    const context = getContext(); // State ist schon initialisiert (siehe unten)

	// 0. CLEANUP
	clearAvailableTabs();

    // 1. CORE & UI INITIALISIEREN (Immer)
	initializeTippy();
    initGlobalMenuListeners(); // Events für Menü
	setupMenuInteractions();   // UI für Menü
    initializeTranslator();    // Übersetzer Button Events
    initThemeListener();       // Theme Switcher Events
    initTTS();                 // Text-to-Speech Events

	initializeJsonToggles(document.body);
	initializeHierarchyToggles();
	if (container) initializeAbstractButtonsFor(container);

    initMath(container); // Math Formeln rendern

	// ===================================================================
	// 2. SEITEN-SPEZIFISCHE LOGIK (Feature-Flags wären hier cool für Tag 3)
	// ===================================================================

	// FALL A: Startseite (Suche)
	if (document.querySelector('.idea-form')) {
		console.log("[Main] Init: Search Page");
		initializeQueryButtonLogic();
		attachQueryListener();
		initFPSMonitor(); // Optional
        initializeMediaButtons(); // Upload/Record
        initializeDragAndDrop();  // D&D
	}

	// FALL B: Ergebnisseite (Visualisierung)
	const vizContainer = document.querySelector('.viz-stack-container');
    const hasData = isDataAvailable();

	if (vizContainer || hasData) {
		console.log("[Main] Init: Results Page");
		disposeAllVisualizations();

		if (hasData && context.jobId) {
			initializePageCache(context); // Achtung: context statt dataObj übergeben oder anpassen

			const titleToSave = getJobTitle();
			if (titleToSave) document.title = `${titleToSave} | Ideenatlas`;
			
            saveJobToHistory(context.jobId, titleToSave).then(() => {
				renderHistoryList();
			});
		}

		initializeDownloadButton();
		initializeVisualizationToggles();
		initializeCardOptimizer();
		initializeExportButtons(); // NEU: Export Button

        // Viz Initialisierung
		initializeAllVisualizations(); 
		initializeColorCodingTriggers();

		if (hasData && vizContainer) {
			if (context.crosshairCoords) initializeAllCrosshairs();
			if (context.embeddingBounds) {
				initializeOutlineRenderer('own');
				initializeOutlineRenderer('nc');
				initializeOutlineRenderer('serendipity');
				initializeLabelRenderer('own');
				initializeLabelRenderer('nc');
				initializeLabelRenderer('serendipity');
			}
		}
        
        // Filtered Results Tabs Logik (war früher auto-run)
        initFilteredLoader();
	}
	
	// Fallback Color Coding (z.B. für Impressum/Legal Pages)
	if (!document.querySelector('.idea-form') && !(vizContainer || hasData)) {
        applyColorCoding();
	}

	document.body.classList.remove('is-loading');
	window.appInitialized = true;
}


// --- BOOTSTRAP LOGIK ---

// Diese Logik ersetzt die alten Listener in menu.js und main.js
document.addEventListener('DOMContentLoaded', async () => {
    // 1. Context laden
    if (window.INITIAL_DATA) {
        initializeContext(window.INITIAL_DATA);
    }
    
    // 2. Sprache laden (ersetzt die Logik aus menu.js)
    let path = window.location.pathname;
    if (path.startsWith('/')) path = path.substring(1);
    let pageName = path.split('/')[0] || 'index';
    pageName = pageName.replace('.html', '');
    
    await initializeLocalization(['common', pageName]);

    // 3. Page Starten
    handlePageChange(document.body);
});

// Listener für dynamische Navigation (SPA)
document.addEventListener('dynamicContentLoaded', (e) => {
	const container = e.detail?.container || document.body;
	const isPartialUpdate = container.classList.contains('filtered-results-list') || container.closest('.viz-content-pane');

	if (isPartialUpdate) {
		console.log("[Main] Partial update detected.");
		initializeTippy();
		applyColorCoding();
		initializeAbstractButtonsFor(container);
		return;
	}

	handlePageChange(container);
});