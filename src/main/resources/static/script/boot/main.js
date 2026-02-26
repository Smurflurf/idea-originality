// ===================================================================
// 1. CSS (for vite)
// ===================================================================
import '/styling/style.css';
import '/styling/recorder.css';
import '/styling/queryPopup.css';
import '/styling/results.css';
import '/styling/tooltips.css';
import '/styling/visualize.css';
import '/styling/downloadPopup.css';

// ===================================================================
// 1. CORE IMPORTS & STATE
// ===================================================================
import { initializeContext, getContext, isDataAvailable, isOfflineMode, getJobTitle } from '/script/core/context.js';
import { t, initializeLocalization, renderPage } from '/script/core/localization.js'; 
import { initFPSMonitor } from '/script/core/fps.js';
import { initPrintHelper } from '/script/core/printHelper.js';
import { executeGlobalCleanup } from '/script/core/lifecycleManager.js';

// ===================================================================
// 2. UI & BASE IMPORTS
// ===================================================================
import { initializeTippy } from '/script/ui/base/tooltips.js';
import { initThemeListener } from '/script/ui/base/themeSwitch.js';
import { initializeJsonToggles } from '/script/ui/interaction/toggleButton.js';
import { initializeAbstractButtonsFor } from '/script/ui/interaction/toggleAbstractButton.js';
import { initializeHierarchyToggles } from '/script/ui/interaction/hierarchyToggle.js';
import { initializeCardOptimizer } from '/script/ui/base/cardOptimizer.js';
import { initializeColorCodingTriggers, applyColorCoding } from '/script/ui/base/colorCoder.js';
import { setupMenuInteractions, renderHistoryList, initGlobalMenuListeners } from '/script/ui/navigation/menu.js';
import { initializeTranslator } from '/script/features/accessibility/translate.js';
import { initTTS } from '/script/features/accessibility/tts.js';
import { initializeSelectionMode } from '/script/ui/interaction/selectionMode.js';
import { initViewportManager } from '/script/ui/base/handleViewport.js';

// ===================================================================
// 3. FEATURE IMPORTS (Queries, Media, Viz, Export)
// ===================================================================
import { initializeQueryButtonLogic } from '/script/features/query/queryButtonManager.js';
import { attachQueryListener } from '/script/features/query/handleQuery.js';
import { initializeMediaButtons } from '/script/features/media/mediaActions.js';
import { initializeDragAndDrop } from '/script/features/media/dragAndDrop.js';
import { initFilteredLoader } from '/script/features/query/filteredResultsLoader.js';

import { initializeVisualizationToggles, clearAvailableTabs } from '/script/viz/core/visualizationToggle.js';
import { disposeAllVisualizations } from '/script/viz/core/zoomAndPan.js';
import { initializeAllVisualizations } from '/script/viz/render/clickablePoints.js';
import { initializeAllCrosshairs } from '/script/viz/render/crosshairRenderer.js';
import { initializeOutlineRenderer } from '/script/viz/render/outlineRenderer.js';
import { initializeLabelRenderer } from '/script/viz/render/labelRenderer.js';
import { initializeExportButtons } from '/script/viz/core/exportVisualization.js';

import { initializeDownloadButton } from '/script/features/download/downloadManager.js';
import { initializePageCache } from '/script/data/pageCache.js';
import { saveJobToHistory } from '/script/data/idb-helper.js';


// ===================================================================
// 4. FEATURE REGISTRY
// Mappt Strings aus dem HTML 'data-features' Attribut auf Funktionen
// ===================================================================
const FEATURE_REGISTRY = {
    
    // Startseite: Suche, Upload, Aufnahme
    'search': async () => {
        console.log("[Main] Starting Feature: Search");
        initializeQueryButtonLogic();
        attachQueryListener();
        initFPSMonitor(); // Optional
        initializeMediaButtons();
        initializeDragAndDrop();
    },

    // Ergebnisseite: Visualisierung (Canvas, Zoom, Layer)
    'visualization': async () => {
        console.log("[Main] Starting Feature: Visualization");
        const context = getContext();

        // UI Toggles & Performance
        initializeVisualizationToggles();
        initializeCardOptimizer();
        initializeExportButtons();
        
        // Die eigentlichen Renderer
        initializeAllVisualizations(); // Punkte & Tooltips
        initializeColorCodingTriggers(); // Farben

        if (isDataAvailable()) {
            if (context.crosshairCoords) initializeAllCrosshairs();
            if (context.embeddingBounds) {
                // Outlines & Labels für alle 3 Layer
                ['own', 'nc', 'serendipity'].forEach(prefix => {
                    initializeOutlineRenderer(prefix);
                    initializeLabelRenderer(prefix);
                });
            }
        }
        
		initFilteredLoader();
    },

    // Ergebnisseite: Metadaten (History, Caching, Download)
    'results-meta': async () => {
        console.log("[Main] Starting Feature: Results Meta");
        const context = getContext();
        
        if (isDataAvailable() && context.jobId) {
            initializePageCache(context);

            const titleToSave = getJobTitle();
            if (titleToSave) document.title = `${titleToSave} | Ideenatlas`;
            
            // Speichern und Menü aktualisieren
            saveJobToHistory(context.jobId, titleToSave).then(() => {
                renderHistoryList();
            });
        }
        initializeDownloadButton();
    },		
	
	'legal-content': async (container) => {
		console.log("[Main] Starting Feature: Legal Content");
		applyColorCoding(); 

		// Den Namen der Seite aus der URL holen (z.B. "impressum")
		let path = window.location.pathname;
		if (path.startsWith('/')) path = path.substring(1);
		let pageName = path.split('/')[0] || 'index';
		pageName = pageName.replace('.html', '');

		// Container ID bauen: "impressum-content"
		const containerId = `${pageName}-content`;

		// Rendern!
		if (document.getElementById(containerId)) {
			renderPage(containerId);
		}
	}
};


// ===================================================================
// 5. MATH (KaTeX) Helper
// ===================================================================
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


// ===================================================================
// 6. MAIN ROUTING LOGIC
// ===================================================================
async function handlePageChange(container) {
	// A. GLOBAL CLEANUP
	executeGlobalCleanup(); 
	
	// B. DATEN UPDATE (WICHTIGSTE ÄNDERUNG)
	// Bei Soft-Navigation hat navigation.js das neue HTML mit dem Inline-Script ausgeführt.
	// Das hat window.INITIAL_DATA aktualisiert.
	// Aber unser Context-Modul hat noch den alten Stand. Wir müssen es zwingen, neu zu lesen.
	if (window.INITIAL_DATA) {
		initializeContext(window.INITIAL_DATA);
	}

	// C. VIZ CLEANUP
	clearAvailableTabs();
	disposeAllVisualizations();

	// D. GLOBAL INIT
	initializeTippy();
	initViewportManager();
	
	initGlobalMenuListeners(); // Singleton: Startet SoftNav & Swipe einmalig
	setupMenuInteractions();   // DOM: Bindet #menu-trigger auf der neuen Seite

	initializeTranslator();
	initializeSelectionMode();
	initThemeListener();
	initTTS();

	initializeJsonToggles(container || document.body);
	initializeHierarchyToggles();
	if (container) initializeAbstractButtonsFor(container);

	initMath(container);

	// E. FEATURE LOADING
	const featureString = document.body.getAttribute('data-features') || '';
	const features = featureString.split(',').map(s => s.trim()).filter(Boolean);

	if (features.length > 0) {
		for (const featureName of features) {
			if (FEATURE_REGISTRY[featureName]) {
				try {
					await FEATURE_REGISTRY[featureName](container);
				} catch (e) {
					console.error(`[Main] Error initializing feature '${featureName}':`, e);
				}
			} else {
				console.warn(`[Main] Unknown feature requested: ${featureName}`);
			}
		}
	} else {
		if (!document.querySelector('.viz-stack-container')) {
			applyColorCoding();
		}
	}

	document.body.classList.remove('is-loading');
	window.appInitialized = true;
}


// ===================================================================
// 7. BOOTSTRAP (Enhanced Safety)
// ===================================================================

async function bootstrapApp() {
	try {
		console.log("[Main] Bootstrapping Application...");

		// 1. Context Safety Check
		if (!window.INITIAL_DATA) {
			console.warn("[Main] No INITIAL_DATA found. Legacy harvest or waiting...");
		}
		initializeContext(window.INITIAL_DATA);

		// 2. Localization
		let path = window.location.pathname;
		if (path.startsWith('/')) path = path.substring(1);
		let pageName = path.split('/')[0] || 'index';
		pageName = pageName.replace('.html', '');
		
		await initializeLocalization(['common', pageName]);
		
		initPrintHelper(); 
				
		// 3. App starten
		await handlePageChange(document.body);
		
		console.log("[Main] Application started successfully.");

	} catch (e) {
		// Dieser Catch-Block ist lebenswichtig für Offline-Files!
		// Wenn ein Rest-Skript von Cloudflare hier reingrätscht, fangen wir es ab.
		console.error("[Main] CRITICAL BOOTSTRAP ERROR:", e);

		// Notfall-Maßnahme: Versuchen, UI trotzdem freizuschalten
		document.body.classList.remove('is-loading');
		alert("Ein Fehler ist beim Starten der Offline-Version aufgetreten. Einige Funktionen sind eventuell eingeschränkt.\n\nFehler: " + e.message);
	}
}

// Start-Logik
if (document.readyState === 'loading') {
	document.addEventListener('DOMContentLoaded', bootstrapApp);
} else {
	// Kurze Verzögerung, um sicherzugehen, dass alle Module geparst sind
	setTimeout(bootstrapApp, 0);
}

// SPA Navigation (Dynamic Content)
document.addEventListener('dynamicContentLoaded', (e) => {
    const container = e.detail?.container || document.body;
    
    // Check: Ist es nur ein kleines Update (z.B. Filter Tabs)?
    const isPartialUpdate = container.classList.contains('filtered-results-list') || container.closest('.viz-content-pane');

    if (isPartialUpdate) {
        console.log("[Main] Partial update detected.");
        initializeTippy();
        applyColorCoding();
        initializeAbstractButtonsFor(container);
        initMath(container);
        return; // Kein voller Reload nötig
    }

    // Voller Seitenwechsel innerhalb der SPA
    handlePageChange(container);
});