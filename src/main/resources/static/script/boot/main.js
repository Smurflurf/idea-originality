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
import { initializeContext, getContext, isDataAvailable, getJobTitle } from '/script/core/context.js';
import { t, initializeLocalization, renderPage } from '/script/core/localization.js'; 
import { initFPSMonitor } from '/script/core/fps.js';

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
        
        // Nachladen der Listen (Tabs)
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
    clearAvailableTabs();
    disposeAllVisualizations(); // Zoom/Pan Listener entfernen

    // B. GLOBAL INIT (Läuft auf JEDER Seite)
    initializeTippy();
    initGlobalMenuListeners(); // EventBus Listener für Menü
    setupMenuInteractions();   // UI Logik (Click Handler)
    initializeTranslator();    // Übersetzer Button
    initThemeListener();       // Theme Logik
    initTTS();                 // TTS Logik

    initializeJsonToggles(container || document.body);
    initializeHierarchyToggles();
    if (container) initializeAbstractButtonsFor(container);
    
    initMath(container);

    // C. FEATURE LOADING (Data-Driven)
    // Wir lesen, welche Features die aktuelle Seite im HTML angefordert hat
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
        // Fallback für Seiten ohne Feature-Flags (z.B. statische Seiten)
        // Einfach nur Farben anwenden, damit es hübsch aussieht.
        if (!document.querySelector('.viz-stack-container')) {
            applyColorCoding();
        }
    }

    document.body.classList.remove('is-loading');
    window.appInitialized = true;
}


// ===================================================================
// 7. BOOTSTRAP
// ===================================================================

// Hard Reload
document.addEventListener('DOMContentLoaded', async () => {
	// 1. Context aus window.INITIAL_DATA laden (Schnittstelle zu Thymeleaf)
	// Wir übergeben INITIAL_DATA (falls vorhanden) oder undefined.
	// Wenn es undefined ist, springt die harvestLegacyGlobals() Logik in context.js an.
	initializeContext(window.INITIAL_DATA);
    
    // 2. Sprache laden
    let path = window.location.pathname;
    if (path.startsWith('/')) path = path.substring(1);
    let pageName = path.split('/')[0] || 'index';
    pageName = pageName.replace('.html', '');
    
    await initializeLocalization(['common', pageName]);

    // 3. App starten
    handlePageChange(document.body);
});

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