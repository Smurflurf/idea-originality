import { triggerPositionUpdateForViz } from '/script/viz/core/zoomAndPan.js';

// Globale Liste der Tabs, damit swipeNavigation darauf zugreifen kann
// Struktur: [ { btn: HTMLElement, content: HTMLElement }, ... ]
export let availableTabs = []; 

/**
 * Gibt den Index des aktuell sichtbaren Tabs zurück.
 */
export function getCurrentTabIndex() {
    return availableTabs.findIndex(t => t.content.classList.contains('active'));
}

/**
 * Setzt die Tab-Liste zurück.
 */
export function clearAvailableTabs() {
    availableTabs = [];
}

/**
 * Öffnet einen Tab anhand seines Index.
 * @param {number} index - Der Index (0 = Own, 1 = Neighbor, 2 = Serendipity)
 * @param {boolean} fromSwipe - true: Scroll-Position beibehalten. false: Nach oben scrollen.
 */
export function activateTabByIndex(index, fromSwipe = false) {
    if (index < 0 || index >= availableTabs.length) return;
    const target = availableTabs[index];
    activateView(target.btn, target.content, fromSwipe);
}

/**
 * Die interne Logik zum Umschalten der Ansicht.
 */
function activateView(btnToActivate, contentToActivate, fromSwipe) {
	// 1. Alle Tabs deaktivieren und verstecken
    availableTabs.forEach(t => {
        t.btn.classList.remove('active');
        t.content.classList.remove('active');
        t.content.style.display = 'none';
        // WICHTIG: Eventuelle Transform-Reste vom Swipen entfernen
        t.content.style.transform = ''; 
    });

    // 2. Ziel-Tab aktivieren
    btnToActivate.classList.add('active');
    
    contentToActivate.style.display = 'block';
    contentToActivate.style.transform = 'translateX(0)';
    contentToActivate.classList.add('active');

    // 3. Scroll-Verhalten steuern
    // Bei einem Klick auf den Header erwarten User, dass sie oben landen.
    // Bei einem Swipe soll die Position dort bleiben, wo sie war.
    //if (!fromSwipe) {
    //    contentToActivate.scrollTop = 0; 
    //}

    // 4. Canvas/Map Update anstoßen (Größe prüfen)
    const contentId = contentToActivate.id;
    let vizContainerId = null;
    
    if (contentId.includes('neighbor')) vizContainerId = 'viz-stack-container-nc';
    else if (contentId.includes('serendipity')) vizContainerId = 'viz-stack-container-serendipity';
    else if (contentId.includes('own')) vizContainerId = 'viz-stack-container-own';

    if (vizContainerId) {
        requestAnimationFrame(() => triggerPositionUpdateForViz(vizContainerId));
    }
}

// --- Helper für die Layer-Toggles (Punkte, Linien, Crosshair) ---

const setupToggle = (buttonId, layerId, isInitiallyActive = true) => {
    const button = document.getElementById(buttonId);
    const layer = document.getElementById(layerId);
    if (!button || !layer) { if(button) button.style.display = 'none'; return; }
    
    button.classList.toggle('active', isInitiallyActive);
    layer.style.display = isInitiallyActive ? 'block' : 'none';
    
    button.addEventListener('click', () => {
        const isActive = button.classList.toggle('active');
        layer.style.display = isActive ? 'block' : 'none';
    });
};

const setupGroupToggle = (buttonId, layerIds) => {
    const button = document.getElementById(buttonId);
    if (!button) return;
    const layers = layerIds.map(id => document.getElementById(id)).filter(Boolean);
    if (layers.length === 0) { button.style.display = 'none'; return; }
    
    const isInitiallyActive = false;
    button.classList.toggle('active', isInitiallyActive);
    layers.forEach(layer => layer.style.display = 'none');
    
    button.addEventListener('click', () => {
        const isActive = button.classList.toggle('active');
        layers.forEach(layer => layer.style.display = isActive ? 'block' : 'none');
    });
};

// --- Initialisierung ---

function initializeScrollHiding() {
	const root = document.body;

    document.querySelectorAll('.viz-content-pane').forEach(pane => {
        let lastScrollY = 0;
        pane.addEventListener('scroll', function() {
            if (this.style.display === 'none') return;
            
            const currentY = this.scrollTop;
            
            // --- FIX 1: Ganz oben (oder beim "Pull Down" im negativen Bereich) ---
            // Wir prüfen das ALLES ERSTES. Wenn wir <= 0 sind, muss der Header da sein.
            if (currentY <= 0) {
                root.classList.remove('is-header-hidden');
                // Wichtig: Wir aktualisieren lastScrollY, damit der Übergang 
                // von "Bounce" (-10px) zu "Oben" (0px) glatt läuft.
                lastScrollY = currentY;
                return;
            }

            // --- FIX 2: Ganz unten (Rubber-Banding ignorieren) ---
            // Verhindert das Teleport-Problem.
            const maxScroll = this.scrollHeight - this.clientHeight;
            if (currentY >= maxScroll - 10) {
                lastScrollY = currentY;
                return;
            }

            // --- Normale Scroll-Logik ---
			const puffer = 5;
			if (currentY > lastScrollY + puffer) {
                // Runterscrollen -> Header verstecken
				root.classList.add('is-header-hidden');
			} else if (currentY < lastScrollY - puffer) {
                // Hochscrollen -> Header zeigen
				root.classList.remove('is-header-hidden');
			}
            lastScrollY = currentY;
        }, { passive: true });
    });
}


export function initializeVisualizationToggles() {
    // 1. Referenzen holen
    const btnOwn = document.getElementById('show-own-viz');
    const btnNeighbor = document.getElementById('show-neighbor-viz');
    const btnSerendipity = document.getElementById('show-serendipity-viz');
    
    const contentOwn = document.getElementById('own-viz-content');
    const contentNeighbor = document.getElementById('neighbor-viz-content');
    const contentSerendipity = document.getElementById('serendipity-viz-content');

    // 2. Array befüllen (Reihenfolge ist wichtig für Swipe-Logik!)
    availableTabs = [];
    if (btnOwn && contentOwn) availableTabs.push({ btn: btnOwn, content: contentOwn });
	if (btnNeighbor && contentNeighbor) availableTabs.push({ btn: btnNeighbor, content: contentNeighbor });
	if (btnSerendipity && contentSerendipity) availableTabs.push({ btn: btnSerendipity, content: contentSerendipity });

	// 3. Click Listener für die Haupt-Tabs
	availableTabs.forEach((tab, index) => {
		tab.btn.addEventListener('click', () => {
			if (tab.content.classList.contains('active')) {
				// Wenn schon aktiv -> Hochscrollen (smooth)
				tab.content.scrollTo({ top: 0, behavior: 'smooth' });
			} else {
				activateTabByIndex(index, false);
			}
		});
	});

    // 4. Listener für die Visualisierungs-Layer (Punkte, Linien etc.)
    ['own', 'nc', 'serendipity'].forEach(prefix => { 
        setupToggle(`viz-toggle-${prefix}-crosshair`, `viz-layer-${prefix}-crosshair-canvas`);
        setupToggle(`viz-toggle-${prefix}-points`, `viz-layer-${prefix}-points`);
        
        // Neighbors-Button ist nur bei 'own' per Default an
        const neighborsButton = document.getElementById(`viz-toggle-${prefix}-neighbors`);
        if (neighborsButton) {
            setupToggle(`viz-toggle-${prefix}-neighbors`, `dynamic-points-${prefix}-neighbors`, (prefix === 'own'));
        }

		setupToggle(`viz-toggle-${prefix}-outlines`, `g-outlines-${prefix}-main`);
		setupToggle(`viz-toggle-${prefix}-labels`, `g-labels-${prefix}-main`, false);
		setupGroupToggle(`viz-toggle-${prefix}-context`, [`g-outlines-${prefix}-context`, `g-labels-${prefix}-context`]);
	});

	// Scroll-Logik starten
	initializeScrollHiding();
}