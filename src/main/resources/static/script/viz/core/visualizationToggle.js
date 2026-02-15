import { triggerPositionUpdateForViz } from '/script/viz/core/zoomAndPan.js';
import { applyColorCoding } from '/script/ui/base/colorCoder.js'; 


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

	// 3. Farben neu berechnen, da Elemente jetzt sichtbar sind (wichtig für CSS-Variablen Scope)
	// und ggf. neu geladene Inhalte
	requestAnimationFrame(() => {
		applyColorCoding();

		// Canvas Update wie gehabt
		const contentId = contentToActivate.id;
		let vizContainerId = null;
		if (contentId.includes('neighbor')) vizContainerId = 'viz-stack-container-nc';
		else if (contentId.includes('serendipity')) vizContainerId = 'viz-stack-container-serendipity';
		else if (contentId.includes('own')) vizContainerId = 'viz-stack-container-own';

		if (vizContainerId) triggerPositionUpdateForViz(vizContainerId);
	});

	
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
        pane._lastScrollY = pane.scrollTop;
        let touchStartY = 0;

        // Falls der Tab aktiv wird (durch Klick oder Swipe-Ende), 
        // sofort den aktuellen Scroll-Wert als Basis nehmen.
        const observer = new MutationObserver(() => {
            if (pane.classList.contains('active')) {
                pane._lastScrollY = pane.scrollTop;
            }
        });
        observer.observe(pane, { attributes: true, attributeFilter: ['class'] });

        // 1. TOUCH (Pull-to-reveal oben)
        pane.addEventListener('touchstart', (e) => {
            touchStartY = e.touches[0].clientY;
        }, { passive: true });

        pane.addEventListener('touchmove', (e) => {
            if (!pane.classList.contains('active')) return;
            const deltaY = e.touches[0].clientY - touchStartY;
            // Wenn oben und nach unten gezogen wird -> Header zeigen
            if (pane.scrollTop <= 0 && deltaY > 10) {
                root.classList.remove('is-header-hidden');
            }
        }, { passive: true });

        // 2. WHEEL (Zwei-Finger oben)
        pane.addEventListener('wheel', (e) => {
            if (!pane.classList.contains('active')) return;
            if (pane.scrollTop <= 0 && e.deltaY < 0) {
                root.classList.remove('is-header-hidden');
            }
        }, { passive: true });

        // 3. SCROLL LOGIK (Isoliert für diesen Tab)
        pane.addEventListener('scroll', function() {
            // NUR der aktive Tab darf den Header kontrollieren
            if (!this.classList.contains('active')) return;
            
            const currentY = this.scrollTop;
            const maxScroll = this.scrollHeight - this.clientHeight;
            
            // Hard-Check oben
            if (currentY <= 2) {
                root.classList.remove('is-header-hidden');
                this._lastScrollY = currentY;
                return;
            }

            // Puffer-Logik
            const diff = currentY - this._lastScrollY;
            const puffer = 10;

            // Verhindert Sprünge beim Tab-Wechsel
            if (Math.abs(diff) > 300) {
                this._lastScrollY = currentY;
                return;
            }

            if (currentY >= maxScroll - 5) return;

            if (diff > puffer) {
                // Runter -> Verstecken
                if (!root.classList.contains('is-header-hidden')) {
                    root.classList.add('is-header-hidden');
                }
                this._lastScrollY = currentY;
            } else if (diff < -puffer) {
                // Hoch -> Zeigen
                if (root.classList.contains('is-header-hidden')) {
                    root.classList.remove('is-header-hidden');
                }
                this._lastScrollY = currentY;
            }
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