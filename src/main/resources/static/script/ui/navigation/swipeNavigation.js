import { availableTabs, getCurrentTabIndex, activateTabByIndex } from '/script/viz/core/visualizationToggle.js';
import { triggerPositionUpdateForViz } from '/script/viz/core/zoomAndPan.js';

// --- KONFIGURATION ---
const CONFIG = {
	threshold: 0.15,
	edgeZone: 20,
	menuWidth: 280,
	rubberBandFactor: 0.4,
	shadowDistanceDivisor: 100,
	maxShadowOpacity: 0.6,
	animDuration: 100
};

let state = {
	isDragging: false,
	startX: 0,
	startY: 0,
	currentX: 0,
	currentY: 0,
	direction: null,
	mode: null,
	activeTabIndex: -1,
	containerWidth: 0,
	menuEl: null,
	overlayEl: null,
	leftShadow: null,
	rightShadow: null
};

let cleanupTimer = null;
let isAnimating = false;
let tabHidingTimeoutId = null;
let isInitialized = false;


// --- INIT ---
export function initializeSwipeNavigation() {
	if (isInitialized) return;
	createShadowElements();
	document.body.style.touchAction = 'pan-y pinch-zoom';
	document.body.addEventListener('touchstart', handleStart, { passive: false });
	document.body.addEventListener('touchmove', handleMove, { passive: false });
	document.body.addEventListener('touchend', handleEnd);
	document.body.addEventListener('touchcancel', handleEnd); 

	isInitialized = true;
}

function createShadowElements() {
	if (!document.querySelector('.swipe-shadow-left')) {
		const l = document.createElement('div');
		l.className = 'swipe-shadow-overlay swipe-shadow-left';
		document.body.appendChild(l);
	}
	if (!document.querySelector('.swipe-shadow-right')) {
		const r = document.createElement('div');
		r.className = 'swipe-shadow-overlay swipe-shadow-right';
		document.body.appendChild(r);
	}
}

// --- HELPER ---
function applyRubberBand(diff, dimension) {
	if (dimension === 0) return 0;
	return (1.0 - (1.0 / ((Math.abs(diff) * CONFIG.rubberBandFactor / dimension) + 1.0))) * dimension * Math.sign(diff);
}

function setShadowIntensity(shadowEl, amount) {
	if (!shadowEl) return;
	const progress = Math.min(Math.abs(amount) / CONFIG.shadowDistanceDivisor, 1);
	const opacity = Math.pow(progress, 2) * CONFIG.maxShadowOpacity;
	shadowEl.style.opacity = opacity;
}

// --- PHASE 1: START ---
function handleStart(e) {
	if (state.isDragging) return;
	
	if (isAnimating) {
		// 1. Breche alle geplanten Aufräum-Aktionen des vorherigen Swipes ab.
		if (cleanupTimer) clearTimeout(cleanupTimer);
		if (tabHidingTimeoutId) clearTimeout(tabHidingTimeoutId);
		cleanupTimer = null;
		tabHidingTimeoutId = null;

		// 2. "Friere" die laufende CSS-Animation sofort ein, indem wir die Transition entfernen.
		//    Die Elemente bleiben genau dort stehen, wo sie gerade sind.
		if (availableTabs.length > 0) {
			availableTabs.forEach(t => {
				// WICHTIG: Die aktuelle Position auslesen und als inline-style setzen,
				// bevor wir die Transition entfernen.
				const currentTransform = window.getComputedStyle(t.content).transform;
				t.content.style.transform = currentTransform;
				t.content.style.transition = 'none';
			});
		}

		// 3. Signalisiere, dass die Animation vorbei ist und ein neuer Drag beginnen kann.
		isAnimating = false;
	}


	if (tabHidingTimeoutId) {
		clearTimeout(tabHidingTimeoutId);
		tabHidingTimeoutId = null;
	}

	const isPopupOpen = document.querySelector('.recorder-overlay.is-visible') ||
		document.querySelector('.download-popup-overlay.is-visible');
	if (isPopupOpen) return;

	const ignoreSelector = 'input, textarea, .history-link, .topic-tabs-scroller, .attachments-container, .viz-stack-container.is-scroll-zoom-active';

	if (e.target.closest(ignoreSelector)) {
		return;
	}

	if (e.touches.length !== 1) return;
	
	const touch = e.touches[0];

	if (cleanupTimer) {
		clearTimeout(cleanupTimer);
		cleanupTimer = null;
	}

	state = {
		isDragging: true,
		activeTouchId: touch.identifier,
		startX: touch.clientX,
		startY: touch.clientY,
		currentX: touch.clientX,
		currentY: touch.clientY,
		direction: null,
		mode: null,
		activeTabIndex: (typeof getCurrentTabIndex === 'function') ? getCurrentTabIndex() : -1,
		containerWidth: window.innerWidth,
		resultsContainer: document.querySelector('.results-container'),
		menuEl: document.getElementById('sidebar-menu'),
		overlayEl: document.getElementById('menu-overlay'),
		leftShadow: document.querySelector('.swipe-shadow-left'),
		rightShadow: document.querySelector('.swipe-shadow-right'),
		stylesPrepared: false
	};
}

// --- PHASE 2: MOVE ---
function setSwipeLock(locked) {
    if (locked) document.body.classList.add('is-swiping-active');
    else document.body.classList.remove('is-swiping-active');
}

function handleMove(e) {
    if (!state.isDragging) return;

    const touch = Array.from(e.changedTouches).find(t => t.identifier === state.activeTouchId);
    if (!touch) return;

    // Aktuelle Position
    const currentX = touch.clientX;
    const currentY = touch.clientY;

    // Totale Distanz seit Start
    const totalDeltaX = currentX - state.startX;
    const totalDeltaY = currentY - state.startY;

    // --- NEU: DER "PRE-CHECK" ---
    // Wir prüfen sofort den Winkel. Ist die Bewegung eher horizontal?
    // Dann verbieten wir dem Browser SOFORT jegliches Scrollen.
    // Wir warten NICHT auf die 10px Schwelle für diesen Block.
    if (Math.abs(totalDeltaX) > Math.abs(totalDeltaY)) {
        if (e.cancelable) e.preventDefault();
    }
    // ----------------------------

    state.currentX = currentX;
    state.currentY = currentY;

    // 1. Richtung bestimmen (erst ab Schwelle, damit wir nicht bei jedem Zittern den Modus locken)
    if (!state.direction) {
        // Schwelle erreicht?
        if (Math.abs(totalDeltaX) > 10 || Math.abs(totalDeltaY) > 10) {
            
            if (Math.abs(totalDeltaX) > Math.abs(totalDeltaY)) {
                // EINDEUTIG HORIZONTAL
                state.direction = 'horizontal';
                setSwipeLock(true); // CSS Lock aktivieren
                prepareStylesForSwipe();
            } else {
                // EINDEUTIG VERTIKAL
                state.direction = 'vertical';
                state.isDragging = false; // Wir klinken uns aus
                cleanupStyles();
                return; // Browser darf weitermachen (scrollen)
            }
        }
    }

    // 2. Ausführung (Nur wenn wir sicher im Horizontal-Modus sind)
    if (state.direction === 'horizontal') {
        // Doppelte Sicherheit
        if (e.cancelable) e.preventDefault();
        
        // Safety check
        if (!state.stylesPrepared) prepareStylesForSwipe();

        if (!state.mode) {
             const isMenuOpen = state.menuEl && state.menuEl.classList.contains('is-open');
             const isEdgeSwipe = state.startX < CONFIG.edgeZone;

             if (isMenuOpen) {
                 state.mode = 'MENU_CLOSING';
             } else if (totalDeltaX > 0 && (state.activeTabIndex <= 0 || isEdgeSwipe)) {
                 state.mode = 'MENU_OPENING';
             } else if (availableTabs.length > 0) {
                 state.mode = 'TABS';
                 prepareAllTabs();
             }
        }

        executeSwipe(totalDeltaX);
    }
}

function prepareStylesForSwipe() {
    if (state.stylesPrepared) return;
    
    if (state.menuEl) state.menuEl.style.transition = 'none';
    if (state.overlayEl) state.overlayEl.style.transition = 'none';

    if (availableTabs.length > 0) {
        availableTabs.forEach(t => {
            t.content.style.transition = 'none';
        });
    }
    state.stylesPrepared = true;
}

function prepareAllTabs() {
    // Header-Höhe messen
    let headerHeight = 0;
    if (state.resultsContainer) {
        const header = state.resultsContainer.querySelector('.viz-toggle-header');
        headerHeight = header ? header.offsetHeight : 0;
        
        // Container einfrieren
        const currentHeight = state.resultsContainer.getBoundingClientRect().height;
        state.resultsContainer.style.height = `${currentHeight}px`;
        state.resultsContainer.classList.add('is-swiping');
    }

    // Alle Tabs durchgehen
    availableTabs.forEach((t, i) => {
        const isNeighbor = Math.abs(i - state.activeTabIndex) <= 1;
        
        if (!isNeighbor) {
            t.content.style.display = 'none';
        } else {
            // 1. Sichtbar machen und positionieren
            t.content.style.display = 'block';
            t.content.style.position = 'absolute';
            t.content.style.top =  0; //`${headerHeight}px`;
            t.content.style.left = '0';
            t.content.style.width = '100%';
            t.content.style.height = '100%'; //`calc(100% - ${headerHeight}px)`;
            t.content.style.overflowY = 'hidden'; 
            
            // 2. FORCED REFLOW (Layout berechnen lassen)
            // Das ist entscheidend, damit width/height für Canvas bekannt sind
            void t.content.offsetHeight; 

            // 3. Visualisierung updaten (Outlines, Punkte, Wrapper-Position)
            const vizContainer = t.content.querySelector('.viz-stack-container');
            if (vizContainer) {
                // Trigger update in zoomAndPan.js
                triggerPositionUpdateForViz(vizContainer.id);

                // FIX: Crosshair explizit einmal neu zeichnen, da der MutationObserver
                // manchmal zu langsam reagiert, wenn display von none auf block wechselt.
                // Wir suchen den Toggle-Button für diesen Container
                let prefix = 'own';
                if (vizContainer.id.includes('nc')) prefix = 'nc';
                if (vizContainer.id.includes('serendipity')) prefix = 'serendipity';
                
                // Wir feuern manuell einen Redraw, falls das Canvas existiert
                const crosshairCanvas = document.getElementById(`viz-layer-${prefix}-crosshair-canvas`);
                if (crosshairCanvas && crosshairCanvas.style.display !== 'none') {
                     // Kleiner Hack: Wir triggern ein Resize-Event auf dem Window oder
                     // rufen die Logik auf, aber am einfachsten ist es, dem Observer
                     // im CrosshairRenderer Futter zu geben, indem wir kurz was am Wrapper ändern
                     // ODER wir verlassen uns darauf, dass triggerPositionUpdateForViz den Wrapper-Style ändert.
                     // Da wir redraw() in Schritt 1 gefixt haben, sollte triggerPositionUpdateForViz reichen!
                }
            }
        }
        
        const dist = Math.abs(i - state.activeTabIndex);
        t.content.style.zIndex = 10 - dist;
    });
}

function executeSwipe(deltaX) {
	if (state.mode === 'MENU_OPENING') {
		let translate = -CONFIG.menuWidth + deltaX;
		if (translate > 0) { setShadowIntensity(state.leftShadow, translate); translate = 0; } else { setShadowIntensity(state.leftShadow, 0); }
		if (state.menuEl) {
			state.menuEl.style.transform = `translateX(${translate}px)`;
			if (state.overlayEl) {
				const rawProgress = (CONFIG.menuWidth + translate) / CONFIG.menuWidth;
				state.overlayEl.style.visibility = 'visible'; state.overlayEl.style.display = 'block'; state.overlayEl.style.opacity = Math.min(1, Math.max(0, rawProgress));
			}
		}
	} else if (state.mode === 'MENU_CLOSING') {
		let translate = deltaX;
		if (translate > 0) { setShadowIntensity(state.leftShadow, translate); translate = 0; } else { setShadowIntensity(state.leftShadow, 0); }
		if (state.menuEl) {
			state.menuEl.style.transform = `translateX(${translate}px)`;
			if (state.overlayEl) { state.overlayEl.style.opacity = 1 - Math.min(1, Math.max(0, Math.abs(translate) / CONFIG.menuWidth)); }
		}
	}
	else if (state.mode === 'TABS') {
		const width = state.containerWidth;
		const idx = state.activeTabIndex;
		let globalOffset = deltaX; // Beginne mit dem normalen Swipe-Abstand

		// Prüfe, ob wir am Anfang oder Ende sind und nach außen swipen
		if ((idx === 0 && deltaX > 0) || (idx === availableTabs.length - 1 && deltaX < 0)) {
			// Wende den Rubber-Band-Effekt auf den globalen Offset an
			globalOffset = applyRubberBand(deltaX, width);

			// Schattenlogik bleibt hier
			if (deltaX > 0) setShadowIntensity(state.leftShadow, deltaX);
			else setShadowIntensity(state.rightShadow, deltaX);
		} else {
			// Im normalen Bereich keine Schatten
			setShadowIntensity(state.leftShadow, 0);
			setShadowIntensity(state.rightShadow, 0);
		}

		// Diese Schleife wird jetzt IMMER ausgeführt und bewegt ALLE sichtbaren Tabs korrekt
		availableTabs.forEach((tab, i) => {
			const basePos = (i - state.activeTabIndex) * width; // Grundposition (0, -width, width)
			tab.content.style.transform = `translateX(${basePos + globalOffset}px)`;
		});
	} else {
		if (deltaX > 0) { setShadowIntensity(state.leftShadow, deltaX); } else { setShadowIntensity(state.rightShadow, deltaX); }
	}
}

// --- PHASE 3: END ---
function handleEnd(e) {
	if (!state.isDragging) return;
	const touch = Array.from(e.changedTouches).find(t => t.identifier === state.activeTouchId);
	if (!touch) return;
	state.isDragging = false;
	state.activeTouchId = null;

	const deltaX = state.currentX - state.startX;


	isAnimating = true;

	const easing = `transform ${CONFIG.animDuration}ms cubic-bezier(0.2, 0.8, 0.2, 1), opacity ${CONFIG.animDuration}ms ease`;

	if (state.menuEl) state.menuEl.style.transition = easing;
	if (state.overlayEl) state.overlayEl.style.transition = easing;
	if (state.leftShadow) { state.leftShadow.style.transition = `opacity ${CONFIG.animDuration}ms ease-out`; state.leftShadow.style.opacity = '0'; }
	if (state.rightShadow) { state.rightShadow.style.transition = `opacity ${CONFIG.animDuration}ms ease-out`; state.rightShadow.style.opacity = '0'; }
	if (availableTabs.length > 0) { availableTabs.forEach(t => t.content.style.transition = easing); }

	let actionTaken = false;

	if (state.mode === 'MENU_OPENING') {
		actionTaken = true; if (deltaX > CONFIG.menuWidth * 0.3) openMenu(); else closeMenu();
	} else if (state.mode === 'MENU_CLOSING') {
		actionTaken = true; if (deltaX < -50) closeMenu(); else openMenu();
	} else if (state.mode === 'TABS') {
		actionTaken = true;
		const threshold = state.containerWidth * CONFIG.threshold;
		if (deltaX < -threshold && state.activeTabIndex < availableTabs.length - 1) { finalizeTabSwitch(state.activeTabIndex + 1); }
		else if (deltaX > threshold && state.activeTabIndex > 0) { finalizeTabSwitch(state.activeTabIndex - 1); }
		else { resetTabs(); }
	}

	if (!actionTaken) { cleanupStyles(); isAnimating = false; return; }

	cleanupTimer = setTimeout(() => { cleanupStyles(); isAnimating = false; cleanupTimer = null; }, CONFIG.animDuration + 50);
}

// --- ACTIONS ---
function openMenu() {
	if (!state.menuEl) return;
	state.menuEl.style.transform = 'translateX(0)';
	if (state.overlayEl) { state.overlayEl.style.visibility = 'visible'; state.overlayEl.style.display = 'block'; state.overlayEl.style.opacity = '1'; }
	if (!state.menuEl.classList.contains('is-open')) { const trigger = document.getElementById('menu-trigger'); if (trigger) trigger.click(); }
}
function closeMenu() {
	if (!state.menuEl) return;
	state.menuEl.style.transform = 'translateX(-100%)';
	if (state.overlayEl) state.overlayEl.style.opacity = '0';
	if (state.menuEl.classList.contains('is-open')) { const closeBtn = document.getElementById('menu-close-btn'); if (closeBtn) closeBtn.click(); }
}
function finalizeTabSwitch(newIndex) {
    const width = state.containerWidth;
    availableTabs.forEach((tab, i) => {
        const finalPos = (i - newIndex) * width;
        tab.content.style.transform = `translateX(${finalPos}px)`;
    });

    // WIR SPEICHERN DIE ID DES TIMERS
    tabHidingTimeoutId = setTimeout(() => {
        availableTabs.forEach((tab, i) => {
            if (i !== newIndex) {
                tab.content.style.display = 'none';
            }
        });
        activateTabByIndex(newIndex, true);
        tabHidingTimeoutId = null; // Aufräumen, nachdem der Job erledigt ist
    }, CONFIG.animDuration);
}
function resetTabs() {
    const width = state.containerWidth;
    const idx = state.activeTabIndex;
    availableTabs.forEach((tab, i) => {
        const finalPos = (i - idx) * width;
        tab.content.style.transform = `translateX(${finalPos}px)`;
    });

    // AUCH HIER DIE ID SPEICHERN
    tabHidingTimeoutId = setTimeout(() => {
        availableTabs.forEach((tab, i) => {
            if (i !== idx) {
                tab.content.style.display = 'none';
            }
        });
        tabHidingTimeoutId = null; // Aufräumen
    }, CONFIG.animDuration);
}
function cleanupStyles() {
	setSwipeLock(false);
	if (state.resultsContainer) {
	    state.resultsContainer.classList.remove('is-swiping');
	    state.resultsContainer.style.height = ''; 
	}
		
	if (state.menuEl) { 
        state.menuEl.style.transition = ''; 
        if (!state.menuEl.classList.contains('is-open')) state.menuEl.style.transform = ''; 
    }
    
	if (state.overlayEl) { 
        state.overlayEl.style.transition = ''; 
        if (!state.menuEl || !state.menuEl.classList.contains('is-open')) { 
            state.overlayEl.style.display = ''; 
            state.overlayEl.style.opacity = ''; 
            state.overlayEl.style.visibility = ''; 
        } 
    }
    
	if (availableTabs.length > 0) { 
        availableTabs.forEach(t => { 
            t.content.style.transition = ''; 
            t.content.style.position = '';
            t.content.style.top = '';
            t.content.style.left = '';
            t.content.style.width = '';
            t.content.style.height = '';
            t.content.style.overflowY = '';
        }); 
    }
}