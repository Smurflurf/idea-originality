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
	animDuration: 100,
	headerFallbackHeight: 0 
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

	let lShadow = document.querySelector('.swipe-shadow-left');
	let rShadow = document.querySelector('.swipe-shadow-right');
	if (!lShadow || !rShadow) {
		createShadowElements(); // Falls sie fehlen (z.B. durch DOM-Reset), neu erstellen
		lShadow = document.querySelector('.swipe-shadow-left');
		rShadow = document.querySelector('.swipe-shadow-right');
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
		if (e.cancelable) e.preventDefault();
		if (!state.stylesPrepared) prepareStylesForSwipe();

		if (!state.mode) {
			const isMenuOpen = state.menuEl && state.menuEl.classList.contains('is-open');
			const isEdgeSwipe = state.startX < CONFIG.edgeZone;

			// NEU: Offline-Check hinzufügen
			const isOffline = document.documentElement.hasAttribute('data-is-offline');

			if (isMenuOpen) {
				state.mode = 'MENU_CLOSING';
			}
			// FIX: Nur in den Menü-Modus gehen, wenn wir NICHT offline sind
			else if (!isOffline && totalDeltaX > 0 && (state.activeTabIndex <= 0 || isEdgeSwipe)) {
				state.mode = 'MENU_OPENING';
			}
			else if (availableTabs.length > 0) {
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
    let headerHeight = 0;
    
    // 1. MESSUNG: Wir holen uns das Padding vom AKTIVEN Tab.
    let currentPaddingTop = '110px'; 
    if (state.activeTabIndex >= 0 && availableTabs[state.activeTabIndex]) {
        const activeContent = availableTabs[state.activeTabIndex].content;
        const computed = window.getComputedStyle(activeContent);
        currentPaddingTop = computed.paddingTop;
    }

    const isHeaderHidden = document.body.classList.contains('is-header-hidden');
    
    if (state.resultsContainer) {
        const header = state.resultsContainer.querySelector('.viz-toggle-header');
        if (header && !isHeaderHidden) {
            headerHeight = header.offsetHeight || CONFIG.headerFallbackHeight;
        }
        
        const currentHeight = state.resultsContainer.getBoundingClientRect().height;
        state.resultsContainer.style.height = `${currentHeight}px`;
        state.resultsContainer.classList.add('is-swiping');
    }

    availableTabs.forEach((t, i) => {
        const isNeighbor = Math.abs(i - state.activeTabIndex) <= 1;
        
        if (!isNeighbor) {
            t.content.style.display = 'none';
        } else {
            t.content.style.display = 'block';
            t.content.style.position = 'absolute';
            t.content.style.top = `${headerHeight}px`;
            t.content.style.left = '0';
            t.content.style.width = '100%';
            
            // Padding zwingen (wie besprochen)
            t.content.style.setProperty('padding-top', currentPaddingTop, 'important');
            
            t.content.style.height = `calc(100% - ${headerHeight}px)`;
            t.content.style.overflowY = 'hidden'; 
            
            // Layout erzwingen
            void t.content.offsetHeight; 

            // FIX: Update verzögern, damit Crosshairs & Punkte korrekte Maße haben
            requestAnimationFrame(() => {
                const vizContainer = t.content.querySelector('.viz-stack-container');
                if (vizContainer) {
                    triggerPositionUpdateForViz(vizContainer.id);
                }
            });
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

		let effectiveDelta = deltaX;
		let showShadowLeft = false;
		let showShadowRight = false;

		// 1. ABSOLUTE RÄNDER (Start und Ende der Liste)
		// Hier wollen wir Rubberband UND Schatten
		if (idx === 0 && deltaX > 0) {
			effectiveDelta = applyRubberBand(deltaX, width);
			showShadowLeft = true;
		}
		else if (idx === availableTabs.length - 1 && deltaX < 0) {
			effectiveDelta = applyRubberBand(deltaX, width);
			showShadowRight = true;
		}
		// 2. "VIRTUELLE" RÄNDER (Mehr als eine Seite swipen)
		// Hier wollen wir Rubberband auf den ÜBERSCHUSS, aber KEINEN Schatten
		else if (Math.abs(deltaX) > width) {
			const sign = Math.sign(deltaX); // 1 für rechts, -1 für links
			const excess = Math.abs(deltaX) - width;

			// Den Überschuss dämpfen
			const rubberBandedExcess = applyRubberBand(excess, width);

			// Zusammenbauen: Volle Breite + gedämpfter Überschuss
			effectiveDelta = sign * (width + rubberBandedExcess);

			// Hier explizit KEINE Schattenflags setzen
		}

		// Schatten anwenden (oder resetten)
		setShadowIntensity(state.leftShadow, showShadowLeft ? deltaX : 0);
		setShadowIntensity(state.rightShadow, showShadowRight ? deltaX : 0);

		// Positionen anwenden
		availableTabs.forEach((tab, i) => {
			const basePos = (i - idx) * width;
			tab.content.style.transform = `translateX(${basePos + effectiveDelta}px)`;
		});
	}
	// Fallback für andere Modi (falls erweitert)
	else {
		if (deltaX > 0) { setShadowIntensity(state.leftShadow, deltaX); }
		else { setShadowIntensity(state.rightShadow, deltaX); }
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
            
            // --- FIX: Inline-Style entfernen ---
            // Damit greift wieder die CSS-Datei (egal ob online oder offline)
            t.content.style.removeProperty('padding-top');
            
            t.content.style.overflowY = '';
        }); 
    }
}