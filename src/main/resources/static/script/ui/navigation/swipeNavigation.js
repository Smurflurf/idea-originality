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
	rightShadow: null,
	didSwipe: false
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
	
	document.body.addEventListener('mousedown', handleStart);
	window.addEventListener('mousemove', handleMove); 
	window.addEventListener('mouseup', handleEnd);

	const blockEvents = ['mouseover', 'mouseenter', 'mouseout', 'mouseleave', 'click', 'focusin', 'focusout'];

	blockEvents.forEach(eventType => {
		document.body.addEventListener(eventType, (e) => {
			// Wenn wir gerade ziehen ODER gerade gewischt haben:
			if (state.isDragging || state.didSwipe) {

				// AUSNAHME: Wenn das Event vom Skript kommt (z.B. trigger.click()), 
				// ist isTrusted false. Das müssen wir durchlassen, damit das Menü aufgeht.
				if (eventType === 'click' && !e.isTrusted) {
					return;
				}

				e.preventDefault();
				e.stopPropagation();
				e.stopImmediatePropagation();
				return false;
			}
		}, { capture: true, passive: false });
	});

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
    
    // Performance: Laufende Animation sofort stoppen
    if (isAnimating) {
        if (cleanupTimer) clearTimeout(cleanupTimer);
        if (tabHidingTimeoutId) clearTimeout(tabHidingTimeoutId);
        cleanupTimer = null;
        tabHidingTimeoutId = null;

		if (availableTabs.length > 0) {
			availableTabs.forEach(t => {
				const style = window.getComputedStyle(t.content);
				t.content.style.transform = style.transform;
				t.content.style.transition = 'none';
			});
		}
		isAnimating = false;
	}

	if (tabHidingTimeoutId) clearTimeout(tabHidingTimeoutId);

	const isPopupOpen = document.querySelector('.recorder-overlay.is-visible') ||
		document.querySelector('.download-popup-overlay.is-visible');
	if (isPopupOpen) return;

	const ignoreSelector = 'input, textarea, .history-link, .topic-tabs-scroller, .attachments-container, .viz-stack-container.is-scroll-zoom-active';
	if (e.target.closest(ignoreSelector)) return;

	let clientX, clientY, identifier;

	if (e.type === 'mousedown') {
		if (e.button !== 0) return;

		// --- NEU: INTELLIGENTE MARGIN-ERKENNUNG ---
		// 1. Suche nach dem Hauptinhalt (Startseite oder Legal)
		let contentElement = document.querySelector('.idea-form') || document.querySelector('.legal-content-wrapper');

		// 2. Wenn nichts gefunden (Results Seite), suche das erste sichtbare Element im aktiven Tab
		if (!contentElement) {
			const activePane = document.querySelector('.viz-content-pane.active');
			if (activePane) {
				// Wir nehmen das erste Kind-Element, das kein HR ist (z.B. h1 oder hierarchy-container)
				contentElement = activePane.querySelector('*:not(hr)');
			}
		}

		let maxTriggerWidth = CONFIG.edgeZone; // Fallback 20px

		if (contentElement) {
			const rect = contentElement.getBoundingClientRect();
			// Wenn das Element zentriert ist, ist rect.left der Abstand zum Rand
			// Wir nutzen Math.max, um immer mindestens die edgeZone zu haben
			maxTriggerWidth = Math.max(rect.left, CONFIG.edgeZone);
		}

		// DEBUG-TIPP: Falls es nicht klappt, schalte das Log ein:
		// console.log("Margin detected:", maxTriggerWidth, "Click at:", e.clientX);

		if (e.clientX > maxTriggerWidth) return;

		clientX = e.clientX;
		clientY = e.clientY;
		identifier = 'mouse';
	} else {
		if (e.touches.length !== 1) return;
		clientX = e.touches[0].clientX;
		clientY = e.touches[0].clientY;
		identifier = e.touches[0].identifier;
	}

	// Schatten-Elemente sicherstellen (unverändert)
	let lShadow = document.querySelector('.swipe-shadow-left');
	let rShadow = document.querySelector('.swipe-shadow-right');
	if (!lShadow || !rShadow) {
		createShadowElements();
		lShadow = document.querySelector('.swipe-shadow-left');
		rShadow = document.querySelector('.swipe-shadow-right');
	}

	state = {
		isDragging: true,
		activeTouchId: identifier, // Hier nutzen wir die ermittelte ID
		startX: clientX,
		startY: clientY,
		currentX: clientX,
		currentY: clientY,
		direction: null,
		mode: null,
		didSwipe: false,
		activeTabIndex: (typeof getCurrentTabIndex === 'function') ? getCurrentTabIndex() : -1,
		containerWidth: window.innerWidth,
		resultsContainer: document.querySelector('.results-container'),
		menuEl: document.getElementById('sidebar-menu'),
		overlayEl: document.getElementById('menu-overlay'),
		leftShadow: lShadow,
		rightShadow: rShadow,
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

    let clientX, clientY;

    // --- NEU: Input Unterscheidung ---
    if (state.activeTouchId === 'mouse') {
        // Maus-Event hat Koordinaten direkt auf 'e'
        // WICHTIG: Prüfen ob Buttons gedrückt sind (falls user Maustaste losgelassen hat außerhalb des Fensters)
        if (e.buttons === 0) {
            handleEnd(e);
            return;
        }
        clientX = e.clientX;
        clientY = e.clientY;
    } else {
        // Touch: Das richtige Touch-Objekt finden
        if (!e.changedTouches) return;
        const touch = Array.from(e.changedTouches).find(t => t.identifier === state.activeTouchId);
        if (!touch) return; // Event gehört nicht zu unserem Finger
        clientX = touch.clientX;
        clientY = touch.clientY;
    }

    // Totale Distanz seit Start
    const totalDeltaX = clientX - state.startX;
    const totalDeltaY = clientY - state.startY;

    // Pre-Check (unverändert)
    if (Math.abs(totalDeltaX) > Math.abs(totalDeltaY)) {
        if (e.cancelable) e.preventDefault();
    }

    state.currentX = clientX;
    state.currentY = clientY;

    // 1. Richtung bestimmen
    if (!state.direction) {
        if (Math.abs(totalDeltaX) > 10 || Math.abs(totalDeltaY) > 10) {
            
            if (Math.abs(totalDeltaX) > Math.abs(totalDeltaY)) {
                state.direction = 'horizontal';
                state.didSwipe = true;
                setSwipeLock(true);
                prepareStylesForSwipe();
            } else {
                state.direction = 'vertical';
                state.isDragging = false;
                cleanupStyles();
                return;
            }
        }
    }

    // 2. Ausführung
    if (state.direction === 'horizontal') {
        if (e.cancelable) e.preventDefault();
        if (!state.stylesPrepared) prepareStylesForSwipe();

        if (!state.mode) {
            const isMenuOpen = state.menuEl && state.menuEl.classList.contains('is-open');
            // Hier nutzen wir die existierende Edge-Logik.
            // Da wir bei Maus in handleStart schon auf <100px geprüft haben,
            // ist das hier für Maus immer true.
            const isEdgeSwipe = state.startX < CONFIG.edgeZone || state.activeTouchId === 'mouse';
            const isOffline = document.documentElement.hasAttribute('data-is-offline');

            if (isMenuOpen) {
                state.mode = 'MENU_CLOSING';
            }
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
    
    // 1. STATE MESSEN: Was zeigt der Browser GERADE an?
    // Wir nehmen den Computed Style des aktiven Tabs als absolute Wahrheit.
    let currentPaddingTop = '110px'; 
    if (state.activeTabIndex >= 0 && availableTabs[state.activeTabIndex]) {
        const activeContent = availableTabs[state.activeTabIndex].content;
        const computed = window.getComputedStyle(activeContent);
        currentPaddingTop = computed.paddingTop;
    }

    const isHeaderHidden = document.body.classList.contains('is-header-hidden');
    
    // Header-Höhe berechnen (für top-Offset)
    if (state.resultsContainer) {
        const header = state.resultsContainer.querySelector('.viz-toggle-header');
        // Wir nehmen die Höhe nur, wenn der Header auch logisch sichtbar sein soll
        if (header && !isHeaderHidden) {
            headerHeight = header.offsetHeight || CONFIG.headerFallbackHeight;
        }
        
        // Container einfrieren
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
            
            // Layout fixieren
            t.content.style.top = `${headerHeight}px`;
            t.content.style.left = '0';
            t.content.style.width = '100%';
            
            // WICHTIG: Das gemessene Padding setzen. 
            // Ohne '!important', da wir das CSS bereinigt haben. Inline gewinnt so oder so.
            t.content.style.paddingTop = currentPaddingTop;
            
            // Höhe berechnen: 100% minus der Header-Platzhalter oben
            t.content.style.height = `calc(100% - ${headerHeight}px)`;
            t.content.style.overflowY = 'hidden'; 
            
            // Hardwarebeschleunigung für smootheres Rendering am Handy
            t.content.style.willChange = 'transform';

            // Layout erzwingen
            void t.content.offsetHeight; 

            // Live-Elemente (Crosshairs) rendern lassen
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
    // Helper: Schatten nur zeigen, wenn es NICHT die Maus ist
    const allowShadow = state.activeTouchId !== 'mouse';

    if (state.mode === 'MENU_OPENING') {
        let translate = -CONFIG.menuWidth + deltaX;
        
        // Wir haben das Menü weiter als "offen" gezogen (Overscroll)
        if (translate > 0) { 
            if (allowShadow) {
                setShadowIntensity(state.leftShadow, translate); 
            }
            translate = 0; // Menü stoppt visuell an der Kante
        } else { 
            setShadowIntensity(state.leftShadow, 0); 
        }

        if (state.menuEl) {
            state.menuEl.style.transform = `translateX(${translate}px)`;
            if (state.overlayEl) {
                const rawProgress = (CONFIG.menuWidth + translate) / CONFIG.menuWidth;
                state.overlayEl.style.visibility = 'visible'; 
                state.overlayEl.style.display = 'block'; 
                state.overlayEl.style.opacity = Math.min(1, Math.max(0, rawProgress));
            }
        }
    } 
    else if (state.mode === 'MENU_CLOSING') {
        let translate = deltaX;
        
        // Wir ziehen das geschlossene Menü noch weiter zu (Overscroll nach links? Unwahrscheinlich aber möglich)
        // Oder wir ziehen es nach rechts über den Bildschirmrand (translate > 0)
        if (translate > 0) { 
            if (allowShadow) {
                setShadowIntensity(state.leftShadow, translate); 
            }
            translate = 0; 
        } else { 
            setShadowIntensity(state.leftShadow, 0); 
        }

        if (state.menuEl) {
            state.menuEl.style.transform = `translateX(${translate}px)`;
            if (state.overlayEl) { 
                state.overlayEl.style.opacity = 1 - Math.min(1, Math.max(0, Math.abs(translate) / CONFIG.menuWidth)); 
            }
        }
    }
    else if (state.mode === 'TABS') {
        const width = state.containerWidth;
        const idx = state.activeTabIndex;
        
        let effectiveDelta = deltaX;
        let showShadowLeft = false;
        let showShadowRight = false;

        // 1. ABSOLUTE RÄNDER
        if (idx === 0 && deltaX > 0) {
            effectiveDelta = applyRubberBand(deltaX, width);
            showShadowLeft = true;
        } 
        else if (idx === availableTabs.length - 1 && deltaX < 0) {
            effectiveDelta = applyRubberBand(deltaX, width);
            showShadowRight = true;
        }
        // 2. VIRTUELLE RÄNDER (Zwischen den Tabs)
        else if (Math.abs(deltaX) > width) {
            const sign = Math.sign(deltaX);
            const excess = Math.abs(deltaX) - width;
            const rubberBandedExcess = applyRubberBand(excess, width);
            effectiveDelta = sign * (width + rubberBandedExcess);
        }

        // Schatten anwenden (Nur wenn Touch!)
        if (allowShadow) {
            setShadowIntensity(state.leftShadow, showShadowLeft ? deltaX : 0);
            setShadowIntensity(state.rightShadow, showShadowRight ? deltaX : 0);
        } else {
            // Maus: Schatten sicherheitshalber auf 0 setzen
            setShadowIntensity(state.leftShadow, 0);
            setShadowIntensity(state.rightShadow, 0);
        }

        // Positionen anwenden
        availableTabs.forEach((tab, i) => {
            const basePos = (i - idx) * width;
            tab.content.style.transform = `translateX(${basePos + effectiveDelta}px)`;
        });
    } 
    else {
        // Fallback
        if (allowShadow) {
            if (deltaX > 0) { setShadowIntensity(state.leftShadow, deltaX); } 
            else { setShadowIntensity(state.rightShadow, deltaX); }
        }
    }
}

// --- PHASE 3: END ---
function handleEnd(e) {
	if (!state.isDragging) return;

	// Check: War es unsere Maus oder unser Finger?
	if (state.activeTouchId === 'mouse') {
		// Bei Maus ist es egal welches MouseUp Event kommt, solange wir im Drag waren
	} else {
		if (!e.changedTouches) return;
		const touch = Array.from(e.changedTouches).find(t => t.identifier === state.activeTouchId);
		if (!touch) return;
	}
	
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
		actionTaken = true; 
		if (deltaX > CONFIG.menuWidth * 0.3) openMenu(); else closeMenu();
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
            
            t.content.style.paddingTop = '';
            t.content.style.willChange = '';
            t.content.style.overflowY = '';
        }); 
    }

    // Interaktion wieder freigeben:
    // Wir warten einen winzigen Moment (Next Tick), um sicherzustellen, 
    // dass der Klick vom Loslassen des Fingers auch wirklich vorbei ist.
    setTimeout(() => {
        state.didSwipe = false;
    }, 0);
}
