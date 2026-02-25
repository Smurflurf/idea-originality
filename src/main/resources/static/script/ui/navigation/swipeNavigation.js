// ui/navigation/swipeNavigation.js

// NEU: Import von getScrollYForTabIndex hinzugefügt
import { availableTabs, getCurrentTabIndex, activateTabByIndex, getScrollYForTabIndex, syncHeaderStateForTab, syncAllTabsToHeaderState } from '/script/viz/core/visualizationToggle.js';
import { triggerPositionUpdateForViz } from '/script/viz/core/zoomAndPan.js';

// --- KONFIGURATION ---
const CONFIG = {
	threshold: 0.15,
	edgeZone: 20,
	menuWidth: 280,
	rubberBandFactor: 0.4,
	shadowDistanceDivisor: 100,
	maxShadowOpacity: 0.6,
	animDuration: 250, // Leicht erhöht für mehr Smoothness
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
	// initialScrollY brauchen wir nicht mehr global, wir holen es pro Tab dynamisch
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
			if (state.isDragging || state.didSwipe) {
				// --- FIX 3: Wenn auf das Overlay geklickt wird, NICHT blockieren!
				// Das erlaubt uns, die Animation durch Klick zu unterbrechen.
				if (e.target.id === 'menu-overlay') return;

				if (eventType === 'click' && !e.isTrusted) return;

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

	const ignoreSelector = 'input, textarea, .history-link, .topic-tabs-scroller, .attachments-container, .viz-stack-container.is-scroll-zoom-active, .tippy-box';
	if (e.target.closest(ignoreSelector)) return;

	
	if (window.tippy) {
		tippy.hideAll({ duration: 0 });
	}

	if (isAnimating) {
		if (cleanupTimer) clearTimeout(cleanupTimer);
		if (tabHidingTimeoutId) clearTimeout(tabHidingTimeoutId);
		cleanupTimer = null;
		tabHidingTimeoutId = null;
		isAnimating = false;
	}

	if (tabHidingTimeoutId) clearTimeout(tabHidingTimeoutId);

	const isPopupOpen = document.querySelector('.recorder-overlay.is-visible') ||
		document.querySelector('.download-popup-overlay.is-visible');
	if (isPopupOpen) return;

	let clientX, clientY, identifier;

	if (e.type === 'mousedown') {
		if (e.button !== 0) return;

		// Prüfen, ob das Menü bereits offen ist
		const menuEl = document.getElementById('sidebar-menu');
		const isMenuOpen = menuEl && menuEl.classList.contains('is-open');

		let contentElement = document.querySelector('.idea-form') || document.querySelector('.legal-content-wrapper');
		if (!contentElement) {
			const activePane = document.querySelector('.viz-content-pane.active');
			if (activePane) contentElement = activePane.querySelector('*:not(hr)');
		}

		let maxTriggerWidth = CONFIG.edgeZone;
		if (contentElement) {
			const rect = contentElement.getBoundingClientRect();
			maxTriggerWidth = Math.max(rect.left, CONFIG.edgeZone);
		}

		// --- FIX 1: Wenn das Menü OFFEN ist, erlauben wir mousedown überall (um es zuzuschieben)
		// Wenn es ZU ist, bleibt es bei der edgeZone-Beschränkung.
		if (!isMenuOpen && e.clientX > maxTriggerWidth) return;

		clientX = e.clientX;
		clientY = e.clientY;
		identifier = 'mouse';
	} else {
		if (e.touches.length !== 1) return;
		clientX = e.touches[0].clientX;
		clientY = e.touches[0].clientY;
		identifier = e.touches[0].identifier;
	}

	let lShadow = document.querySelector('.swipe-shadow-left');
	let rShadow = document.querySelector('.swipe-shadow-right');
	if (!lShadow || !rShadow) {
		createShadowElements();
		lShadow = document.querySelector('.swipe-shadow-left');
		rShadow = document.querySelector('.swipe-shadow-right');
	}

	state = {
		isDragging: true,
		activeTouchId: identifier,
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

	if (state.activeTouchId === 'mouse') {
		if (e.buttons === 0) {
			handleEnd(e);
			return;
		}
		clientX = e.clientX;
		clientY = e.clientY;
	} else {
		if (!e.changedTouches) return;
		const touch = Array.from(e.changedTouches).find(t => t.identifier === state.activeTouchId);
		if (!touch) return;
		clientX = touch.clientX;
		clientY = touch.clientY;
	}

	const totalDeltaX = clientX - state.startX;
	const totalDeltaY = clientY - state.startY;

	if (Math.abs(totalDeltaX) > Math.abs(totalDeltaY)) {
		if (e.cancelable) e.preventDefault();
	}

	state.currentX = clientX;
	state.currentY = clientY;

	if (!state.direction) {
		if (Math.abs(totalDeltaX) > 10 || Math.abs(totalDeltaY) > 10) {

			if (Math.abs(totalDeltaX) > Math.abs(totalDeltaY)) {
				state.direction = 'horizontal';
				state.didSwipe = true;
				setSwipeLock(true);
				prepareStylesForSwipe();

				if (window.getSelection) {
					window.getSelection().removeAllRanges();
				}
			} else {
				state.direction = 'vertical';
				state.isDragging = false;
				cleanupStyles();
				return;
			}
		}
	}

	if (state.direction === 'horizontal') {
		if (e.cancelable) e.preventDefault();
		if (!state.stylesPrepared) prepareStylesForSwipe();

		if (!state.mode) {
			const isMenuOpen = state.menuEl && state.menuEl.classList.contains('is-open');
			const isEdgeStart = state.startX < CONFIG.edgeZone || state.activeTouchId === 'mouse';
			const isOffline = document.documentElement.hasAttribute('data-is-offline');

			if (isMenuOpen) {
				state.mode = 'MENU_CLOSING';
			}
			else if (isEdgeStart) {
				if (totalDeltaX > 0 && !isOffline) {
					state.mode = 'MENU_OPENING';
				} else {
					return;
				}
			}
			else {
				if (state.activeTabIndex <= 0 && totalDeltaX > 0 && !isOffline) {
					state.mode = 'MENU_OPENING';
				}
				else if (availableTabs.length > 0) {
					state.mode = 'TABS';
					prepareAllTabs();
				}
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
	syncAllTabsToHeaderState();

	let currentPaddingTop = '0px';
	if (state.activeTabIndex >= 0 && availableTabs[state.activeTabIndex]) {
		const computed = window.getComputedStyle(availableTabs[state.activeTabIndex].content);
		currentPaddingTop = computed.paddingTop;
	}

	if (state.resultsContainer) {
		state.resultsContainer.classList.add('is-swiping');
	}

	availableTabs.forEach((t, i) => {
		const isNeighbor = Math.abs(i - state.activeTabIndex) <= 1;

		if (!isNeighbor) {
			t.content.style.display = 'none';
		} else {
			t.content.style.display = 'block';
			t.content.style.position = 'absolute';

			t.content.style.top = '0';
			t.content.style.left = '0';
			t.content.style.width = '100%';

			t.content.style.paddingTop = currentPaddingTop;
			t.content.style.height = 'auto';

			t.content.style.willChange = 'transform';

			void t.content.offsetHeight;

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
	const allowShadow = state.activeTouchId !== 'mouse';

	if (state.mode === 'MENU_OPENING' || state.mode === 'MENU_CLOSING') {
		let translate = deltaX;
		if (state.mode === 'MENU_OPENING') translate -= CONFIG.menuWidth;

		if (translate > 0) {
			if (allowShadow) setShadowIntensity(state.leftShadow, translate);
			translate = 0;
		} else {
			setShadowIntensity(state.leftShadow, 0);
		}

		if (state.menuEl) {
			state.menuEl.style.transform = `translateX(${translate}px)`;

			if (state.overlayEl) {
				const rawProgress = 1 - (Math.abs(translate) / CONFIG.menuWidth);

				// --- FIX: SOFORT sichtbar und klickbar machen ---
				state.overlayEl.style.display = 'block';
				state.overlayEl.style.visibility = 'visible';
				state.overlayEl.style.pointerEvents = 'auto'; // Immer auf auto während der Interaktion

				state.overlayEl.style.opacity = Math.min(1, Math.max(0, rawProgress));
			}
		}
	}
	else if (state.mode === 'TABS') {
		const width = state.containerWidth;
		const idx = state.activeTabIndex;

		let effectiveDelta = deltaX;
		let showShadowLeft = false;
		let showShadowRight = false;

		if (idx === 0 && deltaX > 0) {
			effectiveDelta = applyRubberBand(deltaX, width);
			showShadowLeft = true;
		}
		else if (idx === availableTabs.length - 1 && deltaX < 0) {
			effectiveDelta = applyRubberBand(deltaX, width);
			showShadowRight = true;
		}
		else if (Math.abs(deltaX) > width) {
			const sign = Math.sign(deltaX);
			const excess = Math.abs(deltaX) - width;
			const rubberBandedExcess = applyRubberBand(excess, width);
			effectiveDelta = sign * (width + rubberBandedExcess);
		}

		if (allowShadow) {
			setShadowIntensity(state.leftShadow, showShadowLeft ? deltaX : 0);
			setShadowIntensity(state.rightShadow, showShadowRight ? deltaX : 0);
		} else {
			setShadowIntensity(state.leftShadow, 0);
			setShadowIntensity(state.rightShadow, 0);
		}

		availableTabs.forEach((tab, i) => {
			const basePos = (i - idx) * width;
			const finalX = basePos + effectiveDelta;

			// HIER DIE REVOLUTION: 
			// Wir holen die gespeicherte Scroll-Position für GENAU DIESEN Tab
			const myScrollY = getScrollYForTabIndex(i);

			// Wir wenden X (für den Swipe) und Y (für den individuellen Scroll) gleichzeitig an
			// translate3d ist wichtig für die GPU-Beschleunigung
			tab.content.style.transform = `translate3d(${finalX}px, -${myScrollY}px, 0)`;
		});
	}
}

// --- PHASE 3: END ---
function handleEnd(e) {
	if (!state.isDragging) return;

	if (state.activeTouchId === 'mouse') {
	} else {
		if (!e.changedTouches) return;
		const touch = Array.from(e.changedTouches).find(t => t.identifier === state.activeTouchId);
		if (!touch) return;
	}

	state.isDragging = false;
	state.activeTouchId = null;

	const deltaX = state.currentX - state.startX;

	isAnimating = true;

	const easing = `transform ${CONFIG.animDuration}ms cubic-bezier(0.25, 1, 0.5, 1), opacity ${CONFIG.animDuration}ms ease`;

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

	// Failsafe Timer: Nur nötig für Menü-Animationen, da Tabs eigene Logik haben
	if (state.mode !== 'TABS') {
		cleanupTimer = setTimeout(() => { cleanupStyles(); isAnimating = false; cleanupTimer = null; }, CONFIG.animDuration + 20);
	}
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
		const myScrollY = getScrollYForTabIndex(i);
		tab.content.style.transform = `translate(${finalPos}px, -${myScrollY}px)`;
	});

	tabHidingTimeoutId = setTimeout(() => {
		cleanupStyles();

		availableTabs.forEach((tab, i) => {
			if (i !== newIndex) {
				tab.content.style.display = 'none';
			}
		});

		activateTabByIndex(newIndex, true);

		tabHidingTimeoutId = null;
		isAnimating = false;
	}, CONFIG.animDuration);
}

function resetTabs() {
	const width = state.containerWidth;
	const idx = state.activeTabIndex;

	availableTabs.forEach((tab, i) => {
		const finalPos = (i - idx) * width;
		// Reset: Jeder Tab geht zurück auf SEIN Y
		const myScrollY = getScrollYForTabIndex(i);
		tab.content.style.transform = `translate(${finalPos}px, -${myScrollY}px)`;
	});

	tabHidingTimeoutId = setTimeout(() => {
		cleanupStyles();

		availableTabs.forEach((tab, i) => {
			if (i !== idx) {
				tab.content.style.display = 'none';
			}
		});

		tabHidingTimeoutId = null;
		isAnimating = false;
	}, CONFIG.animDuration);
}


function cleanupStyles() {
	setSwipeLock(false);
	if (state.resultsContainer) {
		state.resultsContainer.classList.remove('is-swiping');
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

			t.content.style.paddingTop = '';
			t.content.style.height = '';

			t.content.style.willChange = '';
		});

		window.dispatchEvent(new Event('scroll'));
	}

	setTimeout(() => {
		state.didSwipe = false;
	}, 0);
}