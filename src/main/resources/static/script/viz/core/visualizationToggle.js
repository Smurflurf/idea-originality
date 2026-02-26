import { triggerPositionUpdateForViz } from '/script/viz/core/zoomAndPan.js';
import { applyColorCoding } from '/script/ui/base/colorCoder.js'; 

// =========================================================================
// KONFIGURATION & STATE
// =========================================================================

export let availableTabs = []; 
let isSwitchingTab = false; 
let currentResizeObserver = null; 

const HEADER_HEIGHT = 90; 

// State für Header-Synchronisation
let lastScrollY = 0;
let overlayWantsOpen = false;

let headerDomCache = {
    visible: null,
    pos: null,
    transform: null,
    transition: null,
    manualClass: null
};

// =========================================================================
// HELPER & REGISTRY
// =========================================================================

const tabScrollRegistry = {};

export function getCurrentTabIndex() {
    return availableTabs.findIndex(t => t.content.classList.contains('active'));
}

export function clearAvailableTabs() {
    availableTabs = [];
}

function getCompensatedScroll(paneId) {
    const entry = tabScrollRegistry[paneId];
    return entry ? entry.y : 0; 
}


export function getScrollYForTabIndex(index) {
    if (index < 0 || index >= availableTabs.length) return 0;
    
    if (isSwitchingTab) {
        return getCompensatedScroll(availableTabs[index].content.id);
    }

    const isCurrentActive = availableTabs[index].content.classList.contains('active');
    return isCurrentActive ? window.scrollY : getCompensatedScroll(availableTabs[index].content.id);
}

// =========================================================================
// SCROLL SYNC LOGIC
// =========================================================================

let currentSyncTarget = null;

let ticking = false;
function syncPaneScroll() {
	if (!ticking && !isSwitchingTab && !document.body.classList.contains('is-swiping-active')) {
		window.requestAnimationFrame(() => {
			if (currentSyncTarget) {
				currentSyncTarget.style.transform = `translate3d(0, -${window.scrollY}px, 0)`;
			}
			ticking = false;
		});
		ticking = true;
	}
}

function setupScrollSyncFor(pane, fromSwipe = false) {
    const scrollProxy = document.getElementById('scroll-proxy');
    if (!scrollProxy || !pane) return;

    isSwitchingTab = true; 

    if (currentResizeObserver) currentResizeObserver.disconnect();
    window.removeEventListener('scroll', syncPaneScroll);
    
    currentSyncTarget = pane;

    // 1. Layout lesen (Teuer, aber nur 1x beim Tab-Wechsel)
    const targetHeight = pane.scrollHeight;
    
    // 2. Layout schreiben
    scrollProxy.style.height = `${targetHeight}px`;

    const targetY = getCompensatedScroll(pane.id);

    // Header Cache resetten, damit beim Tab-Wechsel sicher neu gezeichnet wird
    headerDomCache = { visible: null, pos: null, transform: null, transition: null, manualClass: null };
    
    updateHeaderForScrollY(targetY, null, true, fromSwipe);

    window.scrollTo({
        top: targetY,
        left: 0,
        behavior: 'instant'
    });

    pane.style.transform = `translate3d(0, -${targetY}px, 0)`;

    requestAnimationFrame(() => {
        requestAnimationFrame(() => {
            isSwitchingTab = false;
            window.addEventListener('scroll', syncPaneScroll, { passive: true });
            
            currentResizeObserver = new ResizeObserver((entries) => {
                // OPTIMIERUNG: Nur schreiben wenn sich wirklich was ändert
                const newHeight = entries[0].target.scrollHeight;
                if (scrollProxy.style.height !== `${newHeight}px`) {
                    scrollProxy.style.height = `${newHeight}px`;
                }
            });
            currentResizeObserver.observe(pane);
        });
    });
}

// =========================================================================
// HEADER LOGIC (FIXED)
// =========================================================================

export function updateHeaderForScrollY(currentY, isScrollingUp = false, forceUpdate = false, fromSwipe = false) {
    const root = document.body;
    const header = document.querySelector('.viz-toggle-header');
    const menuBtn = document.getElementById('menu-trigger');

    if (!header) return;

    // === SCHRITT 1: ZUSTAND ERMITTELN ===
    const isZone1 = currentY <= HEADER_HEIGHT;
    const isOverscroll = currentY < 0;

    // Intent (Absicht) berechnen: Will der User das Overlay sehen?
    if (forceUpdate) {
        // FIX: Wir messen die VISUELLE Sichtbarkeit des alten Tabs!
        // lastScrollY beinhaltet hier noch exakt die Scroll-Position des Tabs, den wir verlassen.
        let wasVisuallyOpen = overlayWantsOpen; 
        
        // Wenn der Nutzer ganz oben war, hat er den Header zu 100% gesehen (egal was der interne Intent sagt)
        if (lastScrollY < (HEADER_HEIGHT / 2)) {
            wasVisuallyOpen = true; 
        }
        
        // Wir übernehmen die visuelle Wahrheit als neuen Intent
        overlayWantsOpen = wasVisuallyOpen;

        // Ausnahme: Wenn wir auf dem neuen Tab EXAKT oben (Y=0) landen, zwingen wir den Intent 
        // auf false, damit der Header beim Runterscrollen wieder ganz normal natürlich mitscrollt.
        if (currentY === 0) {
            overlayWantsOpen = false;
        }
    } else {
        // Normales Scroll-Verhalten
        if (isOverscroll || currentY === 0) {
            overlayWantsOpen = false;
        } else if (!isZone1 && isScrollingUp !== null) {
            overlayWantsOpen = isScrollingUp;
        }
    }

    // === SCHRITT 2: RENDER-REGELN FESTLEGEN ===
    let isVisible = true;
    let pos = 'absolute';
    let useTransition = false;

    if (isOverscroll) {
        isVisible = true;
        pos = 'fixed';
        useTransition = false;
    } else if (isZone1) {
        isVisible = true;
        if (overlayWantsOpen) {
            // Wenn der Intent "Offen" ist, halten wir ihn fixed fest, damit er nicht rausscrollt
            pos = 'fixed';
            useTransition = !forceUpdate; // Keine Animation beim Tab-Wechsel, nur beim Scrollen
        } else {
            pos = 'absolute';
            useTransition = false;
        }
    } else {
        // Zone 2 (> 90px)
        isVisible = overlayWantsOpen;
        pos = 'fixed';

        // Animation ausschalten bei Tab-Wechsel oder wenn wir nach unten in Zone 2 eintreten
        //const crossingDownwards = (lastScrollY <= HEADER_HEIGHT && currentY > HEADER_HEIGHT && !forceUpdate);
        //useTransition = !(forceUpdate || crossingDownwards);
		useTransition = !forceUpdate;
    }

	// === SCHRITT 3: DOM UPDATES (Nur hier!) ===

	// Wir schreiben nur in den DOM, wenn sich der Wert geändert hat.

	// 1. Sichtbarkeit (Class auf Body)
	if (headerDomCache.visible !== isVisible) {
		if (isVisible) root.classList.remove('is-header-hidden');
		else root.classList.add('is-header-hidden');
		headerDomCache.visible = isVisible;
	}

	// 2. Position
	if (headerDomCache.pos !== pos) {
		header.style.position = pos;
		header.style.top = '0'; // Statisch, kaum Kosten
		if (menuBtn && window.innerWidth <= 768) {
			menuBtn.style.setProperty('position', pos, 'important');
			menuBtn.style.top = '12px';
		}
		headerDomCache.pos = pos;
	}

	// 3. Transform (Overscroll Fix)
	const transformVal = isOverscroll ? 'translateY(0)' : '';
	if (headerDomCache.transform !== transformVal) {
		header.style.transform = transformVal;
		if (menuBtn && window.innerWidth <= 768) menuBtn.style.transform = transformVal;
		headerDomCache.transform = transformVal;
	}

	// 4. Transition
	const transitionVal = useTransition ? '' : 'none';
	if (headerDomCache.transition !== transitionVal) {
		header.style.transition = transitionVal;
		if (menuBtn && window.innerWidth <= 768) menuBtn.style.transition = transitionVal;
		headerDomCache.transition = transitionVal;
	}

	// 5. Manual Scroll Class
	const manualClassVal = (pos === 'absolute' || isOverscroll);
	if (headerDomCache.manualClass !== manualClassVal) {
		if (manualClassVal) header.classList.add('is-manual-scroll');
		else header.classList.remove('is-manual-scroll');
		headerDomCache.manualClass = manualClassVal;
	}
}

// Wird beim Swipen gerufen
export function syncHeaderStateForTab(newScrollY) {
    lastScrollY = newScrollY;
    // Wir tun so, als wäre es ein "forceUpdate" (Tab Wechsel)
    updateHeaderForScrollY(newScrollY, null, true, true);
}

export function syncAllTabsToHeaderState() {
    // Diese Funktion dient nur der Vorbereitung des Swipes (damit Tabs nicht springen)
    // Sie ändert NICHT den sichtbaren Header-Zustand.
    const root = document.body;
    const currentY = window.scrollY;
    const isHeaderHidden = root.classList.contains('is-header-hidden');
    
    let hState = 'OVERLAY_OPEN';
    if (currentY <= HEADER_HEIGHT) hState = 'STATIC';
    else if (isHeaderHidden) hState = 'OVERLAY_CLOSED';

    availableTabs.forEach(tab => {
        const paneId = tab.content.id;
        if (tab.content.classList.contains('active')) {
            tabScrollRegistry[paneId] = { y: currentY };
            return;
        }
        const entry = tabScrollRegistry[paneId];
        let targetY = entry ? entry.y : 0;

        if (hState === 'STATIC') {
            if (targetY <= HEADER_HEIGHT) targetY = currentY; 
        } else if (hState === 'OVERLAY_CLOSED') {
            if (targetY < HEADER_HEIGHT) targetY = HEADER_HEIGHT; 
        } else if (hState === 'OVERLAY_OPEN') {
            if (targetY <= HEADER_HEIGHT) targetY = 0; 
        }
        tabScrollRegistry[paneId] = { y: targetY };
    });
}

function initializeScrollHiding() {
    const root = document.body;
    lastScrollY = window.scrollY;

    window.addEventListener('scroll', () => {
        if (isSwitchingTab || document.body.classList.contains('is-swiping-active')) {
            lastScrollY = window.scrollY; // Trotzdem Position für später merken
            return;
        }
        
        const currentY = window.scrollY;
        const diff = currentY - lastScrollY;
        
        // --- FIX 1: Puffer gegen Micro-Scrolls beim reinen Antippen ---
        if (Math.abs(diff) < 5 && currentY > 0) return;

        const isScrollingUp = diff < 0;
        updateHeaderForScrollY(currentY, isScrollingUp);
        lastScrollY = currentY;
    }, { passive: true });
}

// =========================================================================
// VIEW ACTIVATION
// =========================================================================

export function activateTabByIndex(index, fromSwipe = false) {
    if (index < 0 || index >= availableTabs.length) return;
    const target = availableTabs[index];
    activateView(target.btn, target.content, fromSwipe);
}

function activateView(btnToActivate, contentToActivate, fromSwipe = false) {
    const currentActive = availableTabs.find(t => t.content.classList.contains('active'));
    
    if (currentActive) {
        tabScrollRegistry[currentActive.content.id] = { y: window.scrollY };
        currentActive.btn.classList.remove('active');
        currentActive.content.classList.remove('active');
        currentActive.content.style.display = 'none';
    }

    btnToActivate.classList.add('active');
    contentToActivate.style.display = 'block';
    contentToActivate.classList.add('active');

    setupScrollSyncFor(contentToActivate, fromSwipe);

    requestAnimationFrame(() => {
        applyColorCoding();
        const vizContainer = contentToActivate.querySelector('.viz-stack-container');
        if (vizContainer) triggerPositionUpdateForViz(vizContainer.id);
    });
}

// =========================================================================
// INITIALIZATION (MIT WIEDERHERGESTELLTEN BUTTONS)
// =========================================================================

export function initializeVisualizationToggles() {
    const btnOwn = document.getElementById('show-own-viz');
    const btnNeighbor = document.getElementById('show-neighbor-viz');
    const btnSerendipity = document.getElementById('show-serendipity-viz');
    
    const contentOwn = document.getElementById('own-viz-content');
    const contentNeighbor = document.getElementById('neighbor-viz-content');
    const contentSerendipity = document.getElementById('serendipity-viz-content');

    availableTabs = [];
    if (btnOwn && contentOwn) availableTabs.push({ btn: btnOwn, content: contentOwn });
    if (btnNeighbor && contentNeighbor) availableTabs.push({ btn: btnNeighbor, content: contentNeighbor });
    if (btnSerendipity && contentSerendipity) availableTabs.push({ btn: btnSerendipity, content: contentSerendipity });

    const initialActive = availableTabs.find(t => t.content.classList.contains('active'));
    if (initialActive) setupScrollSyncFor(initialActive.content);
    else if (availableTabs.length > 0) activateTabByIndex(0);
    
    availableTabs.forEach((tab, index) => {
        tab.btn.addEventListener('click', () => {
            if (tab.content.classList.contains('active')) {
                window.scrollTo({ top: 0, behavior: 'smooth' });
            } else {
                activateTabByIndex(index);
            }
        });
    });

    // --- BUTTON TOGGLES (WIEDERHERGESTELLT) ---
    ['own', 'nc', 'serendipity'].forEach(prefix => { 
        
        // 1. Einfache Toggles (Ein Button -> Ein Ziel)
        const setup = (id, targetId, startActive = true) => {
            const btn = document.getElementById(id);
            const target = document.getElementById(targetId);
            if (!btn) return; // Button existiert nicht im DOM -> Überspringen
            
            // Initialer Zustand
            btn.classList.toggle('active', startActive);
            if(target) target.style.display = startActive ? 'block' : 'none';
            
            btn.onclick = (e) => {
                // Verhindert Bubbling (wichtig für Zoom/Pan)
                e.stopPropagation(); 
                const isActive = btn.classList.toggle('active');
                if(target) target.style.display = isActive ? 'block' : 'none';
            };
        };

        // 2. Gruppen Toggles (Ein Button -> Mehrere Ziele, z.B. Context Outline + Context Label)
        const setupGroup = (id, targetIds, startActive = false) => {
            const btn = document.getElementById(id);
            if (!btn) return;

            const targets = targetIds.map(tid => document.getElementById(tid)).filter(Boolean);
            
            btn.classList.toggle('active', startActive);
            targets.forEach(t => t.style.display = startActive ? 'block' : 'none');

            btn.onclick = (e) => {
                e.stopPropagation();
                const isActive = btn.classList.toggle('active');
                targets.forEach(t => t.style.display = isActive ? 'block' : 'none');
            };
        };

        // --- KONFIGURATION DER BUTTONS ---
        
        // Crosshair, Points, Outlines (Standard: AN)
        setup(`viz-toggle-${prefix}-crosshair`, `viz-layer-${prefix}-crosshair-canvas`, true);
        setup(`viz-toggle-${prefix}-points`, `viz-layer-${prefix}-points`, true);
        setup(`viz-toggle-${prefix}-outlines`, `g-outlines-${prefix}-main`, true);
        
        // Labels (Standard: AUS, zu unruhig)
        setup(`viz-toggle-${prefix}-labels`, `g-labels-${prefix}-main`, false);

        // Neighbors (Div Container) - Nur bei 'own' standardmäßig an, sonst aus
        setup(`viz-toggle-${prefix}-neighbors`, `dynamic-points-${prefix}-neighbors`, (prefix === 'own'));

        // Context (Gruppe aus Outline + Labels) - Standard: AUS
        setupGroup(`viz-toggle-${prefix}-context`, [`g-outlines-${prefix}-context`, `g-labels-${prefix}-context`], false);
    });

    initializeScrollHiding();
}