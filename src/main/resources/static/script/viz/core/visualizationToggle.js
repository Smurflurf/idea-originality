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
// PERFORMANCE SCROLL SYNC LOGIC (OPTIMIZED)
// =========================================================================

let currentSyncTarget = null;
let isRenderPending = false;
let currentScrollY = window.scrollY;
let scrollTimeout = null;

// EINZIGER Scroll-Listener für alles (Pane + Header)
function onNativeScroll() {
    if (isSwitchingTab || document.body.classList.contains('is-swiping-active')) return;
    
    currentScrollY = window.scrollY;

    // Perfekter Scroll-Loop: requestAnimationFrame wird nur 1x pro Frame gefeuert
    if (!isRenderPending) {
        isRenderPending = true;
        window.requestAnimationFrame(renderScrollFrame);
    }

    // Scroll-Hover-Sperre: Verbessert Performance über schweren DOM-Elementen drastisch
    if (!document.body.classList.contains('is-actively-scrolling')) {
        document.body.classList.add('is-actively-scrolling');
    }
    clearTimeout(scrollTimeout);
    scrollTimeout = setTimeout(() => {
        document.body.classList.remove('is-actively-scrolling');
    }, 150); // 150ms nach dem letzten Scroll-Event wird Hover wieder aktiviert
}

function renderScrollFrame() {
    isRenderPending = false;

    // 1. GPU Transform anwenden (Höchste Priorität)
    if (currentSyncTarget) {
        currentSyncTarget.style.transform = `translate3d(0, -${currentScrollY}px, 0)`;
    }

    // 2. Header synchronisieren (Nur wenn Nötig!)
    const diff = currentScrollY - lastScrollY;
    
    // Puffer gegen Micro-Scrolls beim reinen Antippen
    if (Math.abs(diff) >= 5 || currentScrollY === 0) {
        const isScrollingUp = diff < 0;
        updateHeaderForScrollY(currentScrollY, isScrollingUp);
        lastScrollY = currentScrollY;
    }
}

function setupScrollSyncFor(pane, fromSwipe = false) {
    const scrollProxy = document.getElementById('scroll-proxy');
    if (!scrollProxy || !pane) return;

    isSwitchingTab = true; 

    if (currentResizeObserver) currentResizeObserver.disconnect();
    window.removeEventListener('scroll', onNativeScroll);
    
    currentSyncTarget = pane;

    // Wir lesen VORHER die Höhe, um Layout-Thrashing zu vermeiden
    const targetHeight = pane.scrollHeight;
    scrollProxy.style.height = `${targetHeight}px`;

    const targetY = getCompensatedScroll(pane.id);

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
            
            // Registriere den EINZIGEN, passiven Scroll-Listener
            window.addEventListener('scroll', onNativeScroll, { passive: true });
            
            // ResizeObserver optimiert: Führt Updates nur in rAF aus und vermeidet ständige Reflows
            currentResizeObserver = new ResizeObserver((entries) => {
                requestAnimationFrame(() => {
                    const newHeight = entries[0].target.scrollHeight;
                    // Nur updaten wenn wirklich nötig
                    if (scrollProxy.style.height !== `${newHeight}px`) {
                        scrollProxy.style.height = `${newHeight}px`;
                    }
                });
            });
            currentResizeObserver.observe(pane);
        });
    });
}

// =========================================================================
// HEADER LOGIC (FIXED & CACHED)
// =========================================================================

// CACHE: Speichert den letzten DOM-Zustand, um unsinnige CSS-Writes zu vermeiden
let headerDomCache = {
    isVisible: null,
    pos: null,
    transform: null,
    transition: null,
    isManualScroll: null
};

export function updateHeaderForScrollY(currentY, isScrollingUp = false, forceUpdate = false, fromSwipe = false) {
    const root = document.body;
    const header = document.querySelector('.viz-toggle-header');
    const menuBtn = document.getElementById('menu-trigger');

    if (!header) return;

    // === SCHRITT 1: ZUSTAND ERMITTELN ===
    const isZone1 = currentY <= HEADER_HEIGHT;
    const isOverscroll = currentY < 0;

    if (forceUpdate) {
        let wasVisuallyOpen = overlayWantsOpen; 
        if (lastScrollY < (HEADER_HEIGHT / 2)) {
            wasVisuallyOpen = true; 
        }
        overlayWantsOpen = wasVisuallyOpen;
        if (currentY === 0) {
            overlayWantsOpen = false;
        }
    } else {
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
            pos = 'fixed';
            useTransition = !forceUpdate; 
        } else {
            pos = 'absolute';
            useTransition = false;
        }
    } else {
        isVisible = overlayWantsOpen;
        pos = 'fixed';
        const crossingDownwards = (lastScrollY <= HEADER_HEIGHT && currentY > HEADER_HEIGHT && !forceUpdate);
        useTransition = !(forceUpdate || crossingDownwards);
    }

    // === SCHRITT 3: DOM UPDATES (CACHED - Nur bei Änderung schreiben!) ===
    
    // Sichtbarkeit
    if (headerDomCache.isVisible !== isVisible) {
        if (isVisible) root.classList.remove('is-header-hidden');
        else root.classList.add('is-header-hidden');
        headerDomCache.isVisible = isVisible;
    }

    // Position
    if (headerDomCache.pos !== pos) {
        header.style.position = pos;
        if (menuBtn && window.innerWidth <= 768) menuBtn.style.setProperty('position', pos, 'important');
        headerDomCache.pos = pos;
    }

    // Transform (Overscroll)
    const newTransform = isOverscroll ? 'translateY(0)' : '';
    if (headerDomCache.transform !== newTransform) {
        header.style.transform = newTransform;
        if (menuBtn && window.innerWidth <= 768) menuBtn.style.transform = newTransform;
        headerDomCache.transform = newTransform;
    }

    // Transition
    const newTransition = useTransition ? '' : 'none';
    if (headerDomCache.transition !== newTransition) {
        header.style.transition = newTransition;
        if (menuBtn && window.innerWidth <= 768) menuBtn.style.transition = newTransition;
        headerDomCache.transition = newTransition;
    }

    // Manual Scroll Class
    const newIsManual = (pos === 'absolute' || isOverscroll);
    if (headerDomCache.isManualScroll !== newIsManual) {
        if (newIsManual) header.classList.add('is-manual-scroll');
        else header.classList.remove('is-manual-scroll');
        headerDomCache.isManualScroll = newIsManual;
    }
}

export function syncHeaderStateForTab(newScrollY) {
    lastScrollY = newScrollY;
    updateHeaderForScrollY(newScrollY, null, true, true);
}

export function syncAllTabsToHeaderState() {
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

// Entfernt: initializeScrollHiding() existiert nicht mehr, Logik ist jetzt in onNativeScroll

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
// INITIALIZATION
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

    ['own', 'nc', 'serendipity'].forEach(prefix => { 
        const setup = (id, targetId, startActive = true) => {
            const btn = document.getElementById(id);
            const target = document.getElementById(targetId);
            if (!btn) return; 
            
            btn.classList.toggle('active', startActive);
            if(target) target.style.display = startActive ? 'block' : 'none';
            
            btn.onclick = (e) => {
                e.stopPropagation(); 
                const isActive = btn.classList.toggle('active');
                if(target) target.style.display = isActive ? 'block' : 'none';
            };
        };

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
        
        setup(`viz-toggle-${prefix}-crosshair`, `viz-layer-${prefix}-crosshair-canvas`, true);
        setup(`viz-toggle-${prefix}-points`, `viz-layer-${prefix}-points`, true);
        setup(`viz-toggle-${prefix}-outlines`, `g-outlines-${prefix}-main`, true);
        setup(`viz-toggle-${prefix}-labels`, `g-labels-${prefix}-main`, false);
        setup(`viz-toggle-${prefix}-neighbors`, `dynamic-points-${prefix}-neighbors`, (prefix === 'own'));
        setupGroup(`viz-toggle-${prefix}-context`, [`g-outlines-${prefix}-context`, `g-labels-${prefix}-context`], false);
    });
}