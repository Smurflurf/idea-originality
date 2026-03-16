import { triggerPositionUpdateForViz, requestSnapshotUpdate, deactivateAllVisualizations } from '/script/viz/core/zoomAndPan.js';
import { applyColorCoding } from '/script/ui/base/colorCoder.js';

// =========================================================================
// KONFIGURATION & STATE
// =========================================================================

export let availableTabs = [];
let isSwitchingTab = false;

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

	const tab = availableTabs[index];
	if (tab.content.classList.contains('active')) {
		return window.scrollY; // WICHTIG: Behebt den Sprung nach unten beim ersten Swipe!
	}

	if (isSwitchingTab) {
		return getCompensatedScroll(tab.content.id);
	}

	return getCompensatedScroll(tab.content.id);
}

function startBackgroundPreRender() {
	// BUGFIX: Erhöht auf 4000ms! Lass dem Nutzer Zeit, die Seite flüssig zu betreten.
	// Nach 4 Sekunden ist meistens eine erste Lese-Pause erreicht.
	setTimeout(async () => {
		const { captureAndSetSnapshot } = await import('/script/viz/core/zoomAndPan.js');

		for (const tab of availableTabs) {
			if (tab.content.classList.contains('active')) continue;

			const vizContainer = tab.content.querySelector('.viz-stack-container');
			if (vizContainer && !vizContainer.classList.contains('has-snapshot')) {

				triggerPositionUpdateForViz(vizContainer.id, true);

				// Wir zwingen den Snapshot in einen echten Idle-Frame des Browsers
				await new Promise(resolve => {
					const doRender = async () => {
						await captureAndSetSnapshot(vizContainer.id);
						resolve();
					};
					if ('requestIdleCallback' in window) {
						requestIdleCallback(doRender, { timeout: 2000 });
					} else {
						setTimeout(doRender, 100);
					}
				});

				// Die Pause zwischen den Tabs von 500ms auf 800ms erhöhen
				await new Promise(r => setTimeout(r, 800));
			}
		}
	}, 4000);
}

// =========================================================================
// SCROLL SYNC LOGIC (NATIV)
// =========================================================================

function setupScrollSyncFor(pane, fromSwipe = false) {
	const scrollProxy = document.getElementById('scroll-proxy');
	if (scrollProxy) scrollProxy.style.display = 'none'; // Proxy ausblenden

	if (!pane) return;

	isSwitchingTab = true;

	const targetY = getCompensatedScroll(pane.id);

	headerDomCache = { visible: null, pos: null, transform: null, transition: null, manualClass: null };

	updateHeaderForScrollY(targetY, null, true, fromSwipe);

	// Hardwarebeschleunigter nativer Sprung, statt JS-Transform
	window.scrollTo({
		top: targetY,
		left: 0,
		behavior: 'instant'
	});

	// FIX: Transform nicht auf 'none' setzen, sondern leeren!
	// So bleibt das CSS 'transform: translateZ(0)' aktiv und der Browser
	// verliert beim ersten Swipe nicht den GPU-Containing-Block.
	pane.style.transform = ''; 

	requestAnimationFrame(() => {
		isSwitchingTab = false;
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

		if (!isVisible && headerDomCache.pos === 'absolute') {
			useTransition = false;
		} else {
			useTransition = !forceUpdate;
		}
	}

	if (headerDomCache.visible !== isVisible) {
		if (isVisible) root.classList.remove('is-header-hidden');
		else root.classList.add('is-header-hidden');
		headerDomCache.visible = isVisible;
	}

	if (headerDomCache.pos !== pos) {
		header.style.position = pos;
		header.style.top = '0';
		if (menuBtn && window.innerWidth <= 768) {
			menuBtn.style.setProperty('position', pos, 'important');
			menuBtn.style.top = '12px';
		}
		headerDomCache.pos = pos;
	}

	const transformVal = isOverscroll ? 'translateY(0)' : '';
	if (headerDomCache.transform !== transformVal) {
		header.style.transform = transformVal;
		if (menuBtn && window.innerWidth <= 768) menuBtn.style.transform = transformVal;
		headerDomCache.transform = transformVal;
	}

	const transitionVal = useTransition ? '' : 'none';
	if (headerDomCache.transition !== transitionVal) {
		header.style.transition = transitionVal;
		if (menuBtn && window.innerWidth <= 768) menuBtn.style.transition = transitionVal;
		headerDomCache.transition = transitionVal;
	}

	const manualClassVal = (pos === 'absolute' || isOverscroll);
	if (headerDomCache.manualClass !== manualClassVal) {
		if (manualClassVal) header.classList.add('is-manual-scroll');
		else header.classList.remove('is-manual-scroll');
		headerDomCache.manualClass = manualClassVal;
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

function initializeScrollHiding() {
	const root = document.body;
	lastScrollY = window.scrollY;

	window.addEventListener('scroll', () => {
		if (isSwitchingTab || document.body.classList.contains('is-swiping-active')) {
			lastScrollY = window.scrollY;
			return;
		}

		const currentY = window.scrollY;
		const diff = currentY - lastScrollY;

		// WICHTIG: Die native Scrollposition sofort für den aktiven Tab cachen
		const currentActive = availableTabs.find(t => t.content.classList.contains('active'));
		if (currentActive) {
			tabScrollRegistry[currentActive.content.id] = { y: currentY };
		}

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
	deactivateAllVisualizations();

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
		if (vizContainer) {
			triggerPositionUpdateForViz(vizContainer.id);

			if (!vizContainer.classList.contains('has-snapshot')) {
				if ('requestIdleCallback' in window) {
					requestIdleCallback(() => requestSnapshotUpdate(vizContainer.id), { timeout: 1500 });
				} else {
					setTimeout(() => requestSnapshotUpdate(vizContainer.id), 400);
				}
			}
		}
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
			if (target) target.style.display = startActive ? 'block' : 'none';

			btn.onclick = (e) => {
				e.stopPropagation();
				const isActive = btn.classList.toggle('active');
				if (target) target.style.display = isActive ? 'block' : 'none';
				requestSnapshotUpdate(`viz-stack-container-${prefix}`);
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
				requestSnapshotUpdate(`viz-stack-container-${prefix}`);
			};
		};

		setup(`viz-toggle-${prefix}-crosshair`, `viz-layer-${prefix}-crosshair-canvas`, true);
		setup(`viz-toggle-${prefix}-points`, `viz-layer-${prefix}-points`, true);
		setup(`viz-toggle-${prefix}-outlines`, `g-outlines-${prefix}-main`, true);
		setup(`viz-toggle-${prefix}-labels`, `g-labels-${prefix}-main`, false);
		setup(`viz-toggle-${prefix}-neighbors`, `dynamic-points-${prefix}-neighbors`, (prefix === 'own'));
		setupGroup(`viz-toggle-${prefix}-context`, [`g-outlines-${prefix}-context`, `g-labels-${prefix}-context`], false);
	});

	initializeScrollHiding();
	startBackgroundPreRender();
}