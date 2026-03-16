import { getJobHistory, toggleJobStar, deleteJobFromHistory } from '/script/data/idb-helper.js';
import { initializeTheme, setTheme, getCurrentSetting, cycleNextTheme, updateActiveStateInUI } from '/script/ui/base/themeSwitch.js';
import { renderPage, t, applyGeneralTranslations } from '/script/core/localization.js';
import { initializeSoftNavigation } from '/script/ui/navigation/navigation.js';
import { initializeSwipeNavigation } from '/script/ui/navigation/swipeNavigation.js';
import { on, EVENTS } from '/script/core/eventBus.js';
import { getTemplate, renderTemplate } from '/script/core/templateManager.js';
import { toggleFaqSelectionMode, openFaqSearch } from '/script/features/faq/faqManager.js';

import "/styling/menu.css";

// --- STATE MANAGEMENT ---

export function setMainContentState(isMenuOpen) {
    const contentSelectors = ['.idea-form', '.results-container', '.legal-content-wrapper', '.no-results-message'];
    contentSelectors.forEach(selector => {
        const el = document.querySelector(selector);
        if (el) el.inert = isMenuOpen;
    });

    if (isMenuOpen) {
        document.body.style.overflow = 'hidden';
        document.documentElement.style.overflow = 'hidden';
        document.body.style.touchAction = 'none';
    } else {
        document.body.style.overflow = '';
        document.documentElement.style.overflow = '';
        document.body.style.touchAction = '';
    }
}

export function closeSidebar() {
    const sidebar = document.getElementById('sidebar-menu');
    const overlay = document.getElementById('menu-overlay');

	document.querySelectorAll('.theme-menu-wrapper.is-active').forEach(el => el.classList.remove('is-active'));
	
    if (sidebar && sidebar.classList.contains('is-open')) {
        sidebar.classList.remove('is-open');
        sidebar.style.transform = '';
        sidebar.style.transition = '';
        sidebar.style.overscrollBehavior = '';
        sidebar.inert = true;
    }

    if (overlay && overlay.classList.contains('is-visible')) {
        overlay.classList.remove('is-visible');
        overlay.style.display = '';
        overlay.style.opacity = '';
        overlay.style.visibility = '';
        overlay.style.transition = '';
    }

    setMainContentState(false);
    document.body.classList.remove('is-swiping-active');
}


// --- 1. GLOBAL LISTENER ---

function initGlobalScrollHideFallback() {
    let lastY = window.scrollY;
    
    window.addEventListener('scroll', () => {
        const header = document.querySelector('.viz-toggle-header');
        if (header && !header.closest('.mockup-isolation-wrapper')) return;

        const trigger = document.getElementById('menu-trigger');
        if (!trigger) return;

        if (window.innerWidth > 768) {
            trigger.classList.remove('is-scrolled-away');
            return;
        }

        const currentY = window.scrollY;
        
        if (currentY > 60 && currentY > lastY) {
            trigger.classList.add('is-scrolled-away'); 
        } else {
            trigger.classList.remove('is-scrolled-away'); 
        }
        
        lastY = currentY;
    }, { passive: true });
}


export function initGlobalMenuListeners() {
    if (window.menuLogicActive) return;
    window.menuLogicActive = true;
    initializeSoftNavigation();
    initializeSwipeNavigation();
	initGlobalScrollHideFallback(); 
	
    on(EVENTS.LANG_CHANGED, (data) => {
        renderHistoryList();
        let path = window.location.pathname;
        if (path.startsWith('/')) path = path.substring(1);
        let pageName = path.split('/')[0] || 'index';
        pageName = pageName.replace('.html', '');
        const dynamicContainerId = `${pageName}-content`;
        if (document.getElementById(dynamicContainerId)) renderPage(dynamicContainerId);
    });
}

// --- 2. DOM INTERACTIONS ---
export function setupMenuInteractions() {
    highlightActiveLink();
    renderHistoryList();
    updateActiveStateInUI(getCurrentSetting());
    
    const trigger = document.getElementById('menu-trigger');
    if (trigger) {
        trigger.classList.remove('is-hidden');
        trigger.classList.remove('is-scrolled-away');
        if (!trigger.innerHTML.trim()) {
            trigger.textContent = '';
            const mIcon = document.createElement('i');
            mIcon.className = 'fa-solid fa-bars';
            trigger.appendChild(mIcon);
        }
    }

    const sidebar = document.getElementById('sidebar-menu');
    const overlay = document.getElementById('menu-overlay');
    const closeBtn = document.getElementById('menu-close-btn');

    if (sidebar && !sidebar.classList.contains('is-open')) {
        sidebar.inert = true;
    }

    if (trigger && !trigger.dataset.listenerAttached) {
        trigger.dataset.listenerAttached = 'true';

        const preventScrollEvents = (e) => { if (e.cancelable) e.preventDefault(); };
        async function openMenu() {
            if (sidebar) sidebar.inert = false;
            sidebar.classList.add('is-open');
            overlay.classList.add('is-visible');
            overlay.addEventListener('touchmove', preventScrollEvents, { passive: false });
            overlay.addEventListener('wheel', preventScrollEvents, { passive: false });
            sidebar.style.overscrollBehavior = 'contain';
            setMainContentState(true);
        }

        trigger.addEventListener('click', openMenu);
        if (closeBtn) closeBtn.addEventListener('click', closeSidebar);
        if (overlay) overlay.addEventListener('click', closeSidebar);
        
        if (sidebar) {
            sidebar.addEventListener('click', (e) => {
                const link = e.target.closest('a');
                if (link && link.href) closeSidebar();
            });

            const footer = sidebar.querySelector('.sidebar-footer');
            const translateMenu = sidebar.querySelector('#translate-menu-wrapper');
            
            if (footer && !sidebar.querySelector('#generated-theme-menu')) {
                const themeMenuContainer = createThemeMenu();
                sidebar.insertBefore(themeMenuContainer, translateMenu || footer);
                applyGeneralTranslations(themeMenuContainer);
                updateActiveStateInUI(getCurrentSetting());
            }

			if (footer && !sidebar.querySelector('#generated-faq-menu')) {
				const faqMenuContainer = createFaqMenu();
				sidebar.insertBefore(faqMenuContainer, footer);
				applyGeneralTranslations(faqMenuContainer);
			}
        }
    }
}


// --- 3-PUNKTE MENÜ (History Items) ---
let globalMenuElement = null;
let currentMenuJobId = null;

function ensureGlobalMenu() {
    // Falls das Element existiert und noch im DOM ist, direkt zurückgeben
    if (globalMenuElement && document.body.contains(globalMenuElement)) {
        return globalMenuElement;
    }

    // Erstellen, in den Body einfügen und übersetzen
    globalMenuElement = renderTemplate('history-options-menu');
    if (!globalMenuElement) return null;

    // Listener nur beim ersten Mal binden
    if (globalMenuElement.dataset.eventsAttached !== 'true') {
        document.getElementById('global-menu-star').addEventListener('click', async (e) => {
            e.stopPropagation();
            closeGlobalMenu();
            if (currentMenuJobId) {
                await toggleJobStar(currentMenuJobId);
                renderHistoryList();
            }
        });

        document.getElementById('global-menu-delete').addEventListener('click', async (e) => {
            e.stopPropagation();
            closeGlobalMenu();
            const msg = (typeof t === 'function') ? t('history.confirm_delete') : "Delete this entry?";
            if (currentMenuJobId && confirm(msg)) {
                await deleteJobFromHistory(currentMenuJobId);
                if ('caches' in window) { try { await caches.delete(`idea-atlas-job-${currentMenuJobId}`); } catch (err) {} }
                renderHistoryList();
            }
        });

        document.addEventListener('click', (e) => {
            if (!globalMenuElement || !globalMenuElement.classList.contains('is-open')) return;
            if (globalMenuElement.contains(e.target)) return;
            if (e.target.closest('.options-btn')) return;
            closeGlobalMenu();
        }, { capture: true });
        
        const historyContainer = document.getElementById('menu-history-container');
        if (historyContainer) historyContainer.addEventListener('scroll', closeGlobalMenu);

        globalMenuElement.dataset.eventsAttached = 'true';
    }

    return globalMenuElement;
}

function openGlobalMenu(event, job) {
    const menu = ensureGlobalMenu();
    currentMenuJobId = job.jobId;

    const linkRow = event.target.closest('.history-link');
    if (linkRow) {
        linkRow.classList.add('force-hover');
    }

    const starLabel = menu.querySelector('#global-menu-star span');
    const starIcon = menu.querySelector('#global-menu-star i');
    const deleteLabel = menu.querySelector('#global-menu-delete span');
    const translateFn = (typeof t === 'function') ? t : (k) => k;

    if (job.starred) {
        starLabel.textContent = translateFn('history.unmark_favorite') || "Unmark Favorite";
        starIcon.className = 'fa-solid fa-star';
        starIcon.style.color = '#fbbc04';
    } else {
        starLabel.textContent = translateFn('history.mark_favorite') || "Mark Favorite";
        starIcon.className = 'fa-regular fa-star';
        starIcon.style.color = '';
    }
    deleteLabel.textContent = translateFn('history.delete_entry') || "Delete";

    const btnRect = event.currentTarget.getBoundingClientRect();
    menu.style.display = 'flex';
    const menuWidth = menu.offsetWidth;
    const menuHeight = menu.offsetHeight;

    // Positionierung relativ zum Viewport (funktioniert auch bei overflow:hidden body)
    let top = btnRect.bottom + 5;
    if (top + menuHeight > window.innerHeight) top = btnRect.top - menuHeight - 5;
    let left = btnRect.right - menuWidth;
    if (left < 10) left = 10;

    menu.style.top = `${top}px`;
    menu.style.left = `${left}px`;
    menu.classList.add('is-open');
}

function closeGlobalMenu() {
    if (globalMenuElement) {
        globalMenuElement.classList.remove('is-open');
        globalMenuElement.style.display = 'none';
    }
    document.querySelectorAll('.options-btn.menu-active').forEach(btn => btn.classList.remove('menu-active'));
    document.querySelectorAll('.history-link.force-hover').forEach(el => el.classList.remove('force-hover'));
}


// --- UI RENDERING FUNCTIONS ---

function createThemeMenu() {
    // 1. Template holen
    const fragment = getTemplate('tpl-generated-theme-menu');
    if (!fragment) return null;

    const wrapper = fragment.firstElementChild;
    
    // 2. Übersetzungen anwenden, da wir getTemplate statt renderTemplate nutzen
    applyGeneralTranslations(wrapper);

    // 3. Logik anbinden
    const mainBtn = wrapper.querySelector('.theme-main-btn');
    mainBtn.addEventListener('click', (e) => {
        e.preventDefault(); 
		e.stopPropagation();
        const translateMenu = document.getElementById('translate-menu-wrapper');
        if (translateMenu) translateMenu.classList.remove('is-active');
        
        const isDesktop = window.matchMedia('(hover: hover)').matches;
        const isArrowClick = e.target.closest('.theme-arrow');
        
        if (isDesktop && !isArrowClick) {
            cycleNextTheme();
            wrapper.classList.remove('is-active');
        } else {
            wrapper.classList.toggle('is-active');
        }
    });

    document.addEventListener('click', (e) => {
        if (!wrapper.contains(e.target)) wrapper.classList.remove('is-active');
    });

    wrapper.querySelectorAll('.theme-submenu-item').forEach(btn => {
        btn.addEventListener('click', (e) => { 
            e.preventDefault(); 
            e.stopPropagation(); 
            setTheme(btn.getAttribute('data-theme-value')); 
        });
    });
    
    return wrapper;
}

function createFaqMenu() {
    const fragment = getTemplate('tpl-faq-menu');
    if (!fragment) return null;
    const wrapper = fragment.firstElementChild;

    // Haupt-Button Logik: Split zwischen Text (Suche) und Pfeil (Submenü)
    const mainBtn = wrapper.querySelector('.theme-main-btn');
    mainBtn.addEventListener('click', (e) => {
        e.preventDefault();
		e.stopPropagation();

		const isDesktop = window.matchMedia('(hover: hover)').matches;
		const isArrowClick = e.target.closest('.theme-arrow');

        // Andere Menüs schließen
        document.querySelectorAll('.theme-menu-wrapper').forEach(el => {
            if (el !== wrapper) el.classList.remove('is-active');
        });

		if (isDesktop && !isArrowClick) {
			openFaqSearch();
			wrapper.classList.remove('is-active');
		} else {
			wrapper.classList.toggle('is-active');
		}
    });
    
    // Klick außerhalb schließt es ebenfalls
    document.addEventListener('click', (e) => {
        if (!wrapper.contains(e.target)) wrapper.classList.remove('is-active');
    });

    // Aktionen für die Buttons im Submenü
	const btnSelect = wrapper.querySelector('#faq-btn-select');
    const btnSearch = wrapper.querySelector('#faq-btn-search');

    if (btnSelect) {
        btnSelect.addEventListener('click', (e) => {
            e.preventDefault();
            wrapper.classList.remove('is-active');
            toggleFaqSelectionMode(e); 
        });
    }

    if (btnSearch) {
        btnSearch.addEventListener('click', (e) => {
            e.preventDefault();
            wrapper.classList.remove('is-active');
            openFaqSearch(); 
        });
    }

    return wrapper;
}

export async function renderHistoryList() {
	const historyContainer = document.getElementById('menu-history-container');
	if (!historyContainer) return;
	ensureGlobalMenu();
	try {
		const jobs = await getJobHistory();
		historyContainer.querySelectorAll('.history-link').forEach(link => {
			if (link._tippy) link._tippy.destroy();
		});
		historyContainer.textContent = ''; // Sicherer als innerHTML = ''

		let fixedLabel = document.getElementById('history-fixed-label');
		if (!fixedLabel) {
			const tpl = getTemplate('tpl-history-fixed-label');
			if (tpl) {
				applyGeneralTranslations(tpl); 
				fixedLabel = tpl.firstElementChild;
				historyContainer.parentNode.insertBefore(fixedLabel, historyContainer);
			}
		}

		let bottomHr = document.getElementById('history-bottom-hr');
		if (!bottomHr) {
			const tpl = getTemplate('tpl-history-bottom-hr');
			if (tpl) {
				bottomHr = tpl.firstElementChild;
				historyContainer.parentNode.insertBefore(bottomHr, historyContainer.nextSibling || null);
			}
		}

		if (jobs.length === 0) {
			if (fixedLabel) fixedLabel.style.display = 'none';
			if (bottomHr) bottomHr.style.display = 'none';
			return;
		}
		if (fixedLabel) fixedLabel.style.display = 'flex';
		if (bottomHr) bottomHr.style.display = 'block';

		const currentPath = window.location.pathname;
		let currentJobId = null;
		if (currentPath.startsWith('/results/')) currentJobId = currentPath.split('/')[2];

		const sortedJobs = [...jobs].sort((a, b) => {
			const aIsActive = (a.jobId === currentJobId);
			const bIsActive = (b.jobId === currentJobId);
			if (aIsActive !== bIsActive) return aIsActive ? -1 : 1;
			if (a.starred !== b.starred) return a.starred ? -1 : 1;
			return b.timestamp - a.timestamp;
		});

		sortedJobs.forEach(job => {
			const tpl = getTemplate('tpl-history-item');
			if (!tpl) return;
			applyGeneralTranslations(tpl); 
			const link = tpl.firstElementChild;
			link.href = `/results/${job.jobId}`;
			if (job.jobId === currentJobId) link.classList.add('active');

			link.querySelector('.history-text').textContent = job.title;

			const starBtn = link.querySelector('.star-btn');
			if (job.starred) starBtn.classList.add('is-starred');

			starBtn.addEventListener('click', async (e) => {
				e.preventDefault(); e.stopPropagation();
				await toggleJobStar(job.jobId);
				renderHistoryList();
			});

			const optionsBtn = link.querySelector('.options-btn');
			optionsBtn.addEventListener('click', (e) => {
				e.preventDefault(); e.stopPropagation();
				if (currentMenuJobId === job.jobId && globalMenuElement && globalMenuElement.classList.contains('is-open')) {
					closeGlobalMenu();
					optionsBtn.classList.remove('menu-active');
				} else {
					closeGlobalMenu();
					optionsBtn.classList.add('menu-active');
					openGlobalMenu(e, job);
				}
			});

			if (window.tippy) {
				tippy(link, { content: job.title, placement: 'right', theme: 'sidebar-tooltip', animation: 'fade', duration: [150, 0], delay: [100, 0], arrow: false, offset: [0, 10] });
			}
			historyContainer.appendChild(link);
		});
	} catch (e) {
		console.error("[Menu] Failed to load history", e);
	}
}

export function highlightActiveLink() {
	const menuLinks = document.querySelectorAll('.sidebar-menu a');
	const currentPath = window.location.pathname;
	menuLinks.forEach(link => {
		if (link.getAttribute('href') === currentPath &&
			!link.classList.contains('history-link') &&
			!link.classList.contains('faq-small-link')) {
			link.classList.add('active');
		} else {
			link.classList.remove('active');
		}
    });
}
