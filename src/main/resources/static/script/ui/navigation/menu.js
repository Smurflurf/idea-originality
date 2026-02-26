import { getJobHistory, toggleJobStar, deleteJobFromHistory } from '/script/data/idb-helper.js';
import { initializeTheme, setTheme, getCurrentSetting, cycleNextTheme, updateActiveStateInUI } from '/script/ui/base/themeSwitch.js';
import { renderPage, t, applyGeneralTranslations } from '/script/core/localization.js';
import { initializeSoftNavigation } from '/script/ui/navigation/navigation.js';
import { initializeSwipeNavigation } from '/script/ui/navigation/swipeNavigation.js';
import { on, EVENTS } from '/script/core/eventBus.js';

import "/styling/menu.css";

// --- STATE MANAGEMENT ---

function setMainContentState(isMenuOpen) {
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
        if (document.querySelector('.viz-toggle-header')) return;

        const trigger = document.getElementById('menu-trigger');
        if (!trigger) return;

        if (window.innerWidth > 768) {
            trigger.classList.remove('is-scrolled-away');
            return;
        }

        const currentY = window.scrollY;
        
        // Erst ab 60px ausblenden, um Flackern am oberen Rand zu vermeiden
        if (currentY > 60 && currentY > lastY) {
            trigger.classList.add('is-scrolled-away'); // Verstecken
        } else {
            trigger.classList.remove('is-scrolled-away'); // Anzeigen
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
             trigger.innerHTML = '<i class="fa-solid fa-bars"></i>';
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
        function openMenu() {
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

            // --- FIX 1: THEME BUTTON LOGIK ---
            const footer = sidebar.querySelector('.sidebar-footer');
            const translateMenu = sidebar.querySelector('#translate-menu-wrapper');
            
            // Wir prüfen jetzt spezifisch auf unsere ID, nicht mehr auf die Klasse
            if (footer && !sidebar.querySelector('#generated-theme-menu')) {
                const themeMenuContainer = createThemeMenu();
                // Einfügen VOR dem Translate-Menu (falls da), sonst vor Footer
                sidebar.insertBefore(themeMenuContainer, translateMenu || footer);
                
                applyGeneralTranslations(themeMenuContainer);
                updateActiveStateInUI(getCurrentSetting());
            }
        }
    }
}


// --- 3-PUNKTE MENÜ (History Items) ---
let globalMenuElement = null;
let currentMenuJobId = null;

function ensureGlobalMenu() {
    // FIX 2 (Teil A): Wenn das Element zwar in der Variable ist, aber nicht mehr im DOM 
    // (durch SPA Navigation gelöscht), müssen wir es neu erstellen.
    if (globalMenuElement && !document.body.contains(globalMenuElement)) {
        globalMenuElement = null;
    }

    if (globalMenuElement) return globalMenuElement;

    const menu = document.createElement('div');
    menu.className = 'options-dropdown';
    // Wichtig: ID oder Klasse, die von navigation.js als "persistent" erkannt wird, 
    // hilft zusätzlich, ist aber hier durch den Check oben abgesichert.
    menu.innerHTML = `
        <button class="dropdown-item" id="global-menu-star">
            <i class="fa-regular fa-star"></i> <span>Favorite</span>
        </button>
        <button class="dropdown-item delete-item" id="global-menu-delete">
            <i class="fa-solid fa-trash"></i> <span>Delete</span>
        </button>
    `;
    document.body.appendChild(menu);
    globalMenuElement = menu;

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

    return menu;
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
    const wrapper = document.createElement('div');
    wrapper.className = 'theme-menu-wrapper';
    // FIX 1: Eindeutige ID hinzufügen
    wrapper.id = 'generated-theme-menu'; 
    
    const mainBtn = document.createElement('button');
    mainBtn.className = 'theme-main-btn';
    mainBtn.innerHTML = `
        <div class="theme-main-btn-content">
            <i class="fa-solid fa-palette theme-icon"></i>
            <span data-i18n="themes.main_label">Theme</span>
        </div>
        <i class="fa-solid fa-chevron-right theme-arrow"></i>
    `;
    mainBtn.addEventListener('click', (e) => {
        e.preventDefault(); e.stopPropagation();
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
    const subMenu = document.createElement('div');
    subMenu.className = 'theme-submenu';
    const createItem = (i18nKey, labelFallback, iconClass, value) => {
        const btn = document.createElement('button');
        btn.className = 'theme-submenu-item';
        btn.setAttribute('data-theme-value', value);
        btn.innerHTML = `<span><i class="${iconClass}"></i> <span data-i18n="${i18nKey}">${labelFallback}</span></span><i class="fa-solid fa-check check-icon"></i>`;
        btn.addEventListener('click', (e) => { e.preventDefault(); e.stopPropagation(); setTheme(value); });
        return btn;
    };
    subMenu.appendChild(createItem('themes.light', 'Light', 'fa-solid fa-sun', 'light'));
    subMenu.appendChild(createItem('themes.dark', 'Dark', 'fa-solid fa-moon', 'dark'));
    subMenu.appendChild(createItem('themes.midnight', 'Midnight', 'fa-solid fa-star', 'midnight'));
    const hr = document.createElement('hr');
    hr.style.cssText = "margin: 6px 0; border: 0; border-top: 1px solid var(--border-subtle);";
    subMenu.appendChild(hr);
    subMenu.appendChild(createItem('themes.system', 'System', 'fa-solid fa-desktop', 'system'));
    wrapper.appendChild(mainBtn);
    wrapper.appendChild(subMenu);
    return wrapper;
}

export async function renderHistoryList() {
    const historyContainer = document.getElementById('menu-history-container');
    if (!historyContainer) return;
    ensureGlobalMenu();
    // ... Rest der Funktion wie gehabt ...
    try {
        const jobs = await getJobHistory();
        historyContainer.querySelectorAll('.history-link').forEach(link => {
            if (link._tippy) link._tippy.destroy();
        });
        historyContainer.innerHTML = '';
        let fixedLabel = document.getElementById('history-fixed-label');
        if (!fixedLabel) {
            fixedLabel = document.createElement('div');
            fixedLabel.id = 'history-fixed-label';
            fixedLabel.style.cssText = "display: none; align-items: center; gap: 10px; padding: 10px 20px 5px; font-size: 0.8rem; color: var(--text-quaternary); text-transform: uppercase; letter-spacing: 1px; font-weight: bold; margin-top: 0px; flex-shrink: 0;";
            fixedLabel.innerHTML = `<i class="fa-regular fa-clock"></i> <span data-i18n="history.label">Recent</span>`;
            historyContainer.parentNode.insertBefore(fixedLabel, historyContainer);
        }
        let bottomHr = document.getElementById('history-bottom-hr');
        if (!bottomHr) {
            bottomHr = document.createElement('hr');
            bottomHr.id = 'history-bottom-hr';
            bottomHr.style.cssText = "border: 0; height: 1px; background: linear-gradient(to right, transparent, rgba(var(--text-primary-rgb), 0.1), transparent); margin: 20px 0; flex-shrink: 0; display: none;";
            historyContainer.parentNode.insertBefore(bottomHr, historyContainer.nextSibling || null);
        }
        if (jobs.length === 0) {
            fixedLabel.style.display = 'none';
            bottomHr.style.display = 'none';
            return;
        }
        fixedLabel.style.display = 'flex';
        bottomHr.style.display = 'block';
        const currentPath = window.location.pathname;
        let currentJobId = null;
        if (currentPath.startsWith('/results/')) currentJobId = currentPath.split('/')[2];

		const sortedJobs = [...jobs].sort((a, b) => {
			// 1. Check: Ist einer der beiden der aktuell besuchte Job?
			const aIsActive = (a.jobId === currentJobId);
			const bIsActive = (b.jobId === currentJobId);

			if (aIsActive !== bIsActive) {
				return aIsActive ? -1 : 1; // Aktueller Job immer ganz nach oben
			}

			// 2. Check: Favoriten (nur wenn keiner der beiden der aktive Job ist)
			if (a.starred !== b.starred) {
				return a.starred ? -1 : 1; // Starred vor Non-Starred
			}

			// 3. Check: Zeitstempel (innerhalb der Gruppen)
			// Falls beide starred oder beide normal sind: Neueste zuerst
			return b.timestamp - a.timestamp;
		});

        sortedJobs.forEach(job => {
            const link = document.createElement('a');
            link.href = `/results/${job.jobId}`;
            link.className = 'history-link';
            if (job.jobId === currentJobId) link.classList.add('active');
            const textSpan = document.createElement('span');
            textSpan.className = 'history-text';
            textSpan.textContent = job.title;
            const actionsDiv = document.createElement('div');
            actionsDiv.className = 'history-actions';
            const starBtn = document.createElement('button');
            starBtn.className = `icon-btn star-btn ${job.starred ? 'is-starred' : ''}`;
            starBtn.innerHTML = `<i class="fa-regular fa-star star-outline"></i><i class="fa-solid fa-star star-fill"></i>`;
            starBtn.addEventListener('click', async (e) => {
                e.preventDefault(); e.stopPropagation();
                await toggleJobStar(job.jobId);
                renderHistoryList();
            });
            const optionsBtn = document.createElement('button');
            optionsBtn.className = 'icon-btn options-btn';
            optionsBtn.innerHTML = '<i class="fa-solid fa-ellipsis-vertical"></i>';
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
            actionsDiv.appendChild(optionsBtn);
            actionsDiv.appendChild(starBtn);
            link.appendChild(textSpan);
            link.appendChild(actionsDiv);
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
        if (link.getAttribute('href') === currentPath && !link.classList.contains('history-link')) {
            link.classList.add('active');
        } else {
            link.classList.remove('active');
        }
    });
}
