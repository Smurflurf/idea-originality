import { getJobHistory, toggleJobStar, deleteJobFromHistory } from '/script/data/idb-helper.js';
import { initializeTheme, setTheme, getCurrentSetting, cycleNextTheme, updateActiveStateInUI } from '/script/ui/base/themeSwitch.js';
import { initializeTranslator, isTranslateModeActive } from '/script/features/accessibility/translate.js';
import { initializeLocalization, renderPage, setLanguage, t, applyGeneralTranslations } from '/script/core/localization.js';
import { initializeSoftNavigation } from '/script/ui/navigation/navigation.js';
import { initializeSwipeNavigation } from '/script/ui/navigation/swipeNavigation.js';

import "/styling/menu.css";


export function initGlobalMenuListeners() {
	if (window.menuLogicActive) return;
	window.menuLogicActive = true;
	window.addEventListener('languageChanged', (e) => {
		console.log("[Menu] Language changed event received.");
		renderHistoryList();

		let path = window.location.pathname;
		if (path.startsWith('/')) path = path.substring(1);
		let pageName = path.split('/')[0] || 'index';
		pageName = pageName.replace('.html', '');

		const dynamicContainerId = `${pageName}-content`;
		if (document.getElementById(dynamicContainerId)) {
			// console.log(`[Menu] Re-rendering content...`); // Optional: Log reduzieren
			renderPage(dynamicContainerId);
		}
	});
}

let globalMenuElement = null;
let currentMenuJobId = null;

function ensureGlobalMenu() {
    if (globalMenuElement) return globalMenuElement;

    const menu = document.createElement('div');
    menu.className = 'options-dropdown'; 
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

    // Event Listener
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
        // Hier wird die Import-Funktion 't' benötigt, falls nicht importiert, englischen Fallback nutzen
        const msg = (typeof t === 'function') ? t('history.confirm_delete') : "Delete this entry?";
        if (currentMenuJobId && confirm(msg)) {
            await deleteJobFromHistory(currentMenuJobId);
            if ('caches' in window) { try { await caches.delete(`idea-atlas-job-${currentMenuJobId}`); } catch (err) { } }
            renderHistoryList();
        }
    });

    // Schließen bei Klick außerhalb
	document.addEventListener('click', (e) => {
		// 1. Wenn Menü gar nicht offen ist: Abbruch
		if (!globalMenuElement || !globalMenuElement.classList.contains('is-open')) return;
		// 2. Wenn Klick IM Menü war: Abbruch (Buttons darin haben eigene Logik)
		if (globalMenuElement.contains(e.target)) return;
		// 3. Wenn Klick auf einem 3-Punkte-Button war: Abbruch
		if (e.target.closest('.options-btn')) return;
 		// 4. Klick war woanders (z.B. Theme-Button, Hintergrund): ZU MACHEN!
		closeGlobalMenu();
	}, { capture: true });
    
    // Schließen bei Scroll im Container
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

    // Labels anpassen
    const starLabel = document.querySelector('#global-menu-star span');
    const starIcon = document.querySelector('#global-menu-star i');
    const deleteLabel = document.querySelector('#global-menu-delete span');
    
    const translateFn = (typeof t === 'function') ? t : (k) => k; // Fallback falls t nicht da

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

    // Positionieren
    const btnRect = event.currentTarget.getBoundingClientRect();
    menu.style.display = 'flex';
    const menuWidth = menu.offsetWidth;
    const menuHeight = menu.offsetHeight;

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



export function setupMenuInteractions() {
    initializeSoftNavigation();
	initializeSwipeNavigation();
	
    // --- TEIL A: Updates bei JEDEM Seitenwechsel (Zustand) ---
    highlightActiveLink();
    renderHistoryList();
    
    // --- TEIL B: Einmaliges Setup (Listener) ---
    if (window.menuListenersInitialized) {
        // Nur den Status aktualisieren (Haken setzen), aber KEINE Listener neu hinzufügen
        updateActiveStateInUI(getCurrentSetting());
        return;
    }
    window.menuListenersInitialized = true;

    const trigger = document.getElementById('menu-trigger');
    const sidebar = document.getElementById('sidebar-menu');
    const overlay = document.getElementById('menu-overlay');
	const closeBtn = document.getElementById('menu-close-btn');

	const preventScrollEvents = (e) => {
		if (e.cancelable) {
			e.preventDefault();
		}
	};

	function setMainContentState(isMenuOpen) {
		const contentSelectors = ['.idea-form', '.results-container', '.legal-content-wrapper', '.no-results-message'];

		contentSelectors.forEach(selector => {
			const el = document.querySelector(selector);
			if (el) el.inert = isMenuOpen; // Verhindert Fokus/Klicks im Hintergrund
		});

		if (isMenuOpen) {
			// Aggressives Locking für Mobile + Desktop
			document.body.style.overflow = 'hidden';
			document.documentElement.style.overflow = 'hidden';
			document.body.style.touchAction = 'none'; // Verhindert Gesten auf dem Body
		} else {
			document.body.style.overflow = '';
			document.documentElement.style.overflow = '';
			document.body.style.touchAction = '';
		}
	}

	if (trigger && sidebar && overlay && closeBtn) {
		function openMenu() {
			sidebar.classList.add('is-open');
			overlay.classList.add('is-visible');

			// Verhindert, dass Swipes auf dem Overlay durchgehen
			overlay.addEventListener('touchmove', preventScrollEvents, { passive: false });
			overlay.addEventListener('wheel', preventScrollEvents, { passive: false });

			// Stellt sicher, dass Scrollen im Menü selbst erlaubt bleibt, 
			// aber nicht auf den Body übergreift ("Scroll Chain" unterbrechen)
			sidebar.style.overscrollBehavior = 'contain';

			setMainContentState(true);
		}
		function closeMenu() {
			// 1. Klassen entfernen
			sidebar.classList.remove('is-open');
			overlay.classList.remove('is-visible');

			// 2. Event Listener aufräumen (Wichtig!)
			overlay.removeEventListener('touchmove', preventScrollEvents);
			overlay.removeEventListener('wheel', preventScrollEvents);

			// 3. State zurücksetzen
			setMainContentState(false);

			// 4. Styles aufräumen
			sidebar.style.transform = '';
			sidebar.style.transition = '';
			sidebar.style.overscrollBehavior = '';

			// Overlay aufräumen
			overlay.style.display = '';
			overlay.style.opacity = '';
			overlay.style.visibility = '';
			overlay.style.transition = '';
		}

        trigger.addEventListener('click', openMenu);
        closeBtn.addEventListener('click', closeMenu);
        overlay.addEventListener('click', closeMenu);
        
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && sidebar.classList.contains('is-open')) closeMenu();
        });

        const observer = new MutationObserver((mutations) => {
            for (const mutation of mutations) {
                if (mutation.attributeName === 'class' && isTranslateModeActive() && sidebar.classList.contains('is-open')) {
                    closeMenu();
                }
            }
        });
        observer.observe(document.body, { attributes: true });

        const footer = sidebar.querySelector('.sidebar-footer');
        const translateMenu = sidebar.querySelector('#translate-menu-wrapper');

        if (footer) {
            const themeMenuContainer = createThemeMenu();
            sidebar.insertBefore(themeMenuContainer, translateMenu || footer);
            applyGeneralTranslations(themeMenuContainer);
			updateActiveStateInUI(getCurrentSetting());
		}
	}


	if (window.visualViewport) {
		const sidebar = document.getElementById('sidebar-menu');

		window.visualViewport.addEventListener('resize', () => {
			// Berechne, wie viel Platz die Tastatur einnimmt
			const keyboardHeight = window.innerHeight - window.visualViewport.height;

			if (keyboardHeight > 100) {
				// Tastatur ist offen: Schiebe das Menü hoch oder passe das Padding an
				// Wir nutzen Padding-Bottom, damit der Inhalt (die Textbox) hochscrollbar bleibt
				sidebar.style.paddingBottom = `${keyboardHeight}px`;

				// Falls die Textbox gerade im Fokus ist, scrolle sie sanft ins Sichtfeld
				const activeEl = document.activeElement;
				if (activeEl && activeEl.id === 'language-filter-input') {
					setTimeout(() => {
						activeEl.scrollIntoView({ behavior: 'smooth', block: 'center' });
					}, 100);
				}
			} else {
				// Tastatur ist zu: Reset
				sidebar.style.paddingBottom = '';
			}
		});
	}
	
	
	setupAutoHideMenu();
}


/**
 * Erstellt das HTML für das Theme-Menü
 */
function createThemeMenu() {
    const wrapper = document.createElement('div');
    wrapper.className = 'theme-menu-wrapper';

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
        btn.addEventListener('click', (e) => {
            e.preventDefault(); e.stopPropagation();
            setTheme(value);
        });
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

	try {
		const jobs = await getJobHistory();
		const oldLinks = historyContainer.querySelectorAll('.history-link');
		oldLinks.forEach(link => {
			if (link._tippy) {
				link._tippy.destroy();
			}
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
            if (historyContainer.nextSibling) historyContainer.parentNode.insertBefore(bottomHr, historyContainer.nextSibling);
            else historyContainer.parentNode.appendChild(bottomHr);
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

        const allSortedByTime = [...jobs].sort((a, b) => b.timestamp - a.timestamp);
        const mostRecentJob = allSortedByTime[0];
        const remainingJobs = jobs.filter(j => j.jobId !== mostRecentJob.jobId);

        const starredJobs = remainingJobs.filter(j => j.starred).sort((a, b) => (b.starredTimestamp || 0) - (a.starredTimestamp || 0));
        const normalJobs = remainingJobs.filter(j => !j.starred).sort((a, b) => b.timestamp - a.timestamp);
        const displayList = [mostRecentJob, ...starredJobs, ...normalJobs];

        displayList.forEach(job => {
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
                renderHistoryList(); // Refresh list immediately
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
			
            tippy(link, {
                content: job.title,
                placement: 'right',
                theme: 'sidebar-tooltip',
                animation: 'fade',
                duration: [150, 0],
                delay: [100, 0],
                arrow: false,
                offset: [0, 10]
            });

            const starItem = document.createElement('button');
            starItem.className = 'dropdown-item mobile-only-action';
            const starLabel = job.starred ? t('history.unmark_favorite') : t('history.mark_favorite');
            const starIconClass = job.starred ? 'fa-solid fa-star' : 'fa-regular fa-star';
            starItem.innerHTML = `<i class="${starIconClass}" style="${job.starred ? 'color: #fbbc04;' : ''}"></i> ${starLabel}`;
            starItem.addEventListener('click', async (e) => {
                e.preventDefault(); e.stopPropagation();
                await toggleJobStar(job.jobId);
                renderHistoryList();
            });

            const deleteItem = document.createElement('button');
            deleteItem.className = 'dropdown-item delete-item';
            deleteItem.innerHTML = `<i class="fa-solid fa-trash"></i> ${t('history.delete_entry')}`;
            deleteItem.addEventListener('click', async (e) => {
                e.preventDefault(); e.stopPropagation();
                if (confirm(t('history.confirm_delete'))) {
                    await deleteJobFromHistory(job.jobId);
                    if ('caches' in window) { try { await caches.delete(`idea-atlas-cache-${job.jobId}`); } catch (err) { } }
                    renderHistoryList();
                }
            });


			actionsDiv.appendChild(optionsBtn);
			actionsDiv.appendChild(starBtn);
			// Dropdown appendChild ENTFERNT

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

function setupAutoHideMenu() {
    if (window.innerWidth > 768) return;
    const menuTrigger = document.getElementById('menu-trigger');
    if (!menuTrigger) return;
    let lastScrollY = window.scrollY; 

    window.addEventListener('scroll', () => {
        const currentScrollY = window.scrollY;
        if (currentScrollY < 50) {
            menuTrigger.classList.remove('is-scrolled-away');
        } else if (currentScrollY > lastScrollY) {
            menuTrigger.classList.add('is-scrolled-away');
        } else {
            menuTrigger.classList.remove('is-scrolled-away');
        }
        lastScrollY = currentScrollY;
    });
}
