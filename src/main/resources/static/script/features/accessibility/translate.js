import { getCsrfToken } from '/script/core/security.js';
import { setLanguage } from '/script/core/localization.js';
import * as TTS from '/script/features/accessibility/tts.js';

// State Management
let activeMode = null; // null, 'translate', 'read'
let currentHoveredElement = null;
const STORAGE_KEY_LANG = 'idea-atlas-translate-lang';
const STORAGE_KEY_TRANSLATIONS = 'idea-atlas-active-translations'; // NEU: Key für SessionStorage
let currentTargetLanguage = localStorage.getItem(STORAGE_KEY_LANG) || 'system';

// Elements
const translateButton = document.getElementById('translate-mode-btn');
const readButton = document.getElementById('read-text-mode-btn');
const translateIndicator = document.getElementById('translate-active-indicator');
const readIndicator = document.getElementById('read-active-indicator');
const translateMenuWrapper = document.getElementById('translate-menu-wrapper');
const menuTrigger = document.getElementById('menu-trigger');

let cursorElement = null;
let activeReadingElement = null; 
const LANG_CACHE_KEY = 'idea-atlas-languages';

export const isTranslateModeActive = () => activeMode !== null;

// --- NEU: PERSISTENCE LOGIC ---

/**
 * Erstellt einen einfachen Hash aus einem String, um ihn als Key zu nutzen.
 * Wir nutzen trim(), um Whitespace-Probleme zu vermeiden.
 */
function getSimpleHash(str) {
    let hash = 0;
    if (str.length === 0) return hash;
    const cleanStr = str.trim(); 
    for (let i = 0; i < cleanStr.length; i++) {
        const char = cleanStr.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash; // Convert to 32bit integer
    }
    return hash.toString();
}

/**
 * Speichert eine Übersetzung im SessionStorage.
 * Struktur: { "Pfad/Zur/Seite": { "Hash_Original": "HTML_Übersetzt" } }
 */
function persistTranslation(originalHtml, translatedHtml) {
    try {
        const path = window.location.pathname;
        let storage = JSON.parse(sessionStorage.getItem(STORAGE_KEY_TRANSLATIONS) || '{}');
        if (!storage[path]) storage[path] = {};
        
        const key = getSimpleHash(originalHtml);
        storage[path][key] = translatedHtml;
        
        sessionStorage.setItem(STORAGE_KEY_TRANSLATIONS, JSON.stringify(storage));
    } catch (e) {
        console.warn("[Translate] Failed to save persistence:", e);
    }
}

/**
 * Löscht eine Übersetzung aus dem Speicher (wenn zurück getoggled wird).
 */
function removePersistedTranslation(originalHtml) {
    try {
        const path = window.location.pathname;
        let storage = JSON.parse(sessionStorage.getItem(STORAGE_KEY_TRANSLATIONS) || '{}');
        
        if (storage[path]) {
            const key = getSimpleHash(originalHtml);
            delete storage[path][key];
            sessionStorage.setItem(STORAGE_KEY_TRANSLATIONS, JSON.stringify(storage));
        }
    } catch (e) {}
}

/**
 * Scannt die Seite und stellt bekannte Übersetzungen wieder her.
 * Wird beim Init und bei dynamicContentLoaded aufgerufen.
 * @param {HTMLElement} container - Der Bereich, der gescannt werden soll (default document.body)
 */
export function restoreTranslations(container = document.body) {
    try {
        const path = window.location.pathname;
        const storage = JSON.parse(sessionStorage.getItem(STORAGE_KEY_TRANSLATIONS) || '{}');
        const pageTranslations = storage[path];

        if (!pageTranslations || Object.keys(pageTranslations).length === 0) return;

        // Wir suchen nach Text-Kandidaten. 
        // Wir nutzen die gleiche Logik wie isValidTarget, aber iterativ.
        const candidates = container.querySelectorAll('p, h1, h2, h3, h4, h5, h6, li, span, div, a');
        
        candidates.forEach(el => {
            // Performance: Überspringe Elemente, die bereits übersetzt sind oder ungültig sind
            if (el.dataset.isTranslated === 'true') return;
            if (!isValidTarget(el)) return;

            // Wir hashen den aktuellen (originalen) Inhalt
            const currentHtml = el.innerHTML;
            const hash = getSimpleHash(currentHtml);

            if (pageTranslations[hash]) {
                // Treffer! Wiederherstellen.
                console.log(`[Translate] Restoring translation for hash ${hash}`);
                el.dataset.originalHtml = currentHtml;
                el.innerHTML = pageTranslations[hash];
                el.dataset.isTranslated = 'true';
            }
        });

    } catch (e) {
        console.warn("[Translate] Error restoring translations:", e);
    }
}

// --- ENDE NEUE LOGIC ---


function getLanguages() {
    const serverData = window.SERVER_LANGUAGES;
    if (serverData && Array.isArray(serverData) && serverData.length > 0) {
        try { localStorage.setItem(LANG_CACHE_KEY, JSON.stringify(serverData)); } catch (e) {}
        return serverData;
    }
    const cachedData = localStorage.getItem(LANG_CACHE_KEY);
    if (cachedData) {
        try {
            const parsed = JSON.parse(cachedData);
            if (Array.isArray(parsed) && parsed.length > 0) return parsed;
        } catch (e) {}
    }
    return [];
}

function renderLanguageList(displayLocale) {
    const container = document.getElementById('translate-language-list');
    if (!container) return;

    const languages = getLanguages(); 

    if (languages.length === 0) {
        container.innerHTML = '<div style="padding:10px; color:var(--text-disabled); text-align:center; font-size:0.8rem;"><i>Offline / No Data</i></div>';
        return;
    }

    const activeLang = document.documentElement.lang;

    let uiLocale = displayLocale;
    if (!uiLocale || uiLocale === 'system') {
        uiLocale = navigator.language || 'en';
    }

    let uiFormatter;
    try {
        uiFormatter = new Intl.DisplayNames([uiLocale], { type: 'language' });
    } catch (e) {
        uiFormatter = { of: (code) => code }; 
    }

    container.innerHTML = '';
    const fragment = document.createDocumentFragment();

    const renderedList = languages.map(lang => {
        const code = lang.code;
        const serverName = lang.name;
        
        let displayName = serverName;
        let nativeName = "";

        try {
            const formattedUiName = uiFormatter.of(code);
            if (formattedUiName && formattedUiName.toLowerCase() !== code.toLowerCase()) {
                displayName = formattedUiName;
            }
            const nativeFormatter = new Intl.DisplayNames([code], { type: 'language' });
            const formattedNativeName = nativeFormatter.of(code);
            if (formattedNativeName) nativeName = formattedNativeName;
        } catch(e) {}

        const uniqueTerms = new Set([code, serverName, displayName, nativeName]);
        const searchString = Array.from(uniqueTerms).filter(Boolean).join(" ").toLowerCase();

        return {
            code: code,
            displayName: displayName,
            nativeName: nativeName,
            searchString: searchString,
            isActive: (code === activeLang) 
        };
    });

    renderedList.sort((a, b) => {
        if (a.isActive) return -1;
        if (b.isActive) return 1;
        return a.displayName.localeCompare(b.displayName);
    });

    renderedList.forEach(item => {
        const btn = document.createElement('button');
        btn.className = 'theme-submenu-item language-option';
        btn.dataset.lang = item.code;
        btn.dataset.search = item.searchString;

        btn.innerHTML = `
            <span>
                <i class="fa-solid fa-globe" style="font-size: 0.8em; opacity: 0.7;"></i> 
                <span>${item.displayName}</span>
            </span>
            <i class="fa-solid fa-check check-icon" style="opacity: ${item.isActive ? '1' : '0'}"></i>
        `;
        
        fragment.appendChild(btn);
    });

    container.appendChild(fragment);
}

function createCursorElement() {
    const el = document.createElement('div');
    el.className = 'custom-translate-cursor';
    document.body.appendChild(el);
    return el;
}

function isValidTarget(element) {
    if (!element) return false;
    if (element.closest('.sidebar-menu')) return false;
    if (element.classList.contains('menu-overlay')) return false;
    if (element.closest('.download-popup-overlay')) return false;
    if (element.closest('.recorder-overlay')) return false;
    const tagName = element.tagName;
    const forbiddenTags = ['HTML', 'BODY', 'HEAD', 'SCRIPT', 'STYLE', 'LINK', 'META', 'IMG', 'SVG', 'PATH', 'CANVAS', 'VIDEO', 'AUDIO', 'HR', 'BR', 'BUTTON', 'INPUT', 'TEXTAREA', 'SELECT', 'LABEL', 'FORM', 'PRE', 'CODE'];
    if (forbiddenTags.includes(tagName)) return false;
    const textContent = element.textContent.trim();
    if (textContent.length === 0) return false;
    
    // Prüfen, ob das Element NUR Text enthält und keine Block-Kinder
    // (um zu vermeiden, dass wir ganze Container übersetzen)
    if (['DIV', 'SECTION', 'MAIN', 'ARTICLE', 'HEADER', 'FOOTER', 'UL', 'LI'].includes(tagName)) {
        const hasDirectText = Array.from(element.childNodes).some(node => node.nodeType === Node.TEXT_NODE && node.textContent.trim().length > 0);
        if (!hasDirectText) return false;
    }
    return true;
}

function moveVirtualCursor(event) {
    if (cursorElement) {
        cursorElement.style.left = event.clientX + 'px';
        cursorElement.style.top = event.clientY + 'px';
    }
}

async function handleTextClick(event) {
    if (event.target.closest('.sidebar-menu')) return;
    if (event.target.classList.contains('is-translating')) { event.preventDefault(); event.stopPropagation(); return; }

    event.preventDefault();
    event.stopPropagation();
    
    const targetElement = event.target;

    if (!isValidTarget(targetElement)) {
        exitSelectionMode();
        return;
    }
    
    const modeToExecute = activeMode; 
    exitSelectionMode();

    let textContent = targetElement.innerText.trim();
    if (targetElement.classList.contains('json-string')) {
        if (textContent.startsWith('"') && textContent.endsWith('"')) textContent = textContent.slice(1, -1);
	}

	if (modeToExecute === 'read') {
		document.querySelectorAll('.is-reading').forEach(el => el.classList.remove('is-reading', 'is-paused'));
		activeReadingElement = targetElement;
		activeReadingElement.classList.add('is-reading');

        const contextState = {
            viewId: null,
            clusterId: null,
            originUrl: window.location.href
        };

        const mainPane = targetElement.closest('.viz-content-pane');
        if (mainPane) {
            contextState.viewId = mainPane.id;
            if (mainPane.id.includes('neighbor') || mainPane.id.includes('serendipity')) {
                const activeTab = mainPane.querySelector('.topic-tab.active');
                if (activeTab) {
                    contextState.clusterId = activeTab.dataset.clusterId;
                }
            }
        }

		TTS.speak(textContent, contextState, {
			onStop: () => {
				document.querySelectorAll('.is-reading').forEach(el => {
					el.classList.remove('is-reading');
					el.classList.remove('is-paused');
				});
				activeReadingElement = null;
			},
			onPause: () => {
				document.querySelectorAll('.is-reading').forEach(el => el.classList.add('is-paused'));
			},
			onResume: () => {
				document.querySelectorAll('.is-reading').forEach(el => el.classList.remove('is-paused'));
			}
		});

	} else if (modeToExecute === 'translate') {
        
        // 1. Check: Zurück zum Original?
        if (targetElement.dataset.isTranslated === 'true' && targetElement.dataset.originalHtml) {
            // Aus Persistence löschen
            removePersistedTranslation(targetElement.dataset.originalHtml);
            
            // DOM zurücksetzen
            targetElement.innerHTML = targetElement.dataset.originalHtml;
            delete targetElement.dataset.isTranslated;
            delete targetElement.dataset.originalHtml;
            return;
        }

        // Original HTML sichern (für Persistence Key und Restore)
        if (!targetElement.dataset.originalHtml) {
            targetElement.dataset.originalHtml = targetElement.innerHTML;
        }
        const originalHtmlContent = targetElement.dataset.originalHtml;

        let targetLangCode = currentTargetLanguage;
        if (targetLangCode === 'system') {
            const browserLang = navigator.language || 'en';
            targetLangCode = browserLang.split('-')[0];
        }

        targetElement.classList.add('is-translating');
        
        // Layout Freeze
        const rect = targetElement.getBoundingClientRect();
        targetElement.style.minHeight = `${rect.height}px`;
        targetElement.style.minWidth = `${rect.width}px`;
        targetElement.style.display = 'inline-block'; 
        targetElement.textContent = ""; 

        try {
            const response = await fetch('/api/translate', {
                method: 'POST',
                headers: { 
                    'Content-Type': 'application/json', 
                    'X-XSRF-TOKEN': getCsrfToken() 
                },
                body: JSON.stringify({ text: textContent, target_lang: targetLangCode })
            });

            if (!response.ok) throw new Error(`Server status: ${response.status}`);

            const reader = response.body.getReader();
            const decoder = new TextDecoder("utf-8");
            let buffer = "";

            while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                const chunk = decoder.decode(value, { stream: true });
                buffer += chunk;
                const lines = buffer.split("\n\n");
                buffer = lines.pop(); 
                for (const line of lines) {
                    if (line.startsWith("data:")) {
                        targetElement.textContent += line.substring(5); 
                    }
                }
            }
            targetElement.dataset.isTranslated = 'true';

            // NEU: Erfolgreiche Übersetzung persistieren
            // Wir speichern das, was jetzt im Element steht (targetElement.innerHTML)
            persistTranslation(originalHtmlContent, targetElement.innerHTML);

        } catch (error) {
            console.error("Translation stream error:", error);
            targetElement.innerHTML = targetElement.dataset.originalHtml;
        } finally {
            targetElement.classList.remove('is-translating');
            targetElement.style.minHeight = '';
            targetElement.style.minWidth = '';
            targetElement.style.display = '';
        }
    }
}

function handleMouseOver(event) {
    let target = event.target;
    if (!isValidTarget(target) && target.parentElement && isValidTarget(target.parentElement)) target = target.parentElement;
    if (isValidTarget(target) && target !== currentHoveredElement) {
        if (currentHoveredElement) currentHoveredElement.classList.remove('translate-highlight');
        currentHoveredElement = target;
        currentHoveredElement.classList.add('translate-highlight');
    }
}

function handleMouseOut(event) {
    if (currentHoveredElement) {
        if (!currentHoveredElement.contains(event.relatedTarget)) {
             currentHoveredElement.classList.remove('translate-highlight');
             currentHoveredElement = null;
        }
    }
}

function enterSelectionMode(mode, initialEvent) {
    if (activeMode === mode) return; 
    activeMode = mode;
    
    document.body.classList.add('translate-mode-active');
    if (translateMenuWrapper) translateMenuWrapper.classList.add('is-active');

    if (!cursorElement) cursorElement = createCursorElement();
    
    if (mode === 'translate') {
        cursorElement.innerHTML = '<i class="fa-solid fa-language"></i>';
        translateButton.classList.add('is-active-mode');
        readButton.classList.remove('is-active-mode');
        if (translateIndicator) translateIndicator.style.opacity = '1';
        if (readIndicator) readIndicator.style.opacity = '0';
        if (menuTrigger) menuTrigger.innerHTML = '<i class="fa-solid fa-language"></i>';
        
    } else if (mode === 'read') {
        cursorElement.innerHTML = '<i class="fa-solid fa-volume-high"></i>';
        readButton.classList.add('is-active-mode');
        translateButton.classList.remove('is-active-mode');
        if (readIndicator) readIndicator.style.opacity = '1';
        if (translateIndicator) translateIndicator.style.opacity = '0';
        if (menuTrigger) menuTrigger.innerHTML = '<i class="fa-solid fa-volume-high"></i>';
    }

    if (initialEvent && initialEvent.clientX !== undefined) {
        cursorElement.style.left = initialEvent.clientX + 'px';
        cursorElement.style.top = initialEvent.clientY + 'px';
    }
    cursorElement.style.display = 'block';
    
    if (menuTrigger) menuTrigger.classList.add('is-translate-active');

    document.addEventListener('mousemove', moveVirtualCursor);
    document.addEventListener('mouseover', handleMouseOver);
    document.addEventListener('mouseout', handleMouseOut);
    document.addEventListener('keydown', handleEscKey);
    
    setTimeout(() => {
        if (activeMode) document.addEventListener('click', handleTextClick, { capture: true });
    }, 50);
}

function exitSelectionMode() {
    if (!activeMode) return;
    activeMode = null;
    
    document.body.classList.remove('translate-mode-active');
    if (translateMenuWrapper) translateMenuWrapper.classList.remove('is-active');
    
    if (translateButton) translateButton.classList.remove('is-active-mode');
    if (readButton) readButton.classList.remove('is-active-mode');
    if (translateIndicator) translateIndicator.style.opacity = '0';
    if (readIndicator) readIndicator.style.opacity = '0';
    
    if (cursorElement) {
        cursorElement.style.display = 'none';
        cursorElement.style.setProperty('display', 'none', 'important'); 
    }
    
    if (menuTrigger) {
        menuTrigger.innerHTML = '<i class="fa-solid fa-bars"></i>';
        menuTrigger.classList.remove('is-translate-active');
    }

    if (currentHoveredElement) {
        currentHoveredElement.classList.remove('translate-highlight');
        currentHoveredElement = null;
    }
    
    document.removeEventListener('mousemove', moveVirtualCursor);
    document.removeEventListener('mouseover', handleMouseOver);
    document.removeEventListener('mouseout', handleMouseOut);
    document.removeEventListener('click', handleTextClick, { capture: true });
    document.removeEventListener('keydown', handleEscKey);
}

function handleEscKey(event) {
    if (event.key === 'Escape') {
        exitSelectionMode();
    }
}

export function initializeTranslator() {
	if (!translateButton) return;
	if (translateIndicator) translateIndicator.style.opacity = '0';
	if (readIndicator) readIndicator.style.opacity = '0';

	const updateSystemCheckmark = () => {
		const sysBtn = document.querySelector('.language-option[data-lang="system"] .check-icon');
		if (sysBtn) {
			const storedSetting = localStorage.getItem('idea-atlas-translate-lang') || 'system';
			sysBtn.style.opacity = (storedSetting === 'system') ? '1' : '0';
		}
	};

    updateSystemCheckmark(); 
    renderLanguageList(currentTargetLanguage);

    // --- NEU: Restore beim Init aufrufen ---
    // (Für den Fall eines Hard Reloads)
    restoreTranslations();

    // Event Listener für dynamischen Content
    document.addEventListener('dynamicContentLoaded', (e) => {
        const container = e.detail && e.detail.container ? e.detail.container : document.body;
        // Kurze Verzögerung, damit das Rendering sicher fertig ist
        setTimeout(() => restoreTranslations(container), 50);
    });

	translateButton.addEventListener('click', (e) => {
	    e.stopPropagation(); 
	    if (activeMode === 'translate') exitSelectionMode();
	    else enterSelectionMode('translate', e);
	});

	if (readButton) {
	    readButton.addEventListener('click', (e) => {
	        e.stopPropagation();
	        if (activeMode === 'read') exitSelectionMode();
	        else enterSelectionMode('read', e);
	    });
	}

    const mainBtn = document.getElementById('translate-main-btn');
    if (mainBtn && translateMenuWrapper) {
        mainBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            document.querySelectorAll('.theme-menu-wrapper').forEach(el => {
                if (el !== translateMenuWrapper) el.classList.remove('is-active');
            });
			const isDesktop = window.matchMedia('(hover: hover)').matches;
			const isArrowClick = e.target.closest('.theme-arrow');
			if (isDesktop && !isArrowClick) {
				translateMenuWrapper.classList.toggle('is-active');
				if (isTranslateModeActive()) exitSelectionMode();
				else enterSelectionMode('translate', e);
			} else {
				translateMenuWrapper.classList.toggle('is-active');
			}
        });
        document.addEventListener('click', (e) => {
            if (!translateMenuWrapper.contains(e.target)) translateMenuWrapper.classList.remove('is-active');
        });
        
        translateMenuWrapper.addEventListener('mouseleave', () => {
            const filterInput = document.getElementById('language-filter-input');
            if (document.activeElement === filterInput) {
            }
        });
	}

	const submenu = document.getElementById('translate-submenu');
	if (submenu) {
		submenu.addEventListener('click', (e) => {
			const btn = e.target.closest('.language-option');
			if (btn) {
				e.preventDefault();
				e.stopPropagation();

				const selectedLang = btn.dataset.lang;
				if (selectedLang !== currentTargetLanguage) {

					let pageName = window.location.pathname.substring(1).split('/')[0] || 'index';
					pageName = pageName.replace('.html', '');

					setLanguage(selectedLang, ['common', pageName]).then(() => {
						currentTargetLanguage = selectedLang;
						renderLanguageList(currentTargetLanguage);
						updateSystemCheckmark();

						const filterInput = document.getElementById('language-filter-input');
						if (filterInput) {
							filterInput.value = '';
							filterInput.blur();
							const container = document.getElementById('translate-language-list');
							if (container) {
								container.querySelectorAll('.language-option').forEach(b => b.style.display = 'flex');
							}
						}
					});
				}
			}
		});
	}
    
    const filterInput = document.getElementById('language-filter-input');
    const langListContainer = document.getElementById('translate-language-list');

    if (filterInput && langListContainer) {
        filterInput.addEventListener('input', (e) => {
            const searchTerm = e.target.value.toLowerCase();
            langListContainer.querySelectorAll('.language-option').forEach(btn => {
                const searchString = btn.dataset.search || "";
                btn.style.display = searchString.includes(searchTerm) ? 'flex' : 'none';
            });
        });

        filterInput.addEventListener('focus', () => {
            if (translateMenuWrapper) translateMenuWrapper.classList.add('is-active');
        });

        filterInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') {
                e.preventDefault();
                const val = filterInput.value.trim().toLowerCase();
                
                const visibleButtons = Array.from(langListContainer.querySelectorAll('.language-option'))
                    .filter(btn => btn.style.display !== 'none');

                let winner = visibleButtons.find(btn => {
                    const searchAttr = btn.dataset.search || "";
                    const parts = searchAttr.split(" ");
                    return parts.some(p => p === val);
                });

				if (!winner && visibleButtons.length === 1) {
					winner = visibleButtons[0];
				}

				if (winner) {
					winner.click();
					if (!isTranslateModeActive()) {
						translateMenuWrapper.classList.remove('is-active');
						enterSelectionMode('translate');
					}
				}                
				filterInput.blur();
            }
        });
        filterInput.addEventListener('click', (e) => e.stopPropagation());
    }
}