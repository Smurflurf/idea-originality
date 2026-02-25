import { getCsrfToken } from '/script/core/security.js';
import { setLanguage, t } from '/script/core/localization.js';
import { getContext } from '/script/core/context.js';


// --- CONFIG & STATE ---
const STORAGE_KEY_LANG = 'idea-atlas-translate-lang';
const STORAGE_KEY_TRANSLATIONS = 'idea-atlas-active-translations';
let currentTargetLanguage = localStorage.getItem(STORAGE_KEY_LANG) || 'system';
const LANG_CACHE_KEY = 'idea-atlas-languages';


// --- CORE FEATURE API ---

/**
 * Führt die Übersetzung auf einem Element aus.
 * Wird von selectionMode.js aufgerufen.
 * @param {HTMLElement} targetElement Das zu übersetzende DOM-Element.
 */
export async function executeTranslation(targetElement) {
    // A. Check: Übersetzung rückgängig machen?
    if (targetElement.dataset.isTranslated === 'true' && targetElement.dataset.originalHtml) {
        removePersistedTranslation(targetElement.dataset.originalHtml);
        targetElement.innerHTML = targetElement.dataset.originalHtml;
        delete targetElement.dataset.isTranslated;
        delete targetElement.dataset.originalHtml;
        return;
    }

    // B. Vorbereitung für die Übersetzung
    if (!targetElement.dataset.originalHtml) {
        targetElement.dataset.originalHtml = targetElement.innerHTML;
    }
    const originalHtmlContent = targetElement.dataset.originalHtml;
    
    let textContent = targetElement.innerText.trim();
    if (targetElement.classList.contains('json-string') && textContent.startsWith('"') && textContent.endsWith('"')) {
        textContent = textContent.slice(1, -1);
    }

    let targetLangCode = currentTargetLanguage;
    if (targetLangCode === 'system') {
        const browserLang = (navigator.language || 'en').split('-')[0];
        targetLangCode = browserLang;
    }

    // UI Feedback & Layout Freeze
    targetElement.classList.add('is-translating');
    const rect = targetElement.getBoundingClientRect();
    targetElement.style.minHeight = `${rect.height}px`;
    targetElement.style.minWidth = `${rect.width}px`;
    targetElement.style.display = 'inline-block';
    targetElement.textContent = "";

    // C. API-Aufruf (Streaming)
    try {
        const response = await fetch('/api/translate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-XSRF-TOKEN': getCsrfToken() },
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


// --- PERSISTENCE & RESTORE LOGIC ---

function getSimpleHash(str) {
    let hash = 0;
    if (str.length === 0) return hash;
    const cleanStr = str.trim();
    for (let i = 0; i < cleanStr.length; i++) {
        hash = ((hash << 5) - hash) + cleanStr.charCodeAt(i);
        hash = hash & hash;
    }
    return hash.toString();
}

function persistTranslation(originalHtml, translatedHtml) {
    try {
        const path = window.location.pathname;
        let storage = JSON.parse(sessionStorage.getItem(STORAGE_KEY_TRANSLATIONS) || '{}');
        if (!storage[path]) storage[path] = {};
        storage[path][getSimpleHash(originalHtml)] = translatedHtml;
        sessionStorage.setItem(STORAGE_KEY_TRANSLATIONS, JSON.stringify(storage));
    } catch (e) { console.warn("[Translate] Persistence error:", e); }
}

function removePersistedTranslation(originalHtml) {
    try {
        const path = window.location.pathname;
        let storage = JSON.parse(sessionStorage.getItem(STORAGE_KEY_TRANSLATIONS) || '{}');
        if (storage[path]) {
            delete storage[path][getSimpleHash(originalHtml)];
            sessionStorage.setItem(STORAGE_KEY_TRANSLATIONS, JSON.stringify(storage));
        }
    } catch (e) {}
}

export function restoreTranslations(container = document.body) {
    try {
        const path = window.location.pathname;
        const storage = JSON.parse(sessionStorage.getItem(STORAGE_KEY_TRANSLATIONS) || '{}');
        const pageTranslations = storage[path];
        if (!pageTranslations) return;

        container.querySelectorAll('p, h1, h2, h3, h4, h5, h6, li, span, div, a').forEach(el => {
            if (el.dataset.isTranslated === 'true') return;
            const currentHtml = el.innerHTML;
            const hash = getSimpleHash(currentHtml);
            if (pageTranslations[hash]) {
                el.dataset.originalHtml = currentHtml;
                el.innerHTML = pageTranslations[hash];
                el.dataset.isTranslated = 'true';
            }
        });
    } catch (e) {}
}


// --- SETTINGS UI INITIALIZATION ---

export function initializeTranslator() {
    renderLanguageList(currentTargetLanguage);
    restoreTranslations();

    document.addEventListener('dynamicContentLoaded', (e) => {
        const container = e.detail?.container || document.body;
        setTimeout(() => restoreTranslations(container), 50);
    });

    const translateMenuWrapper = document.getElementById('translate-menu-wrapper');
    const mainBtn = document.getElementById('translate-main-btn');
    
    // Main Toggle Button Logic (Menu öffnen)
    if (mainBtn && translateMenuWrapper) {
        // Alte Listener entfernen, um Doppel-Bindungen bei SPA-Navigation zu vermeiden
        const newMainBtn = mainBtn.cloneNode(true);
        mainBtn.parentNode.replaceChild(newMainBtn, mainBtn);

        newMainBtn.addEventListener('click', (e) => {
            // FIX: preventDefault verhindert Ghost-Clicks und Fokus-Probleme
            // besonders wichtig auf der Results-Seite, wo body overflow hidden ist.
            e.preventDefault(); 
            e.stopPropagation();
            
            // Schließe andere Menüs (wie das Theme Menü)
            document.querySelectorAll('.theme-menu-wrapper').forEach(el => {
                if (el !== translateMenuWrapper) el.classList.remove('is-active');
            });
            
            translateMenuWrapper.classList.toggle('is-active');
        });

        // Globaler Klick zum Schließen
        document.addEventListener('click', (e) => {
            if (!translateMenuWrapper.contains(e.target)) translateMenuWrapper.classList.remove('is-active');
        });
    }

    // Language Selection Logic
    const submenu = document.getElementById('translate-submenu');
    if (submenu) {
        submenu.addEventListener('click', (e) => {
            const btn = e.target.closest('.language-option');
            if (btn) {
                e.preventDefault(); 
                e.stopPropagation();
                
                const selectedLang = btn.dataset.lang;
                if (selectedLang !== currentTargetLanguage) {
                    let pageName = (window.location.pathname.substring(1).split('/')[0] || 'index').replace('.html', '');
                    setLanguage(selectedLang, ['common', pageName]).then(() => {
                        currentTargetLanguage = selectedLang;
                        renderLanguageList(currentTargetLanguage);
                        // Filter & Checkmarks UI aktualisieren
                        const filterInput = document.getElementById('language-filter-input');
                        if(filterInput) {
                            filterInput.value = '';
                            filterInput.blur();
                        }
                    });
                }
            }
        });
    }
    
    // Filter Input Logic
    const filterInput = document.getElementById('language-filter-input');
    if (filterInput) {
        const langListContainer = document.getElementById('translate-language-list');
        filterInput.addEventListener('input', (e) => {
            const val = e.target.value.toLowerCase();
            langListContainer.querySelectorAll('.language-option').forEach(btn => {
                btn.style.display = (btn.dataset.search || "").includes(val) ? 'flex' : 'none';
            });
        });
        filterInput.addEventListener('click', (e) => e.stopPropagation());
    }
}


// --- HELPER: LANGUAGE LIST RENDERING ---
function getLanguages() {
	// Zuerst im sauberen App-Context nachsehen
	const context = getContext();
	if (context.serverLanguages && context.serverLanguages.length > 0) {
		// Cache updaten für den nächsten Besuch
		try { localStorage.setItem(LANG_CACHE_KEY, JSON.stringify(context.serverLanguages)); } catch (e) { }
		return context.serverLanguages;
	}

	// Fallback: Globales Window (falls Context noch nicht ready, unwahrscheinlich aber sicher ist sicher)
	if (window.SERVER_LANGUAGES && window.SERVER_LANGUAGES.length > 0) {
		try { localStorage.setItem(LANG_CACHE_KEY, JSON.stringify(window.SERVER_LANGUAGES)); } catch (e) { }
		return window.SERVER_LANGUAGES;
	}

	// Fallback: LocalStorage Cache
	try {
		const cached = JSON.parse(localStorage.getItem(LANG_CACHE_KEY));
		return Array.isArray(cached) ? cached : [];
	} catch (e) { return []; }
}


function renderLanguageList(displayLocale) {
    const container = document.getElementById('translate-language-list');
    if (!container) return;

    const submenu = document.getElementById('translate-submenu');
    let topActiveBtn = null;
    if (submenu) {
        topActiveBtn = Array.from(submenu.querySelectorAll('.language-option')).find(btn => !container.contains(btn));
    }

    const languages = getLanguages();
    if (languages.length === 0) {
        container.innerHTML = `<div style="padding:10px; color:var(--text-disabled); text-align:center; font-size:0.8rem;"><i>Offline / No Data</i></div>`;
        return;
    }

    const activeSetting = localStorage.getItem(STORAGE_KEY_LANG) || 'system';

    let uiLocale = displayLocale;
    if (!uiLocale || uiLocale === 'system') uiLocale = navigator.language || 'en';
    
    let uiFormatter, englishFormatter;
    try { uiFormatter = new Intl.DisplayNames([uiLocale], { type: 'language' }); } catch (e) { uiFormatter = { of: (code) => code }; }
    try { englishFormatter = new Intl.DisplayNames(['en'], { type: 'language' }); } catch (e) { englishFormatter = { of: (code) => code }; }

    const fullList = languages.map(lang => {
        const code = lang.code;
        let displayName = lang.name;
        
        // Ein Set filtert automatisch doppelte Begriffe raus
        const uniqueTerms = new Set([code, lang.name]);

        // 1. Name in der aktuellen UI-Sprache (z.B. "Französisch")
        try {
            const uiName = uiFormatter.of(code);
            if (uiName && uiName.toLowerCase() !== code.toLowerCase()) {
                displayName = uiName; // Das zeigen wir im UI an
                uniqueTerms.add(uiName);
            }
        } catch(e) {}

        // 2. Nativer Name (wie die Sprache sich selbst nennt, z.B. "Français")
        try {
            const nativeFormatter = new Intl.DisplayNames([code], { type: 'language' });
            const nativeName = nativeFormatter.of(code);
            if (nativeName) uniqueTerms.add(nativeName);
        } catch(e) {}

        // 3. Englischer Name (als globaler Fallback, z.B. "French")
        try {
            const enName = englishFormatter.of(code);
            if (enName) uniqueTerms.add(enName);
        } catch(e) {}

        const searchString = Array.from(uniqueTerms).filter(Boolean).join(" ").toLowerCase();
        return { code, displayName, searchString };
    });

    fullList.unshift({
        code: 'system',
        displayName: t('themes.system') || 'System Default',
        searchString: 'system default automatic',
    });

    let activeIndex = fullList.findIndex(item => item.code === activeSetting);
    let activeItem = fullList[activeIndex];
    
    if (!activeItem) {
        activeItem = fullList.find(item => item.code === 'system');
        activeIndex = fullList.indexOf(activeItem);
    }

    if (topActiveBtn && activeItem) {
        topActiveBtn.dataset.lang = activeItem.code;
        topActiveBtn.dataset.search = activeItem.searchString;
        const iconClass = activeItem.code === 'system' ? 'fa-solid fa-desktop' : 'fa-solid fa-globe';
        
        topActiveBtn.innerHTML = `
            <span>
                <i class="${iconClass}" style="font-size: 0.8em; opacity: 0.7;"></i> 
                <span>${activeItem.displayName}</span>
            </span>
            <i class="fa-solid fa-check check-icon" style="opacity: 1"></i>
        `;
    }

    if (activeIndex > -1) {
        fullList.splice(activeIndex, 1);
    }

    fullList.sort((a, b) => {
        if (a.code === 'system') return -1;
        if (b.code === 'system') return 1;
        return a.displayName.localeCompare(b.displayName);
    });

    container.innerHTML = '';
    const fragment = document.createDocumentFragment();
    
    fullList.forEach(item => {
        const btn = document.createElement('button');
        btn.className = 'theme-submenu-item language-option';
        btn.dataset.lang = item.code;
        btn.dataset.search = item.searchString;
        const iconClass = item.code === 'system' ? 'fa-solid fa-desktop' : 'fa-solid fa-globe';

        btn.innerHTML = `
            <span>
                <i class="${iconClass}" style="font-size: 0.8em; opacity: 0.7;"></i> 
                <span>${item.displayName}</span>
            </span>
            <i class="fa-solid fa-check check-icon" style="opacity: 0"></i>
        `;
        fragment.appendChild(btn);
    });
    
    container.appendChild(fragment);
}
