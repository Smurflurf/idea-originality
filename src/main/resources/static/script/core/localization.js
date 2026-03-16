import { emit, EVENTS } from '/script/core/eventBus.js';
import { getTemplate } from '/script/core/templateManager.js';
import { getJobHistory, getVizSnapshot } from '/script/data/idb-helper.js';

const STORAGE_KEY = 'idea-atlas-translate-lang';
const CACHE_PREFIX = 'idea-atlas-i18n-cache-'; 
const DEFAULT_LANG = 'en';
const SUPPORTED_LANGS = ['de', 'en'];

let i18nState = {}; 

function getCurrentLang() {
	let lang = localStorage.getItem(STORAGE_KEY) || 'system';
	if (lang === 'system') {
		const browserLang = navigator.language.split('-')[0];
		lang = SUPPORTED_LANGS.includes(browserLang) ? browserLang : DEFAULT_LANG;
	}
	return lang;
}

function setCookie(name, value, days) {
    let expires = "";
    if (days) {
        const date = new Date();
        date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
        expires = "; expires=" + date.toUTCString();
    }
    document.cookie = name + "=" + (value || "") + expires + "; path=/";
}

export function loadFromCacheInstant() {
	const lang = getCurrentLang();
	if (i18nState && Object.keys(i18nState).length > 0 && document.documentElement.lang === lang) {
		return true;
	}

	const cacheKey = CACHE_PREFIX + lang;
    const cachedData = localStorage.getItem(cacheKey);

    if (cachedData) {
        try {
            i18nState = JSON.parse(cachedData);
            document.documentElement.lang = lang;
            document.documentElement.setAttribute('data-i18n-ready', 'true');
            console.log(`[I18N] Instant cache load for '${lang}'.`);
            return true;
        } catch (e) {
            console.warn("[I18N] Cache corrupted.");
        }
    }
    document.documentElement.setAttribute('data-i18n-ready', 'false');
    return false;
}
window.loadFromCacheInstant = loadFromCacheInstant;

export async function initializeLocalization(files = ['common']) {
	const hasCache = loadFromCacheInstant();

	if (hasCache) {
		applyGeneralTranslations(document);
	}

	const lang = getCurrentLang();
	await loadLanguageData(lang, files); 

	applyGeneralTranslations(document);
	document.documentElement.setAttribute('data-i18n-ready', 'true');

	try {
		localStorage.setItem(CACHE_PREFIX + lang, JSON.stringify(i18nState));
	} catch (e) { }
}

export async function setLanguage(newLang, files = ['common', 'sse', 'index', 'results']) {
	const effectiveLang = (newLang === 'system')
		? (SUPPORTED_LANGS.includes(navigator.language.split('-')[0]) ? navigator.language.split('-')[0] : DEFAULT_LANG)
		: newLang;

	localStorage.setItem(STORAGE_KEY, newLang);
	setCookie('lang', effectiveLang, 365);
	document.documentElement.lang = effectiveLang;

	await loadLanguageData(effectiveLang, files);
	
	try {
		localStorage.setItem(CACHE_PREFIX + effectiveLang, JSON.stringify(i18nState));
	} catch (e) {}

	applyGeneralTranslations();
	emit(EVENTS.LANG_CHANGED, { lang: effectiveLang });
	document.documentElement.setAttribute('data-i18n-ready', 'true');
}

export function getI18nData() {
    return i18nState;
}

export async function loadLanguageData(targetLang, files) {
	if (document.documentElement.getAttribute('data-is-offline') === 'true') {
		if (window.OFFLINE_I18N_DATA) {
			i18nState = window.OFFLINE_I18N_DATA;
		}
		return; 
	}

	if (window.OFFLINE_I18N_DATA) {
		i18nState = window.OFFLINE_I18N_DATA;
		return;
	}

    const fetchForLang = async (langCode) => {
        const fileSet = new Set(['common', ...files]);
        const promises = Array.from(fileSet).map(file =>
            fetch(`/assets/i18n/${langCode}/${file}.json`)
                .then(res => {
                    if (!res.ok) throw new Error(`Missing translation file: ${langCode}/${file}.json`);
                    return res.json();
                })
                .catch(() => ({}))
        );
        const results = await Promise.all(promises);
        return Object.assign({}, ...results);
    };

    const baseData = await fetchForLang(DEFAULT_LANG);

    let targetData = {};
    if (targetLang !== DEFAULT_LANG) {
        targetData = await fetchForLang(targetLang);
    }

	i18nState = { ...baseData, ...targetData };
}

export function applyGeneralTranslations(container = document) {
	if (!i18nState || Object.keys(i18nState).length === 0) return;

	const pageTitle = t('page_title');
	if (pageTitle && pageTitle !== 'page_title') {
		if (!document.title.includes('|')) {
			document.title = pageTitle;
		}
	}
	
	container.querySelectorAll('[data-i18n]').forEach(el => {
		const key = el.dataset.i18n;
		const text = t(key);

		if (!text || text === key) return;

		if (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA') {
			el.placeholder = text;
			el.setAttribute('placeholder', text);
		}
		else if (el.hasAttribute('data-tippy-content')) {
			el.setAttribute('data-tippy-content', text);
			if (el._tippy) {
				el._tippy.setContent(text);
			}
		}
		else {
			if (el.children.length === 0) {
				if (el.textContent !== text) el.textContent = text;
			} else {
				if (!el.hasAttribute('data-i18n-preserve-html')) {
					el.textContent = text;
				}
			}
		}
	});
}

export function t(key) {
	if (!key) return null;
	return key.split('.').reduce((prev, curr) => (prev ? prev[curr] : null), i18nState) || key;
}

/* ==========================================================================
   GENERIC PAGE RENDERER
   ========================================================================== */

export function renderPage(containerId) {
	const container = document.getElementById(containerId);
	const data = i18nState;

	if (!container || !data) return;

	container.textContent = ''; 

	if (data.page_title) renderHeading(container, data.page_title, 'h1');
	if (data.intro) renderParagraph(container, [{ text: data.intro }]);

	if (data.structure && Array.isArray(data.structure)) {
		data.structure.forEach(block => {
			switch (block.type) {
				case 'section_title': renderHeading(container, block.text, 'h2'); break;
				case 'subsection_title': renderHeading(container, block.text, 'h3'); break;
				case 'heading': renderHeading(container, block.text, block.level || 'h2'); break;
				case 'paragraph': renderParagraph(container, block.content); break;
				case 'lines_block':
				case 'address_block': renderLinesBlock(container, block); break;
				case 'list': renderList(container, block); break;
				case 'license_section': renderLicenseSection(container, block, data.labels); break;
				case 'warning_box': renderWarningBox(container, block); break;
				case 'faq_search': renderFaqSearch(container, block, data.structure); break;
				case 'cascade_block': renderCascadeBlock(container, block, 2); break;
			}
		});
	}

	if (data.warning_box) {
		const br = document.createElement('br');
		container.appendChild(br);
		renderWarningBox(container, { text: data.warning_box });
	}

	document.dispatchEvent(new CustomEvent('page-rendered', {
		detail: { containerId: containerId }
	}));
}

/* ==========================================================================
   HELPER FUNCTIONS 
   ========================================================================== */

function renderHeading(container, text, tagName) {
	if (!text) return;
	const el = document.createElement(tagName);
	el.textContent = text;
	container.appendChild(el);
}

function appendSmartContent(parent, item) {
	if (typeof item === 'string') {
		parent.appendChild(document.createTextNode(item));
		return;
	}

	if (typeof item === 'object') {
		// 1. Label voranstellen (falls vorhanden)
		if (item.label) {
			const strong = document.createElement('strong');
			strong.textContent = item.label + ": ";
			parent.appendChild(strong);
		}

		// 2. Wir erstellen den Content von innen nach außen
		// Start: Reiner Text
		let contentNode = document.createTextNode(item.text || item.url || "");

		// 3. Kursiv? (Wir wickeln den bisherigen Node ein)
		if (item.italic) {
			const cite = document.createElement('cite');
			cite.appendChild(contentNode);
			contentNode = cite;
		}

		// 4. Fett? (Wir wickeln den bisherigen Node ein)
		if (item.bold) {
			const strong = document.createElement('strong');
			strong.appendChild(contentNode);
			contentNode = strong;
		}

		// 5. Link? (Wir wickeln alles bisherige in ein <a>)
		if (item.url) {
			const anchor = document.createElement('a');
			anchor.href = item.url;
			anchor.appendChild(contentNode); // Hier kommt das (evtl. fett/kursive) Element rein
			
			if (!item.url.startsWith('mailto:')) {
				anchor.target = "_blank";
				anchor.rel = "noopener noreferrer";
			}
			contentNode = anchor;
		}

		// Am Ende den fertigen (verschachtelten) Node an den Parent hängen
		parent.appendChild(contentNode);
	}
}

function renderParagraph(container, segments) {
	if (!segments) return;
	const p = document.createElement('p');

	if (typeof segments === 'string') {
		p.textContent = segments;
	} else if (Array.isArray(segments)) {
		segments.forEach(seg => appendSmartContent(p, seg));
	}
	container.appendChild(p);
}

function renderLinesBlock(container, block) {
	if (block.headline) renderHeading(container, block.headline, 'h2');

	if (block.lines && Array.isArray(block.lines)) {
		const p = document.createElement('p');
		block.lines.forEach((line, index) => {
			if (Array.isArray(line)) {
				line.forEach(segment => appendSmartContent(p, segment));
			} else {
				appendSmartContent(p, line);
			}
			if (index < block.lines.length - 1) {
				p.appendChild(document.createElement('br'));
			}
		});
		container.appendChild(p);
	}
}

function renderList(container, block) {
	const listTag = block.ordered ? 'ol' : 'ul';
	const listEl = document.createElement(listTag);

	if (block.items) {
		block.items.forEach(itemData => {
			const li = document.createElement('li');
			appendSmartContent(li, itemData);
			listEl.appendChild(li);
		});
	}
	container.appendChild(listEl);
}

function renderWarningBox(container, block) {
	const p = document.createElement('p');
	p.className = 'legal-warning-box';
	appendSmartContent(p, { label: block.label, text: block.text });
	container.appendChild(p);
}

function renderLicenseSection(container, block, labels) {
    const h2 = document.createElement('h2');
    h2.textContent = block.title;
    container.appendChild(h2);

    if (block.description) {
        const p = document.createElement('p');
        p.textContent = block.description;
        container.appendChild(p);
    }

    if (block.items && block.items.length > 0) {
        const ul = document.createElement('ul');
        
        block.items.forEach(item => {
            const li = document.createElement('li');
            const a = document.createElement('a');
            a.href = item.url;
            a.target = "_blank";
            a.rel = "noopener noreferrer";
            a.className = "item-link"; 
            a.textContent = item.name;
            
            const strong = document.createElement('strong');
            strong.appendChild(a);
            li.appendChild(strong);
            li.appendChild(document.createElement('br'));

            if (item.purpose) {
                li.appendChild(document.createTextNode(item.purpose));
                li.appendChild(document.createElement('br'));
            }

            const addLicenseLine = (key, value) => {
                const em = document.createElement('em');
                const small = document.createElement('small');
                const st = document.createElement('strong');
                
                const labelTxt = (labels && labels[key]) ? labels[key] : key;
                st.textContent = labelTxt + ": ";
                
                small.appendChild(st);
                small.appendChild(document.createTextNode(value));
                em.appendChild(small);
                
                li.appendChild(em);
                li.appendChild(document.createTextNode(" ")); 
            };

            if (item.license) addLicenseLine('license', item.license);
            if (item.metadata_license) addLicenseLine('metadata_license', item.metadata_license);

            if (item.citation) {
                const div = document.createElement('div');
                div.className = 'citation-block';
				const tempDiv = document.createElement('div');
				tempDiv.textContent = item.citation; 
				div.innerHTML = tempDiv.innerHTML.replace(/\n/g, '<br>'); 
                li.appendChild(div);
            }
            ul.appendChild(li);
		});
		container.appendChild(ul);
	}
}

/* ==========================================================================
   FAQ & CASCADE RENDERER (STRICT HIERARCHY)
   ========================================================================== */

// Hilfsfunktion: Propagiert den Status von unten nach oben (Bottom-Up)
function updateFaqAncestors(element) {
	let currentParent = element.parentElement ? element.parentElement.closest('.faq-category-card') : null;

	while (currentParent) {
		// Prüfen: Gibt es IRGENDWELCHE offenen Items in diesem Container?
		const hasOpenItems = currentParent.querySelectorAll('.faq-item.is-open, .faq-item.is-open-inline').length > 0;

		if (hasOpenItems) {
			currentParent.classList.add('has-active-content'); // Pfeil anzeigen
		} else {
			currentParent.classList.remove('has-active-content'); // Pfeil verstecken
			currentParent.classList.remove('is-expanded-all'); // "Expand All" aufheben
		}

		// Eine Ebene höher gehen
		currentParent = currentParent.parentElement ? currentParent.parentElement.closest('.faq-category-card') : null;
	}
}


function renderCascadeBlock(container, block, level) {
	if (!block.title) {
		const wrapper = document.createElement('div');
		wrapper.className = 'cascade-wrapper';
		if (block.items && Array.isArray(block.items)) {
			block.items.forEach(item => {
				if (item.type === 'cascade_block') renderCascadeBlock(wrapper, item, level + 1);
				else renderFaqItem(wrapper, item);
			});
		}
		container.appendChild(wrapper);
		return;
	}

	const categoryCard = document.createElement('div');
	categoryCard.className = 'faq-category-card';

	const btn = document.createElement('button');
	btn.className = 'faq-category-header';
    const tplHeader = getTemplate('tpl-faq-category-header');
    if (tplHeader) {
        tplHeader.querySelector('h3').textContent = block.title;
        btn.appendChild(tplHeader);
    } else {
        btn.textContent = block.title;
    }

	const bodyWrapper = document.createElement('div');
	bodyWrapper.className = 'faq-category-body-wrapper';

	const bodyInner = document.createElement('div');
	bodyInner.className = 'faq-category-body-inner';

	if (block.items && Array.isArray(block.items)) {
		block.items.forEach(item => {
			if (item.type === 'cascade_block') renderCascadeBlock(bodyInner, item, level + 1);
			else renderFaqItem(bodyInner, item);
		});
	}

	bodyWrapper.appendChild(bodyInner);
	categoryCard.appendChild(btn);
	categoryCard.appendChild(bodyWrapper);
	container.appendChild(categoryCard);

	btn.addEventListener('click', (e) => {
		e.stopPropagation();

		const isCurrentlyActive = categoryCard.classList.contains('has-active-content');

		if (isCurrentlyActive) {
			categoryCard.classList.remove('has-active-content', 'is-expanded-all');
			categoryCard.querySelectorAll('.faq-category-card').forEach(childCard => {
				childCard.classList.remove('has-active-content', 'is-expanded-all');
			});
			categoryCard.querySelectorAll('.faq-item').forEach(item => {
				item.classList.remove('is-open', 'is-open-inline');
			});

			updateFaqAncestors(categoryCard);

		} else {
			document.querySelectorAll('.faq-item').forEach(item => item.classList.remove('is-open', 'is-open-inline'));
			document.querySelectorAll('.faq-category-card').forEach(card => card.classList.remove('has-active-content', 'is-expanded-all'));

			categoryCard.classList.add('has-active-content', 'is-expanded-all');
			categoryCard.querySelectorAll('.faq-category-card').forEach(childCard => {
				childCard.classList.add('has-active-content', 'is-expanded-all');
			});
			categoryCard.querySelectorAll('.faq-item').forEach(item => {
				item.classList.add('is-open-inline');
			});

			updateFaqAncestors(categoryCard);

			setTimeout(() => {
				categoryCard.scrollIntoView({ behavior: 'smooth', block: 'start' });
			}, 250);
		}
	});
}


const htmlPageCache = {};

// 1. Hilfsfunktion: Sucht im DOM UND tief in <template> Tags
function findNodeInDocument(doc, selector) {
    if (!doc) return null;
    let found = doc.querySelector(selector);
    if (found) return found;

    const templates = doc.querySelectorAll('template');
    for (let tpl of templates) {
        if (tpl.content) {
            found = tpl.content.querySelector(selector);
            if (found) return found;
        }
    }
    return null;
}

// 2. Läd die Results-Seite (Cache API + Live Fetch Fallback)
async function loadCachedResultPage() {
    try {
        const history = await getJobHistory();
        if (!history || history.length === 0) return null;

        // Wir nehmen immer den neuesten Job
        const latestJob = history[0]; 
        
        // VERSUCH 1: Offline Cache API
        if ('caches' in window) {
            const cacheName = `idea-atlas-job-${latestJob.jobId}`;
            if (await caches.has(cacheName)) {
                const cache = await caches.open(cacheName);
                const keys = await cache.keys();
                const htmlRequest = keys.find(req => {
                    const path = new URL(req.url).pathname;
                    return path.endsWith(`/results/${latestJob.jobId}`) || path.endsWith(`/results/${latestJob.jobId}/`);
                });

                if (htmlRequest) {
                    const match = await cache.match(htmlRequest, { ignoreSearch: true });
                    if (match) return new DOMParser().parseFromString(await match.text(), 'text/html');
                }
            }
        }

        // VERSUCH 2: Fallback Fetch 
        // Löst das Problem, falls der Nutzer sofort zur FAQ wechselt und der Cache noch nicht gespeichert war!
        const res = await fetch(`/results/${latestJob.jobId}`);
        if (res.ok) {
            return new DOMParser().parseFromString(await res.text(), 'text/html');
        }
    } catch (e) {
        console.warn("[FAQ] Mockup loading error:", e);
    }
    return null;
}

// 3. Mockup Resolver

function applyMockupImageStyles(img) {
    img.className = 'mockup-internal-reset mockup-snapshot-image';
    img.style.borderRadius = '12px';
    img.style.border = '1px solid var(--border-main)';
    img.style.maxWidth = '100%';
    img.style.height = 'auto';
    img.style.display = 'block';
    img.style.boxShadow = '0 4px 15px rgba(0,0,0,0.2)';
}

// A. HANDLER FÜR INDEXED DB (Dynamische Snapshots)
async function createIdbImageNode(type) {
    try {
        const history = await getJobHistory();
        if (!history || history.length === 0) return null;
        
        const latestJob = history[0];
        // Key Format: "UUID_type" (z.B. "f3e346..._own")
        const snapshotKey = `${latestJob.jobId}_${type}`;
        
        const snapshot = await getVizSnapshot(snapshotKey);
        if (!snapshot || !snapshot.dataUrl) return null;

        const img = document.createElement('img');
        img.src = snapshot.dataUrl;
        applyMockupImageStyles(img);

        await img.decode(); // Warten auf Dimensionen
        return img;
    } catch (e) {
        console.warn(`[Localization] IDB image load failed for ${type}:`, e);
        return null;
    }
}

// B. HANDLER FÜR CACHE / URLS (Statische Assets)
async function createCacheImageNode(suffix) {
    let url = ''; // Definiere URL außerhalb des try-Blocks für besseres Logging
    try {
        const history = await getJobHistory();
        if (!history || history.length === 0) {
            console.error("[DIAGNOSTIC] No job history found. Cannot construct cache URL.");
            return null;
        }
        
        const latestJob = history[0];
        url = `/results/${latestJob.jobId}/image/${suffix}`;
        console.log(`[DIAGNOSTIC] Attempting to create image node for cache URL: ${url}`);

        const img = new Image();
        
        const loadPromise = new Promise((resolve, reject) => {
            img.onload = () => {
                console.log(`[DIAGNOSTIC] 'onload' triggered for ${url}. Image dimensions (initial): ${img.naturalWidth}x${img.naturalHeight}`);
                resolve();
            };
            img.onerror = (errEvent) => {
                // Dieses Event ist super wichtig. Wenn es feuert, ist der Link tot oder die Ressource unlesbar.
                console.error(`[DIAGNOSTIC] 'onerror' triggered for ${url}. The image could not be loaded.`, errEvent);
                reject(new Error(`Image resource failed to load from ${url}`));
            };
        });

        img.src = url;

        // Warte auf das Lade-Event
        await loadPromise;
        
        // Jetzt versuche zu dekodieren. Wenn das Bild korrupt ist, schlägt dies fehl.
        try {
            await img.decode();
            console.log(`[DIAGNOSTIC] 'decode' successful for ${url}.`);
        } catch (decodeError) {
            console.error(`[DIAGNOSTIC] 'decode' FAILED for ${url}. The image data is likely corrupt or invalid.`, decodeError);
            // Wir werfen den Fehler erneut, damit er vom äußeren catch-Block gefangen wird.
            throw decodeError; 
        }

        applyMockupImageStyles(img);

        if (suffix.includes('_base')) {
            img.style.filter = 'var(--viz-base-filter)';
        }

        return img;
    } catch (e) {
        // Dieser Block fängt jetzt ALLE Fehler, auch von decode()
        console.error(`[FATAL] createCacheImageNode failed for URL: ${url}. Final Error:`, e);
        
        // Wir erstellen einen expliziten Fehler-Platzhalter
        const errorDiv = document.createElement('div');
        applyMockupImageStyles(errorDiv);
        errorDiv.style.border = "1px dashed var(--btn-danger-hover)";
        errorDiv.style.color = "var(--text-error-soft)";
        errorDiv.style.padding = "20px";
        errorDiv.style.minHeight = "80px";
        errorDiv.innerHTML = `<i class="fa-solid fa-triangle-exclamation"></i> Image Failed<br><small style="opacity: 0.6">${suffix}</small>`;
        return errorDiv;
    }
}

async function resolveMockups(selectors, highlightSelectors = []) {
    const selectorArray = Array.isArray(selectors) ? selectors : [selectors];
    const highlights = highlightSelectors ? (Array.isArray(highlightSelectors) ? highlightSelectors : [highlightSelectors]) :[];
    const fragment = document.createDocumentFragment();

    // Caches laden 
    if (!htmlPageCache['results']) {
        htmlPageCache['results'] = await loadCachedResultPage();
    }
    if (!htmlPageCache['/']) {
        try {
            const res = await fetch('/');
            htmlPageCache['/'] = new DOMParser().parseFromString(await res.text(), 'text/html');
        } catch (e) { htmlPageCache['/'] = null; }
    }

    for (const selector of selectorArray) {
        let foundNode = null;

        // Fall 1: IDB Snapshot (z.B. "idb:own")
        if (selector.startsWith('idb:')) {
            const type = selector.split(':')[1];
            foundNode = await createIdbImageNode(type);
        } 
        // Fall 2: Cache Asset (z.B. "cache:own_base")
        else if (selector.startsWith('cache:')) {
            const suffix = selector.split(':')[1];
            foundNode = await createCacheImageNode(suffix);
        }
        // Fall 3: Klassisches DOM Mockup
        else {
            if (htmlPageCache['results']) foundNode = findNodeInDocument(htmlPageCache['results'], selector);
            if (!foundNode) foundNode = findNodeInDocument(document, selector);
            if (!foundNode && htmlPageCache['/']) foundNode = findNodeInDocument(htmlPageCache['/'], selector);
            
            if (foundNode) {
                // HIER: Highlights übergeben!
                foundNode = prepElementForMockupNode(foundNode, highlights);
            }
        }

        const wrapper = document.createElement('div');
        wrapper.className = 'mockup-isolation-wrapper';
        wrapper.style.pointerEvents = 'none';
        wrapper.style.position = 'relative'; 
        wrapper.style.flexShrink = '0';
        wrapper.style.width = 'max-content'; 

        if (foundNode) {
            wrapper.appendChild(foundNode);
            applyGeneralTranslations(wrapper);
            fragment.appendChild(wrapper);
        } else {
            console.warn(`[FAQ] Mockup element not found for selector: ${selector}. Skipping preview.`);
        }
    }
    return fragment;
}

// 4. Node Vorbereitung
function prepElementForMockupNode(el, highlights = []) {
	const clone = el.cloneNode(true);

	// Wir entfernen Klassen, die das Element im Live-Betrieb verstecken könnten
	const classesToRemove = ['is-scrolled-away', 'is-hidden', 'is-hidden-by-overlay'];
	clone.classList.remove(...classesToRemove);

	// Auch bei Kindern des geklonten Elements aufräumen
	clone.querySelectorAll('*').forEach(child => {
		child.classList.remove(...classesToRemove);
	});

	const elementsToReset = [clone, ...clone.querySelectorAll('.ctrl-btn, .viz-toggle-btn, button, .topic-tab')];
	elementsToReset.forEach(item => {
		if (item.classList && item.classList.contains('active')) {
			item.classList.remove('active');
		}
	});
	
	// Highlights anwenden, BEVOR die IDs gelöscht werden ---
	if (highlights && highlights.length > 0) {
		highlights.forEach(hlSelector => {
			if (clone.matches && clone.matches(hlSelector)) {
				clone.classList.add('mockup-highlight-effect');
				clone.classList.add('active'); // Gehighlightetes Element auch aktivieren
			}
			const children = clone.querySelectorAll(hlSelector);
			children.forEach(child => {
				child.classList.add('mockup-highlight-effect');
				child.classList.add('active'); // Gehighlightetes Element auch aktivieren
			});
		});
	}


    // --- BUGFIX 2: Versteckte Menüs (wie .media-menu) zwingend sichtbar machen ---
    clone.style.setProperty('opacity', '1', 'important');
    clone.style.setProperty('visibility', 'visible', 'important');
    if (clone.style.display === 'none') {
        clone.style.setProperty('display', 'block', 'important');
    }

	// Aufräumen (Jetzt können die IDs sicher gelöscht werden)
	clone.removeAttribute('id');
	clone.querySelectorAll('[id]').forEach(child => child.removeAttribute('id'));

	clone.removeAttribute('autofocus');
	clone.querySelectorAll('[autofocus]').forEach(child => child.removeAttribute('autofocus'));

	// Canvas Fix
	const originalCanvases = el.tagName === 'CANVAS' ? [el] : Array.from(el.querySelectorAll('canvas'));
    const clonedCanvases = clone.tagName === 'CANVAS' ? [clone] : Array.from(clone.querySelectorAll('canvas'));
    originalCanvases.forEach((orig, index) => {
        const cClone = clonedCanvases[index];
        if (cClone && cClone.getContext) {
            cClone.width = orig.width; cClone.height = orig.height;
            try { cClone.getContext('2d').drawImage(orig, 0, 0); } catch(e){}
        }
    });

    // Inputs Fix
    const originalInputs = [el, ...el.querySelectorAll('input, textarea, select')];
    const clonedInputs = [clone, ...clone.querySelectorAll('input, textarea, select')];
    originalInputs.forEach((orig, index) => {
        const clonedChild = clonedInputs[index];
        if (clonedChild && ['INPUT', 'TEXTAREA', 'SELECT'].includes(orig.tagName)) {
            if (orig.type === 'checkbox' || orig.type === 'radio') {
                clonedChild.checked = orig.checked;
                if (orig.checked) clonedChild.setAttribute('checked', 'checked');
            } else {
                const val = orig.value || orig.getAttribute('value') || '';
                clonedChild.value = val;
                if (orig.tagName === 'TEXTAREA') clonedChild.textContent = val;
                else clonedChild.setAttribute('value', val);
            }
        }
    });

    // Das Kernelement markieren
    clone.classList.add('mockup-internal-reset');

    // --- Ancestry Shells (Eltern-Hierarchie nachbilden) ---
    let currentWrapper = clone;
    let currentParent = el.parentElement;
    let levels = 0;

    while (currentParent && currentParent.tagName !== 'BODY' && currentParent.tagName !== 'HTML' && levels < 6) {
        if (currentParent.className && typeof currentParent.className === 'string' && currentParent.className.trim() !== '') {
            const shell = document.createElement('div');
            shell.className = currentParent.className + ' mockup-shell';
            shell.appendChild(currentWrapper);
            currentWrapper = shell;
        }
        currentParent = currentParent.parentElement;
        levels++;
    }

    return currentWrapper;
}

export function buildFaqAnswerContent(container, item) {
    const answerContent = document.createElement('div');
    answerContent.className = 'faq-answer-content';

    const textWrapper = document.createElement('div');
    textWrapper.className = 'faq-answer-text';

    if (item.answer && Array.isArray(item.answer)) {
        item.answer.forEach(ans => {
            if (typeof ans === 'string') renderParagraph(textWrapper, [{ text: ans }]);
            else renderParagraph(textWrapper, ans);
        });
    } else if (item.answer) {
        renderParagraph(textWrapper, [{ text: item.answer }]);
    }

    answerContent.appendChild(textWrapper);

    if (item.ui_mockup) {
        const mockupBox = document.createElement('div');
        mockupBox.className = 'faq-ui-mockup-box';

        const badgeTpl = getTemplate('tpl-mockup-badge');
        if (badgeTpl) {
            const badge = document.createElement('span');
            badge.className = 'mockup-badge';
            badge.appendChild(badgeTpl);
            mockupBox.appendChild(badge);
        }

        const contentDiv = document.createElement('div');
		contentDiv.className = 'mockup-content';
		
		if (item.ui_mockup_stack === true) {
			contentDiv.classList.add('is-stacked');
		}
		
        const loadTpl = getTemplate('tpl-mockup-loading-placeholder');
        if (loadTpl) contentDiv.appendChild(loadTpl);
        
        mockupBox.appendChild(contentDiv);
        answerContent.appendChild(mockupBox);

		resolveMockups(item.ui_mockup, item.highlight_mockup).then(fragment => {
			if (!fragment || fragment.children.length === 0) {
				console.log("[FAQ] No mockups resolved. Removing container.");
				mockupBox.remove();
				return;
			}

            contentDiv.textContent = '';
            contentDiv.appendChild(fragment);
            applyGeneralTranslations(mockupBox);

            requestAnimationFrame(() => scaleMockupsToFit(contentDiv));
            const resizeObserver = new ResizeObserver(() => scaleMockupsToFit(contentDiv));
            resizeObserver.observe(contentDiv);
        });
    }
    
    container.appendChild(answerContent);
}


/* ==========================================================================
   UPDATED RENDER FAQ ITEM (Async)
   ========================================================================== */

   function scaleMockupsToFit(container) {
   	if (!container) return;

   	const wrappers = Array.from(container.querySelectorAll('.mockup-isolation-wrapper'));
   	if (wrappers.length === 0) return;

   	wrappers.forEach(w => {
   		w.style.transform = '';
   		w.style.margin = '';
   		w.style.width = 'auto';
   	});

   	const totalContentWidth = wrappers.reduce((sum, el) => sum + el.offsetWidth, 0) + (wrappers.length - 1) * 20;
   	const containerWidth = container.clientWidth;

   	let scale = 1;
   	const maxAvailableWidth = containerWidth * 0.95;

   	if (totalContentWidth > maxAvailableWidth) {
   		scale = maxAvailableWidth / totalContentWidth;
   	}

   	if (scale < 1) {
   		wrappers.forEach(wrapper => {
               // --- FIX: Zur Mitte hin schrumpfen! ---
   			wrapper.style.transformOrigin = 'center center'; 
   			wrapper.style.transform = `scale(${scale})`;

   			const naturalHeight = wrapper.offsetHeight;
   			const heightDiff = naturalHeight - (naturalHeight * scale);
               // Platz oben und unten gleichmäßig abziehen
   			wrapper.style.marginTop = `-${heightDiff / 2}px`;
   			wrapper.style.marginBottom = `-${heightDiff / 2}px`;

   			const naturalWidth = wrapper.offsetWidth;
   			const widthDiff = naturalWidth - (naturalWidth * scale);
   			wrapper.style.marginLeft = `-${widthDiff / 2}px`;
   			wrapper.style.marginRight = `-${widthDiff / 2}px`;
   		});
   	}
   }

function renderFaqItem(container, item) {
	if (!item.question) return;

	const faqDiv = document.createElement('div');
	faqDiv.className = 'faq-item';
	if (item.id) faqDiv.id = item.id;

	const btn = document.createElement('button');
	btn.className = 'faq-question';

	const tplQuestion = getTemplate('tpl-faq-question-header');
	if (tplQuestion) {
		tplQuestion.querySelector('span').textContent = item.question;
		btn.appendChild(tplQuestion);
	} else {
		// ÄNDERUNG HIER:
		// Statt btn.textContent = item.question;
		// Verpacken wir den Text in einen Span, damit SelectionMode ihn greifen kann.
		const textSpan = document.createElement('span');
		textSpan.textContent = item.question;
		// Optional: Klasse für Styling, falls nötig
		textSpan.className = 'faq-question-text';
		// Damit Klicks auf den Text nicht vom Button "geschluckt" werden im Selection Mode:
		textSpan.style.pointerEvents = 'auto';
		btn.appendChild(textSpan);
		// Falls du Pfeile/Icons hast, füge sie separat hinzu, 
		// aber der Text ist jetzt isoliert.
	}
	
	const answerWrapper = document.createElement('div');
	answerWrapper.className = 'faq-answer-wrapper';

	const answerInner = document.createElement('div');
	answerInner.className = 'faq-answer-inner';

	buildFaqAnswerContent(answerInner, item);

	answerWrapper.appendChild(answerInner);
	faqDiv.appendChild(btn);
	faqDiv.appendChild(answerWrapper);
	container.appendChild(faqDiv);

	btn.addEventListener('click', (e) => {
		e.stopPropagation();
		const wasOpen = faqDiv.classList.contains('is-open') || faqDiv.classList.contains('is-open-inline');

		if (wasOpen) {
			faqDiv.classList.remove('is-open', 'is-open-inline');
			let parent = faqDiv.parentElement.closest('.faq-category-card');
			while (parent) {
				parent.classList.remove('is-expanded-all');
				parent = parent.parentElement ? parent.parentElement.closest('.faq-category-card') : null;
			}
			updateFaqAncestors(faqDiv);
		} else {
			document.querySelectorAll('.faq-item').forEach(item => item.classList.remove('is-open', 'is-open-inline'));
			document.querySelectorAll('.faq-category-card').forEach(card => card.classList.remove('has-active-content', 'is-expanded-all'));
			faqDiv.classList.add('is-open');
			updateFaqAncestors(faqDiv);
			setTimeout(() => {
				const windowHeight = window.innerHeight;
				if (faqDiv.scrollHeight > windowHeight * 0.95) {
					const y = faqDiv.getBoundingClientRect().top + window.scrollY - 65;
					window.scrollTo({ top: y, behavior: 'smooth' });
				} else {
					faqDiv.scrollIntoView({ behavior: 'smooth', block: 'center' });
				}
			}, 250);
		}
	});
}

function renderFaqSearch(container, block, fullStructure) {
    const searchContainer = document.createElement('div');
    searchContainer.className = 'faq-search-container'; 
    searchContainer.id = 'static-faq-search-box';

    const wrapper = document.createElement('div');
    wrapper.className = 'faq-input-wrapper';

    const icon = document.createElement('i');
    icon.className = 'fa-solid fa-magnifying-glass faq-search-icon';

    const input = document.createElement('input');
    input.type = 'text';
    input.className = 'faq-search-input';
    input.placeholder = block.placeholder || t('faq.search_placeholder') || "Search...";
    
    input.readOnly = true; 
    input.style.cursor = 'pointer'; 
    input.style.pointerEvents = 'none';

    wrapper.appendChild(icon);
    wrapper.appendChild(input);
    searchContainer.appendChild(wrapper);
    container.appendChild(searchContainer);


    // 2. Der Klick-Handler (NUR AUF DEM CONTAINER)
    const triggerSearch = async (e) => {
        // Stoppt sofort, falls doch was bubbelt
        e.preventDefault();
        e.stopPropagation();

        const { openFaqSearch } = await import('/script/features/faq/faqManager.js');
        openFaqSearch({ originElement: searchContainer });
    };

    // NUR EIN LISTENER!
    searchContainer.addEventListener('click', triggerSearch);
}

window.t = t;