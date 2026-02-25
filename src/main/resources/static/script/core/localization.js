import { emit, EVENTS } from '/script/core/eventBus.js';

const STORAGE_KEY = 'idea-atlas-translate-lang';
const CACHE_PREFIX = 'idea-atlas-i18n-cache-'; // Neuer Cache Key Prefix
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

/**
 * EXPORTIERTE FUNKTION FÜR SOFORTIGES LADEN
 * Diese Funktion muss so früh wie möglich aufgerufen werden (Inline Script im Head).
 */
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
            // WICHTIG: Setzt den Status sofort auf "ready", wenn Cache da ist.
            // Das verhindert das Ausblenden durch CSS beim Initial-Load.
            document.documentElement.setAttribute('data-i18n-ready', 'true');
            console.log(`[I18N] Instant cache load for '${lang}'.`);
            return true;
        } catch (e) {
            console.warn("[I18N] Cache corrupted.");
        }
    }
    // Wenn kein Cache da ist, müssen wir verstecken bis geladen wurde
    document.documentElement.setAttribute('data-i18n-ready', 'false');
    return false;
}
window.loadFromCacheInstant = loadFromCacheInstant;

/**
 * Lädt Sprachdateien und initialisiert die Seite.
 */
export async function initializeLocalization(files = ['common']) {
	// Versuch, sofort aus dem Cache zu laden (falls noch nicht geschehen)
	const hasCache = loadFromCacheInstant();

	// Falls wir Cache hatten, wenden wir ihn sofort auf den Body an
	if (hasCache) {
		applyGeneralTranslations(document);
	}

	// Unabhängig vom Cache laden wir frische Daten aus dem Netz (für Updates)
	const lang = getCurrentLang();
	await loadLanguageData(lang, files); // Lädt JSON vom Server

	// Nach dem Netz-Laden erneut anwenden und anzeigen
	applyGeneralTranslations(document);
	document.documentElement.setAttribute('data-i18n-ready', 'true');

	// Cache für das nächste Mal aktualisieren
	try {
		localStorage.setItem(CACHE_PREFIX + lang, JSON.stringify(i18nState));
	} catch (e) { }
}

/**
 * Lädt neue Daten, setzt die Sprache und aktualisiert die Seite LIVE.
 */
export async function setLanguage(newLang, files = ['common', 'index', 'results']) {
	const effectiveLang = (newLang === 'system')
		? (SUPPORTED_LANGS.includes(navigator.language.split('-')[0]) ? navigator.language.split('-')[0] : DEFAULT_LANG)
		: newLang;

	localStorage.setItem(STORAGE_KEY, newLang);
	setCookie('lang', effectiveLang, 365);
	document.documentElement.lang = effectiveLang;

	// Beim manuellen Wechsel wollen wir meist frisch laden, Cache nutzen wir hier nicht primär
	await loadLanguageData(effectiveLang, files);
	
	// Aber wir speichern den neuen Stand sofort in den Cache
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

/**
 * Fallback-Strategie: Lädt Englisch als Basis und merged Zielsprache.
 * NEU: Unterstützt jetzt eingebettete Offline-Daten!
 */

export async function loadLanguageData(targetLang, files) {
	// Harter Check für Offline-Modus, um Netzwerk-Calls zu verhindern.
	if (document.documentElement.getAttribute('data-is-offline') === 'true') {
		if (window.OFFLINE_I18N_DATA) {
			i18nState = window.OFFLINE_I18N_DATA;
		}
		return; // SOFORT beenden, kein fetch!
	}

	// 1. OFFLINE CHECK: Wurden Daten "gebacken"? (Bestehende Logik)
	if (window.OFFLINE_I18N_DATA) {
		i18nState = window.OFFLINE_I18N_DATA;
		return;
	}

    // 2. ONLINE LOGIK (Normaler Fetch)
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
	// Sicherheitscheck: Wenn keine Daten da sind, gar nicht erst versuchen,
	// sonst überschreiben wir alles mit Keys.
	if (!i18nState || Object.keys(i18nState).length === 0) return;

	const pageTitle = t('page_title');
	if (pageTitle && pageTitle !== 'page_title') {
		// Wenn der aktuelle Titel ein '|' enthält, wurde er von main.js gesetzt.
		// In diesem Fall überschreiben wir ihn NICHT hart.
		if (!document.title.includes('|')) {
			document.title = pageTitle;
		}
	}
	
	container.querySelectorAll('[data-i18n]').forEach(el => {
		const key = el.dataset.i18n;
		const text = t(key);

		// FIX: Wenn der "übersetzte" Text exakt dem Key entspricht,
		// haben wir keine Übersetzung gefunden.
		// In dem Fall lassen wir lieber den englischen HTML-Fallback stehen,
		// anstatt "search.btn_query" anzuzeigen.
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
   GENERIC PAGE RENDERER (Unverändert gut, bleibt hier für Vollständigkeit)
   ========================================================================== */

export function renderPage(containerId) {
	const container = document.getElementById(containerId);
	const data = i18nState;

	if (!container || !data) return;

	container.innerHTML = '';

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
   HELPER FUNCTIONS (Das "Smart" System)
   ========================================================================== */

// Erzeugt H1, H2, H3...
function renderHeading(container, text, tagName) {
	if (!text) return;
	const el = document.createElement(tagName);
	el.textContent = text;
	container.appendChild(el);
}

/**
 * Der Kern der Wiederverwendung.
 * Verarbeitet einen Eintrag und entscheidet automatisch, wie er gerendert wird.
 * @param {HTMLElement} parent - Wo soll es rein? (p oder li)
 * @param {String|Object} item - Die Daten ("Text" oder {text: "...", url: "..."})
 */
function appendSmartContent(parent, item) {
	// Fall 1: Einfacher String
	if (typeof item === 'string') {
		parent.appendChild(document.createTextNode(item));
		return;
	}

	// Fall 2: Objekt
	if (typeof item === 'object') {

		// A: Label (z.B. "E-Mail", "Metadaten-Lizenz") -> Wird fett + Doppelpunkt
		if (item.label) {
			const strong = document.createElement('strong');
			strong.textContent = item.label + ": ";
			parent.appendChild(strong);
		}

		// B: Der Inhalt selbst (Link oder Text)
		let contentNode;
		if (item.url) {
			contentNode = document.createElement('a');
			contentNode.href = item.url;
			contentNode.textContent = item.text || item.url; // Fallback auf URL als Text

			// Mailto Erkennung für target (normale Links blank, mailto nicht zwingend)
			if (!item.url.startsWith('mailto:')) {
				contentNode.target = "_blank";
				contentNode.rel = "noopener noreferrer";
			}
		} else {
			// Fett oder normal?
			if (item.bold) {
				contentNode = document.createElement('strong');
				contentNode.textContent = item.text;
			} else {
				contentNode = document.createTextNode(item.text || "");
			}
		}

		parent.appendChild(contentNode);
	}
}

// Rendert einen <p> Block mit Inline-Segmenten (für Fließtext mit fetten Wörtern)
function renderParagraph(container, segments) {
	if (!segments) return;
	const p = document.createElement('p');

	// Wenn es nur ein String ist (Legacy Support)
	if (typeof segments === 'string') {
		p.textContent = segments;
	} else if (Array.isArray(segments)) {
		segments.forEach(seg => appendSmartContent(p, seg));
	}

	container.appendChild(p);
}

// Rendert Blöcke, die durch Zeilenumbrüche getrennt sind (Adressen, Kontaktblöcke)
function renderLinesBlock(container, block) {
	if (block.headline) renderHeading(container, block.headline, 'h2');

	if (block.lines && Array.isArray(block.lines)) {
		const p = document.createElement('p');

		block.lines.forEach((line, index) => {

			// NEU: Unterstützung für Segmente in einer Zeile
			if (Array.isArray(line)) {
				// Wenn die Zeile ein Array ist, rendern wir jedes Teil nacheinander
				// Beispiel: [{text: "E-Mail: "}, {text: "support...", url: "..."}]
				line.forEach(segment => appendSmartContent(p, segment));
			} else {
				// Altes Verhalten (String oder einzelnes Objekt)
				appendSmartContent(p, line);
			}

			// Zeilenumbruch nach jeder Zeile außer der letzten
			if (index < block.lines.length - 1) {
				p.appendChild(document.createElement('br'));
			}
		});
		container.appendChild(p);
	}
}

// Rendert Listen (<ul> oder <ol>)
function renderList(container, block) {
	const listTag = block.ordered ? 'ol' : 'ul';
	const listEl = document.createElement(listTag);

	if (block.items) {
		block.items.forEach(itemData => {
			const li = document.createElement('li');
			// Hier nutzen wir wieder unsere smarte Funktion!
			appendSmartContent(li, itemData);
			listEl.appendChild(li);
		});
	}
	container.appendChild(listEl);
}

// Warnbox (wiederverwendet Logik)
function renderWarningBox(container, block) {
	const p = document.createElement('p');
	p.className = 'legal-warning-box';
	// Wir bauen uns ein temporäres Objekt, damit appendSmartContent es versteht
	appendSmartContent(p, { label: block.label, text: block.text });
	container.appendChild(p);
}

// License Section (Spezialfall, da komplexe innere Struktur)
    
function renderLicenseSection(container, block, labels) {
    // 1. Sektions-Titel
    const h2 = document.createElement('h2');
    h2.textContent = block.title;
    container.appendChild(h2);

    // 2. Sektions-Beschreibung
    if (block.description) {
        const p = document.createElement('p');
        p.textContent = block.description;
        container.appendChild(p);
    }

    // 3. Liste der Items
    if (block.items && block.items.length > 0) {
        const ul = document.createElement('ul');
        
        block.items.forEach(item => {
            const li = document.createElement('li');
            
            // A) Name & Link (MANUELL gebaut für CSS Klasse)
            const a = document.createElement('a');
            a.href = item.url;
            a.target = "_blank";
            a.rel = "noopener noreferrer";
            a.className = "item-link"; // WICHTIG: Hier wird die Klasse gesetzt
            a.textContent = item.name;
            
            const strong = document.createElement('strong');
            strong.appendChild(a);
            li.appendChild(strong);
            li.appendChild(document.createElement('br'));

            // B) Purpose (Zweck)
            if (item.purpose) {
                li.appendChild(document.createTextNode(item.purpose));
                li.appendChild(document.createElement('br'));
            }

            // C) Lizenzen & Metadaten (Helper für konsistente Zeilen)
            const addLicenseLine = (key, value) => {
                const em = document.createElement('em');
                const small = document.createElement('small');
                const st = document.createElement('strong');
                
                // Label auflösen (z.B. "metadata_license" -> "Metadaten-Lizenz")
                const labelTxt = (labels && labels[key]) ? labels[key] : key;
                st.textContent = labelTxt + ": ";
                
                small.appendChild(st);
                small.appendChild(document.createTextNode(value));
                em.appendChild(small);
                
                li.appendChild(em);
                // Leerzeichen für Abstand, falls mehrere Lizenzen untereinander stehen würden (selten)
                li.appendChild(document.createTextNode(" ")); 
            };

            if (item.license) addLicenseLine('license', item.license);
            if (item.metadata_license) addLicenseLine('metadata_license', item.metadata_license);

            // D) Citation Block
            if (item.citation) {
                const div = document.createElement('div');
                div.className = 'citation-block';
                // Erlaubt Zeilenumbrüche aus der JSON (\n -> <br>)
                div.innerHTML = item.citation.replace(/\n/g, '<br>');
                li.appendChild(div);
            }

            ul.appendChild(li);
        });
        container.appendChild(ul);
    }
}

window.t = t;