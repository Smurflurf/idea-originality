import { detachPlayer, reattachPlayer } from '/script/features/accessibility/tts.js';
import { initializeLocalization, applyGeneralTranslations, loadLanguageData } from '/script/core/localization.js';
import { disposeAllVisualizations } from '/script/viz/core/zoomAndPan.js';

const PERSISTENT_IDS = ['tts-floating-player'];
const PERSISTENT_CLASSES = ['menu-wrapper', 'menu-overlay', 'tts-floating-player-container', 'custom-translate-cursor'];

let isNavigating = false;
let softNavInitialized = false;
let cachedCoreScripts = null;

export function initializeSoftNavigation() {
    if (softNavInitialized) return;
    softNavInitialized = true;
    
    window.addEventListener('tts-navigate-request', (e) => {
        const targetUrl = e.detail.url;
        if (!isCurrentUrl(targetUrl)) {
            navigateTo(targetUrl);
        } else {
            window.dispatchEvent(new CustomEvent('tts-navigation-finished'));
        }
    });

    document.addEventListener('click', async (e) => {
        const link = e.target.closest('a');
        if (shouldIntercept(link, e)) {
            e.preventDefault();
            if (isCurrentUrl(link.href)) return;
            await navigateTo(link.href);
        }
    });

    window.addEventListener('popstate', () => navigateTo(window.location.href, false));
}

// --- CORE SCRIPT DETECTION (DYNAMISCH VIA IMPORTMAP) ---

function getCoreScripts() {
    if (cachedCoreScripts) return cachedCoreScripts;

    const mapElement = document.querySelector('script[type="importmap"]');
    if (!mapElement) {
        // Fallback, falls ImportMap noch nicht geparsed wurde (sollte nicht passieren)
        return []; 
    }

    try {
        const map = JSON.parse(mapElement.textContent);
        if (map && map.imports) {
            // Wir nehmen die Keys (z.B. "/dist/main.js"), da diese stabil sind.
            cachedCoreScripts = Object.keys(map.imports);
            return cachedCoreScripts;
        }
    } catch (e) {
        console.error("[Nav] ImportMap Parsing Error:", e);
    }
    return [];
}

/**
 * Ermittelt alle Skripte, die TATSÄCHLICH gerade im DOM laufen.
 * Wir schauen NUR auf <script src="..."> Tags. Die Import-Map ignorieren wir hier,
 * da sie nur Definitionen enthält, aber keine laufenden Instanzen. */
function getRunningCoreScriptPaths() {
    const corePaths = new Set();

    // NUR Quelle 2: Aktuell geladene Scripts im DOM
    document.querySelectorAll('script[src]').forEach(script => {
        try {
            // Wir nutzen URL, um Pfad von Domain und Query-Params (?v=...) zu trennen
            const url = new URL(script.src, window.location.origin);
            // Wir speichern den Pfad (z.B. "/dist/main.js")
            corePaths.add(url.pathname);
        } catch (e) {}
    });

    return corePaths;
}

/**
 * Prüft, ob ein Skript-URL bereits in der App läuft.
 * @param {string} src - Die URL des neuen Skripts (z.B. "/dist/main.js?v=1.0")
 * @param {Set<string>} corePaths - Das Set der laufenden Pfade
 */
function isCoreScript(src, corePaths) {
    if (!src) return false;
    try {
        const url = new URL(src, window.location.origin);
        // Wenn der Pfad (ohne Version) schon existiert -> Es ist ein Core Script
        return corePaths.has(url.pathname);
    } catch (e) {
        return false;
    }
}

// --- NAVIGATION LOGIC ---

function isCurrentUrl(url) {
    try {
        const current = new URL(window.location.href);
        const target = new URL(url, window.location.origin);
        return current.pathname === target.pathname && current.search === target.search;
    } catch (e) { return false; }
}

function shouldIntercept(link, e) {
    if (!link || !link.href) return false;
    if (e.ctrlKey || e.metaKey || e.shiftKey || e.altKey) return false;
    if (link.target || link.hasAttribute('download')) return false;
    if (link.getAttribute('href') === '#') return false;
    if (!link.href.startsWith(window.location.origin)) return false;
    return true;
}

function getCleanUrl(src) {
    if (!src) return '';
    try {
        const url = new URL(src, window.location.origin);
        return url.origin + url.pathname;
    } catch (e) { return src.split('?')[0]; }
}

function cleanupGlobalState() {
    // WICHTIG: Visuelle Event-Listener aufräumen, bevor Elemente gelöscht werden!
    if (typeof disposeAllVisualizations === 'function') {
        disposeAllVisualizations();
    }

    window.appInitialized = false;
    // Globale Daten resetten, aber NICHT die Logik-Variablen
    const globalsToReset = [];
    
    globalsToReset.forEach(key => window[key] = null);
    document.body.classList.remove('is-loading');
	document.body.classList.remove('is-centered-content');
}

async function navigateTo(url, pushState = true) {
    if (isNavigating) return;
    isNavigating = true;

    // 1. Aufräumen bevor wir HTML holen
    cleanupGlobalState();
    
    document.documentElement.setAttribute('data-i18n-ready', 'false');
    detachPlayer();

	try {
		const response = await fetch(url);
		if (!response.ok) throw new Error(`Status ${response.status}`);

		const htmlText = await response.text();
		const parser = new DOMParser();
		const newDoc = parser.parseFromString(htmlText, 'text/html');

		const currentLang = document.documentElement.lang || 'en';

		// Für Localization brauchen wir den PageName noch? 
		// Falls ja: behalten. Falls localization.js das anders löst: weg damit.
		// Sicherheitshalber berechnen wir ihn kurz für loadLanguageData, aber nicht für scripts.
		let path = new URL(url, window.location.origin).pathname;
		if (path.startsWith('/')) path = path.substring(1);
		let pageName = path.split('/')[0] || 'index';
		pageName = pageName.replace('.html', '');

		await loadLanguageData(currentLang, ['common', pageName]);

		applyGeneralTranslations(newDoc.body);
		await swapStyles(newDoc);
		swapBodyContent(newDoc);
		
		closeMenu(); 

		document.title = newDoc.title;
		if (pushState) window.history.pushState({}, '', url);

		reattachPlayer();

		await runPageScripts(document.body);

        window.dispatchEvent(new Event('DOMContentLoaded'));
        document.dispatchEvent(new Event('DOMContentLoaded'));
        document.dispatchEvent(new CustomEvent('dynamicContentLoaded', { detail: { container: document.body } }));
        
        window.dispatchEvent(new CustomEvent('languageChanged'));
        window.dispatchEvent(new CustomEvent('tts-navigation-finished'));

        window.scrollTo(0, 0);

        requestAnimationFrame(() => {
            document.documentElement.setAttribute('data-i18n-ready', 'true');
            applyGeneralTranslations(document.body);
        });

    } catch (error) {
        console.error('[Nav] Failed:', error);
        // Fallback: Hard Reload, wenn SPA Navigation fehlschlägt
        window.location.href = url;
    } finally {
        isNavigating = false;
        document.documentElement.setAttribute('data-i18n-ready', 'true');
    }
}

async function swapStyles(newDoc) {
    const currentLinks = Array.from(document.querySelectorAll('link[rel="stylesheet"]'));
    const newLinks = Array.from(newDoc.querySelectorAll('link[rel="stylesheet"]'));
    
    const newHrefs = new Set(newLinks.map(l => getCleanUrl(l.getAttribute('href'))));
    const currentHrefs = new Set(currentLinks.map(l => getCleanUrl(l.getAttribute('href'))));

    const loadPromises = [];
    newLinks.forEach(newLink => {
        const rawHref = newLink.getAttribute('href');
        const cleanHref = getCleanUrl(rawHref);
        
        if (!currentHrefs.has(cleanHref)) {
            const linkTag = document.createElement('link');
            linkTag.rel = 'stylesheet';
            linkTag.href = rawHref;
            const p = new Promise((resolve) => {
                linkTag.onload = resolve;
                linkTag.onerror = resolve; 
            });
            loadPromises.push(p);
            document.head.appendChild(linkTag);
        }
    });

    await Promise.race([Promise.all(loadPromises), new Promise(r => setTimeout(r, 1000))]);

    currentLinks.forEach(link => {
        const cleanHref = getCleanUrl(link.getAttribute('href'));
        if (!newHrefs.has(cleanHref)) {
            link.remove();
        }
    });
}

function swapBodyContent(newDoc) {
    const persistentElements = [];
    
    // 1. Analysiere JETZT, was zur Shell gehört (Dynamisch!)
    const runningCorePaths = getRunningCoreScriptPaths();

    // 2. Persistente Elemente retten
    // (Hier deine bestehende Logik für IDs, Klassen und die laufenden Skripte selbst)
    document.querySelectorAll('script[src]').forEach(script => {
        if (isCoreScript(script.src, runningCorePaths)) {
            persistentElements.push(script);
        }
    });

    PERSISTENT_IDS.forEach(id => {
        const el = document.getElementById(id);
        if (el) persistentElements.push(el);
    });

    Array.from(document.body.children).forEach(child => {
        if (child.id && PERSISTENT_IDS.includes(child.id)) return;
        if (child.tagName === 'SCRIPT') return; 
        if (isPersistentClass(child)) persistentElements.push(child);
    });

    // 3. Body leeren
    document.body.innerHTML = '';

    // 4. Neuen Inhalt einfügen (mit dynamischem Filter)
    const newChildren = Array.from(newDoc.body.children);
    
    newChildren.forEach(newChild => {
		// Persistente ignorieren
		if (newChild.id && PERSISTENT_IDS.includes(newChild.id)) return;
		if (isPersistentClass(newChild)) return;

		// A: Import Maps immer blockieren (es darf nur eine geben)
		if (newChild.tagName === 'SCRIPT' && newChild.type === 'importmap') {
			return;
		}

		// B: Externe Skripte prüfen
		if (newChild.tagName === 'SCRIPT' && newChild.src) {
			// Hier nutzen wir den dynamischen Check
			if (isCoreScript(newChild.src, runningCorePaths)) {
				let blockedPath = "unknown";
				let runningVersion = 'unknown';
				try {
					// 1. Pfad des neuen (blockierten) Skripts
					const blockedUrl = new URL(newChild.src, window.location.origin);
					blockedPath = blockedUrl.pathname;

					// 2. Suche das laufende Skript
					const activeScript = Array.from(document.querySelectorAll('script[src]')).find(s => {
						try {
							return new URL(s.src, window.location.origin).pathname === blockedPath;
						} catch (e) { return false; }
					});

					// 3. Version auslesen
					if (activeScript) {
						// Version aus dem Attribut oder der URL extrahieren
						const src = activeScript.getAttribute('src') || activeScript.src;
						const match = src.match(/[?&]v=([^&]+)/);
						if (match) runningVersion = match[1];
					}

					// 2. Fallback: Wenn im DOM nichts steht, schau tief in die Import-Map
					if (runningVersion === 'unknown') {
						const importMapScript = document.querySelector('script[type="importmap"]');
						if (importMapScript) {
							const map = JSON.parse(importMapScript.textContent);
							if (map.imports) {
								// Wir suchen in den VALUES der Map, ob eine URL unseren Pfad enthält
								for (const specifier in map.imports) {
									const mappedUrl = map.imports[specifier];
									if (mappedUrl.includes(blockedPath)) {
										const vMatch = mappedUrl.match(/[?&]v=([^&]+)/);
										if (vMatch) {
											runningVersion = vMatch[1];
											break;
										}
									}
								}
							}
						}
					}
				} catch (e) {
					console.warn("[Nav] Error extracting version:", e);
				}

				// Auch die Version des geblockten Skripts extrahieren für den Log
				const blockedVMatch = newChild.src.match(/[?&]v=([^&]+)/);
				const blockedVersion = blockedVMatch ? blockedVMatch[1] : 'unknown';

				console.log(`[Nav] Blocked duplicate script: ${blockedPath} (Running: v=${runningVersion} | Blocked: v=${blockedVersion})`);
				return; 
			}
		}
        
        document.body.appendChild(document.adoptNode(newChild));
    });

    // 5. Gerettete Elemente wieder anhängen
    persistentElements.forEach(el => document.body.appendChild(el));

    // 6. Klassen übernehmen
    const oldClasses = Array.from(document.body.classList);
	const keptClasses = oldClasses.filter(c => ['translate-mode-active'].includes(c));
    document.body.className = newDoc.body.className;
    keptClasses.forEach(c => document.body.classList.add(c));
}

function cleanupUnusedScripts(newDoc) {
    const newScriptUrls = new Set();
    newDoc.querySelectorAll('script[src]').forEach(s => {
        newScriptUrls.add(getCleanUrl(s.getAttribute('src')));
    });

    document.querySelectorAll('body script[src]').forEach(currentScript => {
        const currentUrl = getCleanUrl(currentScript.src);
        if (currentUrl.endsWith('menu.js') || currentUrl.endsWith('main.js') || currentUrl.endsWith('navigation.js')) return;

        if (!newScriptUrls.has(currentUrl)) {
            currentScript.remove();
        }
    });
}

async function runPageScripts(container) {
    const runningCorePaths = getRunningCoreScriptPaths();
	const newScripts = Array.from(container.querySelectorAll('script'));
	const loadPromises = []; // Wir sammeln Promises

	const scriptsToRun = newScripts.filter(s => {
		if (s.type === 'importmap') return false;
		if (!s.src) {
			return true;
		}
		const isCore = isCoreScript(s.src, runningCorePaths);
		return !isCore;
	});

	for (const scriptTemplate of scriptsToRun) {
		const newScript = document.createElement('script');

		Array.from(scriptTemplate.attributes).forEach(attr => {
			// FIX 1: 'type="module"' bei Inline-Skripten entfernen (Synchronisierung)
			if (!scriptTemplate.src && attr.name === 'type' && attr.value === 'module') {
				return;
			}

			// FIX 2: 'onload' Attribute entfernen!
			// Wir wollen nicht, dass das HTML die Logik steuert (das macht main.js sicher).
			// Das verhindert den "renderMathInElement" Crash.
			if (attr.name === 'onload') {
				return;
			}

			newScript.setAttribute(attr.name, attr.value);
		});

		if (!scriptTemplate.src) {
			let code = scriptTemplate.textContent;

			// DYNAMISCHE ERKENNUNG:
			// Findet alle "const MEIN_WERT =" oder "let CONFIG =" (nur Großbuchstaben)
			// und wandelt sie in "; window.MEIN_WERT =" um.
			const globalVarRegex = /(?:^|[\r\n;])\s*(?:const|let|var)\s+([A-Z0-9_]{3,})\s*=/g;

			code = code.replace(globalVarRegex, (match, varName) => {
				// Nur ersetzen, wenn der Variablenname wirklich komplett großgeschrieben ist
				// (Verhindert, dass wir lokale Logik-Variablen zerstören)
				if (varName === varName.toUpperCase()) {
					return `; window.${varName} =`;
				}
				return match;
			});

			// Wir packen den Code trotzdem in Klammern, um Scope-Probleme bei
			// nicht-globalen Skripten zu vermeiden, aber die window-Zuweisung durchbricht das.
			newScript.textContent = "{\n" + code + "\n}";
			document.body.appendChild(newScript);
		} else {
            // BEI EXTERNEN SKRIPTEN: WARTEN!
            const p = new Promise((resolve) => {
                newScript.onload = () => resolve();
                newScript.onerror = () => {
                    console.warn("[Nav] Script load failed:", newScript.src);
                    resolve(); // Trotzdem weitermachen
                };
            });
            loadPromises.push(p);
            document.body.appendChild(newScript);
        }
    }

    // Wir warten, bis alle nachgeladenen Skripte (z.B. main.js falls noch nicht da) fertig sind
    if (loadPromises.length > 0) {
        await Promise.all(loadPromises);
    }
    
    return Promise.resolve();
}

function isPersistentClass(element) {
	if (!element.classList || element.classList.length === 0) return false;
	return PERSISTENT_CLASSES.some(c => element.classList.contains(c));
}

function closeMenu() {
	const sidebar = document.getElementById('sidebar-menu');
	const overlay = document.getElementById('menu-overlay');

	if (sidebar) {
		sidebar.classList.remove('is-open');
		sidebar.style.transform = '';
		sidebar.style.transition = '';
		sidebar.style.overscrollBehavior = '';
	}

	if (overlay) {
		overlay.classList.remove('is-visible');
		overlay.style.display = '';
		overlay.style.opacity = '';
		overlay.style.visibility = '';
		overlay.style.transition = '';
        const newOverlay = overlay.cloneNode(true);
        if (overlay.parentNode) {
            overlay.parentNode.replaceChild(newOverlay, overlay);
        }
	}

	document.documentElement.style.overflow = ''; // html-Scroll entfernen
	document.body.style.overflow = '';            // body-Scroll entfernen
	document.body.style.touchAction = '';         // Touch-Eingaben wieder erlauben

	document.querySelectorAll('[inert]').forEach(el => el.inert = false);
}
