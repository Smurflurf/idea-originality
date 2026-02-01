import { detachPlayer, reattachPlayer } from '/script/features/accessibility/tts.js';
import { loadLanguageData, applyGeneralTranslations } from '/script/core/localization.js';
import { disposeAllVisualizations } from '/script/viz/core/zoomAndPan.js';
import { on, emit, EVENTS } from '/script/core/eventBus.js';

// --- KONFIGURATION ---
const PERSISTENT_IDS = ['tts-floating-player'];
const PERSISTENT_CLASSES = ['menu-wrapper', 'menu-overlay', 'tts-floating-player-container', 'custom-translate-cursor'];

// --- STATE ---
let softNavInitialized = false;
let currentNavController = null; // Hält den AbortController der aktuellen Navigation

// --- HELPER: URL & VERSIONING ---

/**
 * Parst eine URL sicher und gibt Pfad und Version zurück.
 * Safety: Fängt Fehler ab, falls src mal keine valide URL ist.
 */
function parseScriptUrl(src) {
    try {
        const url = new URL(src, window.location.origin);
        return {
            path: url.pathname,
            version: url.searchParams.get('v') || 'unknown'
        };
    } catch (e) {
        return { path: src, version: 'unknown' };
    }
}

/**
 * Erstellt eine Map aller aktuell laufenden Core-Skripte.
 * Key: Pfad, Value: Version
 */
function getRunningCoreScripts() {
    const scripts = new Map();
    document.querySelectorAll('script[src]').forEach(script => {
        const info = parseScriptUrl(script.src);
        scripts.set(info.path, info.version);
    });
    return scripts;
}

function isCurrentUrl(url) {
    try {
        const current = new URL(window.location.href);
        const target = new URL(url, window.location.origin);
        return current.pathname === target.pathname && current.search === target.search;
    } catch (e) { return false; }
}

function isPersistent(element) {
    if (!element) return false;
    if (element.id && PERSISTENT_IDS.includes(element.id)) return true;
    if (element.classList && element.classList.length > 0) {
        return PERSISTENT_CLASSES.some(c => element.classList.contains(c));
    }
    return false;
}

// --- CORE NAVIGATION LOGIC ---

export function initializeSoftNavigation() {
    if (softNavInitialized) return;
    softNavInitialized = true;
    
    // TTS Event Listener via Bus
    on(EVENTS.TTS_NAVIGATE, (data) => {
        const targetUrl = data.url;
        if (!isCurrentUrl(targetUrl)) {
            navigateTo(targetUrl);
        } else {
            // Wir sind schon da -> TTS Bescheid geben
            emit(EVENTS.TTS_FINISHED);
        }
    });

    // Link Interceptor
    document.addEventListener('click', (e) => {
        const link = e.target.closest('a');
        // Safety: Ignorieren bei Modifier Keys, Download-Attributen, externen Links
        if (!link || !link.href) return;
        if (e.ctrlKey || e.metaKey || e.shiftKey || e.altKey) return;
        if (link.target || link.hasAttribute('download')) return;
        if (link.getAttribute('href') === '#') return;
        if (!link.href.startsWith(window.location.origin)) return;

        e.preventDefault();
        if (isCurrentUrl(link.href)) return;
        navigateTo(link.href);
    });

    // Back/Forward Button Support
    window.addEventListener('popstate', () => navigateTo(window.location.href, false));
}

async function navigateTo(url, pushState = true) {
    // 1. Race Condition Prevention: Alte Requests abbrechen
    if (currentNavController) {
        currentNavController.abort();
    }
    currentNavController = new AbortController();
    const signal = currentNavController.signal;

    // 2. UI Feedback start
    document.body.classList.add('is-loading');
    
    // Cleanup alter Visualisierungen (Canvas WebGL Contexts freigeben!)
    disposeAllVisualizations();
    detachPlayer();

    try {
        // 3. Fetch (kann abgebrochen werden)
        const response = await fetch(url, { signal });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        
        const htmlText = await response.text();
        const parser = new DOMParser();
        const newDoc = parser.parseFromString(htmlText, 'text/html');

        // 4. Pre-Processing (I18n laden, bevor wir rendern)
        const currentLang = document.documentElement.lang || 'en';
        let path = new URL(url, window.location.origin).pathname;
        if (path.startsWith('/')) path = path.substring(1);
        let pageName = path.split('/')[0] || 'index';
        pageName = pageName.replace('.html', '');

        await loadLanguageData(currentLang, ['common', pageName]);

        // 5. DOM Swap (Styles -> Body -> Scripts)
        // Styles parallel laden für Performance
        await swapStyles(newDoc);
        
        // Body Content austauschen
        await swapBodyContent(newDoc);

        // UI Reset
        closeMobileMenu();
        document.title = newDoc.title;
        
        if (pushState) {
            window.history.pushState({}, '', url);
        }

        // Player wieder einhängen (an persistenten Container)
        reattachPlayer();

        // 6. Scripts ausführen (Hier passiert der Versions-Check)
        await runPageScripts(document.body);

        // 7. Signale senden
        // Legacy Support & Interne Signale
        document.dispatchEvent(new CustomEvent('dynamicContentLoaded', { detail: { container: document.body } }));
        emit(EVENTS.TTS_FINISHED); // Falls TTS gewartet hat

        window.scrollTo(0, 0);

        // I18n final anwenden (falls Attribute im HTML nachgeladen wurden)
        requestAnimationFrame(() => {
            applyGeneralTranslations(document.body);
        });

    } catch (error) {
        if (error.name === 'AbortError') {
            console.log('[Nav] Navigation aborted (new request started).');
        } else {
            console.error('[Nav] Failed:', error);
            // Fallback: Wenn SPA versagt, mach Hard Reload
            window.location.href = url;
        }
    } finally {
        document.documentElement.setAttribute('data-i18n-ready', 'true');
        document.body.classList.remove('is-loading');
        currentNavController = null;
    }
}

/**
 * Tauscht CSS Links aus.
 * Optimierung: Lädt neue Styles vor, bevor alte entfernt werden (FOUC Vermeidung).
 */
async function swapStyles(newDoc) {
    const currentLinks = Array.from(document.querySelectorAll('link[rel="stylesheet"]'));
    const newLinks = Array.from(newDoc.querySelectorAll('link[rel="stylesheet"]'));
    
    // Normalisierung für den Vergleich (ohne Query Params)
    const getBasePath = (href) => {
        try { return new URL(href, window.location.origin).pathname; } catch(e) { return href; }
    };

    const newPaths = new Set(newLinks.map(l => getBasePath(l.getAttribute('href'))));
    const currentPaths = new Set(currentLinks.map(l => getBasePath(l.getAttribute('href'))));

    const loadPromises = [];

    // Neue hinzufügen
    newLinks.forEach(newLink => {
        if (!currentPaths.has(getBasePath(newLink.getAttribute('href')))) {
            const linkTag = document.createElement('link');
            linkTag.rel = 'stylesheet';
            linkTag.href = newLink.getAttribute('href'); 
            const p = new Promise(resolve => {
                linkTag.onload = resolve;
                linkTag.onerror = resolve; // Trotzdem weitermachen
            });
            loadPromises.push(p);
            document.head.appendChild(linkTag);
        }
    });

    // Kurz warten, damit Styles greifen können
    if (loadPromises.length > 0) {
        await Promise.all(loadPromises);
    }

    // Alte entfernen
    currentLinks.forEach(link => {
        if (!newPaths.has(getBasePath(link.getAttribute('href')))) {
            link.remove();
        }
    });
}

/**
 * Tauscht den Body-Inhalt aus, behält aber persistente Elemente.
 * Nutzt DocumentFragment für bessere Performance (nur 1 Reflow).
 */
async function swapBodyContent(newDoc) {
    const runningScripts = getRunningCoreScripts();
    const fragment = document.createDocumentFragment();
    const persistentElements = [];

    // A. Persistente Elemente aus dem aktuellen DOM retten
    // 1. Laufende Skripte
    document.querySelectorAll('script[src]').forEach(script => {
        const info = parseScriptUrl(script.src);
        if (runningScripts.has(info.path)) {
            persistentElements.push(script);
        }
    });

    // 2. UI Elemente (Menü, Player)
    Array.from(document.body.children).forEach(child => {
        if (isPersistent(child)) {
            persistentElements.push(child);
        }
    });

    // B. Body Attribute synchronisieren (Wichtig für Feature-Flags!)
    const newFeatures = newDoc.body.getAttribute('data-features');
    if (newFeatures) document.body.setAttribute('data-features', newFeatures);
    else document.body.removeAttribute('data-features');

    if (newDoc.body.id) document.body.id = newDoc.body.id;
    else document.body.removeAttribute('id');
    
    document.body.className = newDoc.body.className; // Klassen syncen

    // C. Neuen Content aufbauen (im Fragment)
    Array.from(newDoc.body.children).forEach(newChild => {
        // Persistente Elemente im neuen HTML ignorieren wir (wir nehmen die alten)
        if (isPersistent(newChild)) return;
        fragment.appendChild(document.adoptNode(newChild));
    });

    // D. DOM Swap (Hard Reset)
    document.body.innerHTML = '';
    
    // E. Persistente Elemente zurücklegen
    persistentElements.forEach(el => document.body.appendChild(el));
    
    // F. Neuen Content anhängen
    document.body.appendChild(fragment);
    
    // G. Klassen-Restore (Spezialfall für Translate Mode)
    // Da wir className oben überschrieben haben, müssen wir den State prüfen
    // Besser wäre, das im State Manager zu haben, aber so gehts auch:
    const cursor = document.querySelector('.custom-translate-cursor');
    if (cursor && cursor.style.display !== 'none') {
        document.body.classList.add('translate-mode-active');
    }
}

/**
 * Führt Skripte aus dem neuen Content aus oder blockiert sie bei Duplikaten.
 * HIER IST DEIN LOGGING UPDATE.
 */
async function runPageScripts(container) {
    const runningScripts = getRunningCoreScripts(); // Map<Path, Version>
    const newScripts = Array.from(container.querySelectorAll('script'));
    const loadPromises = [];

    for (const scriptTemplate of newScripts) {
        // Fall A: Externes Skript mit SRC
        if (scriptTemplate.src) {
            const { path: newPath, version: newVersion } = parseScriptUrl(scriptTemplate.src);

            // Prüfen, ob das Skript schon läuft
            if (runningScripts.has(newPath)) {
                const runningVersion = runningScripts.get(newPath);

                // --- VERSION CHECK ---
                if (runningVersion !== 'unknown' && newVersion !== 'unknown' && runningVersion !== newVersion) {
                    console.warn(`[Nav] ⚠️ Version Mismatch detected for ${newPath}`);
                    console.warn(`      Running: ${runningVersion}`);
                    console.warn(`      New:     ${newVersion}`);
                    console.warn(`      -> Forcing Reload.`);
                    window.location.reload();
                    return Promise.reject("Version Mismatch - Reloading"); 
                }

                // Gleiche Version -> Blockieren
                console.log(`[Nav] Blocked duplicate: ${newPath} (Running: ${runningVersion} | New: ${newVersion})`);
                scriptTemplate.remove(); 
                continue;
            }

            // Neues Skript -> Ausführen
            const newScript = document.createElement('script');
            Array.from(scriptTemplate.attributes).forEach(attr => newScript.setAttribute(attr.name, attr.value));
            
            const p = new Promise((resolve) => {
                newScript.onload = resolve;
                newScript.onerror = () => {
                    console.warn("[Nav] Script load failed:", newScript.src);
                    resolve(); // Wir blockieren nicht alles wegen einem Fehler
                };
            });
            loadPromises.push(p);
            
            scriptTemplate.parentNode.replaceChild(newScript, scriptTemplate);
        } 
        // Fall B: Inline Skript
        else {
            // Inline Scripts führen wir immer aus (außer es sind ImportMaps, die wir filtern sollten)
            if (scriptTemplate.type === 'importmap') continue;

            const newScript = document.createElement('script');
            Array.from(scriptTemplate.attributes).forEach(attr => newScript.setAttribute(attr.name, attr.value));
            newScript.textContent = scriptTemplate.textContent;
            scriptTemplate.parentNode.replaceChild(newScript, scriptTemplate);
        }
    }

    if (loadPromises.length > 0) {
        await Promise.all(loadPromises);
    }
}

function closeMobileMenu() {
    const sidebar = document.getElementById('sidebar-menu');
    const overlay = document.getElementById('menu-overlay');
    
    if(sidebar) sidebar.classList.remove('is-open');
    if(overlay) overlay.classList.remove('is-visible');
    
    // RESET ALL LOCK STYLES 
    // Wir setzen alle Styles zurück, die das Menü oder Swipe-Gesten gesetzt haben könnten
    document.body.style.overflow = '';
    document.documentElement.style.overflow = '';
    document.body.style.touchAction = '';
    document.documentElement.style.height = ''; 
    document.body.style.height = '';

    // Falls Klassen wie 'is-swiping-active' oder 'translate-mode-active' noch hängen:
    document.body.classList.remove('is-swiping-active');
}