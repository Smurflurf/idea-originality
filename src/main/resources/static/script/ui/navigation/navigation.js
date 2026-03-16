import { detachPlayer, reattachPlayer } from '/script/features/accessibility/tts.js';
import { loadLanguageData, applyGeneralTranslations } from '/script/core/localization.js';
import { disposeAllVisualizations } from '/script/viz/core/zoomAndPan.js';
import { on, emit, EVENTS } from '/script/core/eventBus.js';
import { closeSidebar } from '/script/ui/navigation/menu.js';
import { getCsrfToken } from '/script/core/security.js';

// --- KONFIGURATION ---
const PERSISTENT_IDS = ['tts-floating-player'];
const PERSISTENT_CLASSES = ['menu-wrapper', 'menu-overlay', 'tts-floating-player-container', 'custom-translate-cursor'];

// --- STATE ---
let softNavInitialized = false;
let currentNavController = null;

// --- HELPER ---
function parseScriptUrl(src) {
    try {
        const url = new URL(src, window.location.origin);
        return { path: url.pathname, version: url.searchParams.get('v') || 'unknown' };
    } catch (e) { return { path: src, version: 'unknown' }; }
}

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
    if (element.hasAttribute('data-is-persistent')) return true; 

    if (element.id && PERSISTENT_IDS.includes(element.id)) return true;
    if (element.classList && element.classList.length > 0) {
        return PERSISTENT_CLASSES.some(c => element.classList.contains(c));
    }
    return false;
}

/**
 * Sendet das "fire-and-forget" Cleanup-Signal an den Server.
 * Diese Funktion wird aufgerufen, NACHDEM die Bilder garantiert geladen sind.
 */
function sendCleanupSignal(jobId) {
    const url = `/results/${jobId}/cleanup`;
    const csrfToken = getCsrfToken();

    console.log(`[Nav] Sending cleanup signal for job ${jobId}.`);

    if (navigator.sendBeacon) {
        const headers = { type: 'application/x-www-form-urlencoded' };
        // Spring Security CSRF erwartet den Token im Body bei Beacon
        const blob = new Blob([`_csrf=${csrfToken}`], headers);
        navigator.sendBeacon(url, blob);
    } else {
        fetch(url, {
            method: 'POST',
            headers: { 'X-XSRF-TOKEN': csrfToken },
            keepalive: true 
        }).catch(err => {
            console.warn("Cleanup signal could not be sent:", err);
        });
    }
}

/**
 * Erstellt Promises, die auflösen, sobald die wichtigsten Visualisierungs-
 * Bilder geladen (oder fehlgeschlagen) sind, und ruft dann das Cleanup auf.
 */
function waitForImagesAndCleanup(jobId) {
	// Prüfung auf frischen Generierungs-Flag
	const cleanupKey = `pending_cleanup_${jobId}`;

	// Wenn der Key NICHT existiert, kommen wir aus dem Cache/History -> Abbrechen.
	if (!sessionStorage.getItem(cleanupKey)) {
		console.log(`[Nav] Cached view detected for ${jobId}. Skipping cleanup signal.`);
		return;
	}

	// Key entfernen (damit beim Reload der Seite das Signal nicht nochmal gesendet wird)
	sessionStorage.removeItem(cleanupKey);
	
	
    // Das sind die IDs der Bilder, die über das Netzwerk geladen werden und kritisch sind.
    const criticalImageIds = [
        'viz-layer-own-base',
        'viz-layer-own-points',
        'viz-layer-nc-points',
        'viz-layer-serendipity-points'
    ];

    const imagePromises = criticalImageIds.map(id => {
        return new Promise(resolve => {
            const img = document.getElementById(id);

            // Wenn das Bild nicht auf der Seite ist, müssen wir nicht darauf warten.
            if (!img) {
                resolve();
                return;
            }
            
            // Wenn das Bild bereits aus dem Cache geladen wurde, ist .complete sofort true.
            if (img.complete) {
                resolve();
            } else {
                // Ansonsten warten wir auf das Lade- oder Fehler-Event.
                img.onload = () => resolve();
                img.onerror = () => {
                    console.warn(`[Nav] Image ${id} failed to load, but proceeding with cleanup.`);
                    resolve(); // Wir lösen trotzdem auf, damit das Cleanup nicht blockiert wird.
                };
            }
        });
    });

    console.log(`[Nav] Waiting for ${imagePromises.length} critical images to load...`);

    // Wenn ALLE Promises erfüllt sind (egal ob Erfolg oder Fehler), DANN das Signal senden.
    Promise.all(imagePromises).then(() => {
        console.log("[Nav] All critical images have loaded.");
        sendCleanupSignal(jobId);
    });
}

// --- CORE LOGIC ---

export function initializeSoftNavigation() {
    if (softNavInitialized) return;
    softNavInitialized = true;
    
    on(EVENTS.TTS_NAVIGATE, (data) => {
        const targetUrl = data.url;
        if (!isCurrentUrl(targetUrl)) navigateTo(targetUrl);
        else emit(EVENTS.TTS_FINISHED);
    });

    document.addEventListener('click', (e) => {
        const link = e.target.closest('a');
        if (!link || !link.href) return;
        if (e.ctrlKey || e.metaKey || e.shiftKey || e.altKey) return;
        if (link.target || link.hasAttribute('download')) return;
        if (link.getAttribute('href') === '#') return;
        if (!link.href.startsWith(window.location.origin)) return;

        e.preventDefault();
        closeSidebar();

        if (isCurrentUrl(link.href)) return;
        navigateTo(link.href);
    });

    window.addEventListener('popstate', () => navigateTo(window.location.href, false));
}

async function navigateTo(url, pushState = true) {
    closeSidebar();

    if (currentNavController) currentNavController.abort();
    currentNavController = new AbortController();
    const signal = currentNavController.signal;

    document.body.classList.add('is-loading');
    disposeAllVisualizations();
    detachPlayer();

    try {
        const response = await fetch(url, { signal });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        
        const htmlText = await response.text();
        const parser = new DOMParser();
        const newDoc = parser.parseFromString(htmlText, 'text/html');

        const dataScript = newDoc.querySelector('#initial-data-script');
        if (dataScript) {
            try {
                const dataCode = dataScript.textContent;
                if (dataCode.includes('window.INITIAL_DATA')) {
                    const executeData = new Function(dataCode);
                    executeData();
                }
            } catch (e) {}
        }

        const currentLang = document.documentElement.lang || 'en';
        let path = new URL(url, window.location.origin).pathname;
        if (path.startsWith('/')) path = path.substring(1);
        let pageName = path.split('/')[0] || 'index';
        pageName = pageName.replace('.html', '');

        await loadLanguageData(currentLang, ['common', 'index', 'results', pageName]);

        await swapStyles(newDoc);
        await swapBodyContent(newDoc);

        document.title = newDoc.title;
        window.scrollTo(0, 0);
        
        document.body.classList.remove('is-swiping-active', 'no-scroll');
        document.body.style.overflow = '';
        document.body.style.touchAction = '';
        document.documentElement.style.overflow = '';

        if (pushState) window.history.pushState({}, '', url);

        reattachPlayer();
        await runPageScripts(document.body);

        document.dispatchEvent(new CustomEvent('dynamicContentLoaded', { detail: { container: document.body } }));
        emit(EVENTS.TTS_FINISHED);
        
        // --- FINALE CLEANUP LOGIK ---
        const urlObject = new URL(url, window.location.origin);
        if (urlObject.pathname.startsWith('/results/')) {
            const jobId = urlObject.pathname.split('/')[2];
            if (jobId) {
                // Starte den Warte-Prozess für die Bilder.
                waitForImagesAndCleanup(jobId);
            }
        }
        
        requestAnimationFrame(() => applyGeneralTranslations(document.body));

    } catch (error) {
        if (error.name !== 'AbortError') {
            console.error('[Nav] Failed:', error);
            window.location.href = url;
        }
    } finally {
        document.documentElement.setAttribute('data-i18n-ready', 'true');
        document.body.classList.remove('is-loading');
        currentNavController = null;
    }
}

async function swapStyles(newDoc) {
    const currentLinks = Array.from(document.querySelectorAll('link[rel="stylesheet"]'));
    const newLinks = Array.from(newDoc.querySelectorAll('link[rel="stylesheet"]'));
    
    const getBasePath = (href) => { try { return new URL(href, window.location.origin).pathname; } catch(e) { return href; } };
    const newPaths = new Set(newLinks.map(l => getBasePath(l.getAttribute('href'))));
    const currentPaths = new Set(currentLinks.map(l => getBasePath(l.getAttribute('href'))));

    const loadPromises = [];
    newLinks.forEach(newLink => {
        if (!currentPaths.has(getBasePath(newLink.getAttribute('href')))) {
            const linkTag = document.createElement('link');
            linkTag.rel = 'stylesheet';
            linkTag.href = newLink.getAttribute('href'); 
            const p = new Promise(r => { linkTag.onload = r; linkTag.onerror = r; });
            loadPromises.push(p);
            document.head.appendChild(linkTag);
        }
    });

    if (loadPromises.length > 0) await Promise.all(loadPromises);

    currentLinks.forEach(link => {
        if (!newPaths.has(getBasePath(link.getAttribute('href')))) link.remove();
    });
}

async function swapBodyContent(newDoc) {
    const runningScripts = getRunningCoreScripts();
    const fragment = document.createDocumentFragment();
    const persistentElements = [];

    // Persistente Elemente retten
    Array.from(document.body.children).forEach(child => {
        if (isPersistent(child)) persistentElements.push(child);
    });
    // Auch persistente Scripts retten (z.B. main.js wenn es im Body wäre)
    document.querySelectorAll('script[src]').forEach(script => {
         const info = parseScriptUrl(script.src);
         if (runningScripts.has(info.path)) persistentElements.push(script);
    });

    // Body Attribute syncen
    const newFeatures = newDoc.body.getAttribute('data-features');
    if (newFeatures) document.body.setAttribute('data-features', newFeatures);
    else document.body.removeAttribute('data-features');

    if (newDoc.body.id) document.body.id = newDoc.body.id;
    else document.body.removeAttribute('id');
    
    // Klassen übernehmen (aber vorsichtig)
    const dangerousClasses = ['is-swiping-active', 'no-scroll', 'is-loading'];
    const newClasses = Array.from(newDoc.body.classList).filter(c => !dangerousClasses.includes(c));
    document.body.className = newClasses.join(' ');

    // Neuen Content einfügen (ohne Duplikate der persistenten)
    Array.from(newDoc.body.children).forEach(newChild => {
        if (isPersistent(newChild)) return; 
        fragment.appendChild(document.adoptNode(newChild));
    });

    document.body.innerHTML = '';
    persistentElements.forEach(el => document.body.appendChild(el));
    document.body.appendChild(fragment);
    
    // Translate Cursor Restore
    const cursor = document.querySelector('.custom-translate-cursor');
    if (cursor && cursor.style.display !== 'none') document.body.classList.add('translate-mode-active');
}

async function runPageScripts(container) {
    const runningScripts = getRunningCoreScripts();
    const newScripts = Array.from(container.querySelectorAll('script'));
    const loadPromises = [];

    for (const scriptTemplate of newScripts) {
        // Data Script überspringen, das haben wir schon im Pre-Load gemacht!
        if (scriptTemplate.id === 'initial-data-script') continue; 
        if (scriptTemplate.type === 'importmap') continue;

        if (scriptTemplate.src) {
            const { path } = parseScriptUrl(scriptTemplate.src);
            if (runningScripts.has(path)) { scriptTemplate.remove(); continue; } // Schon geladen

            const newScript = document.createElement('script');
            Array.from(scriptTemplate.attributes).forEach(attr => newScript.setAttribute(attr.name, attr.value));
            const p = new Promise(r => { newScript.onload = r; newScript.onerror = r; });
            loadPromises.push(p);
            scriptTemplate.parentNode.replaceChild(newScript, scriptTemplate);
        } else {
            // Inline Script
            const newScript = document.createElement('script');
            Array.from(scriptTemplate.attributes).forEach(attr => newScript.setAttribute(attr.name, attr.value));
            newScript.textContent = scriptTemplate.textContent;
            scriptTemplate.parentNode.replaceChild(newScript, scriptTemplate);
        }
    }
    if (loadPromises.length > 0) await Promise.all(loadPromises);
}