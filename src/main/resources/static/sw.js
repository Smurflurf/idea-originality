/* ===========================================================
   SERVICE WORKER - ATOMIC VERSIONED CACHE
   =========================================================== */

const APP_VERSION = '@project.version@';
const STATIC_CACHE_NAME = `idea-atlas-static-v${APP_VERSION}`;
const JOB_CACHE_PREFIX = 'idea-atlas-job-';

// Liste der Dateien, die wir unbedingt brauchen.
// Wenn deine main.js jetzt alles enthält, brauchen wir menu.js hier nicht mehr.
// Falls Vite doch splittet, ist es sicher, sie drin zu lassen, aber kein Muss, 
// solange main.js sie importiert und der SW sie dann dynamisch cacht.
const CORE_ASSETS = [
	'/',
	'/dist/main.css',
	'/dist/main.js',
	'/dist/menu.css',
	'/vendor/fontawesome/css/all.min.css',
	'/vendor/fontawesome/webfonts/fa-solid-900.woff2',
	'/vendor/fontawesome/webfonts/fa-regular-400.woff2',
	'/vendor/fontawesome/webfonts/fa-brands-400.woff2',
	'/vendor/fonts/roboto.css',
	'/vendor/tippy/tippy-bundle.umd.min.js',
	'/vendor/tippy/popper.min.js',
	'/vendor/tippy/tippy.css'
];

// --- INSTALL: STRIKTE VERSIONIERUNG ---

self.addEventListener('install', (event) => {
    self.skipWaiting();
    event.waitUntil(
        caches.open(STATIC_CACHE_NAME).then(async (cache) => {
            // Wir bauen explizit Requests mit der Versionsnummer
            const promises = CORE_ASSETS.map(async (assetPath) => {
                // Der Key im Cache soll MIT Parameter sein: "/dist/main.js?v=4.1.179"
                const versionedUrl = `${assetPath}${assetPath === '/' ? '' : '?v=' + APP_VERSION}`;
                
                try {
                    // cache: 'reload' zwingt den Browser, das Netz zu nutzen
                    const response = await fetch(versionedUrl, { cache: 'reload' });
                    
					if (!response.ok) throw new Error(`Status ${response.status}`);

					// Wir speichern es unter der versionierten URL
					await cache.put(versionedUrl, response);
				} catch (error) {
					console.error(`[SW INSTALL ERROR] Failed to fetch: ${url}`, error);
					// Error reporting an Clients
					const allClients = await self.clients.matchAll({ includeUncontrolled: true, type: 'window' });
					for (const client of allClients) {
						client.postMessage({
							type: 'INSTALL_ERROR',
							file: url,
							status: error.message
						});
					}
					throw error;
				}



				//} catch (e) {
                //    console.error(`[SW INSTALL] Failed to cache ${versionedUrl}:`, e);
                //    // Wir werfen keinen Fehler, damit der SW trotzdem installiert wird.
                //    // Fehlende Dateien werden zur Laufzeit nachgeladen.
                //}
            });
            return Promise.all(promises);
        })
    );
});

// --- ACTIVATE: ALTE CACHES RIGOROS LÖSCHEN ---

self.addEventListener('activate', (event) => {
    event.waitUntil(self.clients.claim());
    event.waitUntil(
        caches.keys().then(keys => Promise.all(
            keys.map(key => {
                // Lösche ALLES, was static ist und nicht exakt unsere Version hat
                if (key.startsWith('idea-atlas-static-') && key !== STATIC_CACHE_NAME) {
                    console.log(`[SW] Deleting old cache: ${key}`);
                    return caches.delete(key);
                }
            })
        ))
    );
});

// --- HELPER ---

/**
 * Entfernt gzip-Header, um NS_ERROR_CORRUPTED_CONTENT zu verhindern.
 */
async function cleanResponse(response) {
    if (!response) return null;
    if (response.type === 'opaque') return response;

    const newHeaders = new Headers(response.headers);
    newHeaders.delete('Content-Encoding');
    newHeaders.delete('Content-Length');
    
    if (newHeaders.get('Content-Type')?.includes('text/html')) {
        newHeaders.set('Content-Type', 'text/html; charset=utf-8');
    }

    return new Response(response.body, {
        status: response.status,
        statusText: response.statusText,
        headers: newHeaders
    });
}

/**
 * Normalisiert URLs (entfernt Trailing Slashes und Query Params für den Lookup)
 */
function getNormalizedUrl(urlStr) {
    const u = new URL(urlStr);
    u.search = '';
    u.hash = '';
    let path = u.pathname;
    if (path.endsWith('/') && path.length > 1) path = path.slice(0, -1);
    return u.origin + path;
}

// --- FETCH STRATEGIE ---

self.addEventListener('fetch', (event) => {
    const url = new URL(event.request.url);

    // 1. Ignorieren
    if (event.request.method !== 'GET' || url.pathname.startsWith('/query/') || url.pathname.startsWith('/api/')) {
        return;
    }

    event.respondWith((async () => {
        try {
            // A. RESULTS PAGES (Job Daten)
            if (url.pathname.startsWith('/results/')) {
                const jobId = url.pathname.split('/')[2];
                const jobCacheName = `${JOB_CACHE_PREFIX}${jobId}`;
                const cache = await caches.open(jobCacheName);
                
                // Wir suchen tolerant (mit/ohne Slash)
                const normUrl = getNormalizedUrl(event.request.url);
                const cachedRes = await cache.match(normUrl) || await cache.match(normUrl + '/');

                if (cachedRes) {
                    // WICHTIG: HTML immer cleanen
                    if (event.request.destination === 'document') {
                        return await cleanResponse(cachedRes);
                    }
                    return cachedRes;
                }
                
                // Wenn nicht im Cache: Netz (aber HTML nicht cachen!)
                const netRes = await fetch(event.request);
                if (netRes.ok && event.request.destination !== 'document') {
                    cache.put(event.request, netRes.clone());
                }
                return netRes;
            }

            // B. STATIC ASSETS (Core Files)
            // Hier liegt der Trick: Wir suchen im Cache IMMER nach der versionierten Datei!
            
            // Ist es eine unserer Core-Dateien?
            const cleanPath = url.pathname;
            if (CORE_ASSETS.includes(cleanPath)) {
                const cache = await caches.open(STATIC_CACHE_NAME);
                
                // Wir bauen den Key so, wie wir ihn im Install-Event gespeichert haben:
                // z.B. /dist/main.js?v=4.1.179
                const versionedKey = `${cleanPath}${cleanPath === '/' ? '' : '?v=' + APP_VERSION}`;
                
                const cachedRes = await cache.match(versionedKey);
                if (cachedRes) {
                    return await cleanResponse(cachedRes);
                }
            }

            // C. GENERIC CACHE FALLBACK & NETWORK
            // Fallback für alles andere
            const cachedGeneric = await caches.match(event.request, { ignoreSearch: true });
            if (cachedGeneric) {
                return await cleanResponse(cachedGeneric);
            }

            return await fetch(event.request);

        } catch (err) {
            console.error("[SW Fetch Error]", err);
            return new Response("Network Error", { status: 408 });
        }
    })());
});