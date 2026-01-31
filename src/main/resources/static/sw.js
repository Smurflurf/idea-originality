/* ===========================================================
   SERVICE WORKER - CACHE CONTROL AND OFFLINE FUNCTIONALITY
   =========================================================== */

// Wird von Thymeleaf beim Rendern ersetzt.
// Ändert sich dieser String, erkennt der Browser ein Update.
const APP_VERSION = '@project.version@';

const STATIC_CACHE_NAME = `idea-atlas-static-v${APP_VERSION}`;
const JOB_CACHE_PREFIX = 'idea-atlas-job-';

// Diese Dateien sind kritisch. Schlägt eine davon fehl,
// wird der Service Worker NICHT installiert (Atomic).
const PRECACHE_URLS = [
    '/',
    '/dist/main.css',
    '/dist/menu.css',
    '/dist/main.js',
    '/dist/menu.js',
    '/dist/localization.js',
    '/vendor/fontawesome/css/all.min.css',
    '/vendor/fontawesome/webfonts/fa-solid-900.woff2',
    '/vendor/fontawesome/webfonts/fa-regular-400.woff2',
    '/vendor/fontawesome/webfonts/fa-brands-400.woff2',
    '/vendor/fonts/roboto.css'
];

// --- HELPER FUNCTIONS ---

/**
 * Workaround für iOS/Safari/Chrome Bugs bei Blob-Responses:
 * Setzt den Content-Type Header korrekt, wenn HTML aus Cache geladen wird.
 */
async function fixHtmlHeader(response) {
    const body = await response.blob();
    const headers = new Headers(response.headers);
    headers.set('Content-Type', 'text/html; charset=utf-8');
    return new Response(body, {
        status: response.status,
        statusText: response.statusText,
        headers: headers
    });
}

function getJobIdFromUrl(pathname) {
    const parts = pathname.split('/');
    if (parts.length >= 3 && parts[1] === 'results') {
        return parts[2];
    }
    return 'unknown';
}

// --- LIFECYCLE: INSTALL (Neues Update laden) ---

self.addEventListener('install', (event) => {
    // 1. Zwingt den wartenden SW, sofort aktiv zu werden.
    // Wir warten NICHT darauf, dass der User Tabs schließt.
    self.skipWaiting();

    event.waitUntil(
        caches.open(STATIC_CACHE_NAME).then(async (cache) => {
            console.log(`[SW] Installing v${APP_VERSION}. Downloading files...`);

            // URLs vorbereiten: Wir hängen die Version an, um absolut sicherzugehen,
            // dass wir nicht aus Versehen etwas Altes cachen.
            const urlsWithVersion = PRECACHE_URLS.map(u => {
                if (u === '/') return u;
                const separator = u.includes('?') ? '&' : '?';
                return `${u}${separator}v=${APP_VERSION}`;
            });

            // Wir führen alle Requests parallel aus.
            // map() liefert ein Array von Promises.
            const fetchPromises = urlsWithVersion.map(async (url) => {
                try {
                    // { cache: 'reload' } zwingt den Browser, seinen HTTP-Cache zu ignorieren und
                    // die Datei frisch vom Server zu laden. 
                    const response = await fetch(url, { cache: 'reload' });

                    if (!response.ok) {
                        throw new Error(`HTTP Status ${response.status}`);
                    }

                    return cache.put(url, response);
                } catch (error) {
                    // Logging für Debugging
                    console.error(`[SW INSTALL ERROR] Failed to fetch: ${url}`, error);

                    // Reporting an das Frontend (für Anzeige in der Konsole)
                    const allClients = await self.clients.matchAll({ includeUncontrolled: true, type: 'window' });
                    for (const client of allClients) {
                        client.postMessage({
                            type: 'INSTALL_ERROR',
                            file: url,
                            status: error.message
                        });
                    }

                    // Wir werfen den Fehler weiter. Das markiert die Installation als FAILED.
                    // Der alte SW bleibt aktiv. Das System bricht nicht zusammen.
                    throw error;
                }
            });

            return Promise.all(fetchPromises);
        })
    );
});

// --- LIFECYCLE: ACTIVATE (Aufräumen & Übernehmen) ---

self.addEventListener('activate', (event) => {
    // 2. Sofortige Kontrolle über alle offenen Clients (Tabs) übernehmen.
    // Zusammen mit skipWaiting sorgt das dafür, dass Requests sofort durch den neuen SW gehen.
    event.waitUntil(self.clients.claim());

    event.waitUntil(
        caches.keys().then((cacheNames) => {
            return Promise.all(
                cacheNames.map((cacheName) => {
                    // Lösche alte STATISCHE Caches.
                    // Alles was nicht exakt dem aktuellen Versionsnamen entspricht, fliegt raus.
                    if (cacheName.startsWith('idea-atlas-static-') && cacheName !== STATIC_CACHE_NAME) {
                        console.log('[SW] Cleaning old static cache:', cacheName);
                        return caches.delete(cacheName);
                    }
                    // Job-Caches (User-Daten) fassen wir NICHT an.
                })
            );
        })
    );
});

// --- FETCH STRATEGIES (Anfragen abfangen) ---

self.addEventListener('fetch', (event) => {
    const url = new URL(event.request.url);

    // STRATEGIE A: Network Only
    // API-Aufrufe, Tracking, Uploads -> Nie cachen.
    if (event.request.method !== 'GET' || url.pathname.startsWith('/query/') || url.pathname.startsWith('/api/')) {
        return;
    }

    // STRATEGIE B: Cache First -> Network Fallback (User Content / Results)
    // Wir wollen Job-Ergebnisse offline verfügbar haben.
    if (url.pathname.startsWith('/results/')) {
        const jobId = getJobIdFromUrl(url.pathname);
        const jobCacheName = `${JOB_CACHE_PREFIX}${jobId}`;

        event.respondWith(
            caches.open(jobCacheName).then((cache) => {
                return cache.match(event.request).then((cachedResponse) => {
                    // Treffer im Cache? Zurückgeben!
                    if (cachedResponse) {
                        if (event.request.destination === 'document') {
                            return fixHtmlHeader(cachedResponse);
                        }
                        return cachedResponse;
                    }
                    
                    // Kein Treffer? Netzwerk fragen und Antwort für später cachen.
                    return fetch(event.request).then((networkResponse) => {
                        if (networkResponse.ok) {
                            cache.put(event.request, networkResponse.clone());
                        }
                        return networkResponse;
                    }).catch(() => {
                        // Offline Fallback für HTML Seiten
                        if (event.request.destination === 'document') {
                            return new Response(
                                '<div style="font-family:sans-serif;text-align:center;padding:50px;color:#888;"><h1>Offline</h1><p>Diese Analyse ist nicht lokal gespeichert.</p><a href="/" style="color:#8ab4f8">Zurück zur Startseite</a></div>',
                                { headers: { 'Content-Type': 'text/html' } }
                            );
                        }
                        // Offline Fallback für Assets (Bilder etc.)
                        return new Response('Offline', { status: 404 });
                    });
                });
            })
        );
        return;
    }

    // STRATEGIE C: Cache First (Statische Assets & Navigation)
    // Da wir beim Installieren sichergestellt haben, dass wir die NEUSTE Version
    // im Cache `idea-atlas-static-vX.Y` haben, können wir dem Cache blind vertrauen.
    event.respondWith(
        caches.open(STATIC_CACHE_NAME).then((cache) => {
            return cache.match(event.request).then((cachedResponse) => {
                if (cachedResponse) {
                    return cachedResponse;
                }

                // Wenn etwas nicht im Cache ist (z.B. Lazy Loaded Chunks, die nicht in PRECACHE_URLS waren),
                // holen wir es aus dem Netzwerk.
                return fetch(event.request).then((networkResponse) => {
                    if (networkResponse.ok && networkResponse.type === 'basic') {
                        cache.put(event.request, networkResponse.clone());
                    }
                    return networkResponse;
                }).catch((error) => {
                     // Offline Navigation Fallback -> Startseite
                     // Wenn User offline ist und eine unbekannte URL aufruft, zeige index.html
                     if (event.request.mode === 'navigate') {
                        return cache.match('/');
                    }
                    throw error;
                });
            });
        })
    );
});