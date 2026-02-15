import { deleteJobFromHistory, getJobHistory } from '/script/data/idb-helper.js';
import { renderHistoryList } from '/script/ui/navigation/menu.js';

// WICHTIG: Muss mit dem Namen im Service Worker übereinstimmen!
const CACHE_PREFIX = 'idea-atlas-job'; 
const CACHE_INDEX_KEY = 'cachedJobIndex';

const MAX_CACHED_JOBS = 25;

const getCacheName = (jobId) => `${CACHE_PREFIX}-${jobId}`;

// Hilfsfunktion: URL normalisieren (Slash am Ende weg, Query Params weg)
function getCanonicalUrl() {
    const u = new URL(window.location.href);
    u.search = '';
    u.hash = '';
    // Entferne Trailing Slash, falls vorhanden
    if (u.pathname.endsWith('/') && u.pathname.length > 1) {
        u.pathname = u.pathname.slice(0, -1);
    }
    return u.href;
}

function getImageResponseFromElement(imgElement) {
    return new Promise((resolve) => {
        if (!imgElement || !imgElement.complete || imgElement.naturalWidth === 0) {
            resolve(null);
            return;
        }
        try {
            const canvas = document.createElement('canvas');
            canvas.width = imgElement.naturalWidth;
            canvas.height = imgElement.naturalHeight;
            const ctx = canvas.getContext('2d');
            ctx.drawImage(imgElement, 0, 0);
            
            canvas.toBlob(blob => {
                if (blob) {
                    const response = new Response(blob, {
                        status: 200,
                        statusText: 'OK',
                        headers: { 'Content-Type': 'image/png' }
                    });
                    resolve(response);
                } else {
                    resolve(null);
                }
            }, 'image/png');
        } catch (error) {
            console.warn("Image caching via Canvas failed (probably tainted):", error);
            resolve(null);
		}
	});
}

async function cacheSuccessfulJob(jobId) {
	if (!('caches' in window)) return;

	// 1. SICHERHEITS-CHECK: Ist das eine Fehlerseite?
	// Wenn ja: SOFORT ABBRECHEN. Auf keinen Fall speichern!
	if (document.querySelector('.no-results-message') || document.body.innerText.includes('Error: Job data not found')) {
		console.warn(`[PageCache] Fehlerseite erkannt für Job ${jobId}. Snapshot wird NICHT gespeichert.`);
		return;
	}

	console.log(`[PageCache] Sicherung für Job ${jobId} gestartet...`);
	const cacheName = getCacheName(jobId);
	const cache = await caches.open(cacheName);

	// 2. HTML SNAPSHOT (Der Fix für Fire-and-Forget!)
	try {
		const htmlContent = "<!DOCTYPE html>\n" + document.documentElement.outerHTML;
		const htmlBlob = new Blob([htmlContent], { type: 'text/html; charset=utf-8' });

		const htmlResponse = new Response(htmlBlob, {
			status: 200,
			statusText: 'OK',
			headers: { 'Content-Type': 'text/html; charset=utf-8' }
		});

		// WICHTIG: Wir nutzen die kanonische URL als Key!
		const cleanUrl = getCanonicalUrl();
		console.log(`[PageCache] Speichere HTML unter Key: ${cleanUrl}`);

		await cache.put(cleanUrl, htmlResponse);

	} catch (e) {
		console.error("[PageCache] Fehler beim HTML Snapshot:", e);
	}

    // 2. Assets sammeln (CSS, JS, Bilder)
    // HTML (location.href) fügen wir HIER NICHT MEHR hinzu, das haben wir oben erledigt.
    const assetsToCache = new Set();

    // CSS und Skripte (nur die wichtigen, SW kümmert sich um den Rest, aber sicher ist sicher)
    document.querySelectorAll('link[rel="stylesheet"], script[src]').forEach(el => {
        const url = el.href || el.src;
        if (url && !url.startsWith('data:')) {
            assetsToCache.add(new URL(url, location.href).href);
        }
    });

    // Bilder
    const imageElements = Array.from(document.querySelectorAll('img[src]'));
    imageElements.forEach(img => {
        if (img.src && !img.src.startsWith('data:')) {
            assetsToCache.add(new URL(img.src, location.href).href);
        }
    });

    // 3. Assets cachen
    for (const url of assetsToCache) {
        try {
            // Prüfen ob schon da (vom SW)
            const cachedResponse = await cache.match(url);
            if (cachedResponse) continue;

            // Canvas Trick für Bilder (falls tainted/cors Probleme)
            const correspondingImgElement = imageElements.find(img => new URL(img.src, location.href).href === url);
            if (correspondingImgElement) {
                const response = await getImageResponseFromElement(correspondingImgElement);
                if (response) {
                    await cache.put(url, response);
                    continue;
                }
            }

            // Normaler Fetch für Assets
            // Hier ist cache: 'default' okay, oder 'force-cache'
            const request = new Request(url, { mode: 'no-cors' }); 
            const response = await fetch(request);
            if (response.type === 'opaque' || response.ok) {
				await cache.put(url, response.clone());
			}
		} catch (error) {
			console.warn(`[PageCache] Asset ${url} konnte nicht gesichert werden.`);
		}
	}

	// --- AUFRÄUM-LOGIK (Unverändert) ---
	let cacheIndex = JSON.parse(window.localStorage.getItem(CACHE_INDEX_KEY) || '[]');
	cacheIndex = cacheIndex.filter(id => id !== jobId);
	cacheIndex.push(jobId);

	const allJobs = await getJobHistory(); 
	const starredJobIds = new Set(allJobs.filter(j => j.starred).map(j => j.jobId));
	const deletionCandidates = cacheIndex.filter(id => !starredJobIds.has(id));

	while (deletionCandidates.length > MAX_CACHED_JOBS) {
		const idToDelete = deletionCandidates.shift();
		cacheIndex = cacheIndex.filter(id => id !== idToDelete);
		try {
			await caches.delete(getCacheName(idToDelete));
			await deleteJobFromHistory(idToDelete);
		} catch (e) {}
	}
	window.localStorage.setItem(CACHE_INDEX_KEY, JSON.stringify(cacheIndex));
    if (typeof renderHistoryList === 'function') renderHistoryList();
}

export function initializePageCache(initialData) {
    if (initialData.IS_DATA_AVAILABLE) {
        // Warten bis alles idle ist, dann cachen
        if ('requestIdleCallback' in window) {
            requestIdleCallback(() => cacheSuccessfulJob(initialData.JOB_ID));
        } else {
            setTimeout(() => cacheSuccessfulJob(initialData.JOB_ID), 2000);
        }
    }
}