import { deleteJobFromHistory, getJobHistory } from '/script/data/idb-helper.js';
import { renderHistoryList } from '/script/ui/navigation/menu.js';

// WICHTIG: Muss mit dem Namen im Service Worker übereinstimmen!
const CACHE_PREFIX = 'idea-atlas-job'; 
const CACHE_INDEX_KEY = 'cachedJobIndex';

const MAX_CACHED_JOBS = 25;

const getCacheName = (jobId) => `${CACHE_PREFIX}-${jobId}`;

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
    if (!('caches' in window)) {
        console.warn("Cache API nicht unterstützt.");
        return;
    }

    console.log(`[PageCache] Starte Caching für Job ${jobId} (in Cache: ${getCacheName(jobId)})...`);
    const cacheName = getCacheName(jobId);
    const cache = await caches.open(cacheName);

    const assetsToCache = new Set();
    assetsToCache.add(location.href);

    // CSS und Skripte
    document.querySelectorAll('link[rel="stylesheet"], script[src]').forEach(el => {
        const url = el.href || el.src;
        if (url) assetsToCache.add(new URL(url, location.href).href);
    });

    // Bilder
    const imageElements = Array.from(document.querySelectorAll('img[src]'));
    imageElements.forEach(img => {
        if (img.src) assetsToCache.add(new URL(img.src, location.href).href);
    });

    let successCount = 0;
    for (const url of assetsToCache) {
        try {
            const cachedResponse = await cache.match(url);
            if (cachedResponse) {
                successCount++;
                continue;
            }

            // 1. Versuch: Aus RAM (Canvas) lesen
            const correspondingImgElement = imageElements.find(img => new URL(img.src, location.href).href === url);
            if (correspondingImgElement) {
                const response = await getImageResponseFromElement(correspondingImgElement);
                if (response) {
                    await cache.put(url, response);
                    successCount++;
                    continue;
                }
            }

            // 2. Versuch: Netzwerk Fetch
            // WICHTIG: credentials: 'include' sorgt dafür, dass Cookies gesendet werden.
            // Das verhindert, dass bei geschützten Bildern die Login-Seite statt des Bildes gecacht wird.
            const request = new Request(url, { 
                cache: 'reload',
                credentials: 'include' 
            });
            
            const response = await fetch(request);
            if (response.ok) {
				await cache.put(url, response.clone());
				successCount++;
			}
		} catch (error) {
			console.warn(`[PageCache] Fehler bei ${url}:`, error);
		}
	}
	console.log(`[PageCache] ${successCount} Assets für ${jobId} gesichert.`);

	// --- AUFRÄUM-LOGIK ---
	let cacheIndex = JSON.parse(window.localStorage.getItem(CACHE_INDEX_KEY) || '[]');
	cacheIndex = cacheIndex.filter(id => id !== jobId);
	cacheIndex.push(jobId);

	const allJobs = await getJobHistory(); 
	const starredJobIds = new Set(allJobs.filter(j => j.starred).map(j => j.jobId));
	const deletionCandidates = cacheIndex.filter(id => !starredJobIds.has(id));

	let itemsDeleted = false;
	while (deletionCandidates.length > MAX_CACHED_JOBS) {
		const idToDelete = deletionCandidates.shift();
		cacheIndex = cacheIndex.filter(id => id !== idToDelete);
		try {
			await caches.delete(getCacheName(idToDelete));
			await deleteJobFromHistory(idToDelete);
			itemsDeleted = true;
		} catch (e) {
			console.error(`[PageCache] Fehler beim Löschen von ${idToDelete}:`, e);
		}
	}

	window.localStorage.setItem(CACHE_INDEX_KEY, JSON.stringify(cacheIndex));

	if (itemsDeleted && typeof renderHistoryList === 'function') {
		renderHistoryList();
	}
}

export function initializePageCache(initialData) {
    if (initialData.IS_DATA_AVAILABLE) {
        window.addEventListener('load', () => {
            setTimeout(() => {
                cacheSuccessfulJob(initialData.JOB_ID).catch(console.error);
            }, 1000);
        });
    }
}