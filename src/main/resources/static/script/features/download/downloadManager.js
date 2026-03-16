import { getContext, getJobTitle } from '/script/core/context.js';
import { getI18nData, t } from '/script/core/localization.js'; 
import { getCsrfToken } from '/script/core/security.js';
import { getTemplate, renderTemplate } from '/script/core/templateManager.js';

let offlineLoaderCache = {};

function sanitizeForFilename(title) {
	if (!title || title.trim() === '') return 'untitled-analysis';
	return title.trim().toLowerCase().replace(/\s+/g, '-').replace(/[^\w-]/g, '').substring(0, 50); 
}

function temporarilyRevealAll(doc) {
	const hiddenPanes = doc.querySelectorAll('.viz-content-pane');
	const originalDisplays = [];
	hiddenPanes.forEach(pane => {
		originalDisplays.push({ el: pane, display: pane.style.display });
		pane.style.display = 'block'; 
	});
	return () => {
		originalDisplays.forEach(item => { item.el.style.display = item.display; });
	};
}

// --- INTELLIGENT CACHE/NETWORK FETCHING ---

async function getFromCacheOrNetwork(url) {
    const absoluteUrl = new URL(url, document.baseURI).href;
    
    // VERSUCH 1: Standard Fetch (geht durch den Service Worker!)
    // Wir nutzen 'no-cache', damit der Browser nicht seinen lokalen HTTP-Disk-Cache nutzt,
    // sondern den Service Worker (oder das Netzwerk) fragt.
    try {
        const res = await fetch(absoluteUrl, { cache: 'no-cache' });
        if (res.ok) {
            return await res.text();
        }
    } catch (e) {
        console.warn(`[Bundler] Fetch failed for ${url}, trying manual cache fallback...`);
    }

    // VERSUCH 2: Manueller Cache-Zugriff (nur als absoluter Notfall, falls SW tot ist)
    if ('caches' in window) {
        try {
            const keys = await caches.keys();
            for (const key of keys) {
                // Nur Caches nutzen, die explizit zur AKTUELLEN Seite gehören könnten
                // (Hier wäre es besser, nur den statischen Cache zu prüfen, aber wir kennen den Namen hier nicht)
                if (key.includes('static') || key.includes('job')) {
                    const cache = await caches.open(key);
                    const cachedRes = await cache.match(absoluteUrl, { ignoreSearch: true });
                    if (cachedRes) return await cachedRes.text();
                }
            }
        } catch (e) {}
    }

    throw new Error(`Resource not found: ${url}`);
}


async function getBlobFromCacheOrNetwork(url) {
    const absoluteUrl = new URL(url, document.baseURI).href;

    // VERSUCH 1: Fetch via Service Worker
    try {
        const res = await fetch(absoluteUrl, { cache: 'no-cache' });
        if (res.ok) {
            return await res.blob();
        }
    } catch (e) {
        console.warn(`[Bundler] Blob fetch failed for ${url}, trying fallback...`);
    }

    // VERSUCH 2: Manueller Cache Fallback
    if ('caches' in window) {
        try {
            const keys = await caches.keys();
            for (const key of keys) {
                const cache = await caches.open(key);
                const cachedRes = await cache.match(absoluteUrl, { ignoreSearch: true });
                if (cachedRes) return await cachedRes.blob();
            }
        } catch (e) {}
    }
    
    throw new Error(`Network error for blob: ${url}`);
}

async function resourceToDataURL(url) {
	/*try {
		const blob = await getBlobFromCacheOrNetwork(url);
		return await blobToDataURL(blob);
	} catch (error) {
		return "data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7";
	}*/
	return await getFastDataUrl(url); 
}

/**
 * Konvertiert einen Blob extrem schnell in einen Base64-String.
 * Nutzt den internen Buffer statt schwerfälliger FileReader-Events, wo möglich.
 */
function blobToBase64(blob) {
	return new Promise((resolve, reject) => {
		const reader = new FileReader();
		reader.onloadend = () => resolve(reader.result);
		reader.onerror = reject;
		reader.readAsDataURL(blob);
	});
}


// Einmalig erstellter Canvas zum Wiederverwenden (spart Memory-Allocations)
let sharedCanvas = null;
let sharedCtx = null;
/**
 * Smart-Hybrid: Konvertiert kleine Bilder (Icons/SVGs) sofort,
 * komprimiert große Bilder aber blitzschnell via OffscreenCanvas.
 */
async function getFastDataUrl(url) {
	try {
		const absoluteUrl = new URL(url, document.baseURI).href;
		const blob = await getBlobFromCacheOrNetwork(absoluteUrl);

		const isFont = blob.type.includes('font') || url.match(/\.(woff2?|ttf|otf|eot)$/i);
		const isSvg = blob.type.includes('svg') || url.endsWith('.svg');

		// --- SPEED-PFAD 1: Kleinkram & Assets ---
		// Erhöht auf 200KB: Das deckt fast alle Icons und Thumbnails ab.
		// Diese werden "instant" ohne CPU-Last eingebettet.
		if (isFont || isSvg || blob.size < 200000) {
			return await blobToBase64(blob);
		}

		// --- SPEED-PFAD 2: Große Bilder komprimieren ---
		const isImage = blob.type.startsWith('image/');
		if (!isImage) return await blobToBase64(blob);

		try {
			// createImageBitmap nutzt Hardware-Beschleunigung
			const bitmap = await createImageBitmap(blob);

			// Shared Canvas initialisieren oder anpassen
			if (!sharedCanvas) {
				sharedCanvas = document.createElement('canvas');
				sharedCtx = sharedCanvas.getContext('2d', { alpha: true });
			}
			sharedCanvas.width = bitmap.width;
			sharedCanvas.height = bitmap.height;

			// Blitzschnelles Zeichnen
			sharedCtx.clearRect(0, 0, bitmap.width, bitmap.height);
			sharedCtx.drawImage(bitmap, 0, 0);
			bitmap.close();

			// toDataURL('image/webp') ist extrem optimiert in Chromium/Safari.
			// Wir nutzen Qualität 0.6 für den perfekten Mix aus Speed/Größe.
			const dataUrl = sharedCanvas.toDataURL('image/webp', 0.6);

			return dataUrl;
		} catch (err) {
			return await blobToBase64(blob); // Sicherer Fallback
		}
	} catch (e) {
		return "data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7";
	}
}

/**
 * Erweitert: Behandelt jetzt auch @import Statements rekursiv, 
 * um sicherzustellen, dass Variablen und Base-Styles korrekt geladen werden.
 */
async function embedCssResources(cssText, baseUrl) {
    // 1. Imports behandeln (Rekursiv inlinen)
    // Findet @import "..." oder @import url("...")
    const importRegex = /@import\s+(?:url\()?['"]?([^'"]+)['"]?\)?(?:[^;]*);/g;
    
    // Wir müssen eine Schleife nutzen, da wir asynchrone Ersetzungen brauchen.
    // matchAll gibt uns einen Iterator.
    const imports = [...cssText.matchAll(importRegex)];
    
    for (const match of imports) {
        const fullMatch = match[0]; // z.B. @import './themes/base.css';
        const relativeUrl = match[1]; // z.B. ./themes/base.css
        
        try {
            const absoluteUrl = new URL(relativeUrl, baseUrl).href;
            let importedCss = await getFromCacheOrNetwork(absoluteUrl);
            
            // WICHTIG: Rekursion! Auch im importierten CSS müssen Ressourcen relativ zu DESSEN Pfad aufgelöst werden.
            importedCss = await embedCssResources(importedCss, absoluteUrl);
            
            // Das @import Statement durch den Inhalt ersetzen
            cssText = cssText.replace(fullMatch, importedCss);
        } catch (e) {
            console.warn(`[Bundler] Failed to inline import: ${relativeUrl}`, e);
            // Defekten Import auskommentieren, damit er offline keine Fehler wirft
            cssText = cssText.replace(fullMatch, `/* Import failed: ${relativeUrl} */`);
        }
    }

    // 2. URL Ressourcen (Bilder, Fonts) behandeln (wie bisher)
	const urlRegex = /url\((?!['"]?data:)(['"]?)(.*?)\1\)/g;
	const matches = [...cssText.matchAll(urlRegex)];
	const replacements = await Promise.all(matches.map(async (match) => {
		const originalUrl = match[2];
		const cleanUrl = originalUrl.split('?')[0].split('#')[0];
		try { 
			const dataUrl = await resourceToDataURL(new URL(cleanUrl, baseUrl).href);
			return { from: match[0], to: `url("${dataUrl}")` }; 
		}
		catch (error) { return { from: match[0], to: match[0] }; }
	}));
	
	for (const r of replacements) cssText = cssText.replace(r.from, r.to);
	return cssText;
}

function imageElementToDataURL(imgElement) {
	if (!imgElement || !imgElement.complete || imgElement.naturalWidth === 0) {
		return "data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7";
	}
	const canvas = document.createElement('canvas');
	canvas.width = imgElement.naturalWidth;
	canvas.height = imgElement.naturalHeight;
	const ctx = canvas.getContext('2d');
	ctx.drawImage(imgElement, 0, 0);
	return canvas.toDataURL('image/webp');
}

function blobToDataURL(blob) {
	return new Promise((resolve, reject) => {
		const reader = new FileReader();
		reader.onloadend = () => resolve(reader.result);
		reader.onerror = reject;
		reader.readAsDataURL(blob);
	});
}

/**
 * Nutzt die Performance API, um Skripte zu finden.
 * Kapselt jedes Skript in eine IIFE (Scope-Schutz) und macht Exports global verfügbar.
 */
async function embedActiveScripts(clonedDoc) {
    console.log("[Bundler] Scanning Browser Performance Log for scripts...");
    
    const resources = performance.getEntriesByType("resource");
    
    // 1. Relevante Skripte filtern
    let scriptEntries = resources.filter(r => {
        const name = r.name;
        if (!name.startsWith(window.location.origin)) return false;
        if (name.includes('sw.js')) return false;
        const isJsFile = name.split('?')[0].endsWith('.js');
        if ((name.includes('/api/') || name.includes('/query/')) && !isJsFile) {
            return false;
        }
        return isJsFile || r.initiatorType === 'script';
    });

    const uniqueUrls = [...new Set(scriptEntries.map(r => r.name))];
    
    uniqueUrls.sort((a, b) => {
        const score = (url) => {
            if (url.includes('vendor') || url.includes('tippy') || url.includes('katex')) return 0;
            if (url.includes('core/') || url.includes('base/')) return 1;
            if (url.includes('viz/')) return 2; 
            if (url.includes('features/')) return 3;
            if (url.includes('main.js') || url.includes('index.js')) return 10;
            return 5; 
        };
        return score(a) - score(b);
    });

    console.log(`[Bundler] Found ${uniqueUrls.length} active scripts. Processing...`);
    
    const cloneBody = clonedDoc.body;

    for (const url of uniqueUrls) {
        try {
            const response = await fetch(url);
            if (!response.ok) throw new Error(`Status ${response.status}`);
            
            let jsContent = await response.text();

            jsContent = jsContent.replace(/^\s*import .*$/gm, '');
            jsContent = jsContent.replace(/export\s+(async\s+)?(function|class)\s+([a-zA-Z0-9_$]+)/g, 'window.$3 = $1$2 $3');
            jsContent = jsContent.replace(/export\s+(const|let|var)\s+([a-zA-Z0-9_$]+)\s*=/g, 'window.$2 =');
            jsContent = jsContent.replace(/^\s*export\s*\{([\s\S]*?)\};?/gm, (match, content) => {
                return content.split(',').map(item => {
                    let [local, exported] = item.trim().split(/\s+as\s+/);
                    exported = exported || local;
                    return `try { window.${exported} = ${local}; } catch(e) {}`;
                }).join('\n');
            });
            jsContent = jsContent.replace(/^\s*export\s+default\s+/gm, 'window._default_export_ = ');
            jsContent = jsContent.replace(/^\s*import\s+['"].*\.css['"];?/gm, '');

            const wrappedContent = `
            /* Bundled from: ${url} */
            (function() {
                try {
                    ${jsContent}
                } catch (err) {
                    console.error("Bundled Script Error in ${url.split('/').pop()}:", err);
                }
            })();
            `;

            const scriptTag = clonedDoc.createElement('script');
            scriptTag.removeAttribute('type'); 
            scriptTag.textContent = wrappedContent;
            
            cloneBody.appendChild(scriptTag);

        } catch (e) {
            console.warn(`[Bundler] Failed to bundle ${url}:`, e);
            cloneBody.appendChild(clonedDoc.createComment(` FAILED TO BUNDLE: ${url} `));
        }
    }
}

/**
 * Erstellt die HTML-Datei.
 */
async function createSelfContainedHTML() {
    console.log("[Bundler] Starting HTML generation (Sequential Mode)...");
    
    const clonedDocument = document.cloneNode(true);
    clonedDocument.documentElement.setAttribute('data-is-offline', 'true');
    
    const currentTheme = document.documentElement.getAttribute('data-theme');
    if (currentTheme) clonedDocument.documentElement.setAttribute('data-theme', currentTheme);
	const currentLang = document.documentElement.lang;
	if (currentLang) clonedDocument.documentElement.setAttribute('lang', currentLang);

	const popupsToRemove = [
		'.download-popup-overlay',
		'#download-popup-overlay',
		'.recorder-overlay',
		'.query-popup-modal',
		'.confirmation-dialog-overlay',
		'#tts-floating-player', 
		'.custom-translate-cursor'
	];
    popupsToRemove.forEach(selector => clonedDocument.querySelectorAll(selector).forEach(el => el.remove()));

    const menuElements = ['#menu-trigger', '#sidebar-menu', '#menu-overlay', '.sidebar-footer'];
    menuElements.forEach(selector => clonedDocument.querySelectorAll(selector).forEach(el => el.remove()));

	const highlightClasses = ['is-reading', 'is-paused', 'is-translating', 'translate-highlight'];
	highlightClasses.forEach(cls => {
		clonedDocument.querySelectorAll(`.${cls}`).forEach(el => el.classList.remove(cls));
	});
	clonedDocument.body.classList.remove('translate-mode-active');
	clonedDocument.body.classList.remove('translate-mode-active');
	
	clonedDocument.body.classList.remove('is-loading', 'is-swiping-active', 'no-scroll', 'modal-open', 'is-header-hidden');
    clonedDocument.body.style.overflow = '';
    clonedDocument.body.style.paddingRight = ''; 
    clonedDocument.documentElement.style.overflow = '';

    clonedDocument.querySelectorAll('script').forEach(el => el.remove());
    clonedDocument.querySelectorAll('[inert]').forEach(el => el.removeAttribute('inert'));

    const attrsToRemove = ['data-has-listener', 'data-listener-attached', 'data-loader-initialized'];
    attrsToRemove.forEach(attr => clonedDocument.querySelectorAll(`[${attr}]`).forEach(el => {
        delete el.dataset[attr.replace('data-', '')];
        el.removeAttribute(attr);
    }));

	const safetyScript = clonedDocument.createElement('script');
	safetyScript.textContent = `
	    if(navigator.serviceWorker) { navigator.serviceWorker.register = function() { return Promise.reject("SW disabled offline"); }; }
	    window.onerror = function(msg, url, line) { console.error("OFFLINE ERROR:", msg, url, line); };
	    window.tippy = window.tippy || function() { return []; };
	    try {
	        var bakedTheme = document.documentElement.getAttribute('data-theme');
	        if (bakedTheme && window.localStorage) {
	            window.localStorage.setItem('ideenatlas-theme', bakedTheme);
	        }
	    } catch(e) {}
	`;
	clonedDocument.head.prepend(safetyScript);
	
	const cspMeta = clonedDocument.createElement('meta');
	cspMeta.httpEquiv = "Content-Security-Policy";
	// Erlaubt nur inline-styles, inline-scripts, keine externen Requests
	cspMeta.content = "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; img-src data: blob:; font-src data:;";
	clonedDocument.head.prepend(cspMeta);

    const restoreVisibility = temporarilyRevealAll(clonedDocument);

    // =================================================================
    // CSS EMBEDDING (SEQUENTIAL FIX)
    // Wir iterieren sequenziell, um die Reihenfolge der Stylesheets im DOM 1:1 zu wahren.
    // Promise.all kann bei parallelen Requests die Verarbeitungsreihenfolge durcheinanderbringen.
    // =================================================================
    console.log("[Bundler] Embedding CSS sequentially...");
    
    // Wir holen uns eine statische Liste aller Links
    const cssLinks = Array.from(clonedDocument.querySelectorAll('link[rel="stylesheet"]'));
    
    for (const link of cssLinks) {
        try {
            console.debug(`[Bundler] Processing CSS: ${link.href}`);
            const cssText = await getFromCacheOrNetwork(link.href);
            // Das neue embedCssResources kümmert sich um @import Flattening
            const embeddedCss = await embedCssResources(cssText, link.href);
            
            const styleTag = clonedDocument.createElement('style');
            styleTag.textContent = embeddedCss;
            
            // replaceChild behält die exakte Position im DOM bei
            if (link.parentNode) {
                link.parentNode.replaceChild(styleTag, link);
            }
        } catch (error) {
            console.warn("[Bundler] CSS failed:", link.href, error);
            // Link entfernen oder Kommentar einfügen, damit kein kaputter Link bleibt
			const comment = clonedDocument.createComment(` CSS LOAD FAILED: ${link.href} `);
			if (link.parentNode) link.parentNode.replaceChild(comment, link);
		}
	}

	// Image Embedding
	console.log("[Bundler] Embedding Images...");

	const allImages = Array.from(clonedDocument.querySelectorAll('img[src]'));

	// Wir starten ALLE Konvertierungen gleichzeitig. 
	// Der Browser verwaltet die CPU-Last selbst am effizientesten.
	await Promise.all(allImages.map(async (img) => {
		const src = img.getAttribute('src');
		if (!src || src.startsWith('data:')) return;

		// getFastDataUrl entscheidet nun selbst: Direkt oder Komprimiert
		img.src = await getFastDataUrl(src);
		img.removeAttribute('srcset');
	}));

	restoreVisibility();

	if (getI18nData()) {
		const i18nScript = clonedDocument.createElement('script');
		i18nScript.textContent = `window.OFFLINE_I18N_DATA = ${JSON.stringify(getI18nData())};`;
		clonedDocument.head.appendChild(i18nScript);
	}

	const context = getContext();
	const newDataScript = clonedDocument.createElement('script');
	newDataScript.id = 'initial-data-script';
	newDataScript.textContent = `window.INITIAL_DATA = ${JSON.stringify(context)}; for (const key in window.INITIAL_DATA) { window[key] = window.INITIAL_DATA[key]; }`;
	clonedDocument.head.appendChild(newDataScript);

	const resultsJsonData = await generateResultsJSON();

	const resultsIsland = clonedDocument.createElement('script');
	resultsIsland.id = 'results-data-island';
	resultsIsland.type = 'application/json';
	resultsIsland.textContent = JSON.stringify(resultsJsonData);
	clonedDocument.body.appendChild(resultsIsland);

	if (offlineLoaderCache && Object.keys(offlineLoaderCache).length > 0) {
		const cacheIsland = clonedDocument.createElement('script');
		cacheIsland.id = 'prefetched-data-cache'; 
		cacheIsland.type = 'application/json';
		cacheIsland.textContent = JSON.stringify(offlineLoaderCache);
		clonedDocument.body.appendChild(cacheIsland);
	}

	await embedActiveScripts(clonedDocument);

    const buttonInClone = clonedDocument.getElementById('download-results-btn');
    if (buttonInClone) {
        buttonInClone.querySelector('span').textContent = 'Download JSON';
        buttonInClone.querySelector('i').className = 'fa-solid fa-file-arrow-down';
        buttonInClone.setAttribute('data-is-offline-download', 'true');
		buttonInClone.disabled = false;
		buttonInClone.classList.remove('loading');
	}

	const fragment = getTemplate('tpl-offline-archive-banner');
	if (fragment) {
		// Wir müssen hier `importNode` verwenden, weil wir von einem anderen
		// Dokument (dem aktiven) in unser geklontes Dokument importieren.
		const bannerNode = clonedDocument.importNode(fragment, true);
		clonedDocument.body.appendChild(bannerNode);
	}

	// Das Skript zum Schließen des Banners bleibt
	const closeScript = clonedDocument.createElement('script');
	closeScript.textContent = `
		        document.getElementById('offline-banner-close')?.addEventListener('click', function() { 
		            document.getElementById('offline-archive-banner')?.remove(); 
		        });
		    `;
	clonedDocument.body.appendChild(closeScript);

	console.log(" HTML generation complete.");
	return `<!DOCTYPE html>${clonedDocument.documentElement.outerHTML}`;
}

function triggerDownload(blob, fileName) {
	const url = URL.createObjectURL(blob);
	const a = document.createElement('a');
	a.href = url;
	a.download = fileName;
	document.body.appendChild(a);
	a.click();
	document.body.removeChild(a);
	setTimeout(() => URL.revokeObjectURL(url), 200);
}

function mapResultToJson(result) {
	const payload = result.payload || {};
	let originalPayloadData = {};
	try { if (payload.prettyJson) originalPayloadData = JSON.parse(payload.prettyJson); } catch (e) {}
	return {
		score: result.score, source: payload.type || 'Unknown', contentUrl: result.contentUrl || '#',
		title: payload.title || 'No Title Available', abstract: payload.abstract || 'No Abstract Available',
		clusterHierarchy: payload.namedClusterHierarchy || [], originalPayload: originalPayloadData
	};
}

async function generateResultsJSON() {
	offlineLoaderCache = {};
	const context = getContext();
	const jsonData = {
		summary: { inputTitle: "", inputIdea: "", mainTopics: [], similarTopicFields: [], serendipitousConnections: [] },
		ownIdeaAnalysis: { clusterHierarchy: [] },
		similarTopicFields: [], serendipitousConnections: [], detailedSimilarResults: []
	};

	const fetchClusterDetails = async (tabSelector, contextCardIdPrefix) => {
		const clustersFromDOM = [];
		document.querySelectorAll(tabSelector).forEach(tab => {
			const clusterId = tab.dataset.clusterId;
			const cardId = `#${contextCardIdPrefix}-${clusterId}`;
			clustersFromDOM.push({
				clusterId: clusterId,
				name: tab.querySelector('.tab-title')?.textContent || 'Unknown',
				score: parseFloat((tab.querySelector('.tab-score')?.textContent || '0').replace(',', '.')) || 0,
				summary: document.querySelector(`${cardId} .result-summary p`)?.textContent.trim() || ''
			});
		});

		if (clustersFromDOM.length > 0 && context.queryVector) {
			const topicFieldPromises = clustersFromDOM.map(async (topic) => {
				let detailedResults = [];
				try {
					const response = await fetch('/query/filtered-results', {
						method: 'POST',
						headers: { 'Content-Type': 'application/json', 'X-XSRF-TOKEN': getCsrfToken() },
						body: JSON.stringify({ queryVector: JSON.parse(context.queryVector), clusterId: topic.clusterId })
					});
					if (response.ok) {
						const filteredData = await response.json();
						offlineLoaderCache[topic.clusterId] = filteredData; 
						if (filteredData.pointsData) detailedResults = filteredData.pointsData.map(mapResultToJson);
					}
				} catch (error) {}
				return { clusterId: topic.clusterId, clusterName: topic.name, relevanceScore: topic.score, summary: topic.summary, results: detailedResults };
			});
			return await Promise.all(topicFieldPromises);
		}
		return [];
	};

	const ideaElement = document.querySelector('#own-viz-content .expandable-abstract');
	if (ideaElement) jsonData.summary.inputIdea = ideaElement.textContent.trim();
	if (context.jobTitle) jsonData.summary.inputTitle = context.jobTitle;

	document.querySelectorAll('#own-viz-content > .hierarchy-container .hierarchy-item-box').forEach(el => {
		const nameEl = el.querySelector('.hierarchy-item-name');
		if (nameEl) {
			const confidenceText = el.querySelector('.hierarchy-item-confidence')?.textContent || '';
			const scoreValue = parseFloat(confidenceText.replace(/[()Confidence:\s]/g, '')) || 0.0;
			jsonData.ownIdeaAnalysis.clusterHierarchy.push({ id: el.dataset.clusterId, name: nameEl.textContent, score: scoreValue });
			jsonData.summary.mainTopics.push(nameEl.textContent);
		}
	});

	jsonData.similarTopicFields = await fetchClusterDetails('#neighbor-viz-content .topic-tab', 'context-card');
	jsonData.serendipitousConnections = await fetchClusterDetails('#serendipity-viz-content .topic-tab', 'context-card-serendipity');

	jsonData.summary.similarTopicFields = jsonData.similarTopicFields.map(c => ({ name: c.clusterName, description: c.summary, relevance: c.relevanceScore }));
	jsonData.summary.serendipitousConnections = jsonData.serendipitousConnections.map(c => ({ name: c.clusterName, description: c.summary, relevance: c.relevanceScore }));

	if (context.ownResults && Array.isArray(context.ownResults)) {
		jsonData.detailedSimilarResults = context.ownResults.map(mapResultToJson);
	}
	return jsonData;
}

let activePopup = null;

const onlineClickListener = () => showDownloadPopup();

const offlineClickListener = async (event) => {
	event.preventDefault(); event.stopPropagation();
	const button = event.currentTarget;
	const icon = button.querySelector('i');
	const originalIconClass = icon.className;
	button.disabled = true;
	icon.className = 'fa-solid fa-spinner fa-spin';
	try {
		const dataText = document.getElementById('results-data-island').textContent;
		const blob = new Blob([dataText], { type: 'application/json' });
		const sanitizedTitle = sanitizeForFilename(getJobTitle());
		triggerDownload(blob, `${sanitizedTitle}-ideenatlas.eu.json`);
	} catch (e) { alert('Could not download JSON data.'); } finally {
		button.disabled = false; icon.className = originalIconClass;
	}
};

// Hilfsfunktion zum Sperren/Freigeben des Hintergrunds
function setBackgroundScroll(locked) {
    const contentSelectors = ['.idea-form', '.results-container', '.legal-content-wrapper', '.no-results-message'];
    contentSelectors.forEach(selector => {
        const el = document.querySelector(selector);
        if (el) el.inert = locked; // Verhindert Klicks/Tabbing im Hintergrund
    });

    if (locked) {
        document.body.style.overflow = 'hidden';
        document.documentElement.style.overflow = 'hidden';
        document.body.style.touchAction = 'none';
    } else {
        document.body.style.overflow = '';
        document.documentElement.style.overflow = '';
        document.body.style.touchAction = '';
	}
}

function showDownloadPopup() {
	// Schritt 1: Hole das Popup. Wenn es nicht im DOM ist, erstelle es.
	activePopup = renderTemplate('download-popup-overlay');
	if (!activePopup) return;

	// Schritt 2: Binde die Events, aber nur, wenn sie noch nicht gebunden wurden.
	if (activePopup.dataset.eventsAttached !== 'true') {
		activePopup.querySelector('.download-popup-close-btn').addEventListener('click', hideDownloadPopup);
		activePopup.addEventListener('click', (e) => { if (e.target === activePopup) hideDownloadPopup(); });

		activePopup.querySelector('.download-html-choice-btn').addEventListener('click', () => handleDownloadChoice('html'));
		activePopup.querySelector('.download-json-choice-btn').addEventListener('click', () => handleDownloadChoice('json'));

		activePopup.dataset.eventsAttached = 'true';
	}

	// Schritt 3: Zeige das Popup an (unverändert).
	setBackgroundScroll(true);
	setTimeout(() => {
		if (activePopup) activePopup.classList.add('is-visible');
	}, 10);
}

function hideDownloadPopup() {
	if (!activePopup) return;
	activePopup.classList.remove('is-visible');

	setBackgroundScroll(false);

	setTimeout(() => { if (activePopup) { activePopup.remove(); activePopup = null; } }, 300);
}

async function handleDownloadChoice(format) {
	const buttonId = `download-${format}-choice-btn`;
	const button = document.getElementById(buttonId);
	if (!button) return;
	const icon = button.querySelector('i');
	const originalIconClass = icon.className;
	button.disabled = true;
	icon.className = 'fa-solid fa-spinner fa-spin';
	try {
		const sanitizedTitle = sanitizeForFilename(getJobTitle());
		const fileName = `${sanitizedTitle}-ideenatlas.eu.${format}`;
		if (format === 'html') {
			const pageHtml = await createSelfContainedHTML();
			const htmlBlob = new Blob([pageHtml], { type: 'text/html' });
			triggerDownload(htmlBlob, fileName);
		} else if (format === 'json') {
			const resultsJson = await generateResultsJSON();
			const jsonBlob = new Blob([JSON.stringify(resultsJson)], { type: 'application/json' });
			triggerDownload(jsonBlob, fileName);
		}
	} catch (error) {
		console.error(error);
		alert("Export failed: " + error.message);
	} finally {
		button.disabled = false; 
		icon.className = originalIconClass; 
		hideDownloadPopup();
	}
}

export function initializeDownloadButton() {
	const mainDownloadButton = document.getElementById('download-results-btn');
	if (!mainDownloadButton) return;
    mainDownloadButton.removeEventListener('click', offlineClickListener);
    mainDownloadButton.removeEventListener('click', onlineClickListener);
	if (mainDownloadButton.hasAttribute('data-is-offline-download')) {
		mainDownloadButton.addEventListener('click', offlineClickListener);
	} else {
		mainDownloadButton.addEventListener('click', onlineClickListener);
	}
}