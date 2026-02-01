import { getContext, getJobTitle } from '/script/core/context.js';
import { getI18nData } from '/script/core/localization.js'; 
import { getCsrfToken } from '/script/core/security.js';

function sanitizeForFilename(title) {
	if (!title || title.trim() === '') return 'untitled-analysis';
	return title
		.trim()
		.toLowerCase()
		.replace(/\s+/g, '-') 
		.replace(/[^\w-]/g, '') 
		.substring(0, 50); 
}

function temporarilyRevealAll(doc) {
	const hiddenPanes = doc.querySelectorAll('.viz-content-pane');
	const originalDisplays = [];

	hiddenPanes.forEach(pane => {
		originalDisplays.push({ el: pane, display: pane.style.display });
		pane.style.display = 'block'; 
	});

	return () => {
		originalDisplays.forEach(item => {
			item.el.style.display = item.display;
		});
	};
}

async function resourceToDataURL(url) {
	const absoluteUrl = new URL(url, document.baseURI).href;
	try {
		const response = await fetch(absoluteUrl, { cache: 'default' });
		if (!response.ok) {
			const freshResponse = await fetch(absoluteUrl, { cache: 'no-store' });
			if (!freshResponse.ok) throw new Error(`Network response was not ok`);
			return await blobToDataURL(await freshResponse.blob());
		}
		return await blobToDataURL(await response.blob());
	} catch (error) {
		return "data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7";
	}
}

async function embedCssResources(cssText, baseUrl) {
	const urlRegex = /url\((?!['"]?data:)(['"]?)(.*?)\1\)/g;
	const matches = [...cssText.matchAll(urlRegex)];
	const replacements = await Promise.all(matches.map(async (match) => {
		const originalUrl = match[2];
		try { return { from: match[0], to: `url("${await resourceToDataURL(new URL(originalUrl, baseUrl).href)}")` }; }
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

async function createSelfContainedHTML() {
	const clonedDocument = document.cloneNode(true);
	clonedDocument.documentElement.setAttribute('data-is-offline', 'true');
	
	// 2. Theme explizit übertragen
	const currentTheme = document.documentElement.getAttribute('data-theme');
	if (currentTheme) {
	    clonedDocument.documentElement.setAttribute('data-theme', currentTheme);
	}

	// 3. Sprache explizit übertragen
	const currentLang = document.documentElement.lang;
	if (currentLang) {
	    clonedDocument.documentElement.setAttribute('lang', currentLang);
	}

	
	/*const menuWrapper = clonedDocument.querySelector('.menu-wrapper');
	if (menuWrapper) menuWrapper.remove();*/

	// 1. Die originale Import-Map entfernen (wir bauen gleich eine neue)
	clonedDocument.querySelectorAll('script[type="importmap"]').forEach(el => el.remove());

	// 2. Das Inline-Script entfernen, das localization.js direkt importiert
	// (Das verursacht den CORS Fehler beim Start)
	clonedDocument.querySelectorAll('script[type="module"]').forEach(el => {
		if (el.textContent.includes('loadFromCacheInstant') || el.textContent.includes('/dist/localization.js')) {
			el.remove();
		}
	});

	// 3. Service Worker Registrierung entfernen (funktioniert offline eh nicht und wirft Fehler)
	clonedDocument.querySelectorAll('script').forEach(el => {
		if (el.textContent.includes('navigator.serviceWorker.register')) {
			el.remove();
		}
	});

	// 4. Bereinigung für visualizationToggle.js
	clonedDocument.querySelectorAll('[data-has-listener]').forEach(el => {
	    delete el.dataset.hasListener;
	    el.removeAttribute('data-has-listener');
	});

	// 5. Bereinigung für queryButtonManager.js (falls verwendet)
	clonedDocument.querySelectorAll('[data-listener-attached]').forEach(el => {
	    delete el.dataset.listenerAttached;
	    el.removeAttribute('data-listener-attached');
	});

	// 6. Optional: Wenn Buttons "active" sind, sollen sie das vielleicht bleiben, 
	// aber oft ist ein Reset auf den Standardzustand sicherer:
	// clonedDocument.querySelectorAll('.active').forEach(el => el.classList.remove('active'));
	
	const restoreVisibility = temporarilyRevealAll(clonedDocument);

	try {
		clonedDocument.querySelectorAll('.abstract-wrapper.expanded').forEach(wrapper => {
			wrapper.classList.remove('expanded');
		});
		clonedDocument.querySelectorAll('.hierarchy-list-wrapper').forEach(wrapper => {
			if (wrapper.querySelector('.toggle-hierarchy-btn')) {
				wrapper.classList.add('is-collapsed');
			}
		});
		clonedDocument.querySelectorAll('.result-payload.active').forEach(payload => {
			payload.classList.remove('active');
		});
		clonedDocument.querySelectorAll('.toggle-json-btn.active').forEach(button => {
			button.classList.remove('active');
		});
	} catch (error) {}

	await Promise.all(
		[...clonedDocument.querySelectorAll('link[rel="stylesheet"]')].map(async (link) => {
			try {
				const cssText = await fetch(link.href).then(res => res.text());
				const embeddedCss = await embedCssResources(cssText, link.href);
				const styleTag = clonedDocument.createElement('style');
				styleTag.textContent = embeddedCss;
				link.parentNode.replaceChild(styleTag, link);
			} catch (error) {}
		})
	);

	const originalImages = document.querySelectorAll('img');
	clonedDocument.querySelectorAll('img').forEach((clonedImg, index) => {
		const originalImg = originalImages[index];
		if (originalImg) {
			clonedImg.src = imageElementToDataURL(originalImg);
		}
	});

	restoreVisibility();

	// 1. I18N Dump
	if (getI18nData()) {
		const i18nScript = clonedDocument.createElement('script');
		i18nScript.textContent = `window.OFFLINE_I18N_DATA = ${JSON.stringify(getI18nData())};`;
		clonedDocument.head.prepend(i18nScript);
	}

	// 2. DATA DUMP 
	// Wir löschen das alte Script vom Server
	const oldScript = clonedDocument.getElementById('initial-data-script');
	if (oldScript) oldScript.remove();

	const context = getContext();

	// Neues Script erstellen
	const newDataScript = clonedDocument.createElement('script');
	newDataScript.id = 'initial-data-script';

	// WICHTIG: Daten auch auf window kopieren!
	newDataScript.textContent = `
	        window.INITIAL_DATA = ${JSON.stringify(context)};
	        // Globale Verfügbarkeit für Legacy-Skripte sicherstellen
	        for (const key in window.INITIAL_DATA) {
	            window[key] = window.INITIAL_DATA[key];
	        }
	    `;

	clonedDocument.head.prepend(newDataScript);

	const prefetchedDataCache = {};
	const clusterIds = [...document.querySelectorAll('.topic-tab')].map(tab => tab.dataset.clusterId);
	if (clusterIds.length > 0 && context.queryVector) { 
		await Promise.all(
			clusterIds.map(async (clusterId) => {
				try {
					const response = await fetch('/query/filtered-results', {
						method: 'POST',
						headers: { 'Content-Type': 'application/json', 'X-XSRF-TOKEN': getCsrfToken() },
						body: JSON.stringify({ queryVector: JSON.parse(context.queryVector), clusterId: clusterId })
					});
					if (response.ok) prefetchedDataCache[clusterId] = await response.json();
				} catch (e) {}
			})
		);
		const dataIsland = clonedDocument.createElement('script');
		dataIsland.id = 'prefetched-data-cache';
		dataIsland.type = 'application/json';
		dataIsland.textContent = JSON.stringify(prefetchedDataCache);
		clonedDocument.body.appendChild(dataIsland);
	}

	await Promise.all(
		[...clonedDocument.querySelectorAll('script[src]:not([type="module"])')].map(async (scriptTag) => {
			try {
				const code = await fetch(new URL(scriptTag.src, document.baseURI).href).then(res => res.text());
				const inlineScript = clonedDocument.createElement('script');
				inlineScript.textContent = code;
				scriptTag.parentNode.replaceChild(inlineScript, scriptTag);
			} catch (e) { }
		})
	);

	const entryPoints = [...document.querySelectorAll('script[src][type="module"]')].map(s => new URL(s.src, document.baseURI).pathname);
	if (entryPoints.length > 0) {
		const jsCodeCache = new Map();
		const importRegexes = [
			/(?<prefix>(?:import|export)[\s\S]*?from\s*)(?<quote>['"])(?<specifier>.+?)\k<quote>/g,
			/(?<prefix>import\s*)(?<quote>['"])(?<specifier>.+?)\k<quote>/g,
			/(?<prefix>import\s*\(\s*)(?<quote>['"])(?<specifier>.+?)\k<quote>/g
		];
		const toProcess = [...entryPoints];
		const processed = new Set();
		while (toProcess.length > 0) {
			const path = toProcess.shift();
			if (!path || processed.has(path)) continue;
			processed.add(path);
			try {
				const code = await fetch(path).then(res => res.text());
				jsCodeCache.set(path, code);
				for (const regex of importRegexes) {
					for (const match of code.matchAll(regex)) {
						const specifier = match.groups.specifier;
						if (specifier.startsWith('.') || specifier.startsWith('/')) {
							toProcess.push(new URL(specifier, new URL(path, document.baseURI)).pathname);
						}
					}
				}
			} catch (e) { }
		}

		const importMap = { imports: {} };
		for (const [path, code] of jsCodeCache.entries()) {
			let rewrittenCode = code;
			const replacer = (match, ...args) => {
				const groups = args.pop();
				const absolutePath = new URL(groups.specifier, new URL(path, document.baseURI)).pathname;
				const bareSpecifier = absolutePath.substring(absolutePath.lastIndexOf('/') + 1);
				return match.replace(groups.specifier, bareSpecifier);
			};
			importRegexes.forEach(regex => { rewrittenCode = rewrittenCode.replace(regex, replacer); });
			const bareSpecifier = path.substring(path.lastIndexOf('/') + 1);
			importMap.imports[bareSpecifier] = `data:application/javascript,${encodeURIComponent(rewrittenCode)}`;
		}

		clonedDocument.querySelectorAll('script[src]').forEach(s => s.remove());
		const importMapScript = clonedDocument.createElement('script');
		importMapScript.type = 'importmap';
		importMapScript.textContent = JSON.stringify(importMap, null, 2);
		clonedDocument.head.prepend(importMapScript);

		for (const path of entryPoints) {
			const entryScript = clonedDocument.createElement('script');
			entryScript.type = 'module';
			entryScript.textContent = `import '${path.substring(path.lastIndexOf('/') + 1)}';`;
			clonedDocument.body.appendChild(entryScript);
		}
	}

	clonedDocument.querySelector('.download-popup-overlay')?.remove();

	const resultsJsonData = await generateResultsJSON();
	const dataIsland = clonedDocument.createElement('script');
	dataIsland.id = 'results-data-island';
	dataIsland.type = 'application/json';
	dataIsland.textContent = JSON.stringify(resultsJsonData);
	clonedDocument.body.appendChild(dataIsland);

    // Button im Klon manipulieren, nicht das konstante Element
	const buttonInClone = clonedDocument.getElementById('download-results-btn');
	if (buttonInClone) {
		buttonInClone.querySelector('span').textContent = t('results.download.json_btn'); 
		buttonInClone.setAttribute('data-tippy-content', t('results.download.json_tooltip'));
		buttonInClone.querySelector('i').className = 'fa-solid fa-file-arrow-down';
		buttonInClone.setAttribute('data-is-offline-download', 'true');
	}

	let bannerText = 'This is a self-contained, archived version.';
	try {
		if (window.I18N_DATA && window.I18N_DATA.download && window.I18N_DATA.download.offline_banner) {
			bannerText = window.I18N_DATA.download.offline_banner;
		}
	} catch (e) {
		console.warn("Konnte Banner-Text nicht aus I18N_DATA lesen, nutze Fallback.", e);
	}
	const notice = clonedDocument.createElement('div');
	notice.textContent = bannerText;
	notice.style.cssText = 'position:fixed; bottom:0; left:0; width:100%; background:#333; color:#eee; text-align:center; padding:5px; font-size:12px; z-index:9999;';
	clonedDocument.body.appendChild(notice);

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
	try {
		if (payload.prettyJson) {
			originalPayloadData = JSON.parse(payload.prettyJson);
		}
	} catch (e) {}
	return {
		score: result.score,
		source: payload.type || 'Unknown',
		contentUrl: result.contentUrl || '#',
		title: payload.title || 'No Title Available',
		abstract: payload.abstract || 'No Abstract Available',
		clusterHierarchy: payload.namedClusterHierarchy || [],
		originalPayload: originalPayloadData
	};
}

async function generateResultsJSON() {
	const context = getContext();
	const jsonData = {
		summary: { inputTitle: "", inputIdea: "", mainTopics: [], similarTopicFields: [], serendipitousConnections: [] },
		ownIdeaAnalysis: { clusterHierarchy: [] },
		similarTopicFields: [],
		serendipitousConnections: [],
		detailedSimilarResults: []
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
						if (filteredData.pointsData) {
							detailedResults = filteredData.pointsData.map(mapResultToJson);
						}
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
			const hierarchyItem = { id: el.dataset.clusterId, name: nameEl.textContent, score: scoreValue };
			jsonData.ownIdeaAnalysis.clusterHierarchy.push(hierarchyItem);
			jsonData.summary.mainTopics.push(hierarchyItem.name);
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
	event.preventDefault();
	event.stopPropagation();
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
	} catch (e) {
		alert('Could not download JSON data.');
	} finally {
		button.disabled = false;
		icon.className = originalIconClass;
	}
};

function createDownloadPopup() {
	if (document.getElementById('download-popup-overlay')) return;
	const overlay = document.createElement('div');
	overlay.id = 'download-popup-overlay';
	overlay.className = 'download-popup-overlay';
	overlay.innerHTML = `
        <div class="download-popup-modal">
            <div class="download-popup-header">
                <h2>Download Options</h2>
                <button class="download-popup-close-btn">&times;</button>
            </div>
            <div class="download-popup-body">
                <button id="download-html-choice-btn" class="download-choice-btn">
                    <i class="fa-solid fa-file-code"></i> <span>Download Interactive HTML</span>
                </button>
                <button id="download-json-choice-btn" class="download-choice-btn">
                    <i class="fa-solid fa-file-arrow-down"></i> <span>Download Results as JSON</span>
                </button>
            </div>
        </div>
    `;
	document.body.appendChild(overlay);
	activePopup = overlay;
	overlay.querySelector('.download-popup-close-btn').addEventListener('click', hideDownloadPopup);
	overlay.addEventListener('click', (e) => { if (e.target === overlay) hideDownloadPopup(); });
	document.getElementById('download-html-choice-btn').addEventListener('click', () => handleDownloadChoice('html'));
	document.getElementById('download-json-choice-btn').addEventListener('click', () => handleDownloadChoice('json'));
}

function showDownloadPopup() {
	if (!activePopup) createDownloadPopup();
	setTimeout(() => activePopup.classList.add('is-visible'), 10);
}

function hideDownloadPopup() {
	if (!activePopup) return;
	activePopup.classList.remove('is-visible');
	setTimeout(() => {
		if (activePopup) {
			activePopup.remove();
			activePopup = null;
		}
	}, 300);
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
		console.log(error)
		const errorMsg = t('results.download.error_download_format').replace('{format}', format);
		alert(errorMsg);
	} finally {
		button.disabled = false;
		icon.className = originalIconClass;
		hideDownloadPopup();
	}
}

/**
 * Initialisiert den Haupt-Download-Button. 
 * FIX: Holt sich das Element jetzt immer frisch aus dem DOM.
 */
export function initializeDownloadButton() {
    // Hier die Referenz frisch holen!
	const mainDownloadButton = document.getElementById('download-results-btn');
	if (!mainDownloadButton) return;

    // Alte Listener entfernen (wichtig bei SPA Navigation)
    mainDownloadButton.removeEventListener('click', offlineClickListener);
    mainDownloadButton.removeEventListener('click', onlineClickListener);

	if (mainDownloadButton.hasAttribute('data-is-offline-download')) {
		mainDownloadButton.addEventListener('click', offlineClickListener);
	} else {
		mainDownloadButton.addEventListener('click', onlineClickListener);
	}
}