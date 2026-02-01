import { getCsrfToken } from '/script/core/security.js';
import { t } from '/script/core/localization.js';

// --- STATE ---
let audioContext = null;
let activeSources = [];
let nextStartTime = 0;
let firstChunkStartTime = -1;
let abortController = null;

let isDownloading = false;
let isPaused = false;

let currentCallbacks = {};
let progressFrameId = null;
let stopTimeoutId = null;
let pauseTimeoutId = null;

let masterGainNode = null;

let currentContext = {
	originUrl: null,
	textSnippet: null,
	viewId: null,
	clusterId: null
};

// UI State Tracker
let lastUiState = 'hidden';

// UI Referenzen
let uiContainer = null;
let uiStatusText = null;
let uiPlayBtn = null;
let uiProgressBar = null;


// Initialisierung
let isTtsInitialized = false;


export function initTTS() {
	if (isTtsInitialized) return;

	window.addEventListener('tts-navigation-finished', () => {
		// 1. Versuch: Sofort (falls es eine statische Seite ist oder DOM schon da ist)
		restoreStateAndScroll();
		// 2. Versuch: Warten auf das explizite 'page-rendered' Event (für Impressum/Legal Pages)
		// Wir nutzen { once: true }, damit der Listener sich selbst aufräumt.
		document.addEventListener('page-rendered', () => {
			// Wir prüfen in restoreStateAndScroll ohnehin, ob wir auf der richtigen URL sind.
			restoreStateAndScroll();
		}, { once: true });
    });
    
    // Falls noch andere globale Listener da sind, hier rein.
    isTtsInitialized = true;
}

// --- DOM HELPER ---
function ensurePlayerUI() {
	let el = document.getElementById('tts-floating-player');
	if (!el) {
		const div = document.createElement('div');
		div.id = 'tts-floating-player';
		div.classList.add('tts-floating-player-container');
		div.innerHTML = `
            <div class="tts-top-row">
                <div class="tts-info-wrapper">
                    <div class="tts-status-text" id="tts-status-text">Initializing...</div>
                </div>
                <div class="tts-controls">
                    <button class="tts-btn play-pause-btn" id="tts-play-pause"><i class="fa-solid fa-pause"></i></button>
                    <button class="tts-btn" id="tts-stop"><i class="fa-solid fa-xmark"></i></button>
                </div>
            </div>
            <div class="tts-progress-container" id="tts-progress-bg">
                <div class="tts-progress-bar" id="tts-progress-bar"></div>
            </div>
        `;
		document.body.appendChild(div);
		el = div;
	}
	uiContainer = el;
	uiStatusText = el.querySelector('#tts-status-text');
	uiPlayBtn = el.querySelector('#tts-play-pause');
	uiProgressBar = el.querySelector('#tts-progress-bar');
	const stopBtn = el.querySelector('#tts-stop');

	if (uiPlayBtn) uiPlayBtn.onclick = (e) => { e.stopPropagation(); togglePlayPause(); };
	if (stopBtn) stopBtn.onclick = (e) => { e.stopPropagation(); stop(); };

	el.onclick = (e) => {
		if (e.target.closest('button')) return;
		handlePanicClick();
	};
}

// --- VISUALIZATION LOOP ---
function startProgressLoop() {
	if (progressFrameId) cancelAnimationFrame(progressFrameId);

	const update = () => {
		if (!uiProgressBar) { progressFrameId = requestAnimationFrame(update); return; }

		if (!isDownloading && activeSources.length === 0 && firstChunkStartTime !== -1 && !isPaused) {
			if (audioContext && audioContext.currentTime >= nextStartTime - 0.2) {
				uiProgressBar.style.width = '100%'; uiProgressBar.classList.add('is-finished'); return;
			}
		}

		if (!audioContext || firstChunkStartTime === -1 || isPaused) { progressFrameId = requestAnimationFrame(update); return; }

		const now = audioContext.currentTime;
		const elapsed = Math.max(0, now - firstChunkStartTime);
		const totalKnownDuration = Math.max(0.001, nextStartTime - firstChunkStartTime);
		let percent = (elapsed / totalKnownDuration) * 100;

		if (isDownloading) { uiProgressBar.style.transition = 'none'; percent = Math.min(98, percent); }
		else { uiProgressBar.style.transition = 'width 0.1s linear'; }

		percent = Math.max(0, Math.min(100, percent));
		uiProgressBar.style.width = `${percent}%`;
		progressFrameId = requestAnimationFrame(update);
	};
	progressFrameId = requestAnimationFrame(update);
}

function updateUI(state) {
	ensurePlayerUI();
	lastUiState = state;
	if (state === 'hidden') { uiContainer.classList.remove('is-visible'); if (progressFrameId) cancelAnimationFrame(progressFrameId); return; }
	requestAnimationFrame(() => uiContainer.classList.add('is-visible'));

	let labelKey = ''; let labelFallback = '';
	if (state === 'loading') {
		labelKey = 'tts.loading'; labelFallback = 'Loading...';
		if (uiPlayBtn) { uiPlayBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>'; uiPlayBtn.disabled = true; }
		if (uiContainer.querySelector('.tts-progress-container')) uiContainer.querySelector('.tts-progress-container').classList.add('is-loading');
	} else if (state === 'playing') {
		labelKey = 'tts.playing'; labelFallback = 'Speaking';
		if (uiPlayBtn) { uiPlayBtn.innerHTML = '<i class="fa-solid fa-pause"></i>'; uiPlayBtn.disabled = false; }
		if (uiContainer.querySelector('.tts-progress-container')) uiContainer.querySelector('.tts-progress-container').classList.remove('is-loading');
		startProgressLoop();
	} else if (state === 'paused') {
		labelKey = 'tts.paused'; labelFallback = 'Paused';
		if (uiPlayBtn) { uiPlayBtn.innerHTML = '<i class="fa-solid fa-play"></i>'; uiPlayBtn.disabled = false; }
	}
	if (uiStatusText) { const translated = t(labelKey); uiStatusText.textContent = (translated && translated !== labelKey) ? translated : labelFallback; }
}

window.addEventListener('languageChanged', () => { if (lastUiState && lastUiState !== 'hidden') updateUI(lastUiState); });

/**
 * Liest die transition-duration aus dem CSS eines Elements aus
 * und gibt sie in Millisekunden zurück.
 */
function getTransitionDurationMs(element) {
	if (!element) return 0;

	const style = window.getComputedStyle(element);
	const durationStr = style.transitionDuration || '0s';
	const delayStr = style.transitionDelay || '0s';

	// Helper zum Parsen von "0.3s" oder "300ms"
	const parseTime = (str) => {
		if (!str) return 0;
		// Mehrere Werte (kommagetrennt) splitten
		const values = str.split(',').map(v => v.trim());
		// In ms umwandeln und das Maximum nehmen
		const msValues = values.map(v => {
			if (v.includes('ms')) return parseFloat(v);
			return parseFloat(v) * 1000;
		});
		return Math.max(...msValues);
	};

	const duration = parseTime(durationStr);
	const delay = parseTime(delayStr);

	// Gesamtdauer ist Duration + Delay
	return duration + delay;
}

function handlePanicClick() {
	const playerEl = document.getElementById('tts-floating-player');

	// 1. Animation starten (Klein werden)
	if (playerEl) playerEl.classList.add('is-pressed');

	// --- NEU: Zeit dynamisch aus CSS lesen ---
	// Wir nehmen die Zeit + einen kleinen Sicherheitspuffer (20ms), 
	// damit die Animation garantiert optisch abgeschlossen ist.
	const cssDuration = getTransitionDurationMs(playerEl);
	const waitTime = cssDuration > 0 ? cssDuration + 20 : 0;

	// Helper zum Wiederherstellen (Groß werden)
	const releaseEffect = () => {
		if (playerEl) {
			void playerEl.offsetWidth; // Reflow erzwingen
			playerEl.classList.remove('is-pressed');
		}
	};

	if (!currentContext.originUrl) {
		releaseEffect();
		return;
	}

	if (window.location.href !== currentContext.originUrl) {

		window.addEventListener('tts-navigation-finished', () => {
			// Wenn die neue Seite da ist: Kurz warten, dann aufploppen
			setTimeout(releaseEffect, 100);

			restoreStateAndScroll();
			document.addEventListener('page-rendered', () => {
				restoreStateAndScroll();
			}, { once: true });

		}, { once: true });

		// 2. WARTEN: Exakt so lange warten, wie das CSS vorgibt
		setTimeout(() => {
			window.dispatchEvent(new CustomEvent('tts-navigate-request', {
				detail: { url: currentContext.originUrl }
			}));
		}, waitTime);

	} else {
		// Gleiches Spiel für den Scroll-Fall ohne Navigation
		setTimeout(() => {
			releaseEffect();
			restoreStateAndScroll();
		}, waitTime);
	}
}

/**
 * ZENTRALE FUNKTION FÜR HIGHLIGHTING & SCROLLING
 */
function applyHighlightAndScroll(element) {
	document.querySelectorAll('.is-reading').forEach(el => el.classList.remove('is-reading', 'is-paused'));
	void element.offsetWidth;
	element.classList.add('is-reading');
	if (isPaused) element.classList.add('is-paused');

	const rect = element.getBoundingClientRect();
	const absoluteTop = rect.top + window.scrollY;

	if (absoluteTop < 250) {
		window.scrollTo({ top: 0, behavior: 'auto' });
	} else {
		element.scrollIntoView({ behavior: 'smooth', block: 'center' });
	}
}

async function restoreStateAndScroll() {
	// 1. Haben wir überhaupt Text?
	if (!currentContext.textSnippet) return;

	// 2. WICHTIG: Sind wir auf der richtigen Seite?
	if (window.location.href !== currentContext.originUrl) {
		return;
	}

	// View/Tab Logik für Results Page (bleibt wie gehabt)
	if (currentContext.viewId) {
		const pane = document.getElementById(currentContext.viewId);
		if (pane && !pane.classList.contains('active')) {
			const prefix = currentContext.viewId.replace('-viz-content', '');
			const btnId = `show-${prefix}-viz`;
			const btn = document.getElementById(btnId);
			if (btn) {
				btn.click();
				await new Promise(r => setTimeout(r, 50)); // Kleines Delay für CSS Transition
			}
		}
	}

	if (currentContext.clusterId && currentContext.viewId) {
		const pane = document.getElementById(currentContext.viewId);
		if (pane) {
			const targetTab = pane.querySelector(`.topic-tab[data-cluster-id="${currentContext.clusterId}"]`);
			if (targetTab && !targetTab.classList.contains('active')) {
				targetTab.click();
				await new Promise(r => setTimeout(r, 50));
			}
		}
	}

	// Suche starten (einmalig, kein Polling mehr nötig)
	findAndScroll(currentContext.textSnippet);
}

function findAndScroll(textSnippet) {
	const snippet = textSnippet.trim();
	const searchString = snippet.substring(0, 40).replace(/"/g, '');

	const attemptToFind = () => {
		const candidates = document.querySelectorAll('p, h1, h2, h3, h4, h5, h6, li, span, div.expandable-abstract');
		let bestMatch = null;

		for (const el of candidates) {
			if (el.textContent.includes(searchString)) {
				if (el.closest('.viz-content-pane') || el.closest('.result-card') || el.closest('.legal-content-wrapper')) {
					bestMatch = el;
					break;
				}
			}
		}

		if (bestMatch) {
			const collapsedHierarchy = bestMatch.closest('.hierarchy-list-wrapper.is-collapsed');
			if (collapsedHierarchy) collapsedHierarchy.classList.remove('is-collapsed');

			const abstractWrapper = bestMatch.closest('.abstract-wrapper');
			if (abstractWrapper && !abstractWrapper.classList.contains('expanded')) {
				abstractWrapper.classList.add('expanded');
				bestMatch.scrollTop = 0;
			}

			applyHighlightAndScroll(bestMatch);
			return true; // Erfolg!
		}
		return false; // Nicht gefunden
	};

	// 1. Sofortiger Versuch
	if (!attemptToFind()) {
		// 2. Fallback: Wenn nicht gefunden, auf das Laden der dynamischen Karten warten.
		//    Der Listener räumt sich nach dem ersten Aufruf selbst auf.
		document.addEventListener('filtered-results-rendered', attemptToFind, { once: true });
	}
}

window.addEventListener('tts-navigation-finished', () => {
	setTimeout(restoreStateAndScroll, 200);
});

function initMediaSession(textSnippet) {
	if (!('mediaSession' in navigator)) return;
	navigator.mediaSession.metadata = new MediaMetadata({ title: 'Ideenatlas Voice', artist: 'Assistant', album: 'Summary' });
	navigator.mediaSession.setActionHandler('play', () => resume());
	navigator.mediaSession.setActionHandler('pause', () => pause());
	navigator.mediaSession.setActionHandler('stop', () => stop());
}
function updateMediaSessionState(state) {
	if (!('mediaSession' in navigator)) return;
	navigator.mediaSession.playbackState = state;
}

function initAudioContext() {
	if (!audioContext) {
		audioContext = new (window.AudioContext || window.webkitAudioContext)();
		masterGainNode = audioContext.createGain();
		masterGainNode.connect(audioContext.destination);
		masterGainNode.gain.value = 1.0;
	}
	if (audioContext.state === 'suspended') {
		audioContext.resume();
	}
}

export async function speak(text, contextData = {}, callbacks = {}) {
	if (!text) return;

	if (stopTimeoutId) { clearTimeout(stopTimeoutId); stopTimeoutId = null; }
	stop(true);

	currentContext.textSnippet = text;
	currentContext.originUrl = contextData.originUrl || window.location.href;
	currentContext.viewId = contextData.viewId || null;
	currentContext.clusterId = contextData.clusterId || null;

	currentCallbacks = callbacks;

	isDownloading = true; isPaused = false; activeSources = [];
	if (uiProgressBar) { uiProgressBar.style.width = '0%'; uiProgressBar.classList.remove('is-finished'); uiProgressBar.style.transition = 'none'; }

	updateUI('loading');
	initMediaSession(text);
	updateMediaSessionState('playing');
	initAudioContext();

	masterGainNode.gain.cancelScheduledValues(audioContext.currentTime);
	masterGainNode.gain.setValueAtTime(1, audioContext.currentTime);

	nextStartTime = 0; firstChunkStartTime = -1;
	abortController = new AbortController();

	try {
		const response = await fetch('/api/tts', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json', 'X-XSRF-TOKEN': getCsrfToken() },
			body: JSON.stringify({ text: text }),
			signal: abortController.signal
		});
		if (!response.ok) throw new Error(`Server status: ${response.status}`);

		updateUI('playing');
		startProgressLoop();

		const reader = response.body.getReader();
		let totalBytesRead = 0; let leftoverBytes = new Uint8Array(0); let isFirstChunkProcessed = false;

		while (true) {
			const { done, value } = await reader.read();
			if (done) { isDownloading = false; appendSilenceTailAndFade(); checkIfFinished(); break; }
			let chunk = value;
			if (totalBytesRead < 44) {
				const needed = 44 - totalBytesRead;
				if (chunk.length > needed) { chunk = chunk.slice(needed); }
				else { totalBytesRead += chunk.length; continue; }
			}
			totalBytesRead += value.length;
			const combined = new Uint8Array(leftoverBytes.length + chunk.length);
			combined.set(leftoverBytes); combined.set(chunk, leftoverBytes.length);
			const remainder = combined.length % 2;
			const processableLength = combined.length - remainder;
			const dataToProcess = combined.slice(0, processableLength);
			leftoverBytes = combined.slice(processableLength);
			if (dataToProcess.length > 0) { scheduleChunk(dataToProcess, !isFirstChunkProcessed); isFirstChunkProcessed = true; }
		}
	} catch (e) {
		if (e.name !== 'AbortError') { console.error("[TTS] Error:", e); stop(); }
	}
}

function appendSilenceTailAndFade() {
	if (!audioContext || !masterGainNode) return;
	const silentDuration = 0.2;
	const silentBuffer = audioContext.createBuffer(1, 22050 * silentDuration, 22050);
	const source = audioContext.createBufferSource();
	source.buffer = silentBuffer; source.connect(masterGainNode);
	if (nextStartTime < audioContext.currentTime) nextStartTime = audioContext.currentTime;
	const tailStart = nextStartTime;
	const fadeDuration = 0.15; const fadeStart = Math.max(audioContext.currentTime, tailStart - fadeDuration);
	masterGainNode.gain.cancelScheduledValues(fadeStart); masterGainNode.gain.setValueAtTime(masterGainNode.gain.value, fadeStart); masterGainNode.gain.linearRampToValueAtTime(0, tailStart);
	source.start(tailStart); nextStartTime += silentDuration;
	const sourceEntry = { source }; activeSources.push(sourceEntry);
	source.onended = () => { const index = activeSources.indexOf(sourceEntry); if (index > -1) activeSources.splice(index, 1); checkIfFinished(); };
}

function scheduleChunk(uint8Array, isFirstChunk) {
	if (!abortController || !audioContext) return;
	const int16Count = uint8Array.length / 2; const float32Data = new Float32Array(int16Count); const dataView = new DataView(uint8Array.buffer); const VOLUME_BOOST = 1;
	for (let i = 0; i < int16Count; i++) {
		const int16 = dataView.getInt16(i * 2, true);
		let floatVal = (int16 < 0 ? int16 / 32768 : int16 / 32767) * VOLUME_BOOST;
		if (floatVal > 1.0) floatVal = 1.0; if (floatVal < -1.0) floatVal = -1.0;
		if (isFirstChunk && i < 500) floatVal *= (i / 500);
		float32Data[i] = floatVal;
	}
	const audioBuffer = audioContext.createBuffer(1, float32Data.length, 22050); audioBuffer.getChannelData(0).set(float32Data);
	const source = audioContext.createBufferSource(); source.buffer = audioBuffer; source.connect(masterGainNode);
	if (firstChunkStartTime === -1) { firstChunkStartTime = audioContext.currentTime + 0.05; nextStartTime = firstChunkStartTime; }
	if (nextStartTime < audioContext.currentTime) nextStartTime = audioContext.currentTime;
	source.start(nextStartTime); nextStartTime += audioBuffer.duration;
	const sourceEntry = { source }; activeSources.push(sourceEntry);
	source.onended = () => { const index = activeSources.indexOf(sourceEntry); if (index > -1) activeSources.splice(index, 1); checkIfFinished(); };
}

function checkIfFinished() {
	if (isPaused) { setTimeout(checkIfFinished, 500); return; }
	if (!isDownloading && activeSources.length === 0) {
		if (audioContext && audioContext.currentTime >= nextStartTime - 0.1) {
			if (currentCallbacks.onStop) currentCallbacks.onStop();
			updateUI('hidden'); if (progressFrameId) cancelAnimationFrame(progressFrameId); updateMediaSessionState('none');
			isPaused = false; abortController = null; currentCallbacks = {};
		} else { setTimeout(checkIfFinished, 200); }
	}
}

function togglePlayPause() { if (isPaused) resume(); else pause(); }

export function pause() {
	if (audioContext && !isPaused) {
		const now = audioContext.currentTime;
		masterGainNode.gain.cancelScheduledValues(now); masterGainNode.gain.setValueAtTime(masterGainNode.gain.value, now); masterGainNode.gain.linearRampToValueAtTime(0, now + 0.15);
		isPaused = true; updateUI('paused'); updateMediaSessionState('paused'); if (currentCallbacks.onPause) currentCallbacks.onPause();
		if (pauseTimeoutId) clearTimeout(pauseTimeoutId);
		pauseTimeoutId = setTimeout(() => { if (isPaused && audioContext.state === 'running') audioContext.suspend(); }, 200);
	}
}

export function resume() {
	if (audioContext && isPaused) {
		if (pauseTimeoutId) clearTimeout(pauseTimeoutId); pauseTimeoutId = null;
		const resumeAction = () => {
			const now = audioContext.currentTime;
			masterGainNode.gain.cancelScheduledValues(now); masterGainNode.gain.setValueAtTime(0, now); masterGainNode.gain.linearRampToValueAtTime(1, now + 0.15);
			isPaused = false; updateUI('playing'); updateMediaSessionState('playing'); if (currentCallbacks.onResume) currentCallbacks.onResume();
		};
		if (audioContext.state === 'suspended') audioContext.resume().then(resumeAction); else resumeAction();
	}
}

export function stop(preserveContext = false) {
	if (abortController) { abortController.abort(); abortController = null; }
	isDownloading = false;
	if (audioContext) {
		if (audioContext.state === 'suspended') audioContext.resume();
		const now = audioContext.currentTime;
		masterGainNode.gain.cancelScheduledValues(now); masterGainNode.gain.setValueAtTime(masterGainNode.gain.value, now); masterGainNode.gain.linearRampToValueAtTime(0, now + 0.1);
		const sourcesToStop = [...activeSources]; activeSources = [];
		setTimeout(() => {
			sourcesToStop.forEach(entry => { try { entry.source.stop(); } catch (e) { } });
			if (preserveContext) { masterGainNode.gain.cancelScheduledValues(audioContext.currentTime); masterGainNode.gain.setValueAtTime(1, audioContext.currentTime + 0.1); }
		}, 150);
	} else activeSources = [];
	nextStartTime = 0; firstChunkStartTime = -1; isPaused = false;
	if (stopTimeoutId) clearTimeout(stopTimeoutId); if (pauseTimeoutId) clearTimeout(pauseTimeoutId);
	if (!preserveContext) {
		stopTimeoutId = setTimeout(() => {
			updateUI('hidden'); if (progressFrameId) cancelAnimationFrame(progressFrameId); updateMediaSessionState('none');
			if (currentCallbacks.onStop) currentCallbacks.onStop();
			currentCallbacks = {}; currentContext = { originUrl: null, textSnippet: null };
			stopTimeoutId = null;
		}, 200);
	}
}

export function detachPlayer() { return null; }
export function reattachPlayer(el) { ensurePlayerUI(); }