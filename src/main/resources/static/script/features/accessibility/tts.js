import { getCsrfToken } from '/script/core/security.js';
import { t } from '/script/core/localization.js';
import { emit, on, EVENTS } from '/script/core/eventBus.js';


// --- STATE ---
let audioContext = null;
let activeSources = [];
let nextStartTime = 0;
let firstChunkStartTime = -1;
let abortController = null;
let isDownloading = false;
let isPaused = false;
let masterGainNode = null;
let currentContext = {
	originUrl: null,
	textSnippet: null,
	viewId: null,
	clusterId: null
};

// UI State
let lastUiState = 'hidden';
let uiContainer = null, uiStatusText = null, uiPlayBtn = null, uiProgressBar = null;
let progressFrameId = null;
let stopTimeoutId = null;
let pauseTimeoutId = null;


/**
 * Findet den scrollbaren Container (Pane) oder das nächstbeste scrollbare Element.
 */
function getScrollParent(node) {
	if (node == null) return null;
	// Auf der Results-Seite ist das der Haupt-Container für Scrollen
	if (node.classList.contains('viz-content-pane')) return node;

	// Fallback für andere Seiten (z.B. Impressum)
	if (node.scrollHeight > node.clientHeight && getComputedStyle(node).overflowY !== 'hidden') {
		return node;
	}

	return getScrollParent(node.parentElement);
}

const Highlighter = {
    snippet: null,
    searchString: null,
    observer: null,
    debounceTimer: null,

    start(text) {
        this.snippet = text.trim();
        this.searchString = this.snippet.substring(0, 60).replace(/["'´`]/g, '');
        this.ensureObserver();
        this.run(true); 
    },

    stop() {
        this.snippet = null;
        this.searchString = null;
        if (this.observer) {
            this.observer.disconnect();
            this.observer = null;
        }
        this.clearAll();
    },

    ensureObserver() {
        if (this.observer) return;
        this.observer = new MutationObserver(() => {
            if (this.debounceTimer) clearTimeout(this.debounceTimer);
            this.debounceTimer = setTimeout(() => this.run(false), 50);
        });
        this.observer.observe(document.body, { childList: true, subtree: true, characterData: true });
    },

    clearAll() {
        document.querySelectorAll('.is-reading, .is-paused').forEach(el => {
            el.classList.remove('is-reading', 'is-paused');
        });
    },

	run(shouldScroll) {
		if (!this.searchString) return;

		const candidates = document.querySelectorAll('p, h1, h2, h3, h4, h5, h6, li, span, div.expandable-abstract');
		let bestMatch = null;

		for (const el of candidates) {
			const elText = el.textContent.replace(/["'´`]/g, '');
			if (elText.includes(this.searchString) && (el.closest('.viz-content-pane') || el.closest('.result-card') || el.closest('.legal-content-wrapper'))) {
				bestMatch = el;
				break;
			}
		}

		if (bestMatch) {
			this.clearAll();

			const collapsedHierarchy = bestMatch.closest('.hierarchy-list-wrapper.is-collapsed');
			if (collapsedHierarchy) collapsedHierarchy.classList.remove('is-collapsed');

			const abstractWrapper = bestMatch.closest('.abstract-wrapper:not(.expanded)');
			if (abstractWrapper) abstractWrapper.classList.add('expanded');

			bestMatch.classList.add('is-reading');
			if (isPaused) bestMatch.classList.add('is-paused');
			else bestMatch.classList.remove('is-paused');

			if (shouldScroll) {
				requestAnimationFrame(() => this.scrollIntoViewSafe(bestMatch));
			}
		}
	},

    scrollIntoViewSafe(element) {
	        if (!element) return;
	        
	        // 1. Richtigen Scroll-Container finden (auf Results Page ist das .viz-content-pane)
	        const container = getScrollParent(element);
	        
	        // Wenn kein Container da ist (z.B. statische Seite ohne fixed Header), Fallback auf Native
	        if (!container || container === document.body || container === document.documentElement) {
	            element.scrollIntoView({ behavior: 'smooth', block: 'center' });
	            return;
	        }

	        // 2. Position berechnen
	        const elementRect = element.getBoundingClientRect();
	        const containerRect = container.getBoundingClientRect();
	        const currentScrollTop = container.scrollTop;

	        // Wir wollen, dass die Mitte des Elements in der Mitte des Containers landet
	        const relativeTop = elementRect.top - containerRect.top;
	        const offsetToCenter = relativeTop + (elementRect.height / 2) - (containerRect.height / 2);
	        
	        const targetScrollTop = currentScrollTop + offsetToCenter;

	        // 3. Container scrollen (Body bleibt unberührt!)
	        container.scrollTo({
	            top: targetScrollTop,
	            behavior: 'smooth'
	        });
	    }
	};


// --- INITIALIZATION & NAVIGATION ---
let isTtsInitialized = false;

export function initTTS() {
    if (isTtsInitialized) return;
    
    // Wir hören auf das Signal von navigation.js
    on(EVENTS.TTS_FINISHED, () => {
        if (Highlighter.searchString) {
            setTimeout(() => restoreStateAndScroll(), 200);
        }
    });
    
    isTtsInitialized = true;
}

async function restoreStateAndScroll() {
	if (!currentContext.textSnippet || window.location.href !== currentContext.originUrl) return;

	// View/Tab Logik
	if (currentContext.viewId) {
		const pane = document.getElementById(currentContext.viewId);
		if (pane && !pane.classList.contains('active')) {
			const prefix = currentContext.viewId.replace('-viz-content', '');
			const btnId = `show-${prefix}-viz`;
			const btn = document.getElementById(btnId);
			if (btn) {
				btn.click();
				await new Promise(r => setTimeout(r, 50));
			}
		}
	}

    // --- FIX 4: Warten auf das Event, wenn Tab gewechselt wird ---
	if (currentContext.clusterId && currentContext.viewId) {
		const pane = document.getElementById(currentContext.viewId);
		if (pane) {
			const targetTab = pane.querySelector(`.topic-tab[data-cluster-id="${currentContext.clusterId}"]`);

			if (targetTab && !targetTab.classList.contains('active')) {
				
                // Promise, das auf das Fertig-Event wartet
				const renderPromise = new Promise(resolve => {
                    // Fallback Timer, falls Event verschluckt wird
                    const timeout = setTimeout(resolve, 2000); 

					const listener = (data) => {
						if (data.clusterId === currentContext.clusterId) {
                            clearTimeout(timeout);
							resolve();
						}
					};
					on(EVENTS.FILTERED_RESULTS_RENDERED, listener, { once: true });
				});

				targetTab.click();

				// Warten, bis Inhalt geladen ist!
				await renderPromise;
				await new Promise(r => setTimeout(r, 50)); // Kurzer Render-Puffer
			}
		}
	}

	Highlighter.run(true);
}


// --- AUDIO ENGINE (Rest unverändert, aber wichtig für Kontext) ---

export function executeReading(targetElement) {
    let textContent = targetElement.innerText.trim();
    if (targetElement.classList.contains('json-string') && textContent.startsWith('"') && textContent.endsWith('"')) {
        textContent = textContent.slice(1, -1);
    }

    const contextState = {
        viewId: null,
        clusterId: null,
        originUrl: window.location.href
    };
    const mainPane = targetElement.closest('.viz-content-pane');
    if (mainPane) {
        contextState.viewId = mainPane.id;
        if (mainPane.id.includes('neighbor') || mainPane.id.includes('serendipity')) {
            contextState.clusterId = mainPane.querySelector('.topic-tab.active')?.dataset.clusterId || null;
        }
    }
    speak(textContent, contextState);
}

async function speak(text, contextData = {}) {
    if (!text) return;
    stop(true); 

    currentContext = {
        textSnippet: text,
        originUrl: contextData.originUrl || window.location.href,
        viewId: contextData.viewId || null,
        clusterId: contextData.clusterId || null
    };

    Highlighter.start(text);

    isDownloading = true; isPaused = false; activeSources = [];
    if (uiProgressBar) { uiProgressBar.style.width = '0%'; uiProgressBar.classList.remove('is-finished'); uiProgressBar.style.transition = 'none'; }

    updateUI('loading');
    initMediaSession();
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
        let totalBytesRead = 0, leftoverBytes = new Uint8Array(0), isFirstChunkProcessed = false;
        while (true) {
            const { done, value } = await reader.read();
            if (done) { isDownloading = false; appendSilenceTailAndFade(); checkIfFinished(); break; }
            let chunk = value;
            if (totalBytesRead < 44) {
                const needed = 44 - totalBytesRead;
                if(chunk.length > needed) {
                    chunk = chunk.slice(needed);
                } else {
                    totalBytesRead += chunk.length;
                    continue;
                }
            }
            totalBytesRead += chunk.length;
            const combined = new Uint8Array(leftoverBytes.length + chunk.length);
            combined.set(leftoverBytes);
            combined.set(chunk, leftoverBytes.length);
            const remainder = combined.length % 2;
            const processableLength = combined.length - remainder;
            const dataToProcess = combined.slice(0, processableLength);
            leftoverBytes = combined.slice(processableLength);
            if (dataToProcess.length > 0) {
                scheduleChunk(dataToProcess, !isFirstChunkProcessed);
                isFirstChunkProcessed = true;
            }
        }
    } catch (e) {
        if (e.name !== 'AbortError') { console.error("[TTS] Error:", e); stop(); }
    }
}

function pause() {
    if (audioContext && !isPaused) {
        const now = audioContext.currentTime;
        masterGainNode.gain.cancelScheduledValues(now);
        masterGainNode.gain.linearRampToValueAtTime(0, now + 0.15);
        isPaused = true;
        updateUI('paused');
        updateMediaSessionState('paused');
        Highlighter.run(false); 
        if (pauseTimeoutId) clearTimeout(pauseTimeoutId);
        pauseTimeoutId = setTimeout(() => { if (isPaused && audioContext.state === 'running') audioContext.suspend(); }, 200);
    }
}

function resume() {
    if (audioContext && isPaused) {
        if (pauseTimeoutId) clearTimeout(pauseTimeoutId);
        const resumeAction = () => {
            const now = audioContext.currentTime;
            masterGainNode.gain.cancelScheduledValues(now);
            masterGainNode.gain.linearRampToValueAtTime(1, now + 0.15);
            isPaused = false;
            updateUI('playing');
            updateMediaSessionState('playing');
            Highlighter.run(false);
        };
        if (audioContext.state === 'suspended') audioContext.resume().then(resumeAction);
        else resumeAction();
    }
}

export function stop(preserveContext = false) {
    if (abortController) { abortController.abort(); abortController = null; }
    isDownloading = false;
    if (audioContext) {
        if (audioContext.state === 'suspended') audioContext.resume();
        const now = audioContext.currentTime;
        masterGainNode.gain.cancelScheduledValues(now);
        masterGainNode.gain.linearRampToValueAtTime(0, now + 0.1);
        [...activeSources].forEach(entry => { try { entry.source.stop(); } catch (e) {} });
        activeSources = [];
    }
    nextStartTime = 0; firstChunkStartTime = -1; isPaused = false;
    if (stopTimeoutId) clearTimeout(stopTimeoutId); if (pauseTimeoutId) clearTimeout(pauseTimeoutId);

    if (!preserveContext) {
        Highlighter.stop();
        stopTimeoutId = setTimeout(() => {
            updateUI('hidden');
            if (progressFrameId) cancelAnimationFrame(progressFrameId);
            updateMediaSessionState('none');
            currentContext = { originUrl: null, textSnippet: null, viewId: null, clusterId: null };
        }, 200);
    }
}

// Helpers...
function scheduleChunk(uint8Array, isFirstChunk) {
    if (!abortController || !audioContext) return;
    const int16Count = uint8Array.length / 2;
    const float32Data = new Float32Array(int16Count); 
    const dataView = new DataView(uint8Array.buffer, uint8Array.byteOffset);
    const VOLUME_BOOST = 1;
    for (let i = 0; i < int16Count; i++) {
        const int16 = dataView.getInt16(i * 2, true); 
        let floatVal = (int16 < 0 ? int16 / 32768 : int16 / 32767) * VOLUME_BOOST;
        if (floatVal > 1.0) floatVal = 1.0; if (floatVal < -1.0) floatVal = -1.0;
        if (isFirstChunk && i < 500) floatVal *= (i / 500);
        float32Data[i] = floatVal;
    }
    const audioBuffer = audioContext.createBuffer(1, float32Data.length, 22050);
    audioBuffer.getChannelData(0).set(float32Data);
    const source = audioContext.createBufferSource();
    source.buffer = audioBuffer;
    source.connect(masterGainNode);
    if (firstChunkStartTime === -1) { firstChunkStartTime = audioContext.currentTime + 0.05; nextStartTime = firstChunkStartTime; }
    if (nextStartTime < audioContext.currentTime) { nextStartTime = audioContext.currentTime; }
    source.start(nextStartTime);
    nextStartTime += audioBuffer.duration;
    const sourceEntry = { source };
    activeSources.push(sourceEntry);
    source.onended = () => { const index = activeSources.indexOf(sourceEntry); if (index > -1) activeSources.splice(index, 1); checkIfFinished(); };
}

function ensurePlayerUI() {
    if (document.getElementById('tts-floating-player')) return;
    const div = document.createElement('div');
    div.id = 'tts-floating-player';
    div.classList.add('tts-floating-player-container');
    div.innerHTML = `
        <div class="tts-top-row">
            <div class="tts-info-wrapper"><div class="tts-status-text" id="tts-status-text">...</div></div>
            <div class="tts-controls">
                <button class="tts-btn play-pause-btn" id="tts-play-pause"><i class="fa-solid fa-pause"></i></button>
                <button class="tts-btn" id="tts-stop"><i class="fa-solid fa-xmark"></i></button>
            </div>
        </div>
        <div class="tts-progress-container" id="tts-progress-bg"><div class="tts-progress-bar" id="tts-progress-bar"></div></div>
    `;
    document.body.appendChild(div);
    uiContainer = div;
    uiStatusText = div.querySelector('#tts-status-text');
    uiPlayBtn = div.querySelector('#tts-play-pause');
    uiProgressBar = div.querySelector('#tts-progress-bar');
    div.querySelector('#tts-stop').onclick = (e) => { e.stopPropagation(); stop(); };
    uiPlayBtn.onclick = (e) => { e.stopPropagation(); togglePlayPause(); };
    div.onclick = (e) => { if (!e.target.closest('button')) handlePanicClick(); };
}

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
        uiProgressBar.style.transition = isDownloading ? 'none' : 'width 0.1s linear';
        percent = Math.max(0, Math.min(100, percent));
        uiProgressBar.style.width = `${percent}%`;
        progressFrameId = requestAnimationFrame(update);
    };
    progressFrameId = requestAnimationFrame(update);
}

function updateUI(state) {
    ensurePlayerUI();
    lastUiState = state;
    if (state === 'hidden') {
        uiContainer.classList.remove('is-visible');
        if (progressFrameId) cancelAnimationFrame(progressFrameId);
        return;
    }
    requestAnimationFrame(() => uiContainer.classList.add('is-visible'));
    let labelKey = '';
    if (state === 'loading') {
        labelKey = 'tts.loading';
        if (uiPlayBtn) { uiPlayBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>'; uiPlayBtn.disabled = true; }
        uiContainer.querySelector('.tts-progress-container')?.classList.add('is-loading');
    } else if (state === 'playing') {
        labelKey = 'tts.playing';
        if (uiPlayBtn) { uiPlayBtn.innerHTML = '<i class="fa-solid fa-pause"></i>'; uiPlayBtn.disabled = false; }
        uiContainer.querySelector('.tts-progress-container')?.classList.remove('is-loading');
        startProgressLoop();
    } else if (state === 'paused') {
        labelKey = 'tts.paused';
        if (uiPlayBtn) { uiPlayBtn.innerHTML = '<i class="fa-solid fa-play"></i>'; uiPlayBtn.disabled = false; }
    }
    if (uiStatusText) uiStatusText.textContent = t(labelKey) || labelKey;
}

function handlePanicClick() {
    const playerEl = document.getElementById('tts-floating-player');
    if (playerEl) playerEl.classList.add('is-pressed');
    const waitTime = getTransitionDurationMs(playerEl) + 20;

    const releaseEffect = () => { if (playerEl) { void playerEl.offsetWidth; playerEl.classList.remove('is-pressed'); }};

    if (!currentContext.originUrl) { releaseEffect(); return; }

    if (window.location.href !== currentContext.originUrl) {
        on(EVENTS.TTS_FINISHED, () => setTimeout(releaseEffect, 100), { once: true });
        setTimeout(() => emit(EVENTS.TTS_NAVIGATE, { url: currentContext.originUrl }), waitTime);
    } else {
        setTimeout(() => {
            releaseEffect();
            restoreStateAndScroll();
        }, waitTime);
    }
}

function getTransitionDurationMs(element) {
    if (!element) return 0;
    const style = window.getComputedStyle(element);
    const durationStr = style.transitionDuration || '0s';
    const values = durationStr.split(',').map(v => v.trim());
    const msValues = values.map(v => v.includes('ms') ? parseFloat(v) : parseFloat(v) * 1000);
    return Math.max(...msValues);
}

function initMediaSession() {
    if (!('mediaSession' in navigator)) return;
    navigator.mediaSession.metadata = new MediaMetadata({ title: 'Ideenatlas Voice', artist: 'Assistant' });
    navigator.mediaSession.setActionHandler('play', resume);
    navigator.mediaSession.setActionHandler('pause', pause);
    navigator.mediaSession.setActionHandler('stop', stop);
}

function updateMediaSessionState(state) {
    if ('mediaSession' in navigator) navigator.mediaSession.playbackState = state;
}

function initAudioContext() {
    if (!audioContext) {
        audioContext = new (window.AudioContext || window.webkitAudioContext)();
        masterGainNode = audioContext.createGain();
        masterGainNode.connect(audioContext.destination);
    }
    if (audioContext.state === 'suspended') audioContext.resume();
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
    masterGainNode.gain.cancelScheduledValues(fadeStart);
    masterGainNode.gain.linearRampToValueAtTime(0, tailStart);
    source.start(tailStart); nextStartTime += silentDuration;
    const sourceEntry = { source }; activeSources.push(sourceEntry);
    source.onended = () => { const index = activeSources.indexOf(sourceEntry); if (index > -1) activeSources.splice(index, 1); checkIfFinished(); };
}

function checkIfFinished() {
    if (isPaused) { setTimeout(checkIfFinished, 500); return; }
    if (!isDownloading && activeSources.length === 0) {
        if (audioContext && audioContext.currentTime >= nextStartTime - 0.1) {
            stop();
        } else { setTimeout(checkIfFinished, 200); }
    }
}

function togglePlayPause() { if (isPaused) resume(); else pause(); }

export function detachPlayer() { return null; }
export function reattachPlayer() { ensurePlayerUI(); }