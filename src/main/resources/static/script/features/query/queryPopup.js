import * as gravitySpinner from '/script/animation/gravitySpinner.js';
import { initTextAnimator, prepareTextExplosion, triggerPreparedExplosion, resizeAndScaleCanvas, stopTextAnimator } from '/script/animation/textAnimator.js';
import { animConfig } from '/script/animation/animationConfig.js';
import { sendQuery } from '/script/features/query/handleQuery.js';
import { t, applyGeneralTranslations } from '/script/core/localization.js';
import { renderTemplate } from '/script/core/templateManager.js';

// --- KEIN MASTER LOOP MEHR ---
// Wir steuern nur die globalen Variablen für DeltaTime grob, oder lassen die Module das selbst machen.
// Die Module oben haben ihre eigenen Loops wieder.

let activePopup = null;
const eventQueue = [];
let isProcessing = false;
let isFinalizing = false;

let heldIdeaData = null;
let heldClusterData = null;
let imagesLoaded = 0;
const TOTAL_IMAGES = 7;

function processQueue() {
    if (eventQueue.length === 0) {
        isProcessing = false;
        return;
    }
    isProcessing = true;
    const { status, data } = eventQueue.shift();
    updateQueryPopup(status, data);
    setTimeout(processQueue, 75);
}

function revealContent() {
    if (!activePopup) return;
    isFinalizing = true;

    const headerText = activePopup.querySelector('.recorder-header h2');
    const statusText = activePopup.querySelector('.recorder-modal .popup-status-text');
    const spinnerCanvas = document.getElementById('spinner-canvas');

    resizeAndScaleCanvas();

    let blackHoleCoords = null;
    if (spinnerCanvas) {
        const canvasRect = spinnerCanvas.getBoundingClientRect();
        const internalCoords = gravitySpinner.getGravityCenter();
        if (canvasRect.width > 0 && internalCoords.y > 20) {
            blackHoleCoords = {
                x: canvasRect.left + internalCoords.x,
                y: canvasRect.top + internalCoords.y
            };
        }
    }

    if (!blackHoleCoords) blackHoleCoords = { x: window.innerWidth / 2, y: window.innerHeight / 2 };

    const preparedHeaderTextData = prepareTextExplosion(headerText);
    const preparedStatusTextData = prepareTextExplosion(statusText);

    if (headerText) headerText.style.visibility = 'hidden';
    if (statusText) statusText.style.visibility = 'hidden';
    
    triggerPreparedExplosion(preparedHeaderTextData, blackHoleCoords);
    triggerPreparedExplosion(preparedStatusTextData, blackHoleCoords);

    setTimeout(() => {
        if (!activePopup) return;

        if (headerText) {
            headerText.textContent = t('popup.header_visualizing');
            headerText.style.visibility = 'visible';
            headerText.style.opacity = '1';
        }

        const contentArea = activePopup.querySelector('.popup-content-area');
        if (contentArea) contentArea.classList.add('is-revealed');

        const ideaBlock = document.getElementById('idea-block');
        const clusterBlock = document.getElementById('cluster-block');
        
        if (heldIdeaData && ideaBlock) {
            ideaBlock.querySelector('.scrollable-content-box').textContent = `"${heldIdeaData}"`;
            ideaBlock.style.display = 'flex';
        }
        
        if (heldClusterData && Array.isArray(heldClusterData) && clusterBlock) {
            const scrollBox = clusterBlock.querySelector('.scrollable-content-box');
            scrollBox.textContent = ''; 
            
            heldClusterData.forEach(entry => {
                const p = document.createElement('div');
                p.textContent = `• ${entry.name} (${Number(entry.confidence).toFixed(2)})`;
                scrollBox.appendChild(p);
            });
            clusterBlock.style.display = 'block';
        }

        const modal = activePopup.querySelector('.recorder-modal');
        if (modal) {
            modal.classList.add('is-expanded');
            
            let trackerFrameId;
            const modalElement = activePopup.querySelector('.recorder-modal');
            function trackModalBoundary() {
                if (!activePopup) { cancelAnimationFrame(trackerFrameId); animConfig.repulsionBoundary = null; return; }
                animConfig.repulsionBoundary = modalElement.getBoundingClientRect();
                trackerFrameId = requestAnimationFrame(trackModalBoundary);
            }
            trackModalBoundary();

            setTimeout(() => {
                cancelAnimationFrame(trackerFrameId);
                animConfig.repulsionBoundary = null;
            }, 550); 
        }
    }, 150);
}

export function showQueryPopup() {
    if (activePopup) activePopup.remove();
	
    animConfig.frameCount = 0;
    eventQueue.length = 0;
    isProcessing = false;
    isFinalizing = false;
    heldIdeaData = null;
    heldClusterData = null;
    imagesLoaded = 0;

    activePopup = renderTemplate('query-popup-overlay');
    if (!activePopup) return;
    
    // Events binden (kein Check auf eventsAttached nötig, da das Popup bei jedem Aufruf neu erstellt wird)
	activePopup.querySelectorAll('.intent-btn').forEach(button => {
		button.addEventListener('click', () => {
			const intent = button.dataset.intent;
			const modal = activePopup.querySelector('.recorder-modal');
			modal.classList.remove('is-selecting-intent');
			modal.classList.add('is-processing');
			modal.querySelector('.recorder-header h2').textContent = t('popup.header_processing');

			requestAnimationFrame(() => {
				const spinnerCanvas = document.getElementById('spinner-canvas');
				if (spinnerCanvas) {
					gravitySpinner.init(spinnerCanvas, revealContent);
					gravitySpinner.addParticle
				}
				const textExplosionCanvas = document.getElementById('text-explosion-canvas');
				if (textExplosionCanvas) {
					textExplosionCanvas.width = textExplosionCanvas.clientWidth;
					textExplosionCanvas.height = textExplosionCanvas.clientHeight;
					initTextAnimator(textExplosionCanvas);
				}
			});
			sendQuery(intent);
		});
	});

	activePopup.querySelector('.recorder-close-btn').addEventListener('click', hideQueryPopup);
	setTimeout(() => { if (activePopup) activePopup.classList.add('is-visible'); }, 10);
}

export function hideQueryPopup() {
    if (activePopup) {
		const form = document.querySelector('.idea-form');
		if (form) form.inert = false;
		const menuTrigger = document.getElementById('menu-trigger');
		if (menuTrigger) menuTrigger.classList.remove('is-hidden');
		
        gravitySpinner.stop();
		stopTextAnimator();
        activePopup.remove();
        activePopup = null;
    }
}

function updateQueryPopup(status, data) {
    const allowedFinalizingEvents = ['IMAGE_READY', 'COMPLETE', 'ERROR', 'RENDERING'];
    if (isFinalizing && !allowedFinalizingEvents.includes(status)) { return; }
    if (!activePopup) return;

    const modal = activePopup.querySelector('.recorder-modal');
    const statusText = activePopup.querySelector('.recorder-modal .popup-status-text');
    const closeBtn = activePopup.querySelector('.recorder-close-btn');

    gravitySpinner.addParticle();
	
    // 1. Sichere Text-Zuweisung (verhindert [object Object] und URLs)
    if (status !== 'ERROR') {
        const localizedText = t(`sse.${status}`);
        
        if (localizedText && localizedText !== `sse.${status}`) {
            // Übersetzung gefunden
            statusText.setAttribute('data-i18n', `sse.${status}`);
            statusText.textContent = localizedText;
        } else if (!isFinalizing) {
            // Fallback: Zeigt den englischen String aus dem Java-Backend, 
            // ABER NUR, wenn es echter Text ist (kein Objekt/Array und keine URL)
            if (typeof data === 'string' && !data.startsWith('/')) {
                statusText.removeAttribute('data-i18n');
                statusText.textContent = data;
            }
        }
    }

    // 2. Status-Logik (ohne redundante Text-Zuweisungen im default-Block)
    switch (status) {
        case 'EXTRACTING_COMPLETE': heldIdeaData = data; break;
        case 'STILL_CLUSTERING': heldClusterData = data; break;
        case 'CREATING_OWN_VISUALIZATIONS':
            gravitySpinner.triggerBlackHoleExplosion();
            break;
        case 'IMAGE_READY':
            imagesLoaded++;
            gravitySpinner.setRingProgress(imagesLoaded / TOTAL_IMAGES);
            break;
        case 'COMPLETE':
            gravitySpinner.setRingProgress(1);
            break;
        case 'ERROR':
            statusText.removeAttribute('data-i18n');
            statusText.textContent = `Error: ${data}`;
            gravitySpinner.stop();
            modal.classList.add('is-error');
            closeBtn.style.display = 'flex';
            closeBtn.addEventListener('click', hideQueryPopup);
            eventQueue.length = 0;
            break;
        default:
            // Keine Aktion erforderlich, Text wurde oben bereits gesetzt
            break;
    }
}

export function queueSseEvent(status, data) {
    console.log("SSE:", status, "Data:", data);
    eventQueue.push({ status, data });
    if (!isProcessing) processQueue();
}