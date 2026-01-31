import * as gravitySpinner from '/script/animation/gravitySpinner.js';
import { initTextAnimator, prepareTextExplosion, triggerPreparedExplosion, resizeAndScaleCanvas } from '/script/animation/textAnimator.js';
import { animConfig } from '/script/animation/animationConfig.js';
import { sendQuery } from '/script/features/query/handleQuery.js';
import { t, applyGeneralTranslations } from '/script/core/localization.js';

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

    resizeAndScaleCanvas(); // Text Animator sicherstellen

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
            ideaBlock.querySelector('.scrollable-content-box').innerHTML = `"${heldIdeaData}"`;
			ideaBlock.style.display = 'flex';
		}
		if (heldClusterData && Array.isArray(heldClusterData) && clusterBlock) {
			const formattedLines = heldClusterData.map(entry => `• ${entry.name} (${Number(entry.confidence).toFixed(2)})`);
			clusterBlock.querySelector('.scrollable-content-box').innerHTML = formattedLines.join('<br>');
			clusterBlock.style.display = 'block';
		}

        const modal = activePopup.querySelector('.recorder-modal');
        if (modal) {
            modal.classList.add('is-expanded');
            
            // Einfacher Tracker für die Boundary (reicht völlig aus)
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

	const form = document.querySelector('.idea-form');
	if (form) form.inert = true;
	
	const menuTrigger = document.getElementById('menu-trigger');
	if (menuTrigger) menuTrigger.classList.add('is-hidden');
	
    animConfig.frameCount = 0;
    eventQueue.length = 0;
    isProcessing = false;
    isFinalizing = false;
    heldIdeaData = null;
    heldClusterData = null;
    imagesLoaded = 0;

    // Kein Master Loop Start hier!

	const overlay = document.createElement('div');
	overlay.className = 'recorder-overlay';

	overlay.innerHTML = `
		    <canvas id="text-explosion-canvas" class="text-explosion-canvas"></canvas>
		    <div class="recorder-modal query-popup-modal is-selecting-intent">
		        <div class="spinner-container"><canvas class="spinner-canvas" id="spinner-canvas"></canvas></div>
		        <div class="recorder-header">
		            <h2 data-i18n="popup.title">What is your goal?</h2>
		            <button class="recorder-close-btn">×</button>
		        </div>
		        <div class="query-popup-body">
		            <div class="popup-intent-selection">
		                <button class="intent-btn" data-intent="question"><i class="fa-solid fa-circle-question"></i><div class="intent-text-wrapper"><strong data-i18n="popup.btn_question.title">I need answers</strong><p data-i18n="popup.btn_question.desc">You gave a question or incomplete information.</p></div></button>
		                <button class="intent-btn" data-intent="idea"><i class="fa-solid fa-lightbulb"></i><div class="intent-text-wrapper"><strong data-i18n="popup.btn_idea.title">Extract my idea</strong><p data-i18n="popup.btn_idea.desc">Synthesize core idea after brainstorming.</p></div></button>
		                <button class="intent-btn" data-intent="summarize"><i class="fa-solid fa-file-lines"></i><div class="intent-text-wrapper"><strong data-i18n="popup.btn_summarize.title">Summarize documents</strong><p data-i18n="popup.btn_summarize.desc">Summarize given data and extract methodologies.</p></div></button>
						<button class="intent-btn" data-intent="none"><i class="fa-solid fa-expand"></i><div class="intent-text-wrapper"><strong data-i18n="popup.btn_none.title">No special treatment</strong><p data-i18n="popup.btn_none.desc">I do not have a specific intend.</p></div></button>
		            </div>
		            <div class="animation-placeholder"></div>
		            <span class="popup-status-text" data-i18n="popup.status.uploading">Uploading data & starting process...</span>
		            <div class="popup-content-area">
		                <div id="idea-block" class="popup-info-block" style="display: none;"><h4 data-i18n="headers.extracted_idea">Extracted Idea:</h4><div class="scrollable-content-box"></div></div>
		                <div id="cluster-block" class="popup-info-block" style="display: none;"><h4 data-i18n="card.topic_cluster_tooltip">Cluster Hierarchy:</h4><div class="scrollable-content-box"></div></div>
		            </div>
		        </div>
		    </div>
		`;
	document.body.appendChild(overlay);
	applyGeneralTranslations(overlay); 
	activePopup = overlay;

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
					// FORCE RESET: Wir zwingen den Spinner, alles neu zu berechnen
					// Sie müssen ggf. in gravitySpinner.js eine reset() methode haben oder init() muss das tun.
					gravitySpinner.init(spinnerCanvas, revealContent);
				}

				// Dasselbe für TextAnimator
				const textExplosionCanvas = document.getElementById('text-explosion-canvas');
				if (textExplosionCanvas) {
					// Auch hier wichtig: Canvas Dimensionen neu setzen
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

	const preProcessingEvents = ['EXTRACTING_COMPLETE', 'EMBEDDING_COMPLETE', 'REDUCING_8_COMPLETE', 'REDUCING_2_COMPLETE', 'CLUSTERING_COMPLETE'];
	
	const statusMap = {
		'EXTRACTING_COMPLETE': 'popup.status.embedding',
		'STILL_CLUSTERING': 'popup.status.clustering',
		'CREATING_OWN_VISUALIZATIONS': 'popup.status.visualizing',
		'IMAGE_READY': 'popup.status.visualizing',
		'COMPLETE': 'popup.status.finalizing',
		'ERROR': 'popup.status.error'
	};

	if (statusMap[status]) {
		statusText.setAttribute('data-i18n', statusMap[status]);
		statusText.textContent = t(statusMap[status]);
	} else if (status === 'ERROR') {
		statusText.removeAttribute('data-i18n');
		statusText.textContent = t('popup.status.error') + ": " + data;
	} 
	
    if (preProcessingEvents.includes(status)) {
        gravitySpinner.addParticle();
    }

    switch (status) {
        case 'EXTRACTING_COMPLETE': heldIdeaData = data; break;
        case 'STILL_CLUSTERING': heldClusterData = data; break;
        case 'CREATING_OWN_VISUALIZATIONS':
            gravitySpinner.triggerBlackHoleExplosion();
            gravitySpinner.addParticle();
            break;
        case 'IMAGE_READY':
            gravitySpinner.addParticle();
            imagesLoaded++;
            gravitySpinner.setRingProgress(imagesLoaded / TOTAL_IMAGES);
            break;
        case 'COMPLETE':
            gravitySpinner.setRingProgress(1);
            break;
        case 'ERROR':
            statusText.textContent = `Error: ${data}`;
            gravitySpinner.stop();
            modal.classList.add('is-error');
            closeBtn.style.display = 'flex';
            closeBtn.addEventListener('click', hideQueryPopup);
            eventQueue.length = 0;
            break;
        default: if (!isFinalizing) statusText.textContent = data; break;
    }
}

export function queueSseEvent(status, data) {
    console.log("SSE:", status, "Data:", data);
    eventQueue.push({ status, data });
    if (!isProcessing) processQueue();
}