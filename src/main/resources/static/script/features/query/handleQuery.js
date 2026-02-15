import { showQueryPopup, hideQueryPopup, queueSseEvent } from '/script/features/query/queryPopup.js';
import { attachedFiles } from '/script/features/media/attachmentManager.js';
import { getCsrfToken } from '/script/core/security.js';
import { applyGeneralTranslations, t } from '/script/core/localization.js';
// NEU: Event Bus importieren
import { emit, EVENTS } from '/script/core/eventBus.js';

// BFCache Restore
window.addEventListener('pageshow', function(event) {
	if (event.persisted) {
		hideQueryPopup();
		const queryButton = document.querySelector('.idea-form .run-button');
		if (queryButton) queryButton.disabled = false;
	}
});

export function sendQuery(intent) {
	const formData = new FormData();
	const ideaTextarea = document.querySelector('textarea[name="idea-text"]');
	if (ideaTextarea) {
		const ideaText = ideaTextarea.value.trim();
		if (ideaText.length > 0) formData.append('idea-text', ideaText);
	}
	if (attachedFiles.size > 0) {
		for (const file of attachedFiles.values()) formData.append('files', file, file.name);
	}

	formData.append('intent', intent);

	const queryButton = document.querySelector('.idea-form .run-button');
	if (queryButton) queryButton.disabled = true;

	fetch('/query/init', {
		method: 'POST',
		headers: { 'X-XSRF-TOKEN': getCsrfToken() },
		body: formData
	})
		.then(response => {
			if (!response.ok) return response.json().then(err => {
				throw new Error(err.message || t('errors.server_error'));
			});
			return response.json();
		})
		.then(data => {
			const jobId = data.jobId;
			if (!jobId) throw new Error(t('errors.no_job_id')); 
			let hasReceivedTerminalEvent = false;

			const eventSource = new EventSource(`/query/status/${jobId}`);
			eventSource.onopen = function() {
				fetch(`/query/start/${jobId}`, {
					method: 'POST',
					headers: { 'X-XSRF-TOKEN': getCsrfToken() }
				}).catch(console.error);
			};

			eventSource.addEventListener('update', function(event) {
				const eventData = JSON.parse(event.data);
				queueSseEvent(eventData.status, eventData.data);

				if (eventData.status === 'COMPLETE' || eventData.status === 'ERROR') {
					hasReceivedTerminalEvent = true;
				}
				if (eventData.status === 'COMPLETE') {
					eventSource.close();
					queueSseEvent('RENDERING', t('popup.status.finalizing')); 
					
					sessionStorage.setItem(`pending_cleanup_${jobId}`, 'true');
					
                    // --- FIX 1: Soft Navigation statt Hard Reload ---
                    // Wir geben dem Popup kurz Zeit für die Animation, dann feuern wir das Event.
					setTimeout(() => { 
                        emit(EVENTS.TTS_NAVIGATE, { url: `/results/${jobId}` });
                    }, 100);

				} else if (eventData.status === 'ERROR') {
					eventSource.close();
					if (queryButton) queryButton.disabled = false;
				}
			});

			eventSource.onerror = function(error) {
				if (hasReceivedTerminalEvent) return;
				queueSseEvent('ERROR', t('errors.connection_lost'));
				eventSource.close();
				if (queryButton) queryButton.disabled = false;
			};
		})
		.catch(error => {
			console.error('Error:', error);
			queueSseEvent('ERROR', t('errors.init_failed_prefix') + " " + error.message);
			if (queryButton) queryButton.disabled = false;
		});
}

const GDPR_CONSENT_KEY = 'idea-atlas-gdpr-consent-given';

function showConsentPopup() {
	hideQueryPopup();
	const overlay = document.createElement('div');
	overlay.className = 'recorder-overlay is-visible';
	overlay.innerHTML = `
        <div class="recorder-modal query-popup-modal is-consent-mode">
            <div class="recorder-header">
                 <h2 data-i18n="popup.consent.title"><i class="fa-solid fa-shield-halved"></i> Notice on Data Processing</h2>
            </div>
            <div class="query-popup-body">
                <div class="consent-content-wrapper">
                    <p data-i18n="popup.consent.main_text">To optimize your query, it is securely forwarded to Google Gemini AI.</p>
                    <p class="google-notice" data-i18n="popup.consent.privacy_guarantee">Server located in EU, data not used for training.</p>
                    <div class="consent-popup-buttons">
                        <button id="consent-privacy" class="recorder-btn text-only" data-i18n="popup.consent.privacy_btn">Privacy Policy</button>
                        <button id="consent-agree" class="recorder-btn" data-i18n="popup.consent.agree_btn">Got it</button>
                    </div>
                </div>
            </div>
        </div>
    `;
	document.body.appendChild(overlay);
	applyGeneralTranslations(overlay);

	document.getElementById('consent-agree').addEventListener('click', () => {
		localStorage.setItem(GDPR_CONSENT_KEY, 'true');
		overlay.remove();
		showQueryPopup();
	});
	document.getElementById('consent-privacy').addEventListener('click', () => {
		window.open('/privacy', '_blank');
	});
}

export function attachQueryListener() {
	const ideaForm = document.querySelector('.idea-form');
	if (!ideaForm) return;
	if (ideaForm.dataset.listenerAttached === 'true') return;

	ideaForm.addEventListener('submit', function(event) {
		event.preventDefault();
		const consentGiven = localStorage.getItem(GDPR_CONSENT_KEY) === 'true';
		if (consentGiven) showQueryPopup();
		else showConsentPopup();
	});

	ideaForm.dataset.listenerAttached = 'true';
}