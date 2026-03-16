import { showQueryPopup, hideQueryPopup, queueSseEvent } from '/script/features/query/queryPopup.js';
import { attachedFiles } from '/script/features/media/attachmentManager.js';
import { getCsrfToken } from '/script/core/security.js';
import { t } from '/script/core/localization.js';
import { emit, EVENTS } from '/script/core/eventBus.js';
import { renderTemplate } from '/script/core/templateManager.js';

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
	const customEditor = document.getElementById('editor-content');
	const hiddenInput = document.getElementById('idea-text-hidden') || document.querySelector('[name="idea-text"]');

	let ideaText = '';
	if (customEditor) {
		ideaText = customEditor.innerText.trim();
	} else if (hiddenInput) {
		ideaText = hiddenInput.value.trim();
	}

	if (ideaText.length > 0) {
		formData.append('idea-text', ideaText);
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

    const overlay = renderTemplate('consent-popup-overlay');
    if (!overlay) return;

    // Events nur einmal binden
    if (overlay.dataset.eventsAttached === 'true') return;

	overlay.querySelector('#consent-agree').addEventListener('click', () => {
		localStorage.setItem(GDPR_CONSENT_KEY, 'true');
		overlay.remove();
		showQueryPopup();
	});
	overlay.querySelector('#consent-privacy').addEventListener('click', () => {
		window.open('/privacy', '_blank');
	});

    overlay.dataset.eventsAttached = 'true';
}

export function attachQueryListener() {
	const ideaForm = document.querySelector('.idea-form');
	if (!ideaForm) return;
	if (ideaForm.dataset.listenerAttached === 'true') return;

	ideaForm.addEventListener('submit', function(event) {
		event.preventDefault();

		const queryButton = ideaForm.querySelector('.run-button');
		if (queryButton && queryButton.classList.contains('is-disabled')) {
			return; // Aktion abbrechen
		}

		const consentGiven = localStorage.getItem(GDPR_CONSENT_KEY) === 'true';
		if (consentGiven) showQueryPopup();
		else showConsentPopup();
	});

	ideaForm.dataset.listenerAttached = 'true';
}