import { attachedFiles } from '/script/features/media/attachmentManager.js';
import { on, EVENTS } from '/script/core/eventBus.js';


// Variable im Modul-Scope statt globaler window-Variable.
// Sie merkt sich, ob der document-Listener schon gesetzt wurde.
let globalsInitialized = false;

/**
 * Zentralisierte Funktion zum Prüfen des Button-Status.
 * Holt sich die Elemente jedes Mal frisch aus dem DOM, 
 * da sie durch SPA-Navigation ausgetauscht worden sein könnten.
 */
function updateQueryButtonState() {
	const queryButton = document.querySelector('.run-button');
	const ideaTextarea = document.querySelector('textarea[name="idea-text"]');

	// Wenn Elemente nicht da sind (z.B. falsche Seite), nichts tun
	if (!queryButton || !ideaTextarea) return;

	const hasText = ideaTextarea.value.trim().length > 0;
	const hasAttachments = attachedFiles.size > 0;

	// Button aktivieren, wenn Text ODER Anhänge da sind
	queryButton.disabled = !(hasText || hasAttachments);
}

export function initializeQueryButtonLogic() {
	const ideaTextarea = document.querySelector('textarea[name="idea-text"]');
	// Feature Detection: Wenn wir nicht auf der Suchseite sind, abbrechen.
	if (!ideaTextarea) return;

	// 1. Lokaler Listener: Input Event auf dem Textfeld
	// Dieser muss bei jedem Seitenaufruf (handlePageChange) neu an das 
	// NEUE Textarea-Element gebunden werden.
	ideaTextarea.addEventListener('input', updateQueryButtonState);

	if (!globalsInitialized) {
		on(EVENTS.ATTACHMENTS_CHANGED, () => {
			updateQueryButtonState();
		});
		globalsInitialized = true;
	}
	
	updateQueryButtonState();
}