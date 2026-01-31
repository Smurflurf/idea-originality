import { attachedFiles } from '/script/features/media/attachmentManager.js';

// 1. Export hinzufügen
export function initializeQueryButtonLogic() {
    const queryButton = document.querySelector('.run-button');
    const ideaTextarea = document.querySelector('textarea[name="idea-text"]');

    // Feature Detection: Wenn die Elemente nicht da sind, brich ab.
    if (!queryButton || !ideaTextarea) return;

    // Alte Listener entfernen ist schwer, da anonyme Funktionen.
    // Aber da wir bei SPA den Body tauschen, sind die alten Elemente eh weg (Garbage Collection).
    // Wir müssen uns nur um die neuen kümmern.

    function updateQueryButtonState() {
        const hasText = ideaTextarea.value.trim().length > 0;
        const hasAttachments = attachedFiles.size > 0;
        queryButton.disabled = !(hasText || hasAttachments);
    }

    // Input Listener neu binden
    ideaTextarea.addEventListener('input', updateQueryButtonState);

    // Initialer Check für den aktuellen State
    updateQueryButtonState();
}

// Globaler Event Listener für Attachments (bleibt global, da idempotent)
if (!window.queryButtonManagerInitialized) {
    document.addEventListener('attachmentsChanged', () => {
        // Hier holen wir die Elemente JEDES MAL frisch aus dem DOM
        const queryButton = document.querySelector('.run-button');
        const ideaTextarea = document.querySelector('textarea[name="idea-text"]');

        if (queryButton && ideaTextarea) {
             const hasText = ideaTextarea.value.trim().length > 0;
             const hasAttachments = attachedFiles.size > 0;
             queryButton.disabled = !(hasText || hasAttachments);
        }
    });
    window.queryButtonManagerInitialized = true;
}

// 2. Selbst-Start beim ersten Laden (Hard Refresh)
// Wir prüfen, ob wir schon im Modul-Kontext sind oder warten müssen
if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', initializeQueryButtonLogic);
} else {
    initializeQueryButtonLogic();
}