import { initFileUpload } from '/script/features/media/handleUpload.js';
import { openAudioRecorder } from '/script/features/media/handleRecord.js';

export function initializeMediaButtons() {
    const uploadButton = document.getElementById('upload-button');
    const recordButton = document.getElementById('record-button');
    const mediaToggleCheckbox = document.getElementById('media-toggle');
    const mediaToggleLabel = document.querySelector('.media-button-label');
    const mediaMenu = document.querySelector('.media-menu');

	if (mediaToggleLabel) {
	        // Verhindert, dass der Browser Mausbewegungen als Textauswahl oder Drag-Start wertet.
	        // Das garantiert, dass das 'click'-Event am Ende gefeuert wird.
	        mediaToggleLabel.addEventListener('mousedown', (e) => {
	            e.preventDefault();
	        });
	    }
	
    if (uploadButton) {
        uploadButton.addEventListener('click', () => {
            initFileUpload();
            if (mediaToggleCheckbox) mediaToggleCheckbox.checked = false;
        });
    }

    if (recordButton) {
         recordButton.addEventListener('click', () => {
            openAudioRecorder();
            if (mediaToggleCheckbox) mediaToggleCheckbox.checked = false;
        });
    }

	// Globaler Klick-Handler für das Menü-Schließen (document.addEventListener)
	// Das ist okay hier drin, solange initializeMediaButtons nur EINMAL aufgerufen wird.
    document.addEventListener('click', (event) => {
        if (!mediaToggleCheckbox || !mediaToggleCheckbox.checked) {
            return;
        }
        const target = event.target;
        const clickedInsideMenu = target.closest('.media-menu');
        const clickedOnButton = target.closest('.media-button-label');
        const clickedOnCheckbox = target === mediaToggleCheckbox;
        if (!clickedInsideMenu && !clickedOnButton && !clickedOnCheckbox) {
            mediaToggleCheckbox.checked = false;
        }
    });
}