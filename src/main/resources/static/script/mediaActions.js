import { initFileUpload } from './handleUpload.js';
import { openAudioRecorder } from './handleRecord.js';

document.addEventListener('DOMContentLoaded', () => {
    const uploadButton = document.getElementById('upload-button');
    const recordButton = document.getElementById('record-button');
    const mediaToggleCheckbox = document.getElementById('media-toggle');

    if (uploadButton) {
        uploadButton.addEventListener('click', () => {
            initFileUpload();
            mediaToggleCheckbox.checked = false;
        });
    }

    if (recordButton) {
         recordButton.addEventListener('click', () => {
            openAudioRecorder();
            mediaToggleCheckbox.checked = false;
        });
    }
});