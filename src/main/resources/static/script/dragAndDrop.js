import { addAttachment } from './attachmentManager.js';

document.addEventListener('DOMContentLoaded', () => {
    const dropZone = document.querySelector('.textarea-wrapper');
    if (!dropZone) return;

    dropZone.addEventListener('dragenter', (event) => {
        event.preventDefault();
        event.stopPropagation();
        dropZone.classList.add('drag-over');
    });

    dropZone.addEventListener('dragover', (event) => {
        event.preventDefault();
        event.stopPropagation();
        dropZone.classList.add('drag-over');
    });

    dropZone.addEventListener('dragleave', (event) => {
        event.preventDefault();
        event.stopPropagation();
        dropZone.classList.remove('drag-over');
    });

    dropZone.addEventListener('drop', (event) => {
        event.preventDefault();
        event.stopPropagation();
        dropZone.classList.remove('drag-over');

        const files = event.dataTransfer.files;

        if (files && files.length > 0) {
            console.log(`${files.length} Datei(en) per Drag & Drop hinzugefügt.`);
            
            for (const file of files) {
                addAttachment(file, file.type);
            }
        }
    });
});