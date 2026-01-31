import { addAttachment } from '/script/features/media/attachmentManager.js';

export function initFileUpload() {
     const fileInput = document.createElement('input');
    fileInput.type = 'file';
    fileInput.multiple = true;
    fileInput.style.display = 'none';

    fileInput.addEventListener('change', (event) => {
        const files = event.target.files;
        if (files.length > 0) {
            for (const file of files) {
                addAttachment(file, file.type);
            }
        }
    });

    fileInput.click();
}