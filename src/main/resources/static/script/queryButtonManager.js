import { attachedFiles } from './attachmentManager.js';

const queryButton = document.querySelector('.run-button');
const ideaTextarea = document.querySelector('textarea[name="idea-text"]');

function updateQueryButtonState() {
    if (!queryButton || !ideaTextarea) return;

    const hasText = ideaTextarea.value.trim().length > 0;
    const hasAttachments = attachedFiles.size > 0;

    if (hasText || hasAttachments) {
        queryButton.disabled = false;
    } else {
        queryButton.disabled = true;
    }
}

if (ideaTextarea) {
    ideaTextarea.addEventListener('input', updateQueryButtonState);
}
document.addEventListener('attachmentsChanged', updateQueryButtonState);

updateQueryButtonState();