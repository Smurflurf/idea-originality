import { emit, EVENTS } from '/script/core/eventBus.js';


const attachedFiles = new Map();
export { attachedFiles };

function dispatchAttachmentsChangedEvent() {
    emit(EVENTS.ATTACHMENTS_CHANGED);
}

export function addAttachment(file, fileType) {
    const container = document.getElementById('attachments-container');
    container.classList.add('has-attachments');

    const attachmentId = `attachment-${Date.now()}-${Math.random()}`;
    attachedFiles.set(attachmentId, file);
    dispatchAttachmentsChangedEvent();

    const card = document.createElement('div');
    card.className = 'attachment-card';
    card.id = attachmentId;

    const preview = document.createElement('div');
    preview.className = 'attachment-preview';
    
    if (fileType.startsWith('image/')) {
        const img = document.createElement('img');
        img.src = URL.createObjectURL(file);
        img.onload = () => URL.revokeObjectURL(img.src);
        preview.appendChild(img);
    } else if (fileType === 'audio') {
        const audio = document.createElement('audio');
        audio.src = URL.createObjectURL(file);
        audio.controls = true;
        preview.appendChild(audio);
    } else {
        const icon = document.createElement('i');
        icon.className = 'fa-regular fa-file-lines';
        preview.appendChild(icon);
    }

    const caption = document.createElement('div');
    caption.className = 'attachment-caption';
    caption.textContent = file.name;

    const removeBtn = document.createElement('button');
    removeBtn.className = 'remove-btn';
    removeBtn.innerHTML = '<i class="fa-solid fa-xmark"></i>';
    removeBtn.addEventListener('click', () => {
        attachedFiles.delete(card.id);
        card.remove();
        dispatchAttachmentsChangedEvent();
        if (container.children.length === 0) {
            container.classList.remove('has-attachments');
        }
    });

    card.appendChild(preview);
    card.appendChild(caption);
    card.appendChild(removeBtn);

    container.appendChild(card);
}