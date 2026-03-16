import { emit, EVENTS } from '/script/core/eventBus.js';
import { getTemplate } from '/script/core/templateManager.js';
import { t } from '/script/core/localization.js';

const attachedFiles = new Map();
export { attachedFiles };

// --- TOKEN & FILE CONFIG ---
export const MAX_TOKENS = 220000;

const ALLOWED_MIME_TYPES =[
    'application/pdf', 
    'text/plain', 'text/csv', 'text/markdown', 'application/json',
    'image/png', 'image/jpeg', 'image/webp', 'image/heic', 'image/heif', 
    'audio/wav', 'audio/ogg', 'audio/flac', 'audio/mp3', 'audio/aiff', 'audio/aac',
	'video/mp4',
	'video/mpeg',
	'video/mov',
	'video/avi',
	'video/x-flv',
	'video/mpg',
	'video/webm',
	'video/wmv',
	'video/3gpp'
];

// Schätzt die Tokens einer einzelnen Datei basierend auf deinen Regeln
export function estimateFileTokens(file) {
    const type = file.type || '';
    const size = file.size; // in Bytes

    if (type.startsWith('image/')) {
        return 5000; // Pauschal 5k pro Bild
    }
    if (type.startsWith('text/') || type === 'application/json') {
        return size; // 1 Byte = 1 Token (sehr sicher)
    }
    if (type === 'application/pdf') {
        // PDF: Heuristik. 1 MB = ~50.000 Tokens (1 Byte = 0.05 Tokens)
        return Math.ceil(size * 0.05); 
    }
    if (type.startsWith('audio/')) {
        // Audio: 1 MB Audio (mp3/webm) = ca. 1 Minute = ca. 2000 Tokens. 
        // Wir rechnen sicherheitshalber mit 5000 Tokens pro MB.
        return Math.ceil((size / (1024 * 1024)) * 5000);
    }
    
    return Math.ceil(size * 0.05); // Fallback
}

// Berechnet die Tokens aller aktuell angehängten Dateien
export function getTotalFileTokens() {
    let total = 0;
    for (const file of attachedFiles.values()) {
        total += estimateFileTokens(file);
    }
    return total;
}

function validateFile(file) {
    const fileType = file.type || ''; 
    
    // 1. Typ-Prüfung
    if (!ALLOWED_MIME_TYPES.includes(fileType)) {
        alert(t('errors.unsupported_file') || `Der Dateityp von "${file.name}" wird nicht unterstützt.`);
        return false;
    }

    // 2. Token-Prüfung für DIESE Datei im Kontext der bereits angehängten Dateien
    const currentFilesTokens = getTotalFileTokens();
    const newFileTokens = estimateFileTokens(file);
    
    // Wir prüfen hier nur die Dateien (Textfeld-Tokens kommen gleich im ButtonManager dazu)
    if (currentFilesTokens + newFileTokens > MAX_TOKENS) {
        alert(t('errors.too_many_tokens') || `Datei "${file.name}" ist zu groß. Das Limit von ${MAX_TOKENS} Tokens würde überschritten.`);
        return false;
    }
    
    return true;
}

function dispatchAttachmentsChangedEvent() {
    emit(EVENTS.ATTACHMENTS_CHANGED);
}

export function addAttachment(file, fileType) {
	// Wenn die Datei nicht durch den Filter kommt, abbrechen!
	if (!validateFile(file)) {
		return;
	}

	const container = document.getElementById('attachments-container');
    container.classList.add('has-attachments');

    const attachmentId = `attachment-${Date.now()}-${Math.random()}`;
    attachedFiles.set(attachmentId, file);
    dispatchAttachmentsChangedEvent();

    // 1. Template holen
    const fragment = getTemplate('tpl-attachment-card');
    if (!fragment) return;
    
    const card = fragment.firstElementChild;
    card.id = attachmentId;

    // 2. Elemente befüllen
    const preview = card.querySelector('.attachment-preview');
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

    card.querySelector('.attachment-caption').textContent = file.name;

    // 3. Delete Listener
    const removeBtn = card.querySelector('.remove-btn');
    removeBtn.addEventListener('click', () => {
        attachedFiles.delete(card.id);
        card.remove();
        dispatchAttachmentsChangedEvent();
        if (container.children.length === 0) {
            container.classList.remove('has-attachments');
        }
    });

    container.appendChild(card);
}

/**
 * Setzt den Attachment-Manager komplett zurück.
 * Leert die interne Map, räumt die UI auf und benachrichtigt andere Module.
 */
export function clearAllAttachments() {
	attachedFiles.clear();

	const container = document.getElementById('attachments-container');
	if (container) {
		container.innerHTML = ''; // Alle Kind-Elemente entfernen
		container.classList.remove('has-attachments');
	}

	// Wichtig: Andere Teile der App informieren (z.B. den Query Button),
	// damit diese ihren Zustand auch aktualisieren.
	dispatchAttachmentsChangedEvent();
}

// Event-Listener für den Back-Forward Cache
window.addEventListener('pageshow', (event) => {
	// Das 'persisted' Flag ist nur true, wenn die Seite aus dem bfcache geladen wird.
	if (event.persisted) {
		console.log("[AttachmentManager] Page restored from bfcache. Clearing all attachments.");
		clearAllAttachments();
	}
});
