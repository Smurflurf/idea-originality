let activePopup = null;

export function showQueryPopup() {
    if (activePopup) {
        activePopup.remove();
    }

    const overlay = document.createElement('div');
    overlay.className = 'recorder-overlay';

    overlay.innerHTML = `
        <div class="recorder-modal">
            <div class="recorder-header">
                <h2>Processing</h2>
            </div>
            <div class="query-popup-body">
                <div class="spinner-container">
                    <div></div><div></div><div></div>
                </div>
                <span class="popup-status-text">Uploading data & starting process...</span>
                <div class="popup-extracted-idea" style="display: none;"></div>
            </div>
        </div>
    `;

    document.body.appendChild(overlay);
    activePopup = overlay;

    setTimeout(() => {
        if (activePopup) {
            activePopup.classList.add('is-visible');
        }
    }, 10);
}

export function hideQueryPopup() {
    if (activePopup) {
        activePopup.remove();
        activePopup = null;
    }
}

export function updateQueryPopup(status, data) {
    if (!activePopup) return;

    const spinner = activePopup.querySelector('.spinner-container');
    const statusText = activePopup.querySelector('.popup-status-text');
    const ideaTextContainer = activePopup.querySelector('.popup-extracted-idea');

    if (!spinner || !statusText || !ideaTextContainer) {
        console.error("Popup-Elemente für das Update konnten nicht gefunden werden.");
        return;
    }

    switch(status) {
        case 'EXTRACTING_IDEA':
            spinner.className = 'spinner-container state-1';
            statusText.textContent = 'Extracting idea with LLM...';
            break;
        case 'EXTRACTING_COMPLETE':
            spinner.className = 'spinner-container state-2';
            statusText.textContent = 'Embedding idea into vectorspace...';
            ideaTextContainer.textContent = `Extracted Idea: "${data}"`;
            ideaTextContainer.style.display = 'block';
            break;
        case 'EMBEDDING_COMPLETE':
            spinner.className = 'spinner-container state-3';
            statusText.textContent = 'Querying database...';
            break;
        case 'QUERYING_COMPLETE':
            spinner.className = 'spinner-container state-complete';
            statusText.textContent = 'Loading results...';
            break;
        case 'ERROR':
            statusText.textContent = `Error: ${data}`;
            spinner.style.display = 'none';
            break;
    }
}