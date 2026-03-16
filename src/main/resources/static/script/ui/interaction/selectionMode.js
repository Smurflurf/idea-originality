import { closeSidebar } from '/script/ui/navigation/menu.js';
import * as Translate from '/script/features/accessibility/translate.js';
import * as TTS from '/script/features/accessibility/tts.js';
import { getTemplate, renderTemplate } from '/script/core/templateManager.js';

// --- STATE ---
let activeMode = null; // null, 'translate', 'read'
let currentHoveredElement = null;
let cursorElement = null;

// --- CONFIG ---
const VALID_TAGS = ['DIV', 'SECTION', 'MAIN', 'ARTICLE', 'HEADER', 'FOOTER', 'UL', 'LI', 'P', 'H1', 'H2', 'H3', 'H4', 'H5', 'H6', 'SPAN', 'A', 'LABEL'];
const FORBIDDEN_SELECTORS = [
	'.sidebar-menu', 
	'.menu-overlay', 
	'.download-popup-overlay', 
	'.recorder-overlay', 
	'.is-translating', 
	'.expand-button',
	'.toggle-hierarchy-btn',
	'.toggle-json-btn',
];

export const isSelectionModeActive = () => activeMode !== null;

// --- INITIALIZATION ---
export function initializeSelectionMode() {
    const translateBtn = document.getElementById('translate-mode-btn');
    const readBtn = document.getElementById('read-text-mode-btn');

    if (translateBtn) {
        const newTranslateBtn = translateBtn.cloneNode(true);
        translateBtn.parentNode.replaceChild(newTranslateBtn, translateBtn);
        
        newTranslateBtn.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            toggleMode('translate', e); // Event weitergeben
        });
    }

    if (readBtn) {
        const newReadBtn = readBtn.cloneNode(true);
        readBtn.parentNode.replaceChild(newReadBtn, readBtn);

        newReadBtn.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            toggleMode('read', e); // Event weitergeben
        });
    }
    
    if (!window.selectionModeKeyHandlerAttached) {
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && activeMode) stopMode();
        });
        window.selectionModeKeyHandlerAttached = true;
    }
}

// --- MODE CONTROL ---
// FIX 3: Event empfangen
function toggleMode(mode, event) {
    if (activeMode === mode) {
        stopMode();
    } else {
        startMode(mode, event);
    }
}

function startMode(mode, event) {
    closeSidebar();
    
    activeMode = mode;
    document.body.classList.add('translate-mode-active');
    
    const translateMenu = document.getElementById('translate-menu-wrapper');
    if (translateMenu) translateMenu.classList.add('is-active');

    if (!cursorElement || !document.body.contains(cursorElement)) {
        cursorElement = document.querySelector('.custom-translate-cursor');
        if (!cursorElement) {
            createCursorElement();
        }
    }
    cursorElement.style.display = 'block';
    
    // FIX 3: Cursor sofort positionieren
    if (cursorElement && event) {
        cursorElement.style.left = event.clientX + 'px';
        cursorElement.style.top = event.clientY + 'px';
    }
    
    updateUI(mode);
    
    document.addEventListener('mousemove', moveVirtualCursor);
    document.addEventListener('mouseover', handleMouseOver);
    document.addEventListener('mouseout', handleMouseOut);
    
    setTimeout(() => {
        document.addEventListener('click', handleClick, { capture: true });
    }, 50);
}

export function stopMode() {
    if (!activeMode) return;
    activeMode = null;

    document.body.classList.remove('translate-mode-active');
    
    const translateMenu = document.getElementById('translate-menu-wrapper');
    if (translateMenu) translateMenu.classList.remove('is-active');

    // UI Reset
    const translateBtn = document.getElementById('translate-mode-btn');
    const readBtn = document.getElementById('read-text-mode-btn');
    const menuTrigger = document.getElementById('menu-trigger');

    if (translateBtn) translateBtn.classList.remove('is-active-mode');
    if (readBtn) readBtn.classList.remove('is-active-mode');
    if (menuTrigger) {
        menuTrigger.innerHTML = '<i class="fa-solid fa-bars"></i>';
        menuTrigger.classList.remove('is-translate-active');
    }
    
    const tIndicator = document.getElementById('translate-active-indicator');
    const rIndicator = document.getElementById('read-active-indicator');
    if (tIndicator) tIndicator.style.opacity = '0';
    if (rIndicator) rIndicator.style.opacity = '0';

    if (cursorElement) {
        cursorElement.style.display = 'none';
        cursorElement.style.setProperty('display', 'none', 'important');
    }

    if (currentHoveredElement) {
        currentHoveredElement.classList.remove('translate-highlight');
        currentHoveredElement = null;
    }

    // Cleanup Listeners
    document.removeEventListener('mousemove', moveVirtualCursor);
    document.removeEventListener('mouseover', handleMouseOver);
    document.removeEventListener('mouseout', handleMouseOut);
    document.removeEventListener('click', handleClick, { capture: true });
}

// --- UI UPDATES ---
function updateUI(mode) {
	const translateBtn = document.getElementById('translate-mode-btn');
	const readBtn = document.getElementById('read-text-mode-btn');
	const menuTrigger = document.getElementById('menu-trigger');

	const tIndicator = document.getElementById('translate-active-indicator');
	const rIndicator = document.getElementById('read-active-indicator');

	if (mode === 'translate') {
		cursorElement.textContent = '';
		const icon = document.createElement('i');
		icon.className = 'fa-solid fa-language';
		cursorElement.appendChild(icon);

		if (translateBtn) translateBtn.classList.add('is-active-mode');
		if (readBtn) readBtn.classList.remove('is-active-mode');
		if (tIndicator) tIndicator.style.opacity = '1';
		if (rIndicator) rIndicator.style.opacity = '0';

		if (menuTrigger) {
			menuTrigger.textContent = '';
			const mIcon = document.createElement('i');
			mIcon.className = 'fa-solid fa-language';
			menuTrigger.appendChild(mIcon);
		}
	} else {
		cursorElement.textContent = '';
		const icon = document.createElement('i');
		icon.className = 'fa-solid fa-volume-high';
		cursorElement.appendChild(icon);

		if (readBtn) readBtn.classList.add('is-active-mode');
		if (translateBtn) translateBtn.classList.remove('is-active-mode');
		if (rIndicator) rIndicator.style.opacity = '1';
		if (tIndicator) tIndicator.style.opacity = '0';

		if (menuTrigger) {
			menuTrigger.textContent = '';
			const mIcon = document.createElement('i');
			mIcon.className = 'fa-solid fa-volume-high';
			menuTrigger.appendChild(mIcon);
		}
	}

	if (menuTrigger) menuTrigger.classList.add('is-translate-active');
}


function createCursorElement() {
    cursorElement = document.createElement('div');
    cursorElement.className = 'custom-translate-cursor';
    document.body.appendChild(cursorElement);
}

function moveVirtualCursor(event) {
    if (cursorElement) {
        cursorElement.style.left = event.clientX + 'px';
        cursorElement.style.top = event.clientY + 'px';
    }
}

// --- TARGET VALIDATION ---
function isValidTarget(element) {
    if (!element) return false;
    for (const selector of FORBIDDEN_SELECTORS) {
        if (element.closest(selector)) return false;
    }
    const tagName = element.tagName;
    if (!VALID_TAGS.includes(tagName)) return false;
	
    const textContent = element.textContent.trim();
    if (textContent.length === 0) return false;
	
    if (['DIV', 'SECTION', 'MAIN', 'ARTICLE', 'HEADER', 'FOOTER', 'UL', 'LI'].includes(tagName)) {
        const hasDirectText = Array.from(element.childNodes).some(node => 
            node.nodeType === Node.TEXT_NODE && node.textContent.trim().length > 0
        );
        if (!hasDirectText) return false;
    }
    return true;
}

function handleMouseOver(event) {
    let target = event.target;
    if (!isValidTarget(target) && target.parentElement && isValidTarget(target.parentElement)) {
        target = target.parentElement;
    }
    if (isValidTarget(target) && target !== currentHoveredElement) {
        if (currentHoveredElement) currentHoveredElement.classList.remove('translate-highlight');
        currentHoveredElement = target;
        currentHoveredElement.classList.add('translate-highlight');
    }
}

function handleMouseOut(event) {
    if (currentHoveredElement) {
        if (!currentHoveredElement.contains(event.relatedTarget)) {
             currentHoveredElement.classList.remove('translate-highlight');
             currentHoveredElement = null;
        }
    }
}

function handleClick(event) {
    if (event.target.closest('.sidebar-menu')) return; 
    
    event.preventDefault();
    event.stopPropagation();
    
    const targetElement = currentHoveredElement || event.target;

    if (!isValidTarget(targetElement)) {
        stopMode();
        return;
    }
    
    const modeToExecute = activeMode;
    stopMode(); // Modus beenden

    if (modeToExecute === 'translate') {
        Translate.executeTranslation(targetElement);
    } else if (modeToExecute === 'read') {
        TTS.executeReading(targetElement);
    }
}