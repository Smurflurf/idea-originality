import { initializeTippy } from '/script/ui/base/tooltips.js';
import { getTemplate } from '/script/core/templateManager.js';
import { applyGeneralTranslations } from '/script/core/localization.js';

let isListenerInitialized = false;

/**
 * Initialisiert einen globalen Click-Listener, der das Öffnen und Schließen von Abstracts steuert.
 */
function initializeGlobalAbstractToggleListener() {
    if (isListenerInitialized) {
        return;
    }

    document.body.addEventListener('click', function(event) {
        const expandButton = event.target.closest('.expand-button');
        if (!expandButton) {
            return;
        }
        event.stopPropagation();
        
        const wrapper = expandButton.closest('.abstract-wrapper');
        if (!wrapper) return;

        const parentContext = wrapper.closest('.tippy-content') || document.body;
        const currentlyExpanded = parentContext.querySelector('.abstract-wrapper.expanded');

        // Wenn ein anderes Abstract geöffnet ist, schließe es und setze seine Scroll-Position zurück.
        if (currentlyExpanded && currentlyExpanded !== wrapper) {
            const pToClose = currentlyExpanded.querySelector('.expandable-abstract');
            if (pToClose) {
                pToClose.scrollTop = 0; // WICHTIG: Scroll-Position auf 0 setzen.
            }
            currentlyExpanded.classList.remove('expanded');
        }
        
        // Das geklickte Abstract umschalten
        const wasJustClosed = wrapper.classList.contains('expanded');
        wrapper.classList.toggle('expanded');
        
        // Wenn es gerade geschlossen wurde, setze auch hier die Scroll-Position zurück.
        if (wasJustClosed) {
            const pToToggle = wrapper.querySelector('.expandable-abstract');
            if (pToToggle) {
                pToToggle.scrollTop = 0;
            }
        }
    });

    isListenerInitialized = true;
}

/**
 * Erstellt die "Mehr anzeigen"-Buttons für alle notwendigen Elemente
 * innerhalb eines bestimmten Containers.
 * @param {HTMLElement} container - Das Elternelement, in dem gesucht werden soll.
 */
export function initializeAbstractButtonsFor(container) {
    initializeGlobalAbstractToggleListener();
    
    container.querySelectorAll('.expandable-abstract:not(.processed-for-toggle)').forEach(abstractElement => {
        const needsButton = abstractElement.scrollHeight > abstractElement.clientHeight + 2;

        if (needsButton) {
            const wrapper = document.createElement('div');
            wrapper.className = 'abstract-wrapper';
            abstractElement.parentNode.insertBefore(wrapper, abstractElement);
            wrapper.appendChild(abstractElement);

            // Template holen
            const fragment = getTemplate('tpl-abstract-expand-btn');
            if (fragment) {
                const expandButton = fragment.firstElementChild;
                wrapper.appendChild(expandButton);
                
                // Falls data-tippy-content übersetzt werden muss
                applyGeneralTranslations(wrapper);
            }
        }
        abstractElement.classList.add('processed-for-toggle');
    });
    
    initializeTippy();
}