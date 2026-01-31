import { initializeTippy } from '/script/ui/base/tooltips.js';

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
    
    // Hinzufügen einer Klasse, um bereits verarbeitete Elemente zu ignorieren
    container.querySelectorAll('.expandable-abstract:not(.processed-for-toggle)').forEach(abstractElement => {
        // Prüfen, ob der Inhalt tatsächlich länger ist als der sichtbare Bereich
        const needsButton = abstractElement.scrollHeight > abstractElement.clientHeight + 2;

        if (needsButton) {
            const wrapper = document.createElement('div');
            wrapper.className = 'abstract-wrapper';
            abstractElement.parentNode.insertBefore(wrapper, abstractElement);
            wrapper.appendChild(abstractElement);

            const expandButton = document.createElement('span');
            expandButton.className = 'expand-button';
            expandButton.innerHTML = '<i class="fas fa-ellipsis"></i>';
            expandButton.setAttribute('data-tippy-content', 'Show full description');
            wrapper.appendChild(expandButton);
        }
        // Element als verarbeitet markieren
        abstractElement.classList.add('processed-for-toggle');
    });
    
    initializeTippy();
}