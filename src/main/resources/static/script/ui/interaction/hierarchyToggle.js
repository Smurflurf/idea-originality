let isInitialized = false;

export function initializeHierarchyToggles() {
    if (isInitialized) {
        return; // Verhindert, dass der Listener mehrfach hinzugefügt wird
    }

    document.body.addEventListener('click', function(event) {
        
        // 1. Prüfen, ob auf einen "Ausklappen"-Button geklickt wurde.
        const toggleButton = event.target.closest('.toggle-hierarchy-btn');
        if (toggleButton) {
            event.stopPropagation(); 
            const wrapper = toggleButton.closest('.hierarchy-list-wrapper');
            if (wrapper) {
                wrapper.classList.toggle('is-collapsed');
            }
            return; 
        }

        // 2. Prüfen, ob auf das letzte Element einer eingeklappten Liste geklickt wurde.
        const itemBox = event.target.closest('.hierarchy-item-box');
        if (itemBox) {
            const wrapper = itemBox.closest('.hierarchy-list-wrapper.is-collapsed');
            if (wrapper && itemBox.closest('li:last-child')) {
                event.stopPropagation();
                wrapper.classList.remove('is-collapsed');
            }
        }
    });

    isInitialized = true;
}