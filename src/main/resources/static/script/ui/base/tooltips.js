export function initializeTippy() {
    // 1. HARD-CHECK: Auf echten Touch-Geräten (Smartphones) keine Tooltips initialisieren.
    // Das ist die beste Performance-Optimierung.
    const isTouchDevice = ('ontouchstart' in window) || (navigator.maxTouchPoints > 0);
    if (isTouchDevice) {
        return; // Funktion sofort beenden
    }

    // 2. Desktop & Hybrid-Geräte Konfiguration
    tippy('[data-tippy-content]:not([_tippy])', {
        delay: [500, 0], // Etwas Verzögerung, damit sie nicht sofort aufpoppen
        theme: 'custom-dark',
        animation: 'shift-away',
        
        // Performance: Tippy-Instanz erst erstellen, wenn der Tooltip wirklich gebraucht wird
        lazy: true, 

        // Trigger 'focus' entfernen hilft oft gegen ungewollte Popups bei Touch-Hybriden
        trigger: 'mouseenter',

        // 3. SWIPE-GUARD
        // Diese Funktion wird direkt vor dem Anzeigen des Tooltips ausgeführt.
        onShow(instance) {
            // Wenn der Body die Klasse vom Swipe-Skript hat, wird die Anzeige abgebrochen.
            if (document.body.classList.contains('is-swiping-active')) {
                return false; // Verhindert die Anzeige
            }
            
            // Inhalt aktualisieren (falls sich das Attribut geändert hat)
            const content = instance.reference.getAttribute('data-tippy-content');
            if (content) {
                instance.setContent(content);
            }
            return true;
        }
    });
}