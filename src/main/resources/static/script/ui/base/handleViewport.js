/**
 * Verarbeitet Viewport-Änderungen (z.B. Öffnen der mobilen Tastatur)
 * und sorgt für eine perfekte Zentrierung der Eingabefelder.
 */
export function initViewportManager() {
    if (!window.visualViewport || window.viewportManagerAttached) return;
    window.viewportManagerAttached = true;

    const handleResize = () => {
        const vv = window.visualViewport;
        const windowHeight = window.innerHeight;
        const keyboardHeight = windowHeight - vv.height;
        const isKeyboardOpen = keyboardHeight > 100;

        // 1. Sidebar & Body-Padding anpassen (für Scroll-Spielraum)
        const sidebar = document.getElementById('sidebar-menu');
        if (sidebar) sidebar.style.paddingBottom = isKeyboardOpen ? `${keyboardHeight}px` : '';
        
        // Wichtig: Padding am Body erlaubt es uns, das Element manuell in die Mitte zu schieben,
        // auch wenn die Seite eigentlich nicht lang genug zum Scrollen wäre.
        document.body.style.paddingBottom = isKeyboardOpen ? `${keyboardHeight}px` : '';

        // 2. Exakte Zentrierung des aktiven Elements
        if (isKeyboardOpen) {
            const activeEl = document.activeElement;
            const isInput = activeEl && (activeEl.tagName === 'TEXTAREA' || activeEl.tagName === 'INPUT');
            
            if (isInput) {
                // Kurzer Timeout, damit die Layout-Engine die neuen Paddings verarbeitet hat
                setTimeout(() => {
                    const rect = activeEl.getBoundingClientRect();
                    
                    /**
                     * KORREKTUR-LOGIK:
                     * Die Textarea hat padding-top: 12px und padding-bottom: 70px.
                     * Um die *optische* Mitte des Textbereichs zu treffen, müssen wir den 
                     * Versatz (70 - 12) / 2 = 29px korrigieren.
                     */
                    let visualOffset = 0;
                    if (activeEl.getAttribute('name') === 'idea-text') {
                        visualOffset = 29; 
                    }

                    // Berechnung: Wo liegt die Mitte des Elements im Dokument minus der optischen Korrektur?
                    const elementMidInDocument = window.scrollY + rect.top + (rect.height / 2);
                    // Ziel: Dieser Punkt soll in der Mitte des Sichtfensters (vv.height / 2) liegen.
                    const targetScrollY = (elementMidInDocument - visualOffset) - (vv.height / 2);

                    window.scrollTo({
                        top: Math.max(0, targetScrollY),
                        behavior: 'smooth'
                    });
                }, 150);
            }
        }
    };

    window.visualViewport.addEventListener('resize', handleResize);
}