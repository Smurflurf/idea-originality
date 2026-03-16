/**
 * Verarbeitet Viewport-Änderungen (z.B. Öffnen der mobilen Tastatur)
 * und sorgt für eine perfekte Zentrierung der Eingabefelder.
 */
export function initViewportManager() {
    if (!window.visualViewport || window.viewportManagerAttached) return;
    window.viewportManagerAttached = true;

    // HILFSFUNKTION FIX: Prüft, ob ein Element ein Eingabefeld ist 
    // (inklusive contenteditable divs wie unserem neuen Custom-Editor!)
    const isEditableElement = (el) => {
        return el && (el.tagName === 'TEXTAREA' || el.tagName === 'INPUT' || el.isContentEditable);
    };

    const handleResize = () => {
        const vv = window.visualViewport;
        const windowHeight = window.innerHeight;
        
        // 1. SICHERHEITS-CHECK: Ist überhaupt ein Textfeld aktiv?
        const activeEl = document.activeElement;
        const isInputFocused = isEditableElement(activeEl);

        let keyboardHeight = 0;
        let isKeyboardOpen = false;

        if (isInputFocused) {
            keyboardHeight = Math.max(0, windowHeight - vv.height);
            isKeyboardOpen = keyboardHeight > 100;
        }

        // ---> NEU: Ort des Eingabefeldes bestimmen <---
        const isInSidebar = activeEl && !!activeEl.closest('#sidebar-menu');

        // 2. Paddings anpassen
        const sidebar = document.getElementById('sidebar-menu');
        if (sidebar) {
            // Padding in der Sidebar hinzufügen, damit der unterste Punkt nicht
            // hinter der Tastatur verschwindet.
            sidebar.style.paddingBottom = isKeyboardOpen ? `${keyboardHeight}px` : '';
        }
        
        // Das Body-Padding für die Hauptseite NUR setzen, wenn wir NICHT im Menü sind
        if (!isInSidebar) {
            document.body.style.paddingBottom = isKeyboardOpen ? `${keyboardHeight}px` : '';
        } else {
            document.body.style.paddingBottom = '';
        }

        // 3. Exakte Zentrierung des aktiven Elements
        if (isKeyboardOpen && isInputFocused) {
            setTimeout(() => {
                if (document.activeElement !== activeEl) return;

                // =========================================================
                // IOS SAFARI FIX:
                // Wenn wir im fixierten Menü sind, bloß KEIN window.scrollTo aufrufen!
                // =========================================================
                if (isInSidebar) {
                    // Wir stellen nur sicher, dass das Feld innerhalb des Menüs gescrollt wird
                    activeEl.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
                    return; // GANZ WICHTIG: Abbruch!
                }

                // =========================================================
                // Normales Verhalten (für die große Textarea auf der Startseite)
                // =========================================================
                const rect = activeEl.getBoundingClientRect();
                
                let visualOffset = 0;
                // FIX: Offset greift nun beim alten Textarea-Namen ODER der neuen Custom-Editor ID
                if (activeEl.getAttribute('name') === 'idea-text' || activeEl.id === 'editor-content') {
                    visualOffset = 29; 
                }

                const elementMidInDocument = window.scrollY + rect.top + (rect.height / 2);
                const targetScrollY = (elementMidInDocument - visualOffset) - (vv.height / 2);

                window.scrollTo({
                    top: Math.max(0, targetScrollY),
                    behavior: 'smooth'
                });
            }, 150);
        }
    };

    // Auf Viewport-Änderungen hören
    window.visualViewport.addEventListener('resize', handleResize);

    // Zusätzlich auf das Verlassen von Eingabefeldern hören (Sicherheitsnetz)
    document.addEventListener('focusout', (e) => {
        if (isEditableElement(e.target)) {
            setTimeout(handleResize, 100);
        }
    });
    
    // Wenn ein Feld angetippt wird, sofort berechnen
    document.addEventListener('focusin', (e) => {
        if (isEditableElement(e.target)) {
            handleResize();
        }
    });
}
