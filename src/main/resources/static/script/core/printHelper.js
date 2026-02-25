// in /script/core/printHelper.js

export function initPrintHelper() {
    let previousTheme = null;

    // Vor dem Drucken:
    window.addEventListener('beforeprint', () => {
        const html = document.documentElement;
        
        // 1. Aktuelles Theme merken und auf LIGHT erzwingen (für korrekte Button-Farben)
        previousTheme = html.getAttribute('data-theme');
        html.setAttribute('data-theme', 'light');

        // 2. Viz-Pane Logik (Original von dir, unverändert)
        const activePane = document.querySelector('.viz-content-pane.active');
        if (activePane) {
            activePane.dataset.savedTransform = activePane.style.transform;
            activePane.style.transform = '';
            activePane.style.position = 'static';
            activePane.style.overflow = 'visible';
            activePane.style.height = 'auto';
        }
    });

    // Nach dem Drucken:
    window.addEventListener('afterprint', () => {
        const html = document.documentElement;

        // 1. Theme wiederherstellen
        if (previousTheme) {
            html.setAttribute('data-theme', previousTheme);
        }

        // 2. Viz-Pane Logik wiederherstellen
        const activePane = document.querySelector('.viz-content-pane.active');
        if (activePane) {
            activePane.style.transform = activePane.dataset.savedTransform || '';
            activePane.style.position = ''; 
            activePane.style.overflow = '';
            activePane.style.height = '';
            
            window.dispatchEvent(new Event('scroll'));
        }
    });
}