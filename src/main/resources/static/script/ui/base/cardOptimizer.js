/**
 * Nutzt das Intersection Observer API, um die Render-Last von
 * Ergebnis-Karten zu reduzieren, die nicht im sichtbaren Bereich sind.
 */
export function initializeCardOptimizer() {
    const cards = document.querySelectorAll('.result-card');
    if (cards.length === 0) return;

    // Der Observer wird einmal erstellt und beobachtet dann alle Karten
    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            // 'isIntersecting' ist true, wenn die Karte im Viewport ist
            if (entry.isIntersecting) {
                // Mache die Karte "aktiv" für den Browser
                entry.target.classList.add('is-visible');
            } else {
                // Sage dem Browser, dass er sich um diese Karte nicht kümmern muss
                entry.target.classList.remove('is-visible');
            }
        });
    }, {
        // Optional: Ein 'rootMargin' sorgt dafür, dass die Karten schon
        // geladen werden, bevor sie ganz im Bild sind, was flüssiger wirkt.
        rootMargin: '200px 0px 200px 0px'
    });

    // Weise den Observer an, jede einzelne Karte zu beobachten
    cards.forEach(card => {
        observer.observe(card);
    });
}

document.addEventListener('DOMContentLoaded', initializeCardOptimizer);