/**
 * Nutzt das Intersection Observer API, um die Render-Last von
 * Ergebnis-Karten zu reduzieren, die nicht im sichtbaren Bereich sind.
 */
export function initializeCardOptimizer() {
    const cards = document.querySelectorAll('.result-card');
    if (cards.length === 0) return;

    const observer = new IntersectionObserver((entries) => {
        if (document.body.classList.contains('is-swiping-active')) {
            return;
        }

        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.classList.add('is-visible');
            } else {
                entry.target.classList.remove('is-visible');
            }
        });
    }, {
        rootMargin: '200px 0px 200px 0px'
    });

    cards.forEach(card => observer.observe(card));
}