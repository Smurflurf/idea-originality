const cleanupQueue = [];

/**
 * Registriert eine Funktion, die beim nächsten Seitenwechsel (oder Reload)
 * ausgeführt werden soll, um Speicher freizugeben (Observer disconnecten, EventListener entfernen).
 * @param {Function} cleanupFn - Die Funktion, die das Aufräumen übernimmt.
 */
export function registerCleanup(cleanupFn) {
    if (typeof cleanupFn === 'function') {
        cleanupQueue.push(cleanupFn);
    }
}

/**
 * Führt alle registrierten Aufräum-Funktionen aus und leert die Liste.
 * Wird typischerweise von main.js oder navigation.js aufgerufen.
 */
export function executeGlobalCleanup() {
    // Wir arbeiten die Liste von hinten ab (LIFO), oft sicherer bei Abhängigkeiten
    while (cleanupQueue.length > 0) {
        const fn = cleanupQueue.pop();
        try {
            fn();
        } catch (e) {
            // WICHTIG: Wenn ein Cleanup fehlschlägt, darf das die App nicht crashen!
            // Das war vermutlich der Grund, warum dein Menü vorhin nicht ging.
            console.warn("[Lifecycle] Cleanup error:", e);
        }
    }
}