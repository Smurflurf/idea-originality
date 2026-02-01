// DIESE KLASSE IST NUR EIN TEMPLATE, EIN GEDANKENEXPERIMENT.
// CODE REFACTORING ARBEITET DARAUF HIN SIE UNNÖTIG ZU MACHEN; CONSIDER THIS A TODO



const activeInstances = [];

/**
 * Aktiviert eine Liste von Modul-Klassen.
 * @param {Array<Class>} ModuleClasses - Ein Array von Klassen (nicht Instanzen!).
 * @param {Object} context - Daten für den Start.
 */
export async function activateModules(ModuleClasses, context = {}) {
    // 1. Alte Instanzen aufräumen
    cleanupModules();

    console.groupCollapsed(`[Lifecycle] Switching context...`);

    // 2. Neue Instanzen erstellen und starten
    for (const ModuleClass of ModuleClasses) {
        try {
            // Instanz erstellen
            const instance = new ModuleClass();
            
            // In Liste speichern (WICHTIG: bevor init läuft)
            activeInstances.push(instance);
            
            // Starten
            await instance.init(context);
            
        } catch (err) {
            console.error(`[Lifecycle] Critical Error loading module:`, err);
        }
    }
    console.groupEnd();
}

/**
 * Stoppt alle aktiven Module.
 */
export function cleanupModules() {
    // Rückwärts durchgehen (LIFO)
    while (activeInstances.length > 0) {
        const instance = activeInstances.pop();
        if (instance && typeof instance.dispose === 'function') {
            instance.dispose();
        }
    }
}