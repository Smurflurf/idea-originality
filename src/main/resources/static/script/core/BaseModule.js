/**
 * Abstrakte Basisklasse für alle Module.
 * Kümmert sich um Lifecycle-Management, AbortController und Error-Handling.
 */
export class BaseModule {
    
    constructor(name = 'AnonymousModule') {
        this.name = name;
        this.isInitialized = false;
        this.abortController = null;
        this.cleanupTasks = []; // Für Timer, RAFs, etc.
    }

    /**
     * Startet das Modul. NICHT ÜBERSCHREIBEN!
     * Implementiere stattdessen `onInit(context)`.
     */
    async init(context = {}) {
        if (this.isInitialized) {
            console.warn(`[${this.name}] Bereits initialisiert.`);
            return;
        }

        this.abortController = new AbortController();
        this.isInitialized = true;

        try {
            // Ruft die eigentliche Logik der Unterklasse auf
            await this.onInit(context);
            // console.log(`[${this.name}] Started`);
        } catch (err) {
            console.error(`[${this.name}] [!] Fehler beim Start:`, err);
            // Bei Fehler sofort aufräumen
            this.dispose(); 
            throw err;
        }
    }

    /**
     * Stoppt das Modul und räumt auf. NICHT ÜBERSCHREIBEN!
     * Implementiere stattdessen `onDispose()`.
     */
    dispose() {
        if (!this.isInitialized) return;

        // 1. Unterklasse aufräumen lassen
        try {
            this.onDispose();
        } catch (err) {
            console.warn(`[${this.name}] Fehler beim onDispose:`, err);
        }

        // 2. Event Listener killen (Der "Safety by Design" Hammer)
        if (this.abortController) {
            this.abortController.abort();
            this.abortController = null;
        }

        // 3. Manuelle Cleanup Tasks (Timeouts, RAFs) ausführen
        this.cleanupTasks.forEach(task => task());
        this.cleanupTasks = [];

        this.isInitialized = false;
        // console.log(`[${this.name}] ⏏ Stopped`);
    }

    /**
     * Hilfsmethode: Fügt einen Event Listener hinzu, der automatisch
     * entfernt wird, wenn das Modul gestoppt wird.
     */
    addListener(target, type, listener, options = {}) {
        if (!this.abortController) return;

        // Wir mischen das AbortSignal in die Optionen
        const safeOptions = { 
            ...options, 
            signal: this.abortController.signal 
        };
        
        target.addEventListener(type, listener, safeOptions);
    }

    /**
     * Hilfsmethode: Setzt ein Interval, das beim Dispose gelöscht wird.
     */
    setInterval(callback, ms) {
        const id = setInterval(callback, ms);
        this.cleanupTasks.push(() => clearInterval(id));
        return id;
    }

    /**
     * Hilfsmethode: Setzt ein Timeout, das beim Dispose gelöscht wird.
     */
    setTimeout(callback, ms) {
        const id = setTimeout(callback, ms);
        this.cleanupTasks.push(() => clearTimeout(id));
        return id;
    }

    // --- ABSTRAKTE METHODEN (Zum Überschreiben) ---

    /**
     * Hier kommt deine Start-Logik rein.
     * @param {Object} context - Globale Daten (z.B. INITIAL_DATA)
     */
    async onInit(context) {
        // Optional zu implementieren
    }

    /**
     * Hier kommt deine Aufräum-Logik rein (z.B. Canvas leeren, Variablen nullen).
     * Event Listener musst du hier NICHT entfernen, das macht die Basisklasse.
     */
    onDispose() {
        // Optional zu implementieren
    }
}