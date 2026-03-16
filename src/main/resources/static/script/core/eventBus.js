// Internes Target
const bus = new EventTarget();

// Konstanten für Event-Namen (Verhindert Tippfehler!)
export const EVENTS = {
    // State Changes
    LANG_CHANGED: 'app:language-changed',
    ATTACHMENTS_CHANGED: 'app:attachments-changed',
	THEME_CHANGED: 'app:theme-changed',
		
    // UI Signals
    TTS_NAVIGATE: 'app:tts-navigate',
    TTS_FINISHED: 'app:tts-finished',
	
    // Data Signals
    FILTERED_RESULTS_RENDERED: 'app:filtered-results-rendered'
};

/**
 * Sendet ein Event über den Bus.
 * @param {string} eventName - Einer der EVENTS keys
 * @param {any} detail - Daten payload (optional)
 */
export function emit(eventName, detail = null) {
    // console.debug(`[Bus] Emit: ${eventName}`, detail); // Debugging Log
    bus.dispatchEvent(new CustomEvent(eventName, { detail }));
}

/**
 * Hört auf ein Event.
 * @param {string} eventName 
 * @param {Function} callback - Erhält (detail) als Argument
 */
export function on(eventName, callback) {
    // Referenz speichern, damit off() später genau diese Funktion wiederfindet
    const wrapper = (e) => callback(e.detail);
    callback._busWrapper = wrapper; 
    bus.addEventListener(eventName, wrapper);
}

export function off(eventName, callback) {
    if (callback._busWrapper) {
        bus.removeEventListener(eventName, callback._busWrapper);
        delete callback._busWrapper;
    }
}

/**
 * Hört einmalig auf ein Event.
 */
export function once(eventName, callback) {
    bus.addEventListener(eventName, (e) => {
        callback(e.detail);
    }, { once: true });
}