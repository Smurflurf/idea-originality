import * as cloudSimulator from '/script/animation/cloudSimulator.js';

const BAKE_FPS = 30; // Wir können die FPS etwas erhöhen für mehr Flüssigkeit

let bakedFrames = [];      // Die Queue, die unsere Frames enthält
let isBaking = false;
let consumerIndex = 0;     // Verfolgt, welcher Frame als nächstes abgespielt werden soll
let lastValidFrame = null; // Speichert den zuletzt gerenderten Frame als Fallback

const bakeCanvas = document.createElement('canvas');
const bakeCtx = bakeCanvas.getContext('2d');

/**
 * Startet den Streaming-Prozess im Hintergrund.
 * @param {HTMLCanvasElement} targetCanvas - Das Ziel-Canvas, auf das gezeichnet wird.
 * @param {object} explosionOrigin - {x, y} Koordinaten für das Zentrum der Explosion.
 */
export function initiateBaking(targetCanvas, explosionOrigin) {
    if (isBaking || !targetCanvas) return;

    console.log("Pre-Renderer: Starte Streaming der Wolkenanimation...");
    isBaking = true;

    // Reset für einen sauberen Neustart
    bakedFrames = [];
    consumerIndex = 0;
    lastValidFrame = null;
    
    // NEU: Lese die Dimensionen direkt vom Ziel-Canvas aus.
    const width = targetCanvas.clientWidth;
    const height = targetCanvas.clientHeight;

    const dpr = window.devicePixelRatio || 1;
    bakeCanvas.width = width * dpr;
    bakeCanvas.height = height * dpr;
    bakeCtx.scale(dpr, dpr);

    cloudSimulator.initCloudSimulator(bakeCanvas);
    cloudSimulator.triggerCloudExplosion(explosionOrigin.x, explosionOrigin.y);

    bakeNextFrame();
}

function bakeNextFrame() {
    // Der Bäcker stoppt nur, wenn er explizit abgebrochen wird.
    if (!isBaking) return;

    // Simulation und Zeichnen auf den unsichtbaren Canvas
    bakeCtx.clearRect(0, 0, bakeCanvas.width, bakeCanvas.height);
    cloudSimulator.drawClouds(bakeCtx);

    // Den gerenderten Frame als Bild-Objekt erstellen und zur Queue hinzufügen
    const image = new Image();
    image.src = bakeCanvas.toDataURL('image/webp', 0.8); // WebP ist effizienter
    bakedFrames.push(image);

    // Den nächsten Frame im Leerlauf des Browsers anfordern
    if ('requestIdleCallback' in window) {
        requestIdleCallback(bakeNextFrame);
    } else {
        setTimeout(bakeNextFrame, 1000 / BAKE_FPS);
    }
}

/**
 * Holt den nächsten verfügbaren Frame aus dem Stream.
 * @returns {HTMLImageElement|null} Das Bild-Objekt des Frames.
 */
export function getFrame() {
    // NEUE LOGIK: Stream-artiges Abrufen
    if (!isBaking && consumerIndex >= bakedFrames.length) {
        return lastValidFrame; // Nichts mehr zu tun, den letzten Frame zeigen
    }

    // Prüfen, ob ein neuer, noch nicht abgespielter Frame in der Queue bereitliegt
    if (consumerIndex < bakedFrames.length) {
        lastValidFrame = bakedFrames[consumerIndex];
        consumerIndex++;

        // Speicher-Management: Alte, bereits abgespielte Frames aus der Queue entfernen
        // Wir lassen immer ein paar alte Frames als Puffer
        if (consumerIndex > 30) {
            bakedFrames.splice(0, consumerIndex - 10);
            consumerIndex = 10;
        }
    }
    
    // Gibt entweder den brandneuen Frame oder den letzten gültigen Frame zurück
    return lastValidFrame; 
}

/**
 * Setzt die Wiedergabe an den Anfang des Streams.
 */
export function resetPlayback() {
    consumerIndex = 0;
}

/**
 * Bricht den Prozess ab und gibt den gesamten Speicher frei.
 */
export function cancel() {
    isBaking = false;
    bakedFrames = [];
    lastValidFrame = null;
    consumerIndex = 0;
    console.log("Pre-Renderer: Streaming gestoppt und Speicher freigegeben.");
}