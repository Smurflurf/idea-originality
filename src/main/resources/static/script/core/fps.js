/**
 * FPS Monitor - Korrigierte Singleton-Version
 */

let isInitialized = false;
let animationId = null;

export function initFPSMonitor() {
    // 1. Singleton-Check: Existiert der Monitor bereits im DOM?
    let wrapper = document.getElementById('fps-counter-wrapper');
    
    if (wrapper) {
        console.log('[FPS] Monitor already exists, skipping creation.');
        return;
    }

    console.log('[FPS] Initializing Monitor...');

    // 2. Container erstellen
    wrapper = document.createElement('div');
    wrapper.id = 'fps-counter-wrapper';
    wrapper.popover = "manual"; 
    
    // Initial: Nicht persistent, da versteckt
    wrapper.setAttribute('data-is-persistent', 'false');

    wrapper.style.cssText = `
        inset: auto; 
        margin: 0; 
        position: fixed; 
        top: 5px; 
        right: 0px; 
        z-index: 2147483647; 
        display: none; 
        flex-direction: row; 
        align-items: flex-start; 
        pointer-events: auto; 
        background: rgba(0,0,0,0.8); 
        padding: 4px; 
        border-radius: 4px; 
        backdrop-filter: blur(4px); 
        cursor: pointer; 
        border: 1px solid rgba(255,255,255,0.1); 
        gap: 4px;
        box-shadow: 0 4px 15px rgba(0,0,0,0.5);
    `;
    
    // Canvas & Context Setup
    const canvas = document.createElement('canvas');
    const PR = Math.round(window.devicePixelRatio || 1);
    const CSS_WIDTH = 250;
    const CSS_HEIGHT = 60; 
    const WIDTH = CSS_WIDTH * PR;
    const HEIGHT = CSS_HEIGHT * PR;
    canvas.width = WIDTH;
    canvas.height = HEIGHT;
    canvas.style.cssText = `width:${CSS_WIDTH}px; height:${CSS_HEIGHT}px; display:block; border-right: 1px solid #444; flex-shrink: 0;`;
    const ctx = canvas.getContext('2d');
    ctx.fillStyle = '#111';
    ctx.fillRect(0, 0, WIDTH, HEIGHT);

    const infoPanel = document.createElement('div');
    infoPanel.style.cssText = `
        background: transparent; color: #00ff00; font-family: 'Courier New', monospace; 
        font-size: 11px; padding-left: 4px; line-height: 1.3; width: 80px; 
        height: ${CSS_HEIGHT}px; display: flex; flex-direction: column; 
        justify-content: space-between; user-select: none; flex-shrink: 0;
    `;
    infoPanel.innerHTML = 'init...';

    wrapper.appendChild(canvas);
    wrapper.appendChild(infoPanel);
    document.body.appendChild(wrapper);

    // --- SECRET TRIGGER LOGIC (Globaler Schutz) ---
    if (!isInitialized) {
        let tapCount = 0;
        let lastTapTime = 0;
        const TAP_LIMIT = 7;
        const TIME_WINDOW = 500; 

        document.addEventListener('pointerdown', (e) => {
            const btn = e.target.closest('.run-button');
            if (!btn) return;

            const currentTime = Date.now();
            if (currentTime - lastTapTime > TIME_WINDOW) {
                tapCount = 1;
            } else {
                tapCount++;
            }
            lastTapTime = currentTime;

            if (tapCount >= TAP_LIMIT) {
                tapCount = 0;
                // Wir suchen das Element jedes Mal frisch, falls es durch Navigation verschoben wurde
                const currentWrapper = document.getElementById('fps-counter-wrapper');
                if (!currentWrapper) return;

                if (currentWrapper.style.display !== 'none') {
                    // DEAKTIVIEREN
                    currentWrapper.hidePopover();
                    currentWrapper.style.display = 'none';
                    currentWrapper.setAttribute('data-is-persistent', 'false');
                    console.log('[FPS] Monitor DISABLED & NON-PERSISTENT');
                } else {
                    // AKTIVIEREN
                    window._lastFrameTime = performance.now();
                    currentWrapper.style.display = 'flex';
                    currentWrapper.showPopover();
                    currentWrapper.setAttribute('data-is-persistent', 'true');
                    console.log('[FPS] Monitor ENABLED & PERSISTENT');
                }
            }
        }, true);
        isInitialized = true;
    }

    // --- MONITORING LOGIC ---
    let prevTime = performance.now();
    let frames = 0;
    let fpsHistory = []; 
    let maxFps = 0;
    let avgFps = 60;
    let pitFps = 120; 
    let pitHoldTime = 0; 
    const PIT_HOLD_DURATION = 1300; 
    let lastGraphY = HEIGHT; 
    let lastPitGraphY = HEIGHT; 
    const UPDATE_DIVIDER = 2; 
    let frameSkipCounter = 0;

    wrapper.addEventListener('click', () => {
        pitFps = 120; pitHoldTime = 0; maxFps = 0; avgFps = 60; fpsHistory = [];
        ctx.fillStyle = '#111';
        ctx.fillRect(0, 0, WIDTH, HEIGHT);
        lastGraphY = HEIGHT; lastPitGraphY = HEIGHT; 
        infoPanel.innerHTML = '<span style="color:#fff">RESET</span>';
    });

    function updateGraph(fps) {
        const now = performance.now();
        if (fps < pitFps) { pitFps = fps; pitHoldTime = now + PIT_HOLD_DURATION; } 
        else if (now > pitHoldTime) { pitFps = fps; }

        const scrollStep = 1 * PR;
        const padding = 2 * PR;
        const graphHeight = HEIGHT - padding;

        ctx.globalCompositeOperation = 'copy';
        ctx.drawImage(canvas, scrollStep, 0, WIDTH - scrollStep, HEIGHT, 0, 0, WIDTH - scrollStep, HEIGHT);
        ctx.globalCompositeOperation = 'source-over';
        ctx.fillStyle = '#111';
        ctx.fillRect(WIDTH - scrollStep, 0, scrollStep, HEIGHT);

        const y60 = HEIGHT - Math.min(HEIGHT, (60 / 70) * HEIGHT);
        const y30 = HEIGHT - Math.min(HEIGHT, (30 / 70) * HEIGHT);
        ctx.fillStyle = '#222';
        ctx.fillRect(WIDTH - scrollStep, y60, scrollStep, 1 * PR); 
        ctx.fillRect(WIDTH - scrollStep, y30, scrollStep, 1 * PR);

        const scaledPit = Math.min(pitFps, 70);
        const currentPitY = HEIGHT - ((scaledPit / 70) * graphHeight);
        ctx.lineWidth = 1.5 * PR; ctx.strokeStyle = '#ff3333'; ctx.beginPath();
        ctx.moveTo(WIDTH - scrollStep * 2, lastPitGraphY); ctx.lineTo(WIDTH - (scrollStep/2), currentPitY);
        ctx.stroke(); lastPitGraphY = currentPitY;

        const scaledFps = Math.min(fps, 70); 
        const currentY = HEIGHT - ((scaledFps / 70) * graphHeight);
        ctx.lineWidth = 2 * PR; ctx.strokeStyle = '#3399ff'; ctx.beginPath();
        ctx.moveTo(WIDTH - scrollStep * 2, lastGraphY); ctx.lineTo(WIDTH - (scrollStep/2), currentY);
        ctx.stroke(); lastGraphY = currentY;
    }

    function animate() {
        const currentWrapper = document.getElementById('fps-counter-wrapper');
        // Falls das Element gelöscht wurde (nicht persistent beim Seitenwechsel), Loop stoppen!
        if (!currentWrapper) {
            animationId = null;
            return; 
        }

        if (currentWrapper.style.display !== 'none') {
            const time = performance.now();
            frames++;
            if (time >= prevTime + 1000) {
                const fps = Math.round((frames * 1000) / (time - prevTime));
                if (fps > 0) {
                    fpsHistory.push(fps);
                    if (fpsHistory.length > 20) fpsHistory.shift(); 
                    const sum = fpsHistory.reduce((a, b) => a + b, 0);
                    avgFps = Math.round(sum / fpsHistory.length);
                    infoPanel.innerHTML = `
                        <span style="color:#3399ff; font-weight:bold; font-size:12px;">NOW:${fps}</span>
                        <span style="color:#aaa">AVG:${avgFps}</span>
                        <span style="color:#ff3333">PIT:${Math.round(pitFps)}</span>
                    `;
                }
                prevTime = time;
                frames = 0;
            }

            frameSkipCounter++;
            if (frameSkipCounter >= UPDATE_DIVIDER) {
                const instantFps = 1000 / (time - (window._lastFrameTime || time));
                window._lastFrameTime = time;
                if (isFinite(instantFps)) updateGraph(instantFps);
                frameSkipCounter = 0;
            } else {
                window._lastFrameTime = time; 
            }
        }
        animationId = requestAnimationFrame(animate);
    }

    // Nur einen Loop starten
    if (!animationId) {
        window._lastFrameTime = performance.now();
        animationId = requestAnimationFrame(animate);
    }
}