import { animConfig } from '/script/animation/animationConfig.js';

let canvas;
let ctx;
let particles = [];
let stagedParticles = [];
let animationFrameId = null;
let resizeObserver = null;
let currentDpr = 1; // Für setTransform Reset

const FRICTION = 0.9999;
const FADE_SPEED = 0.005;
const PHYSICS_THROTTLE_RATE = 5;

// --- GPU OPTIMIERUNG: Entfernung von { willReadFrequently: true } ---
// Dadurch wird das Canvas im VRAM der Grafikkarte abgelegt, was das
// Kopieren per drawImage ins Haupt-Canvas drastisch beschleunigt.
const spriteSheetCanvas = document.createElement('canvas');
spriteSheetCanvas.width = 1024; 
spriteSheetCanvas.height = 512;
const spriteCtx = spriteSheetCanvas.getContext('2d'); 
let spriteX = 0;
let spriteY = 0;
const ROW_HEIGHT = 64; 
const charCache = {};

function getCachedCharSprite(char, font, color) {
    const cacheKey = `${char}-${font}-${color}`;
    if (charCache[cacheKey]) return charCache[cacheKey];

    spriteCtx.font = font;
    const width = Math.ceil(spriteCtx.measureText(char).width) + 8; // Extra padding
    
    // Zeilenumbruch auf dem Sprite-Sheet
    if (spriteX + width > spriteSheetCanvas.width) {
        spriteX = 0;
        spriteY += ROW_HEIGHT;
    }

    spriteCtx.font = font;
    spriteCtx.fillStyle = color;
    spriteCtx.textAlign = 'center';
    spriteCtx.textBaseline = 'middle';
    
    // Wir zeichnen exakt in die Mitte der reservierten "Zelle"
    spriteCtx.fillText(char, spriteX + width / 2, spriteY + ROW_HEIGHT / 2);

    const spriteInfo = {
        x: spriteX,
        y: spriteY,
        w: width,
        h: ROW_HEIGHT
    };

    spriteX += width;
    charCache[cacheKey] = spriteInfo;
    
    return spriteInfo;
}

export function resizeAndScaleCanvas() {
    if (!canvas || !ctx) return;
    
    // --- HARTE OPTIMIERUNG: DPR CAPPING ---
    currentDpr = Math.min(window.devicePixelRatio || 1, 1.25); 
    const rect = canvas.getBoundingClientRect();
    
    const targetWidth = Math.floor(rect.width * currentDpr);
    const targetHeight = Math.floor(rect.height * currentDpr);

    if (canvas.width !== targetWidth || canvas.height !== targetHeight) {
        canvas.width = targetWidth;
        canvas.height = targetHeight;
        ctx.scale(currentDpr, currentDpr);
    }
}

export function initTextAnimator(canvasElement) {
    if (!canvasElement) return;
    
    if (animationFrameId) { cancelAnimationFrame(animationFrameId); animationFrameId = null; }
    if (resizeObserver && canvas) { resizeObserver.unobserve(canvas); resizeObserver = null; }

    canvas = canvasElement;
    ctx = canvas.getContext('2d');
    
    particles = [];
    stagedParticles = [];
    
    resizeObserver = new ResizeObserver(resizeAndScaleCanvas);
    resizeObserver.observe(canvas);
    resizeAndScaleCanvas(); 

    animate();
}

export function prepareTextExplosion(element) {
    if (!element || !element.textContent.trim() || !ctx) return [];

    const particleData = [];
    const text = element.textContent;
    const characters = text.split('');
    const elementRect = element.getBoundingClientRect();
    const elementStyle = getComputedStyle(element);

    ctx.font = elementStyle.font;
    ctx.textAlign = 'left';
    ctx.textBaseline = 'top';

    let currentX = elementRect.left;
    const currentY = elementRect.top;

    characters.forEach(char => {
        if (char === ' ') {
             currentX += ctx.measureText(char).width;
             return;
        }

        const metrics = ctx.measureText(char);
        const charWidth = metrics.width;
        
        const charCenterX = currentX + charWidth / 2;
        const charCenterY = currentY + elementRect.height / 2;
        
        // Buchstaben sofort aufs Sprite Sheet schreiben
        const cachedRender = getCachedCharSprite(char, elementStyle.font, elementStyle.color);

        particleData.push({
            text: char,
            cachedRender: cachedRender, 
            initialX: charCenterX,
            initialY: charCenterY,
            offsetX: 0, offsetY: 0,
            vx: 0, vy: 0,
            rotation: 0, rotationSpeed: 0,
            opacity: 1,
            visible: true 
        });
        currentX += charWidth;
    });
    return particleData;
}

export function triggerPreparedExplosion(particleData, shockwaveOrigin) {
    if (!particleData || !shockwaveOrigin) return;

    particleData.forEach(p => {
        const dx = p.initialX - shockwaveOrigin.x;
        const dy = p.initialY - shockwaveOrigin.y;
        const dist = Math.sqrt(dx * dx + dy * dy) || 1;
        const speed = Math.random() * 5 + 2;

        p.vx = (dx / dist) * speed + (Math.random() - 0.5) * 1;
        p.vy = (dy / dist) * speed + (Math.random() - 0.5) * 1;
        p.rotationSpeed = (Math.random() - 0.5) * 8;
        
        stagedParticles.push(p);
    });
}

function animate() {
    animationFrameId = requestAnimationFrame(animate);

    animConfig.frameCount++;

    if (stagedParticles.length > 0) {
        particles.push(...stagedParticles);
        stagedParticles.length = 0;
    }

    if (particles.length === 0) return;

    const rect = canvas.getBoundingClientRect();
    ctx.clearRect(0, 0, (rect.width + 1) | 0, (rect.height + 1) | 0);

    const physicsGroup = animConfig.frameCount % PHYSICS_THROTTLE_RATE;
    const dt = 1/60; 
    const timeFactor = 60 * dt * animConfig.speed;
    const throttledTimeFactor = timeFactor * PHYSICS_THROTTLE_RATE;

    let i = particles.length;
    while (i--) {
        const p = particles[i];
        if (!p.visible) continue;
        
        if (i % PHYSICS_THROTTLE_RATE === physicsGroup) {
            if (animConfig.repulsionBoundary) {
                const currentX = p.initialX + p.offsetX;
                const currentY = p.initialY + p.offsetY;
                const boundary = animConfig.repulsionBoundary;
                
                if (currentX >= boundary.left && currentX <= boundary.right && currentY >= boundary.top && currentY <= boundary.bottom) {
                    const REPULSION_STRENGTH = 0.05;
                    const repulsionCenterY = boundary.top + boundary.height / 2;
                    p.vy += (currentY - repulsionCenterY) * REPULSION_STRENGTH * throttledTimeFactor;
                }
            }
            const frictionFactor = Math.pow(FRICTION, throttledTimeFactor);
            p.vx *= frictionFactor; p.vy *= frictionFactor;
            p.opacity -= FADE_SPEED * throttledTimeFactor;
            if (p.opacity <= 0) { p.visible = false; continue; }
        }

        p.offsetX += p.vx * timeFactor;
        p.offsetY += p.vy * timeFactor;
        p.rotation += p.rotationSpeed * timeFactor;

        const currentX = p.initialX + p.offsetX;
        const currentY = p.initialY + p.offsetY;

        if (currentX < -20 || currentX > rect.width + 20 || currentY < -20 || currentY > rect.height + 20) continue;

        // --- HARTE OPTIMIERUNG: Keine save()/restore() calls mehr! ---
        ctx.translate(currentX, currentY);
        ctx.rotate(p.rotation);
        ctx.globalAlpha = p.opacity;
        
        const cache = p.cachedRender;
        // Rendern aus dem globalen Texture Atlas
        ctx.drawImage(
            spriteSheetCanvas, 
            cache.x, cache.y, cache.w, cache.h, 
            -cache.w / 2, -cache.h / 2, cache.w, cache.h
        );
        
        // Reset via direkter Matrix-Zuweisung (viel schneller als ctx.restore)
        ctx.setTransform(currentDpr, 0, 0, currentDpr, 0, 0); 
    }
    ctx.globalAlpha = 1.0; // Sicherer Reset am Ende
    
    if (animConfig.frameCount % 180 === 0) {
         // GC FIX: In-Place Filterung, ohne ein neues Array zu erstellen!
         let writeIdx = 0;
         for (let j = 0; j < particles.length; j++) {
             if (particles[j].visible) {
                 particles[writeIdx++] = particles[j];
             }
         }
         particles.length = writeIdx;
    }
}

export function stopTextAnimator() {
    if (animationFrameId) {
        cancelAnimationFrame(animationFrameId);
        animationFrameId = null;
    }
    particles = [];
    stagedParticles = [];
}