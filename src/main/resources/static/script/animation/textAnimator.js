import { animConfig } from '/script/animation/animationConfig.js';

let canvas;
let ctx;
let particles = [];
let stagedParticles = [];
let animationFrameId = null;
let resizeObserver = null;

const FRICTION = 0.9999;
const FADE_SPEED = 0.005;
const PHYSICS_THROTTLE_RATE = 5;

// Das ist die "Old" resizeLogic, die robust funktioniert
export function resizeAndScaleCanvas() {
    if (!canvas || !ctx) return;
    const dpr = window.devicePixelRatio || 1;
    const rect = canvas.getBoundingClientRect();
    
    if (canvas.width !== rect.width * dpr || canvas.height !== rect.height * dpr) {
        canvas.width = rect.width * dpr;
        canvas.height = rect.height * dpr;
        ctx.scale(dpr, dpr);
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
        const metrics = ctx.measureText(char);
        const charWidth = metrics.width;
        
        const charCenterX = currentX + charWidth / 2;
        const charCenterY = currentY + elementRect.height / 2;
        
        particleData.push({
            text: char,
            font: elementStyle.font,
            color: elementStyle.color,
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

    // Frame Counting für Physik Throttling
    animConfig.frameCount++; // Wichtig für animationConfig usage

    if (stagedParticles.length > 0) {
        particles.push(...stagedParticles);
        stagedParticles.length = 0;
    }

    if (particles.length === 0) return;

    const rect = canvas.getBoundingClientRect();
    ctx.clearRect(0, 0, rect.width, rect.height);

    const physicsGroup = animConfig.frameCount % PHYSICS_THROTTLE_RATE;
    
    // Fallback DeltaTime falls nicht gesetzt (da wir masterAnimate entfernt haben)
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

        ctx.save();
        ctx.translate(currentX, currentY);
        ctx.rotate(p.rotation);
        ctx.font = p.font;
        ctx.fillStyle = p.color;
        ctx.globalAlpha = p.opacity;
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText(p.text, 0, 0);
        ctx.restore();
    }
    
    if (animConfig.frameCount % 180 === 0) {
         particles = particles.filter(p => p.visible);
    }
}