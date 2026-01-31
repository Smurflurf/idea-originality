import { spinnerConfig, animConfig, REFERENCE_WIDTH } from '/script/animation/animationConfig.js';

let mainCanvas = null;
let cloudStampCanvas = null;
const preRenderedClouds = {}; 
let cloudBuffer = null;
let cloudBufferCtx = null;

const POOL_SIZE = animConfig.maxCloudParticles || 100; 
const cloudPool = [];

// Performance Settings
const DOWNSCALE_FACTOR = 0.5; 
let logicalWidth = 0;
let logicalHeight = 0;
let devicePixelRatio = 1;

function getCssColor(varName) {
    if (typeof getComputedStyle === 'function') {
        const val = getComputedStyle(document.body).getPropertyValue(varName).trim();
        if (val) return val;
    }
    return null;
}

function getCurrentCloudColors() {
    const c1 = getCssColor('--cloud-rgb-1');
    const c2 = getCssColor('--cloud-rgb-2');
    const c3 = getCssColor('--cloud-rgb-3');

    if (c1 && c2 && c3) {
        return [
            `rgba(${c1}, OPACITY)`,
            `rgba(${c2}, OPACITY)`,
            `rgba(${c3}, OPACITY)`
        ];
    }
    return spinnerConfig.cloudColors;
}

function createCloudStamp() {
    const stampSize = 128; 

    cloudStampCanvas = document.createElement('canvas');
    cloudStampCanvas.width = stampSize;
    cloudStampCanvas.height = stampSize;
    
    const stampCtx = cloudStampCanvas.getContext('2d');
    const center = stampSize / 2;
    const radius = stampSize / 2;

    const gradient = stampCtx.createRadialGradient(center, center, 0, center, center, radius);
    gradient.addColorStop(0, 'rgba(255, 255, 255, 0.5)');
	gradient.addColorStop(0.6, 'rgba(255, 255, 255, 0.15)');
	gradient.addColorStop(1, 'rgba(255, 255, 255, 0)');
	
	stampCtx.fillStyle = gradient;
    stampCtx.beginPath();
    stampCtx.arc(center, center, radius, 0, Math.PI * 2);
    stampCtx.fill();
}

function createPreRenderedClouds(colors) {
    if (!cloudStampCanvas) return;
    const stampSize = cloudStampCanvas.width; 

    for (const colorTemplate of colors) {
        if (!preRenderedClouds[colorTemplate]) {
            const cloudCanvas = document.createElement('canvas');
            cloudCanvas.width = stampSize;
            cloudCanvas.height = stampSize;
            const pCtx = cloudCanvas.getContext('2d');
            
            pCtx.fillStyle = colorTemplate.replace('OPACITY', '1.0');
            pCtx.fillRect(0, 0, stampSize, stampSize);
            
            pCtx.globalCompositeOperation = 'destination-in';
            pCtx.drawImage(cloudStampCanvas, 0, 0);
            
            preRenderedClouds[colorTemplate] = cloudCanvas;
        }
    }
}

function initPool() {
    if (cloudPool.length > 0) return;
    for (let i = 0; i < POOL_SIZE; i++) {
        cloudPool.push({
            active: false, x: 0, y: 0, vx: 0, vy: 0, size: 0, color: '',
            creationTime: 0, fadeInDuration: 0, fadeToMinDuration: 0, maxLifetime: 0
        });
    }
}

export function initCloudSimulator(canvas) {
    mainCanvas = canvas;
    cloudBuffer = document.createElement('canvas');
    // FIX: 'desynchronized' entfernt, da es in Firefox Probleme macht.
    // 'willReadFrequently' hilft manchmal bei Software-Fallback.
    cloudBufferCtx = cloudBuffer.getContext('2d', { alpha: true });

    if (!cloudStampCanvas) createCloudStamp();
    initPool();
}

export function resizeCloudLayers(widthCss, heightCss) {
    if (!cloudBuffer) return;
    
    logicalWidth = widthCss;
    logicalHeight = heightCss;
    devicePixelRatio = window.devicePixelRatio || 1;

    // Integer Values erzwingen (| 0)
    cloudBuffer.width = (logicalWidth * devicePixelRatio * DOWNSCALE_FACTOR) | 0;
    cloudBuffer.height = (logicalHeight * devicePixelRatio * DOWNSCALE_FACTOR) | 0;

    const scale = devicePixelRatio * DOWNSCALE_FACTOR;
    cloudBufferCtx.scale(scale, scale);
}

export function triggerCloudExplosion(x, y) {
    const currentColors = getCurrentCloudColors();
    createPreRenderedClouds(currentColors);

    const effectiveWidth = logicalWidth || REFERENCE_WIDTH;
    const scaleFactor = effectiveWidth / REFERENCE_WIDTH;

    const currentTime = Date.now();
    let particlesSpawned = 0;
    
    for (let i = 0; i < cloudPool.length; i++) {
        if (particlesSpawned >= spinnerConfig.cloudParticleCount) break;
        
        const p = cloudPool[i];
        if (p.active) continue;

        p.active = true;
        
        const angle = Math.random() * Math.PI * 2;
        const speed = Math.random() * spinnerConfig.cloudParticleSpeed * animConfig.speed * scaleFactor;
        const startRadius = Math.random() * 15 * scaleFactor;
        const startAngle = Math.random() * Math.PI * 2;

        p.x = x + Math.cos(startAngle) * startRadius;
        p.y = y + Math.sin(startAngle) * startRadius;
        p.vx = Math.cos(angle) * speed;
        p.vy = Math.sin(angle) * speed;
        
        p.size = spinnerConfig.cloudParticleSize * scaleFactor * (0.8 + Math.random() * 0.4);
        p.color = currentColors[Math.floor(Math.random() * currentColors.length)];
        
        p.creationTime = currentTime;
        p.fadeInDuration = spinnerConfig.cloudFadeInTime * (0.8 + Math.random() * 0.4);
        p.fadeToMinDuration = spinnerConfig.cloudFadeToMinTime * (0.8 + Math.random() * 0.4);
        p.maxLifetime = 9999999; 

        particlesSpawned++;
    }
}

export function drawClouds(mainCtx) {
    if (!cloudBufferCtx || !mainCanvas || !cloudBuffer) return;

    const currentTime = Date.now();
    const timeFactor = 60 * animConfig.deltaTime * animConfig.speed;

    cloudBufferCtx.globalCompositeOperation = 'source-over';
    
    // FIX: Explizite Integer-Werte für clearRect helfen Firefox beim Compositing
    cloudBufferCtx.clearRect(0, 0, (logicalWidth + 1) | 0, (logicalHeight + 1) | 0);

    let activeCount = 0;

    for (let i = 0; i < cloudPool.length; i++) {
        const p = cloudPool[i];
        if (!p.active) continue;

        activeCount++;

        const frictionFactor = Math.pow(spinnerConfig.cloudParticleFriction, timeFactor);
        p.vx *= frictionFactor;
        p.vy *= frictionFactor;
        p.x += p.vx * timeFactor;
        p.y += p.vy * timeFactor;
        
        const age = currentTime - p.creationTime;
        let finalOpacity = 0;
        const isFadingIn = age < p.fadeInDuration;

        if (isFadingIn) {
            finalOpacity = (age / p.fadeInDuration) * spinnerConfig.cloudMaxOpacity;
        } else if (age < p.fadeInDuration + p.fadeToMinDuration) {
            const fadeProgress = (age - p.fadeInDuration) / p.fadeToMinDuration;
            finalOpacity = spinnerConfig.cloudMaxOpacity - (spinnerConfig.cloudMaxOpacity - spinnerConfig.cloudMinOpacity) * fadeProgress;
        } else {
            finalOpacity = spinnerConfig.cloudMinOpacity;
        }

        if ((!isFadingIn && finalOpacity < 0.01) || age > p.maxLifetime) {
            p.active = false;
            continue;
        }

        const preRenderedImage = preRenderedClouds[p.color];
        
        if (preRenderedImage) {
            cloudBufferCtx.globalAlpha = finalOpacity;
            const drawSize = p.size | 0;
            const drawX = (p.x - (drawSize / 2)) | 0;
            const drawY = (p.y - (drawSize / 2)) | 0;
            cloudBufferCtx.drawImage(preRenderedImage, drawX, drawY, drawSize, drawSize);
        }
    }

    if (activeCount > 0) {
        mainCtx.save();
        mainCtx.setTransform(1, 0, 0, 1, 0, 0); 
        mainCtx.globalAlpha = 1.0;
        
        mainCtx.drawImage(
            cloudBuffer, 
            0, 0, cloudBuffer.width, cloudBuffer.height, 
            0, 0, mainCanvas.width, mainCanvas.height    
        );
        mainCtx.restore();
    }
}

export function reset() {
    cloudPool.forEach(p => p.active = false);
}