import { spinnerConfig, animConfig, REFERENCE_WIDTH } from '/script/animation/animationConfig.js';

let mainCanvas = null;
const preRenderedClouds = {}; 

// --- TRICK: SPRITE BUCKETING ---
// Vermeidet teure drawImage Skalierung zur Laufzeit. 
// Wir rendern die Wolken in festen Größen vor.
const SIZE_BUCKETS = [96, 160, 224, 288, 384]; 

const POOL_SIZE = animConfig.maxCloudParticles || 100; 
const cloudPool = [];

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

function createPreRenderedCloudBuckets(colors) {
    for (const colorTemplate of colors) {
        if (preRenderedClouds[colorTemplate]) continue;
        
        preRenderedClouds[colorTemplate] = [];
        const baseColor = colorTemplate.replace('OPACITY', '1.0');

        // Für jeden Bucket (Größe) ein eigenes Canvas erstellen
        for (const size of SIZE_BUCKETS) {
            const canvas = document.createElement('canvas');
            canvas.width = size;
            canvas.height = size;
            const ctx = canvas.getContext('2d', { alpha: true });
            const center = size / 2;

            // 1. Basis-Farbe zeichnen
            ctx.fillStyle = baseColor;
            ctx.fillRect(0, 0, size, size);

            // 2. Alpha-Maske via Radial Gradient ausstanzen (destination-in)
            ctx.globalCompositeOperation = 'destination-in';
            const gradient = ctx.createRadialGradient(center, center, 0, center, center, center);
            gradient.addColorStop(0, 'rgba(255, 255, 255, 0.5)');
            gradient.addColorStop(0.6, 'rgba(255, 255, 255, 0.15)');
            gradient.addColorStop(1, 'rgba(255, 255, 255, 0)');
            
            ctx.fillStyle = gradient;
            ctx.fillRect(0, 0, size, size);

            preRenderedClouds[colorTemplate].push(canvas);
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
    initPool();
    
    const currentColors = getCurrentCloudColors();
    createPreRenderedCloudBuckets(currentColors);
}

export function triggerCloudExplosion(x, y) {
    const currentColors = getCurrentCloudColors();

    const effectiveWidth = mainCanvas ? mainCanvas.clientWidth : REFERENCE_WIDTH;
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
    if (!mainCanvas) return;

    const currentTime = Date.now();
    const timeFactor = 60 * animConfig.deltaTime * animConfig.speed;

    for (let i = 0; i < cloudPool.length; i++) {
        const p = cloudPool[i];
        if (!p.active) continue;

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

        const buckets = preRenderedClouds[p.color];
        if (buckets) {
            // --- HARTE OPTIMIERUNG ---
            mainCtx.globalAlpha = finalOpacity;
            
            // Bucket Selection: Finde die passendste vorgenerierte Größe
            let targetSize = p.size;
            let bucketIndex = 0;
            for(let b = 0; b < SIZE_BUCKETS.length; b++) {
                bucketIndex = b;
                if (targetSize <= SIZE_BUCKETS[b]) break;
            }
            
            const renderImg = buckets[bucketIndex];
            const actualSize = SIZE_BUCKETS[bucketIndex];
            
            // Bitwise OR 0 schneidet Kommastellen extrem schnell ab (verhindert Sub-Pixel Anti-Aliasing)
            const drawX = (p.x - (actualSize / 2)) | 0;
            const drawY = (p.y - (actualSize / 2)) | 0;
            
            // Zeichnen OHNE Skalierungsparameter! (Wahnsinniger Speed-Boost)
            mainCtx.drawImage(renderImg, drawX, drawY);
        }
    }
    
    // Alpha am Ende wieder auf 1.0 setzen
    mainCtx.globalAlpha = 1.0; 
}

export function reset() {
    cloudPool.forEach(p => p.active = false);
}