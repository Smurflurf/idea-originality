import { animConfig, spinnerConfig } from '/script/animation/animationConfig.js';
import * as cloudSimulator from '/script/animation/cloudSimulator.js';

// Pools
const mainParticlePool = [];
const tntParticlePool = [];

let blackHoleCache = null;
const HOLE_CACHE_SIZE = 100; 

// --- GC & DOM FIX: Cached Colors ---
// Verhindert getComputedStyle-Aufrufe und String-Manipulationen im Render-Loop
let cachedRingBgColor = "rgba(204, 224, 255, 0.2)";
let cachedRingFgColor = "#cce0ff";
let cachedSupernovaBaseColor = "#ffffff";

function preRenderBlackHole() {
    if (blackHoleCache) return;
    blackHoleCache = document.createElement('canvas');
    blackHoleCache.width = HOLE_CACHE_SIZE;
    blackHoleCache.height = HOLE_CACHE_SIZE;
    const ctx = blackHoleCache.getContext('2d', { alpha: true });
    
    const center = HOLE_CACHE_SIZE / 2;
    const radius = (HOLE_CACHE_SIZE / 2) - 2; 
    
    const gradient = ctx.createRadialGradient(center, center, radius * 0.7, center, center, radius);
    gradient.addColorStop(0, '#000');
    gradient.addColorStop(1, 'rgba(0, 0, 0, 0)');
    
    ctx.fillStyle = gradient;
    ctx.beginPath();
    ctx.arc(center, center, radius, 0, Math.PI * 2);
    ctx.fill();
}

let supernovaFlashCache = null;
const FLASH_CACHE_SIZE = 100;

function preRenderSupernovaFlash() {
    if (supernovaFlashCache) return;
    supernovaFlashCache = document.createElement('canvas');
    supernovaFlashCache.width = FLASH_CACHE_SIZE;
    supernovaFlashCache.height = FLASH_CACHE_SIZE;
    const ctx = supernovaFlashCache.getContext('2d', { alpha: true });
    
    const center = FLASH_CACHE_SIZE / 2;
    const radius = FLASH_CACHE_SIZE / 2;
    
    const gradient = ctx.createRadialGradient(center, center, 0, center, center, radius);
    gradient.addColorStop(0, `rgba(255, 255, 255, 1)`); 
    gradient.addColorStop(1, `rgba(255, 255, 255, 0)`);
    
    ctx.fillStyle = gradient; 
    ctx.beginPath(); 
    ctx.arc(center, center, radius, 0, Math.PI * 2); 
    ctx.fill();
}


let canvas, ctx;
let animationFrameId = null;
let gravityCenter = { x: 0, y: 0 };
let layoutWidth = 0, layoutHeight = 0;
let currentDpr = 1;
let isLayoutInitialized = false;
let initialParticlesSpawned = false; // FIX: Verhindert Race Condition beim Spawnen

let supernovaParticlesCreated = false;
let supernovaCenterX = 0, supernovaCenterY = 0;
let onSupernovaCallback = null;

const STATE = { ORBITING: 0, GROWING: 1, IMPLODING: 2, RING: 3 };
let currentState = STATE.ORBITING;
let growthStartTime = 0, implosionStartTime = 0, supernovaStartTime = 0;
let ringProgress = 0;

const PHYSICS_THROTTLE_RATE = 3;

function initPools() {
    if (mainParticlePool.length === 0) {
        for (let i = 0; i < spinnerConfig.maxMainParticles; i++) {
            mainParticlePool.push({ active: false, x: 0, y: 0, z: 0, vx: 0, vy: 0, vz: 0, isSwallowed: false, swallowedTime: 0, swallowDuration: 500, createdAt: 0, maxLife: 0 });
        }
    }
    if (tntParticlePool.length === 0) {
        for (let i = 0; i < spinnerConfig.maxTntParticles; i++) {
            tntParticlePool.push({ active: false, x: 0, y: 0, vx: 0, vy: 0, size: 0, baseColor: '', creationTime: 0, maxLifetime: 0 });
        }
    }
}

function getCssColor(varName, defaultColor) {
    if (typeof getComputedStyle === 'function') { const val = getComputedStyle(document.body).getPropertyValue(varName); return val ? val.trim() : defaultColor; }
    return defaultColor;
}

function cacheGlobalColors() {
    // Wird nur 1x beim Start aufgerufen statt in jedem Frame!
    cachedRingBgColor = getCssColor('--color-spinner-bg', "rgba(204, 224, 255, 0.2)");
    cachedRingFgColor = getCssColor('--color-spinner', spinnerConfig.spinnerRingColor);
    
    // Berechne die Supernova-Basis-Farbe ohne OPACITY-Platzhalter vor
    cachedSupernovaBaseColor = spinnerConfig.supernovaColor.replace(/,\s*OPACITY\s*\)/, ')').replace('rgba', 'rgb');
}

function spawnMainParticle(forceX, forceY) {
    const p = mainParticlePool.find(p => !p.active);
    if (!p) return;
    const angle = Math.random() * Math.PI * 2;
    const radius = Math.random() * 40 + 30;
    p.active = true;
    p.x = (forceX !== undefined) ? forceX : gravityCenter.x + Math.cos(angle) * radius;
    p.y = (forceY !== undefined) ? forceY : gravityCenter.y + Math.sin(angle) * radius;
    p.z = Math.random() * 100 - 50;
    p.vx = (Math.random() - 0.5); p.vy = (Math.random() - 0.5); p.vz = (Math.random() - 0.5) * 0.5;
    p.isSwallowed = false; p.swallowedTime = 0; p.createdAt = Date.now();
    p.maxLife = spinnerConfig.particleMinLife + Math.random() * (spinnerConfig.particleMaxLife - spinnerConfig.particleMinLife);
}

function spawnTntExplosion(centerX, centerY, currentTime) {
    let count = 0;
    for (let i = 0; i < tntParticlePool.length; i++) {
        if (count >= spinnerConfig.tntParticleCount) break;
        const p = tntParticlePool[i];
        if (p.active) continue;
        const angle = Math.random() * Math.PI * 2;
        const speed = Math.random() * spinnerConfig.tntParticleSpeed * animConfig.speed;
        p.active = true; p.x = centerX; p.y = centerY;
        p.vx = Math.cos(angle) * speed; p.vy = Math.sin(angle) * speed;
        
        p.size = 1.5 + Math.random() * 3.0; 
        const rawColor = spinnerConfig.tntColors[Math.floor(Math.random() * spinnerConfig.tntColors.length)];
        p.baseColor = rawColor.replace('OPACITY', '1.0');
        p.creationTime = currentTime; 
        p.maxLifetime = 500 + Math.random() * (spinnerConfig.tntMaxLifetime + 500); 
        count++;
    }
}

function animate() {
    animationFrameId = requestAnimationFrame(animate);
    
    const currentTime = Date.now();
    if (!animConfig.lastFrameTime) animConfig.lastFrameTime = currentTime;
    let dt = (currentTime - animConfig.lastFrameTime) / 1000.0;
    if (dt > 0.1) dt = 0.1;
    animConfig.deltaTime = dt;
    animConfig.lastFrameTime = currentTime;
    animConfig.frameCount++;
    
    const currentWidth = canvas.clientWidth;
    const currentHeight = canvas.clientHeight;

    if (currentWidth === 0 || currentHeight === 0) return; 

    if (layoutWidth !== currentWidth || layoutHeight !== currentHeight) {
        layoutWidth = currentWidth;
        layoutHeight = currentHeight;
        
        currentDpr = Math.min(window.devicePixelRatio || 1, 1.25);
        canvas.width = Math.floor(layoutWidth * currentDpr);
        canvas.height = Math.floor(layoutHeight * currentDpr);
        
        gravityCenter.x = layoutWidth / 2;
        gravityCenter.y = layoutHeight / 2;
        isLayoutInitialized = true;
        
        const modal = canvas.closest('.recorder-modal');
        const header = modal?.querySelector('.recorder-header');
        const contentArea = modal?.querySelector('.popup-content-area');
        
        if (modal && header && contentArea) {
            const headerRect = header.getBoundingClientRect();
            const contentRect = contentArea.getBoundingClientRect();
            const modalRect = modal.getBoundingClientRect();
            
            const availableHeight = contentRect.top - headerRect.bottom;
            
            if (availableHeight > 20) {
                const animationDiameter = Math.min(layoutWidth, availableHeight);
                const finalRadius = (animationDiameter / 2) * 0.6;
                spinnerConfig.supernovaMaxRadius = finalRadius;
                spinnerConfig.ringRadius = finalRadius;

                const headerBottomInCanvas = headerRect.bottom - modalRect.top;
                gravityCenter.y = headerBottomInCanvas + (availableHeight / 2) - 24; 
                gravityCenter.x = layoutWidth / 2;
            }
        }
    }
    
    // Unabhängig davon, ob vorher schon durch SSE-Events Partikel eingefügt wurden, 
    // zwingen wir das System, beim Start genau initialParticleCount hinzuzufügen.
    if (!initialParticlesSpawned && layoutWidth > 0) {
        for (let i = 0; i < spinnerConfig.initialParticleCount; i++) {
            spawnMainParticle();
        }
        initialParticlesSpawned = true;
    }
    
    ctx.setTransform(currentDpr, 0, 0, currentDpr, 0, 0);
    ctx.clearRect(0, 0, layoutWidth, layoutHeight);

    updateGravityCenter();
    updateAndDrawState(currentTime);
    updateAndDrawParticles(currentTime);
}

function updateGravityCenter() { 
    // FIX 1: Wiggling entfernt. 
    // Der Code hier hat den Mittelpunkt leicht in X-Richtung wackeln lassen.
    // gravityCenter.x += (Math.random() - 0.5) * spinnerConfig.centerWander * animConfig.speed; 
}

function updateAndDrawState(currentTime) {
    const centerX = gravityCenter.x;
    const centerY = gravityCenter.y;
    switch (currentState) {
        case STATE.ORBITING: {
            const pulse = Math.sin(currentTime * 0.005) * spinnerConfig.blackHolePulseRadius;
            drawBlackHole(centerX, centerY, spinnerConfig.blackHoleInitialRadius + pulse);
            drawProgressRing(centerX, centerY, spinnerConfig.ringRadius, ringProgress);
            break;
        }
        case STATE.GROWING: {
            const elapsed = currentTime - growthStartTime;
            const progress = Math.min(elapsed / (spinnerConfig.growthDuration / animConfig.speed), 1.0);
            const easeOutQuad = progress * (2 - progress);
            const currentRadius = spinnerConfig.blackHoleInitialRadius + (spinnerConfig.ringRadius / 1.5 - spinnerConfig.blackHoleInitialRadius) * easeOutQuad;
            drawBlackHole(centerX, centerY, currentRadius);
            if (progress >= 1.0) {
                currentState = STATE.IMPLODING;
                implosionStartTime = currentTime;
            }
            break;
        }
        case STATE.IMPLODING: {
            const elapsed = currentTime - implosionStartTime;
            const progress = Math.min(elapsed / (spinnerConfig.implosionDuration / animConfig.speed), 1.0);
            drawBlackHole(centerX, centerY, spinnerConfig.ringRadius * (1 - progress));
            if (progress >= 1.0) {
                mainParticlePool.forEach(p => p.active = false);
                spinnerConfig.initialParticleCount = 0;
                currentState = STATE.RING;
                supernovaStartTime = currentTime;
                supernovaCenterX = centerX;
                supernovaCenterY = centerY;
                if (onSupernovaCallback) onSupernovaCallback();
            }
            break;
        }
        case STATE.RING: {
            if (!supernovaParticlesCreated) {
                spawnTntExplosion(supernovaCenterX, supernovaCenterY, currentTime);
                cloudSimulator.triggerCloudExplosion(supernovaCenterX, supernovaCenterY);
                supernovaParticlesCreated = true;
            }
            cloudSimulator.drawClouds(ctx);
            
            updateAndDrawTntParticles(currentTime);
            
            const supernovaElapsed = currentTime - supernovaStartTime;
            if (supernovaElapsed < spinnerConfig.supernovaDuration / animConfig.speed) {
                drawSupernovaRings(supernovaCenterX, supernovaCenterY, supernovaElapsed / (spinnerConfig.supernovaDuration / animConfig.speed));
            }
            drawProgressRing(centerX, centerY, spinnerConfig.ringRadius, ringProgress);
            break;
        }
    }
}

function updateAndDrawParticles(currentTime) {
    const physicsGroup = animConfig.frameCount % PHYSICS_THROTTLE_RATE;
    const centerX = gravityCenter.x;
    const centerY = gravityCenter.y;
    const timeFactor = 60 * animConfig.deltaTime * animConfig.speed;
    for (let i = 0; i < spinnerConfig.maxMainParticles; i++) {
        const p = mainParticlePool[i];
        if (!p.active) continue;
        if (currentState === STATE.GROWING && !p.isSwallowed) {
            const elapsed = currentTime - growthStartTime;
            const progress = Math.min(elapsed / (spinnerConfig.growthDuration / animConfig.speed), 1.0);
            const blackHoleRadius = spinnerConfig.blackHoleInitialRadius + (spinnerConfig.ringRadius - spinnerConfig.blackHoleInitialRadius) * (progress * (2 - progress));
            const dxCenter = centerX - p.x; const dyCenter = centerY - p.y;
            if ((dxCenter * dxCenter + dyCenter * dyCenter) < blackHoleRadius * blackHoleRadius) {
                p.isSwallowed = true; p.swallowedTime = currentTime; p.vx = 0; p.vy = 0;
            }
        }
        if (!p.isSwallowed) {
            if (currentState === STATE.RING) {
                const dampingFactor = Math.pow(spinnerConfig.orbitalDamping, timeFactor);
                p.vx *= dampingFactor; p.vy *= dampingFactor;
                const dx = centerX - p.x; const dy = centerY - p.y;
                const distSq = dx * dx + dy * dy;
                if (distSq > spinnerConfig.repulsionRadius * spinnerConfig.repulsionRadius) {
                    const dist = Math.sqrt(distSq);
                    p.vx += (dx / dist) * spinnerConfig.orbitalGravity * timeFactor;
                    p.vy += (dy / dist) * spinnerConfig.orbitalGravity * timeFactor;
                }
                if (i % PHYSICS_THROTTLE_RATE === physicsGroup && spinnerConfig.particleGravity > 0) {
                    for (let j = 0; j < spinnerConfig.maxMainParticles; j++) {
                        if (i === j) continue;
                        const p2 = mainParticlePool[j];
                        if (!p2.active) continue;
                        const dx2 = p2.x - p.x; const dy2 = p2.y - p.y;
                        let distSq2 = dx2 * dx2 + dy2 * dy2; if (distSq2 < 10) distSq2 = 10;
                        const dist2 = Math.sqrt(distSq2);
                        const force = (spinnerConfig.particleGravity / distSq2) * PHYSICS_THROTTLE_RATE;
                        p.vx += (dx2 / dist2) * force * timeFactor;
                        p.vy += (dy2 / dist2) * force * timeFactor;
                    }
                }
            } else {
                const dx = centerX - p.x; const dy = centerY - p.y;
                p.vx += dx * spinnerConfig.centerGravity * timeFactor;
                p.vy += dy * spinnerConfig.centerGravity * timeFactor;
            }
        }
        if (currentTime - p.createdAt > p.maxLife) { p.active = false; continue; }
        
        const speedSq = p.vx * p.vx + p.vy * p.vy;
        const maxSpdSq = spinnerConfig.maxSpeed * spinnerConfig.maxSpeed;
        if (speedSq > maxSpdSq) { 
            const speed = Math.sqrt(speedSq);
            p.vx = (p.vx / speed) * spinnerConfig.maxSpeed; 
            p.vy = (p.vy / speed) * spinnerConfig.maxSpeed; 
        }
        
        if (!p.isSwallowed) { p.x += p.vx * timeFactor; p.y += p.vy * timeFactor; } 
        else { const dxCenter = gravityCenter.x - p.x; const dyCenter = gravityCenter.y - p.y; p.x += dxCenter * 0.05 * timeFactor; p.y += dyCenter * 0.05 * timeFactor; }
        p.z += p.vz * timeFactor; if (p.z < -50 || p.z > 50) p.vz *= -0.9;
        drawParticle(p, currentTime);
    }
}

function updateAndDrawTntParticles(currentTime) {
    const timeFactor = 60 * animConfig.deltaTime * animConfig.speed;
    const physicsGroup = animConfig.frameCount % PHYSICS_THROTTLE_RATE;
    
    for (let i = 0; i < spinnerConfig.maxTntParticles; i++) {
        const p = tntParticlePool[i];
        if (!p.active) continue;
        
        const age = currentTime - p.creationTime;
        if (age >= p.maxLifetime) { p.active = false; continue; }
        
        if (i % PHYSICS_THROTTLE_RATE === physicsGroup) {
            const frictionFactor = Math.pow(spinnerConfig.tntParticleFriction, timeFactor);
            p.vx *= frictionFactor; p.vy *= frictionFactor;
        }
        
        p.x += p.vx * timeFactor; 
        p.y += p.vy * timeFactor;
        
        const opacity = 1.0 - (age / p.maxLifetime);
        if (opacity < 0.01) continue;

        ctx.fillStyle = p.baseColor;
        ctx.globalAlpha = opacity;
        ctx.beginPath(); 
        
        // FIX 2: Auch bei der Explosion `| 0` entfernt für butterweiches Fliegen
        ctx.arc(p.x, p.y, p.size, 0, Math.PI * 2); 
        ctx.fill();
    }
    ctx.globalAlpha = 1.0; 
}

function drawBlackHole(x, y, radius) {
    if (!blackHoleCache) return;
    
    const baseRadius = (HOLE_CACHE_SIZE / 2) - 2;
    const scale = radius / baseRadius;
    const size = HOLE_CACHE_SIZE * scale;
    
    // FIX 2b: Auch hier Sub-Pixel-Werte zulassen
    ctx.drawImage(blackHoleCache, x - size/2, y - size/2, size, size);
}

function drawProgressRing(x, y, radius, progress) {
    if (radius <= 0 || progress <= 0) return;
    
    // FIX 1b & FIX 2b: `| 0` wurde entfernt, damit der Ring extrem smooth läuft und wackelfrei bleibt
    ctx.beginPath(); ctx.arc(x, y, radius, -Math.PI / 2, -Math.PI / 2 + progress * Math.PI * 2);
    ctx.strokeStyle = cachedRingBgColor; ctx.lineWidth = 12; ctx.lineCap = "round"; ctx.stroke();
    
    ctx.beginPath(); ctx.arc(x, y, radius, -Math.PI / 2, -Math.PI / 2 + progress * Math.PI * 2);
    ctx.strokeStyle = cachedRingFgColor; ctx.lineWidth = 8; ctx.lineCap = "round"; ctx.stroke();
}

function drawSupernovaRings(x, y, progress) {
    const easeOutQuint = 1 - Math.pow(1 - progress, 5);
    const opacity = 1 - progress;
    
    const flashRadius = 40; 
    const flashOpacity = (1 - progress) * (1 - progress);
    
    if (supernovaFlashCache && flashOpacity > 0.01) {
        ctx.globalAlpha = flashOpacity;
        const scale = (flashRadius * 2) / FLASH_CACHE_SIZE;
        const size = FLASH_CACHE_SIZE * scale;
        ctx.drawImage(supernovaFlashCache, x - size/2, y - size/2, size, size);
        ctx.globalAlpha = 1.0; 
    }
    
    const waveRadius = easeOutQuint * spinnerConfig.supernovaMaxRadius;
    const waveWidth = Math.max(0.1, 8 * opacity);
    
    ctx.globalAlpha = Math.max(0, opacity);
    ctx.beginPath(); ctx.arc(x, y, waveRadius, 0, Math.PI * 2);
    ctx.strokeStyle = cachedSupernovaBaseColor; 
    ctx.lineWidth = waveWidth; 
    ctx.stroke();
    ctx.globalAlpha = 1.0; 
}

function drawParticle(p, currentTime) {
    const age = currentTime - p.createdAt;
    const lifeProgress = age / p.maxLife;
    let fadeOpacity = 1 - lifeProgress;
    let scale = 1 + (p.z / 50) * spinnerConfig.depthFactor;
    if (p.isSwallowed) {
        const elapsed = currentTime - p.swallowedTime;
        const progress = Math.min(elapsed / p.swallowDuration, 1.0);
        fadeOpacity *= (1 - progress); scale *= (1 - progress);
    }
    const baseOpacity = 0.5 + (p.z / 50) * 0.4;
    const finalOpacity = baseOpacity * fadeOpacity;
    if (finalOpacity <= 0.01) return;
    const radius = spinnerConfig.baseRadius * scale;
    
    ctx.globalAlpha = Math.max(0, finalOpacity);
    
    // FIX 2: Dies war der Auslöser für das Stottern! 
    // `p.x | 0` schnitt alle Kommastellen ab und zwang den langsamen Partikel
    // auf exakte Pixel-Koordinaten. Ohne `| 0` interpoliert der Canvas die Sub-Pixel
    // und die Partikel gleiten butterweich, egal wie stark der FPS-Throttle greift!
    ctx.beginPath(); ctx.arc(p.x, p.y, Math.max(0, radius), 0, Math.PI * 2);
    
    ctx.fillStyle = '#8ab4f8'; 
    ctx.fill();
    ctx.globalAlpha = 1.0;
}

export function init(canvasElement, onSupernova) {
    if (!canvasElement) return;
	if (animationFrameId) {
		cancelAnimationFrame(animationFrameId);
		animationFrameId = null;
	}

	canvas = canvasElement;
	ctx = canvas.getContext('2d');
    
    initPools();
    preRenderBlackHole();    
    preRenderSupernovaFlash(); 
    cacheGlobalColors(); 
    
    cloudSimulator.initCloudSimulator(canvas);
    cloudSimulator.reset(); 
    
    // Reset Time & Flags
    animConfig.lastFrameTime = 0; 
    isLayoutInitialized = false; 
    initialParticlesSpawned = false; // FIX: Reset Flag
    
    layoutWidth = 0; layoutHeight = 0;
    currentState = STATE.ORBITING;
    ringProgress = 0;
    
    mainParticlePool.forEach(p => p.active = false);
    tntParticlePool.forEach(p => p.active = false);
    supernovaParticlesCreated = false;
    supernovaCenterX = 0; supernovaCenterY = 0;
    onSupernovaCallback = onSupernova;
    
    animate();
}

export function stop() {
    if (animationFrameId) { cancelAnimationFrame(animationFrameId); animationFrameId = null; }
    mainParticlePool.forEach(p => p.active = false);
    tntParticlePool.forEach(p => p.active = false);
}

export function triggerBlackHoleExplosion() {
    if (currentState !== STATE.ORBITING || !canvas) return;
    supernovaParticlesCreated = false; currentState = STATE.GROWING; growthStartTime = Date.now();
}
export function addParticle() {
    let activeCount = 0;
    for (let i = 0; i < mainParticlePool.length; i++) {
        if (mainParticlePool[i].active) activeCount++;
    }
    if (!canvas || activeCount >= spinnerConfig.maxParticles) return;
    if (currentState === STATE.ORBITING || currentState === STATE.GROWING) spawnMainParticle();
}
export function setRingProgress(progress) { ringProgress = progress; }
export function getGravityCenter() { return { ...gravityCenter }; }