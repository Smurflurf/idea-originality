import { animConfig, spinnerConfig } from '/script/animation/animationConfig.js';
import * as cloudSimulator from '/script/animation/cloudSimulator.js';

// Pools
const mainParticlePool = [];
const tntParticlePool = [];

// --- CACHING: TNT ---
const tntCache = {}; 
const CACHE_SIZE = 32;   
const CACHE_RADIUS = 14;

function preRenderTntParticles() {
    spinnerConfig.tntColors.forEach(colorTemplate => {
        const color = colorTemplate.replace('OPACITY', '1.0');
        if (tntCache[color]) return;
        const c = document.createElement('canvas');
        c.width = CACHE_SIZE; c.height = CACHE_SIZE;
        const ctx = c.getContext('2d');
        ctx.fillStyle = color;
        ctx.beginPath();
        ctx.arc(CACHE_SIZE/2, CACHE_SIZE/2, CACHE_RADIUS, 0, Math.PI * 2);
        ctx.fill();
        tntCache[color] = c;
    });
}

// --- CACHING: BLACK HOLE (NEU FÜR FIREFOX) ---
let blackHoleCache = null;
const HOLE_CACHE_SIZE = 100; // Ausreichend für den Glow

function preRenderBlackHole() {
    if (blackHoleCache) return;
    blackHoleCache = document.createElement('canvas');
    blackHoleCache.width = HOLE_CACHE_SIZE;
    blackHoleCache.height = HOLE_CACHE_SIZE;
    const ctx = blackHoleCache.getContext('2d');
    
    const center = HOLE_CACHE_SIZE / 2;
    // Radius etwas kleiner als Max, damit Glow Platz hat
    const radius = (HOLE_CACHE_SIZE / 2) - 2; 
    
    const gradient = ctx.createRadialGradient(center, center, radius * 0.7, center, center, radius);
    gradient.addColorStop(0, '#000');
    gradient.addColorStop(1, 'rgba(0, 0, 0, 0)');
    
    ctx.fillStyle = gradient;
    ctx.beginPath();
    ctx.arc(center, center, radius, 0, Math.PI * 2);
    ctx.fill();
}

// --- Core Variables ---
let canvas, ctx;
let animationFrameId = null;
let gravityCenter = { x: 0, y: 0 };
let layoutWidth = 0, layoutHeight = 0;
let isLayoutInitialized = false;

// State Variables
let supernovaParticlesCreated = false;
let supernovaCenterX = 0, supernovaCenterY = 0;
let onSupernovaCallback = null;

const STATE = { ORBITING: 0, GROWING: 1, IMPLODING: 2, RING: 3 };
let currentState = STATE.ORBITING;
let growthStartTime = 0, implosionStartTime = 0, supernovaStartTime = 0;
let ringProgress = 0;

const PHYSICS_THROTTLE_RATE = 3;

// --- POOL INIT ---
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
        
        p.size = 1.5 + Math.random() * 3.0; // Variable Größe
        const rawColor = spinnerConfig.tntColors[Math.floor(Math.random() * spinnerConfig.tntColors.length)];
        p.baseColor = rawColor.replace('OPACITY', '1.0');
        p.creationTime = currentTime; 
        p.maxLifetime = 500 + Math.random() * (spinnerConfig.tntMaxLifetime + 500); 
        count++;
    }
}

// --- RENDER LOOP ---

function animate() {
    animationFrameId = requestAnimationFrame(animate);
    
    // --- ZEIT ---
    const currentTime = Date.now();
    if (!animConfig.lastFrameTime) animConfig.lastFrameTime = currentTime;
    let dt = (currentTime - animConfig.lastFrameTime) / 1000.0;
    if (dt > 0.1) dt = 0.1;
    animConfig.deltaTime = dt;
    animConfig.lastFrameTime = currentTime;
    animConfig.frameCount++;
    
    // --- LAYOUT ---
    const currentWidth = canvas.clientWidth;
    const currentHeight = canvas.clientHeight;

    if (currentWidth === 0 || currentHeight === 0) return; 

    if (layoutWidth !== currentWidth || layoutHeight !== currentHeight) {
        layoutWidth = currentWidth;
        layoutHeight = currentHeight;
        
        const dpr = window.devicePixelRatio || 1;
        canvas.width = layoutWidth * dpr;
        canvas.height = layoutHeight * dpr;
        ctx.scale(dpr, dpr);
        
        cloudSimulator.resizeCloudLayers(layoutWidth, layoutHeight);
        
        gravityCenter.x = layoutWidth / 2;
        gravityCenter.y = layoutHeight / 2;
    }

    // Gravity Center Tracking
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
    
    const activeCount = mainParticlePool.filter(p => p.active).length;
    if (activeCount === 0 && layoutWidth > 0) {
        for (let i = 0; i < spinnerConfig.initialParticleCount; i++) {
            spawnMainParticle();
        }
    }
    
    // FIX: Ganzzahlen für clearRect
    ctx.clearRect(0, 0, (layoutWidth + 1) | 0, (layoutHeight + 1) | 0);

    updateGravityCenter();
    updateAndDrawState(currentTime);
    updateAndDrawParticles(currentTime);
}

// ... Update & Draw Funktionen ...

function updateGravityCenter() { gravityCenter.x += (Math.random() - 0.5) * spinnerConfig.centerWander * animConfig.speed; }

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
                const dist = Math.sqrt(dx * dx + dy * dy);
                if (dist > spinnerConfig.repulsionRadius) {
                    p.vx += (dx / dist) * spinnerConfig.orbitalGravity * timeFactor;
                    p.vy += (dy / dist) * spinnerConfig.orbitalGravity * timeFactor;
                }
                if (i % PHYSICS_THROTTLE_RATE === physicsGroup && spinnerConfig.particleGravity > 0) {
                    for (let j = 0; j < spinnerConfig.maxMainParticles; j++) {
                        if (i === j) continue;
                        const p2 = mainParticlePool[j];
                        if (!p2.active) continue;
                        const dx2 = p2.x - p.x; const dy2 = p2.y - p.y;
                        let distSq = dx2 * dx2 + dy2 * dy2; if (distSq < 10) distSq = 10;
                        const dist2 = Math.sqrt(distSq);
                        const force = (spinnerConfig.particleGravity / distSq) * PHYSICS_THROTTLE_RATE;
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
        const speed = Math.sqrt(p.vx * p.vx + p.vy * p.vy);
        if (speed > spinnerConfig.maxSpeed) { p.vx = (p.vx / speed) * spinnerConfig.maxSpeed; p.vy = (p.vy / speed) * spinnerConfig.maxSpeed; }
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

        const cachedImg = tntCache[p.baseColor];
        if (cachedImg) {
            ctx.globalAlpha = opacity;
            const diameter = p.size * 2;
            ctx.drawImage(cachedImg, p.x - p.size, p.y - p.size, diameter, diameter);
            ctx.globalAlpha = 1.0;
        } else {
            ctx.fillStyle = p.baseColor;
            ctx.globalAlpha = opacity;
            ctx.beginPath(); ctx.arc(p.x, p.y, p.size, 0, Math.PI * 2); ctx.fill();
            ctx.globalAlpha = 1.0;
        }
    }
}

// --- OPTIMIERTER BLACK HOLE RENDERER ---
function drawBlackHole(x, y, radius) {
    if (!blackHoleCache) return;
    
    // Um das schwarze Loch auf den gewünschten Radius zu bringen,
    // müssen wir das Cache-Bild skalieren.
    // Das Cache-Bild repräsentiert einen Kreis mit Radius = (HOLE_CACHE_SIZE / 2) - 2
    // Wir wollen Ziel-Radius 'radius'.
    
    const baseRadius = (HOLE_CACHE_SIZE / 2) - 2;
    const scale = radius / baseRadius;
    
    const size = HOLE_CACHE_SIZE * scale;
    
    ctx.drawImage(blackHoleCache, x - size/2, y - size/2, size, size);
}

function getCssColor(varName, defaultColor) {
    if (typeof getComputedStyle === 'function') { const val = getComputedStyle(document.body).getPropertyValue(varName); return val ? val.trim() : defaultColor; }
    return defaultColor;
}
function drawProgressRing(x, y, radius, progress) {
    if (radius <= 0 || progress <= 0) return;
    const ringBgColor = getCssColor('--color-spinner-bg', "rgba(204, 224, 255, 0.2)");
    const ringFgColor = getCssColor('--color-spinner', spinnerConfig.spinnerRingColor);
    ctx.beginPath(); ctx.arc(x, y, radius, -Math.PI / 2, -Math.PI / 2 + progress * Math.PI * 2);
    ctx.strokeStyle = ringBgColor; ctx.lineWidth = 12; ctx.lineCap = "round"; ctx.stroke();
    ctx.beginPath(); ctx.arc(x, y, radius, -Math.PI / 2, -Math.PI / 2 + progress * Math.PI * 2);
    ctx.strokeStyle = ringFgColor; ctx.lineWidth = 8; ctx.lineCap = "round"; ctx.stroke();
}
function drawSupernovaRings(x, y, progress) {
    const easeOutQuint = 1 - Math.pow(1 - progress, 5);
    const opacity = 1 - progress;
    const flashRadius = 20;
    const flashOpacity = (1 - progress) * (1 - progress);
    
    // Flash ist auch ein Gradient, hier könnten wir auch cachen, 
    // aber es ist nur kurz sichtbar.
    const gradient = ctx.createRadialGradient(x, y, 0, x, y, flashRadius);
    gradient.addColorStop(0, `rgba(255, 255, 255, ${flashOpacity.toFixed(2)})`); gradient.addColorStop(1, `rgba(255, 255, 255, 0)`);
    ctx.fillStyle = gradient; ctx.beginPath(); ctx.arc(x, y, flashRadius, 0, Math.PI * 2); ctx.fill();
    
    const waveRadius = easeOutQuint * spinnerConfig.supernovaMaxRadius;
    const waveWidth = Math.max(0.1, 8 * opacity);
    ctx.beginPath(); ctx.arc(x, y, waveRadius, 0, Math.PI * 2);
    ctx.strokeStyle = spinnerConfig.supernovaColor.replace('OPACITY', opacity.toFixed(2));
    ctx.lineWidth = waveWidth; ctx.stroke();
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
    ctx.beginPath(); ctx.arc(p.x, p.y, Math.max(0, radius), 0, Math.PI * 2);
    ctx.fillStyle = `rgba(138, 180, 248, ${Math.max(0, finalOpacity)})`; ctx.fill();
}

// --- INIT ---
export function init(canvasElement, onSupernova) {
    if (!canvasElement) return;
	if (animationFrameId) {
		cancelAnimationFrame(animationFrameId);
		animationFrameId = null;
	}

	canvas = canvasElement;
	ctx = canvas.getContext('2d');
    
    initPools();
    preRenderTntParticles(); // TNT Cache
    preRenderBlackHole();    // Black Hole Cache (NEU)
    
    cloudSimulator.initCloudSimulator(canvas);
    cloudSimulator.reset(); 
    
    // Reset Time
    animConfig.lastFrameTime = 0; 
    
    isLayoutInitialized = false; 
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
    const activeCount = mainParticlePool.filter(p => p.active).length;
    if (!canvas || activeCount >= spinnerConfig.maxParticles) return;
    if (currentState === STATE.ORBITING || currentState === STATE.GROWING) spawnMainParticle();
}
export function setRingProgress(progress) { ringProgress = progress; }
export function getGravityCenter() { return { ...gravityCenter }; }