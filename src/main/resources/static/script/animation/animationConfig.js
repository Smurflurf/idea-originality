export const animConfig = {
	// Globale Steuerung für die Animationsgeschwindigkeit
	speed: 1.2,

	repulsionBoundary: null,
    frameCount: 0,
	deltaTime: 0, 
	lastFrameTime: 0
};

export const FRAMES_PER_SECOND = 60;
export const REFERENCE_WIDTH = 420;

export const spinnerConfig = {
	// LIMITS FÜR OBJECT POOLING
	maxMainParticles: 50,
	maxTntParticles: 150,
	maxCloudParticles: 60, 
	
	
	initialParticleCount: 2,
	centerGravity: 0.0008,
	moonGravity: 0.00015,
	particleGravity: 0.02,
	orbitalGravity: 0.05, 
	orbitalDamping: 0.999, 
	repulsionRadius: 25, 
	maxSpeed: 5.0,
	baseRadius: 5,
	depthFactor: 0.5,
	centerWander: 0.15,
	ringRadius: 35,
	blackHoleInitialRadius: 5,
	blackHolePulseRadius: 2,
	growthDuration: 1200,
	implosionDuration: 250,
	catapultForce: 12,
	supernovaDuration: 800,
	supernovaMaxRadius: 120,
	supernovaColor: "rgba(255, 255, 255, OPACITY)",
	tntParticleCount: 65,
	tntParticleSpeed: 15,
	tntParticleFriction: 0.97,
	tntMaxLifetime: 2000,
	tntColors: ["rgba(255, 200, 100, OPACITY)", "rgba(255, 150, 80, OPACITY)", "rgba(220, 220, 200, OPACITY)"],
	cloudParticleCount: 15,
	cloudParticleSize: 220,
	cloudParticleSpeed: 1.9,
	cloudParticleFriction: 0.995,
	cloudFadeInTime: 3000,
	cloudFadeToMinTime: 25000,
	cloudMinOpacity: 0.25,
	cloudMaxOpacity: 0.55,
	cloudPixelJitter: 25,
	cloudColors: ["rgba(80, 100, 180, OPACITY)", "rgba(140, 90, 180, OPACITY)", "rgba(180, 100, 140, OPACITY)"],
	spinnerRingColor: "#cce0ff",
	maxParticles: 10,
	particleMinLife: 35000,
	particleMaxLife: 115000
};