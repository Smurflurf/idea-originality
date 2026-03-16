import { emit, EVENTS } from '/script/core/eventBus.js';

const STORAGE_KEY = 'ideenatlas-theme';
const DARK = 'dark';
const LIGHT = 'light';
const SYSTEM = 'system';
const MIDNIGHT = 'midnight'; 
const THEME_ORDER = [LIGHT, DARK, MIDNIGHT];

let systemMediaQuery = window.matchMedia('(prefers-color-scheme: dark)');

function getSystemTheme() {
    return systemMediaQuery.matches ? DARK : LIGHT;
}

function applyTheme(themeSetting) {
    let effectiveTheme = themeSetting;
    if (themeSetting === SYSTEM) {
        effectiveTheme = getSystemTheme();
    }
    document.documentElement.setAttribute('data-theme', effectiveTheme);
	emit(EVENTS.THEME_CHANGED, { theme: effectiveTheme });
}


export function initThemeListener() {
    // Listener nur hinzufügen, wenn wir das Theme-System starten
    systemMediaQuery.addEventListener('change', () => {
        const currentSetting = localStorage.getItem(STORAGE_KEY) || SYSTEM;
        if (currentSetting === SYSTEM) {
            applyTheme(SYSTEM);
            updateActiveStateInUI(SYSTEM); 
		}
	});

	// Initial einmal ausführen
	initializeTheme();
}

export function initializeTheme() {
	let storedTheme = null;

	// 1. Versuch: Storage
	try {
		storedTheme = localStorage.getItem(STORAGE_KEY);
	} catch (e) {
		// Falls Cookies/Storage deaktiviert sind oder file:// Zugriff blockiert ist
		console.warn("Theme storage access failed:", e);
	}

	// 2. Versuch: HTML Attribut (Wichtig für Offline-Snapshots!)
	if (!storedTheme) {
		storedTheme = document.documentElement.getAttribute('data-theme');
	}

	// 3. Fallback: System
	applyTheme(storedTheme || SYSTEM);
}

export function setTheme(themeSetting) {
    localStorage.setItem(STORAGE_KEY, themeSetting);
    applyTheme(themeSetting);
    updateActiveStateInUI(themeSetting);
}

export function cycleNextTheme() {
    const currentSetting = getCurrentSetting();
    const currentIndex = THEME_ORDER.indexOf(currentSetting);
    const nextIndex = (currentIndex + 1) % THEME_ORDER.length;
    const nextTheme = THEME_ORDER[nextIndex];
    
    setTheme(nextTheme);
	return nextTheme;
}

export function updateActiveStateInUI(themeSetting) {
	// 1. Das "effektive" Theme ermitteln (was sieht der User gerade?)
	let effectiveTheme = themeSetting;
	if (themeSetting === SYSTEM) {
		effectiveTheme = getSystemTheme(); // Gibt 'dark' oder 'light' zurück
	}

	// 2. Alle Buttons durchgehen
	document.querySelectorAll('.theme-submenu-item').forEach(btn => {
		const btnValue = btn.getAttribute('data-theme-value');
		const checkIcon = btn.querySelector('.check-icon');

		if (!checkIcon || !btnValue) return;

		let isChecked = false;

		if (btnValue === SYSTEM) {
			// Der System-Button kriegt den Haken NUR, wenn auch "System" eingestellt ist
			isChecked = (themeSetting === SYSTEM);
		} else {
			// Die konkreten Themes kriegen den Haken, wenn sie aktiv sind
			// (Egal ob durch manuelle Wahl oder durch Systemzwang)
			isChecked = (btnValue === effectiveTheme);
		}

		checkIcon.style.opacity = isChecked ? '1' : '0';
	});
}

export function getCurrentSetting() {
    return localStorage.getItem(STORAGE_KEY) || SYSTEM;
}