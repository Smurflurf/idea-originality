// ./core/context.js

// Initialer State (Defaults)
let appState = {
	jobId: null,
	isDataAvailable: false,
	jobTitle: '',
	queryVector: null,
	crosshairCoords: null,
	embeddingBounds: null,
	outlineData: {},
	ownResults: [],
	ownColorMap: {},
	ownLabels: [],
	neighborColorMap: {},
	neighborLabels: [],
	serendipityResults: [],
	serendipityColorMap: {},
	serendipityLabels: [],
	contextLabels: [],
	serverLanguages: []
};

/**
 * Sammelt alte globale Variablen ein (Legacy Support)
 */
function harvestLegacyGlobals() {
	if (!window.JOB_ID && !window.OWN_RESULTS_DATA) return null;
	return {
		JOB_ID: window.JOB_ID,
		JOB_TITLE: window.JOB_TITLE,
		IS_DATA_AVAILABLE: window.IS_DATA_AVAILABLE,
		QUERY_VECTOR: window.QUERY_VECTOR,
		CROSSHAIR_COORDS: window.CROSSHAIR_COORDS,
		EMBEDDING_BOUNDS: window.EMBEDDING_BOUNDS,
		OUTLINE_DATA: window.OUTLINE_DATA,
		OWN_RESULTS_DATA: window.OWN_RESULTS_DATA,
		OWN_IDEA_COLOR_MAP: window.OWN_IDEA_COLOR_MAP,
		OWN_LABELS_DATA: window.OWN_LABELS_DATA,
		NEIGHBOR_CLUSTER_COLOR_MAP: window.NEIGHBOR_CLUSTER_COLOR_MAP,
		NEIGHBOR_LABELS_DATA: window.NEIGHBOR_LABELS_DATA,
		SERENDIPITY_RESULTS: window.SERENDIPITY_RESULTS,
		SERENDIPITY_COLOR_MAP: window.SERENDIPITY_COLOR_MAP,
		SERENDIPITY_LABELS_DATA: window.SERENDIPITY_LABELS_DATA,
		CONTEXT_LABELS_DATA: window.CONTEXT_LABELS_DATA
	};
}

export function initializeContext(data = {}) {
	// Prio: Übergebenes Data-Objekt > Legacy Globals > Leeres Objekt
	const d = data || harvestLegacyGlobals() || {};

	const val = (keyServer, keyInternal, defaultVal) => {
		if (d[keyServer] !== undefined && d[keyServer] !== null) return d[keyServer];
		if (d[keyInternal] !== undefined && d[keyInternal] !== null) return d[keyInternal];
		return defaultVal;
	};

	// WICHTIG: Wir erstellen ein komplett neues State-Objekt, 
	// um sicherzustellen, dass keine Referenzen auf alte Arrays/Objekte bestehen bleiben.
	appState = {
		jobId: val('JOB_ID', 'jobId', null),
		isDataAvailable: !!(d.IS_DATA_AVAILABLE ?? d.isDataAvailable ?? false),
		jobTitle: val('JOB_TITLE', 'jobTitle', ''),
		queryVector: val('QUERY_VECTOR', 'queryVector', null),
		crosshairCoords: val('CROSSHAIR_COORDS', 'crosshairCoords', null),
		embeddingBounds: val('EMBEDDING_BOUNDS', 'embeddingBounds', null),
		outlineData: val('OUTLINE_DATA', 'outlineData', {}),
		ownResults: val('OWN_RESULTS_DATA', 'ownResults', []),
		ownColorMap: val('OWN_IDEA_COLOR_MAP', 'ownColorMap', {}),
		ownLabels: val('OWN_LABELS_DATA', 'ownLabels', []),
		neighborColorMap: val('NEIGHBOR_CLUSTER_COLOR_MAP', 'neighborColorMap', {}),
		neighborLabels: val('NEIGHBOR_LABELS_DATA', 'neighborLabels', []),
		serendipityResults: val('SERENDIPITY_RESULTS', 'serendipityResults', []),
		serendipityColorMap: val('SERENDIPITY_COLOR_MAP', 'serendipityColorMap', {}),
		serendipityLabels: val('SERENDIPITY_LABELS_DATA', 'serendipityLabels', []),
		contextLabels: val('CONTEXT_LABELS_DATA', 'contextLabels', []),
	};

	if (window.SERVER_LANGUAGES) {
		appState.serverLanguages = window.SERVER_LANGUAGES;
		delete window.SERVER_LANGUAGES;
	} else if (d.serverLanguages) {
		appState.serverLanguages = d.serverLanguages;
	}

	// Optional: Debugging
	console.log("[Context] Re-Initialized:", appState.jobId);
}

export function getContext() { return appState; }
export function isDataAvailable() { return appState.isDataAvailable; }
export function getJobTitle() { return appState.jobTitle || 'Analysis'; }
export function isOfflineMode() {
	return !!window.INITIAL_DATA; 
//	return document.documentElement.getAttribute('data-is-offline') === 'true'; 
}
//TODO DAS HIE RÜBERPRÜFEN