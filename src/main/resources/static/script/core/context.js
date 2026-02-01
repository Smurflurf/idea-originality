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

	// Data Maps
	ownResults: [],
	ownColorMap: {},
	ownLabels: [],

	neighborColorMap: {},
	neighborLabels: [],

	serendipityResults: [],
	serendipityColorMap: {},
	serendipityLabels: [],

	contextLabels: [],

	// Config
	serverLanguages: []
};

/**
 * Sammelt alte globale Variablen ein und baut daraus ein Objekt,
 * das aussieht wie das neue INITIAL_DATA.
 */
function harvestLegacyGlobals() {
    // Wir prüfen, ob zumindest eine der Haupt-Variablen existiert, 
    // um nicht unnötig leere Objekte zu erzeugen.
    if (!window.JOB_ID && !window.OWN_RESULTS_DATA) return null;

    console.log("[Context] Legacy Mode detected. Harvesting global variables...");

    return {
        // Core
        JOB_ID: window.JOB_ID,
        JOB_TITLE: window.JOB_TITLE,
        IS_DATA_AVAILABLE: window.IS_DATA_AVAILABLE,
        QUERY_VECTOR: window.QUERY_VECTOR,
        CROSSHAIR_COORDS: window.CROSSHAIR_COORDS,
        EMBEDDING_BOUNDS: window.EMBEDDING_BOUNDS,
        OUTLINE_DATA: window.OUTLINE_DATA,

        // Own
        OWN_RESULTS_DATA: window.OWN_RESULTS_DATA,
        OWN_IDEA_COLOR_MAP: window.OWN_IDEA_COLOR_MAP,
        OWN_LABELS_DATA: window.OWN_LABELS_DATA,

        // Neighbors
        NEIGHBOR_CLUSTER_COLOR_MAP: window.NEIGHBOR_CLUSTER_COLOR_MAP,
        NEIGHBOR_LABELS_DATA: window.NEIGHBOR_LABELS_DATA,

        // Serendipity
        SERENDIPITY_RESULTS: window.SERENDIPITY_RESULTS,
        SERENDIPITY_COLOR_MAP: window.SERENDIPITY_COLOR_MAP,
        SERENDIPITY_LABELS_DATA: window.SERENDIPITY_LABELS_DATA,

        // Context
        CONTEXT_LABELS_DATA: window.CONTEXT_LABELS_DATA
    };
}

/**
 * Initialisiert den Context.
 * Akzeptiert ENTWEDER das Thymeleaf-Format (Screaming Snake) ODER das interne Format (CamelCase).
 */
export function initializeContext(data = {}) {
	// Falls data null ist, fangen wir das ab
	const d = data || harvestLegacyGlobals() || {};

	// Helper: Versucht erst Key A (Server), dann Key B (Intern), dann Default
	const val = (keyServer, keyInternal, defaultVal) => {
		if (d[keyServer] !== undefined && d[keyServer] !== null) return d[keyServer];
		if (d[keyInternal] !== undefined && d[keyInternal] !== null) return d[keyInternal];
		return defaultVal;
	};


	appState = {
		...appState,

		// Primitives
		jobId: val('JOB_ID', 'jobId', null),
		isDataAvailable: !!(d.IS_DATA_AVAILABLE ?? d.isDataAvailable ?? false),
		jobTitle: val('JOB_TITLE', 'jobTitle', ''),
		queryVector: val('QUERY_VECTOR', 'queryVector', null),
		crosshairCoords: val('CROSSHAIR_COORDS', 'crosshairCoords', null),
		embeddingBounds: val('EMBEDDING_BOUNDS', 'embeddingBounds', null),
		outlineData: val('OUTLINE_DATA', 'outlineData', {}),

		// Own
		ownResults: val('OWN_RESULTS_DATA', 'ownResults', []),
		ownColorMap: val('OWN_IDEA_COLOR_MAP', 'ownColorMap', {}),
		ownLabels: val('OWN_LABELS_DATA', 'ownLabels', []),

		// Neighbors
		neighborColorMap: val('NEIGHBOR_CLUSTER_COLOR_MAP', 'neighborColorMap', {}),
		neighborLabels: val('NEIGHBOR_LABELS_DATA', 'neighborLabels', []),

		// Serendipity
		serendipityResults: val('SERENDIPITY_RESULTS', 'serendipityResults', []),
		serendipityColorMap: val('SERENDIPITY_COLOR_MAP', 'serendipityColorMap', {}),
		serendipityLabels: val('SERENDIPITY_LABELS_DATA', 'serendipityLabels', []),

		// Context
		contextLabels: val('CONTEXT_LABELS_DATA', 'contextLabels', []),
	};

	// Spezialfall: Languages werden oft separat in window gehängt, 
	// wir checken hier, ob sie im data-Objekt sind (Offline Fall) oder global (Online Fall)
	if (window.SERVER_LANGUAGES) {
		appState.serverLanguages = window.SERVER_LANGUAGES;
		delete window.SERVER_LANGUAGES;
	} else if (d.serverLanguages) {
		appState.serverLanguages = d.serverLanguages;
	}

	console.log("[Context] Initialized:", appState);
}

export function getContext() {
	return appState;
}

export function isDataAvailable() { return appState.isDataAvailable; }
export function getJobTitle() { return appState.jobTitle || 'Analysis'; }