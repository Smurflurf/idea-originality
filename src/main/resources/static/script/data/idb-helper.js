const DB_NAME = 'IdeaAtlasCache';
const JOB_STORE = 'jobs'; 
const SNAPSHOT_STORE = 'viz-snapshots';
const DB_VERSION = 4; 

let dbPromise = null;

function openDb() {
    if (dbPromise) return dbPromise;
    dbPromise = new Promise((resolve, reject) => {
        const request = indexedDB.open(DB_NAME, DB_VERSION);
        
        request.onerror = () => reject("Error opening IndexedDB.");
        request.onsuccess = () => resolve(request.result);
        
        request.onupgradeneeded = (event) => {
            const db = event.target.result;
            
            // 1. Alten, ungenutzten Store löschen, falls er existiert
            if (db.objectStoreNames.contains('resources')) {
                db.deleteObjectStore('resources');
                console.log("Unbenutzter 'resources' Store wurde entfernt.");
            }

            // 2. Job-Metadaten Store erstellen
            if (!db.objectStoreNames.contains(JOB_STORE)) {
                const jobStore = db.createObjectStore(JOB_STORE, { keyPath: 'jobId' });
                jobStore.createIndex('timestamp', 'timestamp', { unique: false });
            }
			
			// 3. Snapshot Store erstellen			
			if (!db.objectStoreNames.contains(SNAPSHOT_STORE)) {
				db.createObjectStore(SNAPSHOT_STORE, { keyPath: 'id' });
			}
        };
    });
    return dbPromise;
}

// --- Asset-Methoden ---
export async function saveAsset(jobId, url, content) {
    const db = await openDb();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(ASSET_STORE, 'readwrite');
        tx.objectStore(ASSET_STORE).put({ id: `${jobId}_${url}`, jobId, url, content });
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject("Failed to save asset.");
    });
}

export async function getAsset(jobId, url) {
    const db = await openDb();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(ASSET_STORE, 'readonly');
        const req = tx.objectStore(ASSET_STORE).get(`${jobId}_${url}`);
        req.onsuccess = () => resolve(req.result ? req.result.content : null);
        req.onerror = () => reject("Failed to get asset.");
    });
}

export async function saveVizSnapshot(snapshotData) {
    const db = await openDb();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(SNAPSHOT_STORE, 'readwrite');
        tx.objectStore(SNAPSHOT_STORE).put(snapshotData);
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject("Failed to save snapshot.");
    });
}

export async function getVizSnapshot(id) {
    const db = await openDb();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(SNAPSHOT_STORE, 'readonly');
        const req = tx.objectStore(SNAPSHOT_STORE).get(id);
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject("Failed to get snapshot.");
    });
}

/**
 * Toggelt den Stern-Status eines Jobs.
 */
export async function toggleJobStar(jobId) {
    const db = await openDb();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(JOB_STORE, 'readwrite');
        const store = tx.objectStore(JOB_STORE);
        
        const getReq = store.get(jobId);
        
        getReq.onsuccess = () => {
            const job = getReq.result;
            if (job) {
                job.starred = !job.starred;
                // Wenn gestarrt, setze Timestamp für Sortierung (LIFO Queue bei Stars)
                // Wenn ent-starrt, Timestamp egal (oder 0)
                job.starredTimestamp = job.starred ? Date.now() : 0;
                
                store.put(job);
            }
        };
        
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject("Failed to toggle star.");
    });
}

/**
 * Speichert einen Job in der Historie.
 * @param {string} jobId - Die UUID des Jobs.
 * @param {string} title - Der Titel 
 */
export async function saveJobToHistory(jobId, title) {
    const db = await openDb();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(JOB_STORE, 'readwrite');
        const store = tx.objectStore(JOB_STORE);
        
        // Zuerst prüfen, ob der Job schon existiert (um 'starred' Status zu erhalten)
        const getReq = store.get(jobId);
        
        getReq.onsuccess = () => {
            const existingJob = getReq.result;
            
            const jobData = {
                jobId: jobId,
                title: title.length > 40 ? title.substring(0, 37) + '...' : title,
                timestamp: Date.now(), // Immer aktualisieren bei Besuch
                // Behalte Status bei, sonst false
                starred: existingJob ? existingJob.starred : false,
                starredTimestamp: existingJob ? existingJob.starredTimestamp : 0
            };

            store.put(jobData);
        };
        
        getReq.onerror = () => {
            // Fallback falls Get fehlschlägt (sollte nicht passieren)
            store.put({ jobId, title, timestamp: Date.now(), starred: false, starredTimestamp: 0 });
        };

        tx.oncomplete = () => resolve();
        tx.onerror = () => reject("Failed to save job history.");
    });
}

/**
 * Holt alle Jobs, sortiert nach Zeitstempel (neueste zuerst).
 */
export async function getJobHistory() {
    const db = await openDb();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(JOB_STORE, 'readonly');
        const store = tx.objectStore(JOB_STORE);
        const index = store.index('timestamp');
        
        // openCursor(null, 'prev') sortiert absteigend (neueste zuerst)
        const request = index.openCursor(null, 'prev');
        const results = [];

        request.onsuccess = (event) => {
            const cursor = event.target.result;
            if (cursor) {
                results.push(cursor.value);
                cursor.continue();
            } else {
                resolve(results);
            }
        };
        request.onerror = () => reject("Failed to load history.");
    });
}

/**
 * Löscht einen spezifischen Job aus der Historie.
 * @param {string} jobId - Die ID des zu löschenden Jobs.
 */
export async function deleteJobFromHistory(jobId) {
	console.log(`[IDB] Versuche Job zu löschen: ${jobId}`);
	try {
		const db = await openDb();
		return new Promise((resolve, reject) => {
			// Wir öffnen eine Transaktion über BEIDE Stores
			const tx = db.transaction([JOB_STORE, SNAPSHOT_STORE], 'readwrite');

			// 1. Job löschen
			tx.objectStore(JOB_STORE).delete(jobId);

			// 2. Zugehörige Snapshots löschen, um Speicherlecks zu verhindern
			const snapStore = tx.objectStore(SNAPSHOT_STORE);
			snapStore.delete(`${jobId}_own`);
			snapStore.delete(`${jobId}_nc`);
			snapStore.delete(`${jobId}_serendipity`);

			tx.oncomplete = () => {
				console.log(`[IDB] Transaktion abgeschlossen. Job ${jobId} ist weg.`);
				resolve();
			};

			tx.onerror = (e) => {
				console.error(`[IDB] Fehler beim Löschen:`, e);
				reject("Failed to delete job from history.");
			};
		});
	} catch (e) {
		console.error("[IDB] Kritischer Fehler beim Datenbankzugriff:", e);
	}
}
