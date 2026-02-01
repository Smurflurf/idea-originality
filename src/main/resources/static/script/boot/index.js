/**
 * Zentraler Einstiegspunkt für die index.html Seite.
 */
import '/styling/style.css';
import '/styling/recorder.css';
import '/styling/queryPopup.css';
import '/styling/tooltips.css';

import '/script/ui/navigation/menu.js';
import '/script/features/query/handleQuery.js';
import '/script/features/query/queryButtonManager.js';
import '/script/features/media/mediaActions.js';
import '/script/features/media/dragAndDrop.js';
import '/script/features/media/attachmentManager.js';

document.addEventListener("DOMContentLoaded", (event) => {
	// --- CUSTOM FPS MONITOR V11 (VALLEY HOLD) START ---
	// --- CUSTOM FPS MONITOR V11 END ---



	// Viewport Textarea Verschiebung um immer im view zu bleiben
	// TODO
});
