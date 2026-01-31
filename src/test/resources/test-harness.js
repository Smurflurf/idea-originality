
import { showQueryPopup, queueSseEvent, hideQueryPopup } from './script/queryPopup.js';

const ideaForm = document.querySelector('.idea-form');
if (!ideaForm) {
    console.error("Test-Harness: Konnte das .idea-form Element nicht finden.");
} else {
    ideaForm.addEventListener('submit', function(event) {
        // Verhindern, dass das Formular wirklich abgeschickt wird
        event.preventDefault(); 
        
        console.log("Test-Harness: Formular-Submit abgefangen. Starte Mock-Prozess.");

        const queryButton = ideaForm.querySelector('.run-button');
        queryButton.disabled = true;
        
        // 1. Das Popup anzeigen.
        showQueryPopup();

        // Flag, um die Race Condition zu verhindern.
        let hasReceivedTerminalEvent = false;

        // 2. SSE-Verbindung zum Mock-Server aufbauen.
        const eventSource = new EventSource(`/mock-query/status`);

        // 3. Sobald die Verbindung steht, das Start-Signal senden.
        eventSource.onopen = function() {
            console.log("Test-Harness: SSE-Verbindung zum Mock-Server offen. Sende Startsignal.");
            fetch(`/mock-query/start`, { method: 'POST' })
                .catch(error => console.error("Konnte Startsignal nicht an Mock-Server senden:", error));
        };

        // 4. Eingehende Events vom Mock-Server verarbeiten.
        eventSource.addEventListener('update', (e) => {
            const eventData = JSON.parse(e.data);
            
            // Event an die Popup-Queue weiterleiten, um das UI zu aktualisieren.
            queueSseEvent(eventData.status, eventData.data); 

            // Wenn ein finaler Status vom Mock-Backend kommt, setzen wir den Flag.
            if (eventData.status === 'COMPLETE' || eventData.status === 'ERROR' || eventData.status === 'CLOSE_POPUP') {
                console.log(`Terminal event '${eventData.status}' received from mock backend.`);
                hasReceivedTerminalEvent = true;
            }

            // Auf das spezielle Schließen-Signal vom Test-Server reagieren.
            if (eventData.status === 'CLOSE_POPUP') {
                hideQueryPopup();
                eventSource.close();
                queryButton.disabled = false; // Button wieder aktivieren
            }
        });

        // 5. Generische Verbindungsfehler behandeln.
        eventSource.onerror = function(error) {
            // **DIE KORREKTUR:** Nur handeln, wenn wir noch keine End-Nachricht bekommen haben.
            if (hasReceivedTerminalEvent) {
                console.log("SSE 'onerror' triggered, but a terminal event was already received. Ignoring.");
                return;
            }

            console.error("Test-Harness: Fehler mit der Mock-SSE-Verbindung:", error);
            queueSseEvent('ERROR', 'Connection to mock server lost.');
            eventSource.close();
            queryButton.disabled = false;
        };
    });
}