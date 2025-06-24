import { showQueryPopup, hideQueryPopup, updateQueryPopup } from './queryPopup.js';
import { attachedFiles } from './attachmentManager.js';

document.addEventListener('DOMContentLoaded', () => {
    const ideaForm = document.querySelector('.idea-form');
    if (!ideaForm) return;

    ideaForm.addEventListener('submit', function(event) {
        event.preventDefault();

        const queryButton = ideaForm.querySelector('.run-button');
        queryButton.disabled = true;
        showQueryPopup();

        const formData = new FormData();
        const ideaTextarea = document.querySelector('textarea[name="idea-text"]');
        if (ideaTextarea) {
            const ideaText = ideaTextarea.value.trim();
            if (ideaText.length > 0) formData.append('idea-text', ideaText);
        }
        if (attachedFiles.size > 0) {
            for (const file of attachedFiles.values()) formData.append('files', file, file.name);
        }
        
        fetch('/query/start', { method: 'POST', body: formData })
        .then(response => response.json())
        .then(data => {
            const jobId = data.jobId;
            if (!jobId) throw new Error("Keine Job-ID vom Server erhalten.");
            
            const eventSource = new EventSource(`/query/status/${jobId}`);
            
            eventSource.addEventListener('update', function(event) {
                const eventData = JSON.parse(event.data);
                
                updateQueryPopup(eventData.status, eventData.data);
                
                if (eventData.status === 'COMPLETE') {
                    eventSource.close();
                    window.location.href = `/results/${jobId}`;
                }
            });
            
            eventSource.onerror = function(error) {
                console.error("Fehler mit der SSE-Verbindung:", error);
                updateQueryPopup('ERROR', 'Connection to server lost.');
                eventSource.close();
                queryButton.disabled = false;
            };

            eventSource.onopen = function() {
                console.log("SSE-Verbindung erfolgreich geöffnet.");
            };
        })
        .catch(error => {
            console.error('Error starting the query:', error);
            alert('Could not start the query process.');
            hideQueryPopup();
            queryButton.disabled = false;
        });
    });
});