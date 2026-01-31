let isInitialized = false;

/**
 * Wandelt rohen JSON-Text in farbige HTML-Spans um.
 * Das macht einzelne Keys und Values für den Translator greifbar.
 */
function formatJsonToHtml(jsonString) {
    if (!jsonString) return '';
    try {
        const obj = JSON.parse(jsonString);
        // Neu formatieren mit Einrückung
        let json = JSON.stringify(obj, null, 2);
        
        // HTML Escaping (wichtig!)
        json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

        // Regex Magie für Syntax Highlighting
        return json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function (match) {
            let cls = 'json-number';
            if (/^"/.test(match)) {
                if (/:$/.test(match)) {
                    cls = 'json-key';
                } else {
                    cls = 'json-string';
                }
            } else if (/true|false/.test(match)) {
                cls = 'json-boolean';
            } else if (/null/.test(match)) {
                cls = 'json-null';
            }
            return '<span class="' + cls + '">' + match + '</span>';
        });
    } catch (e) {
        console.warn("JSON Formatting failed", e);
        return jsonString; // Fallback auf rohen Text
    }
}

export function initializeJsonToggles() {
    if (isInitialized) {
        return;
    }

    document.body.addEventListener('click', function(event) {
        const button = event.target.closest('.toggle-json-btn');
        
        if (!button) {
            return;
        }

        button.classList.toggle('active');
        const payloadContainer = button.nextElementSibling;
        
        if (payloadContainer && payloadContainer.classList.contains('result-payload')) {
            payloadContainer.classList.toggle('active');

            // +++ NEUE LOGIK: JSON formatieren beim ersten Öffnen +++
            const preElement = payloadContainer.querySelector('pre');
            if (preElement && !preElement.dataset.isFormatted) {
                const rawJson = preElement.textContent;
                const formattedHtml = formatJsonToHtml(rawJson);
                preElement.innerHTML = formattedHtml;
                preElement.dataset.isFormatted = 'true';
            }
        }
    });

    isInitialized = true;
}