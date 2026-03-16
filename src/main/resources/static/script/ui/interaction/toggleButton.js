let isInitialized = false;

/**
 * Wandelt rohen JSON-Text in farbige HTML-Spans um.
 * Das macht einzelne Keys und Values für den Translator greifbar.
 */
function formatJsonToFragment(jsonString) {
    const fragment = document.createDocumentFragment();
    if (!jsonString) return fragment;
    
    let json;
    try {
        const obj = JSON.parse(jsonString);
        json = JSON.stringify(obj, null, 2);
    } catch (e) {
        console.warn("JSON Formatting failed", e);
        fragment.appendChild(document.createTextNode(jsonString));
        return fragment;
    }

    const regex = /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g;
    let lastIndex = 0;
    
    let match;
    while ((match = regex.exec(json)) !== null) {
        if (match.index > lastIndex) {
            fragment.appendChild(document.createTextNode(json.substring(lastIndex, match.index)));
        }
        
        let cls = 'json-number';
        if (/^"/.test(match[0])) {
            if (/:$/.test(match[0])) {
                cls = 'json-key';
            } else {
                cls = 'json-string';
            }
        } else if (/true|false/.test(match[0])) {
            cls = 'json-boolean';
        } else if (/null/.test(match[0])) {
            cls = 'json-null';
        }
        
        // Füge das Highlight-Wort als Span hinzu
        const span = document.createElement('span');
        span.className = cls;
        span.textContent = match[0];
        fragment.appendChild(span);
        
        lastIndex = regex.lastIndex;
    }
    
    // Den restlichen Text am Ende anfügen
    if (lastIndex < json.length) {
        fragment.appendChild(document.createTextNode(json.substring(lastIndex)));
    }
    
    return fragment;
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

            const preElement = payloadContainer.querySelector('pre');
            if (preElement && !preElement.dataset.isFormatted) {
                const rawJson = preElement.textContent;
                const fragment = formatJsonToFragment(rawJson);
                preElement.textContent = ''; 
                preElement.appendChild(fragment);
                preElement.dataset.isFormatted = 'true';
            }
        }
    });

    isInitialized = true;
}