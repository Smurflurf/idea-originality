import { attachedFiles, getTotalFileTokens, MAX_TOKENS } from '/script/features/media/attachmentManager.js';
import { on, EVENTS } from '/script/core/eventBus.js';

let globalsInitialized = false;

function setupCustomEditor() {
    const editor = document.getElementById('editor-content');
    const hiddenInput = document.getElementById('idea-text-hidden');
    const placeholder = document.getElementById('editor-placeholder');

    if (!editor || !hiddenInput) return;

    // FIX: Globale Schriftart auslesen und explizit setzen (wie im Label Renderer)
    const bodyFontFamily = window.getComputedStyle(document.body).fontFamily || "sans-serif";
    editor.style.fontFamily = bodyFontFamily;
    if (placeholder) {
        placeholder.style.fontFamily = bodyFontFamily;
    }

    // Verhindert das Einfügen von formatiertem Text/HTML aus der Zwischenablage
	editor.addEventListener('paste', (e) => {
		e.preventDefault();
		const text = (e.originalEvent || e).clipboardData.getData('text/plain');

		let success = false;
		try {
			// Standard-Methode
			success = document.execCommand('insertText', false, text);
		} catch (err) {
			success = false;
		}

		// Sicherer Fallback für Firefox Mobile und Safari,
		// falls execCommand blockiert wird oder fehlschlägt.
		if (!success) {
			const selection = window.getSelection();
			if (selection && selection.rangeCount) {
				const range = selection.getRangeAt(0);
				range.deleteContents();
				const textNode = document.createTextNode(text);
				range.insertNode(textNode);

				// Cursor sauber ans Ende des eingefügten Textes setzen
				range.setStartAfter(textNode);
				range.collapse(true);
				selection.removeAllRanges();
				selection.addRange(range);
			}
		}

		// FIREFOX MOBILE BUGFIX:
		// Wir erzwingen hier manuell das 'input'-Event nach 10 Millisekunden.
		// Das garantiert, dass updateStempel() läuft, die Scrollbar berechnet 
		// wird und dein Token-Zähler den kopierten Text registriert.
		setTimeout(() => {
			editor.dispatchEvent(new Event('input', { bubbles: true }));
		}, 10);
	});

    // Dynamische Berechnung der Sperrzonen basierend auf den ECHTEN Button-Positionen
	const updateStempel = () => {
	    // Kurzer Reset der Höhe, damit der Textcontainer schrumpfen kann
	    editor.style.setProperty('--dynamic-height', '0px');

	    const st = editor.scrollTop;
	    const ch = editor.clientHeight > 0 ? editor.clientHeight : 250; 
	    
	    const textScrollHeight = editor.scrollHeight;
	    if (textScrollHeight > ch + 2) {
	        editor.style.overflowY = 'auto'; // Text ist lang genug -> Scrollbar an
	    } else {
	        editor.style.overflowY = 'hidden'; // Text ist kurz -> Scrollbar KOMPLETT aus
	        if (st > 0) editor.scrollTop = 0; // Zurücksetzen, falls man von unten Text löscht
	    }

	    const sh = Math.max(textScrollHeight, ch);


        const editorRect = editor.getBoundingClientRect();
        
        // CSS-Werte auslesen, um Padding und Scrollbars abzuziehen
        const computedStyle = window.getComputedStyle(editor);
        const paddingRight = parseFloat(computedStyle.paddingRight) || 0;
        const paddingTop = parseFloat(computedStyle.paddingTop) || 0;

        // DER FIX: Die echte Grenze des Textflusses (innerhalb von Padding & Scrollbar)
        // clientWidth ist die Breite minus Scrollbar. 
        const contentRight = editorRect.left + editor.clientWidth - paddingRight;
        const contentTop = editorRect.top + paddingTop;

        const mediaBtn = document.querySelector('.media-button-label');
        const runBtn = document.querySelector('.run-button');

        // Fallback-Werte
        let mediaTop = st + 12;
        let mediaBottom = st + 60;
        let runTop = st + ch - 70;
        let runBottom = st + ch;
        
        let mediaDepth = 60; 
        let runDepth = 130;  

        const paddingY = 0;  // Vertikaler Abstand des Textes zu den Buttons
        const paddingX = 4; // Horizontaler Abstand des Textes links der Buttons

        // Wenn der Editor sichtbar gerendert ist, exakte Koordinaten auslesen
        if (editorRect.height > 0) {
            if (mediaBtn) {
                const rect = mediaBtn.getBoundingClientRect();
                
                // Y-Position relativ zum echten Text-Anfang berechnen
                const relTop = rect.top - contentTop;
                const relBottom = rect.bottom - contentTop;
                
                mediaTop = st + relTop - paddingY;
                mediaBottom = st + relBottom + paddingY;
                
                // X-Tiefe präzise vom inneren Text-Rand (contentRight) aus messen!
                mediaDepth = Math.max(0, contentRight - rect.left + paddingX);
            }

            if (runBtn) {
                const rect = runBtn.getBoundingClientRect();
                
                const relTop = rect.top - contentTop;
                const relBottom = rect.bottom - contentTop;
                
                runTop = st + relTop - paddingY;
                runBottom = st + relBottom + paddingY;
                
                // X-Tiefe präzise vom inneren Text-Rand messen!
                runDepth = Math.max(0, contentRight - rect.left + paddingX);
            }
        }

        // Sicherstellen, dass keine negativen Y-Werte im Polygon entstehen
        mediaTop = Math.max(0, mediaTop);
        mediaBottom = Math.max(0, mediaBottom);
        runTop = Math.max(0, runTop);
        runBottom = Math.max(0, runBottom);

        const maxWidth = Math.max(mediaDepth, runDepth);

        // Maßgeschneiderte Konturen je Button!
        const shape = `polygon(
            100% 0px,
            100% ${mediaTop}px,
            calc(100% - ${mediaDepth}px) ${mediaTop}px,
            calc(100% - ${mediaDepth}px) ${mediaBottom}px,
            100% ${mediaBottom}px,
            100% ${runTop}px,
            calc(100% - ${runDepth}px) ${runTop}px,
            calc(100% - ${runDepth}px) ${runBottom}px,
            100% ${runBottom}px,
            100% 100%
        )`;

        editor.style.setProperty('--dynamic-width', `${maxWidth}px`);
        editor.style.setProperty('--dynamic-height', `${sh}px`);
        editor.style.setProperty('--dynamic-shape', shape);
    };

    // Bei jedem Scroll-Event sofort die Form verschieben
	editor.addEventListener('scroll', updateStempel);

	// Verarbeitung von Eingaben
	    editor.addEventListener('input', () => {
	        const text = editor.innerText;
	        hiddenInput.value = text;
	        
	        if (placeholder) {
	            const html = editor.innerHTML.trim().toLowerCase();
	            const textContent = editor.textContent || '';
	            
	            // Ein contenteditable ist visuell nur dann leer, wenn sowohl der TextContent leer ist
	            // ALS AUCH das HTML keine neuen Blöcke/Divs enthält (wie sie beim Enter-Drücken entstehen).
	            const isTextEmpty = textContent.length === 0;
	            
	            // Gängige leere HTML-Strukturen, die Browser bei einem leeren Feld (oder nach dem Löschen eines Enters) hinterlassen:
	            const emptyHtmlStates =[
	                '',
	                '<br>',
	                '<br/>',
	                '<div></div>',
	                '<div><br></div>',
	                '<div><br/></div>',
	                '<p></p>',
	                '<p><br></p>',
	                '<p><br/></p>'
	            ];
	            
	            // Wir entfernen unsichtbare Formatierungs-Leerzeichen im HTML-String nur für diesen Check
	            const cleanHtml = html.replace(/\s/g, '');
	            const isHtmlEmpty = emptyHtmlStates.includes(cleanHtml);
	            
	            const isEmpty = isHtmlEmpty && isTextEmpty;
	            
	            if (!isEmpty) {
	                placeholder.classList.add('is-hidden');
	            } else {
	                placeholder.classList.remove('is-hidden');
	            }
	        }

	        updateStempel(); 
	        updateQueryButtonState();
	    });

    // ResizeObserver
    if (window.ResizeObserver) {
        const wrapper = document.querySelector('.textarea-wrapper');
        const ro = new ResizeObserver(() => updateStempel());
        if (wrapper) ro.observe(wrapper);
        ro.observe(editor);
    }

    // Einmal Initial anstoßen
    updateStempel();
    editor.dispatchEvent(new Event('input'));
}

function updateQueryButtonState() {
	const queryButton = document.querySelector('.run-button');
	const ideaInput = document.getElementById('idea-text-hidden');
    const customEditor = document.getElementById('custom-editor');

	if (!queryButton || !ideaInput) return;

	const textValue = ideaInput.value.trim();
	const hasText = textValue.length > 0;
	const hasAttachments = attachedFiles.size > 0;

    const textTokens = Math.ceil(textValue.length * 0.5);
    const fileTokens = getTotalFileTokens();
	const totalTokens = textTokens + fileTokens;

	if (totalTokens > MAX_TOKENS) {
		queryButton.classList.add('is-disabled');
		if (customEditor) customEditor.style.borderColor = 'var(--color-danger-light)';
		queryButton.title = `Token-Limit überschritten! (${totalTokens} / ${MAX_TOKENS})`;
		return;
	} else {
		if (customEditor) customEditor.style.borderColor = '';
		queryButton.title = '';
	}

	queryButton.classList.toggle('is-disabled', !(hasText || hasAttachments));
}

export function initializeQueryButtonLogic() {
    setupCustomEditor();

	if (!globalsInitialized) {
		on(EVENTS.ATTACHMENTS_CHANGED, () => {
			updateQueryButtonState();
		});
		on(EVENTS.LANG_CHANGED, () => {
			const editor = document.getElementById('editor-content');
			if (editor) {
				setTimeout(() => editor.dispatchEvent(new Event('scroll')), 50);
			}
		});
		globalsInitialized = true;
	}
	
	updateQueryButtonState();
}
