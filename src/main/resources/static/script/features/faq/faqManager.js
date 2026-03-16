import { getTemplate, renderTemplate } from '/script/core/templateManager.js';
import { applyGeneralTranslations, t, buildFaqAnswerContent } from '/script/core/localization.js';
import { closeSidebar, setMainContentState } from '/script/ui/navigation/menu.js';
import { stopMode } from '/script/ui/interaction/selectionMode.js';
import { on, EVENTS } from '/script/core/eventBus.js';

// --- STATE ---
let faqData = null;
let isSelectionMode = false;
let faqCursorElement = null;

// Cache leeren bei Sprachwechsel
on(EVENTS.LANG_CHANGED, () => {
    faqData = null;
});

function silenceInteraction(e) {
    e.stopImmediatePropagation();
}

// --- HELPER: DATA LOADING ---
async function ensureFaqData() {
	if (faqData) return faqData;

	let lang = document.documentElement.lang || 'en';
	let path = `/assets/i18n/${lang}/faq.json`;

	try {
		let res = await fetch(path);

		// Wenn die Datei für die aktuelle Sprache nicht gefunden wird,
		// und wir nicht ohnehin schon bei 'en' sind, lade Englisch!
		if (!res.ok && lang !== 'en') {
			console.log(`[FAQ] Language '${lang}' not found. Falling back to 'en'.`);
			lang = 'en';
			path = `/assets/i18n/en/faq.json`;
			res = await fetch(path);
		}

		if (!res.ok) throw new Error(`FAQ completely missing (tried ${lang})`);
        const json = await res.json();
        
        // Flache Liste für die Suche erstellen
        const flatItems = [];
        const traverse = (items) => {
            items.forEach(item => {
                if (item.items) traverse(item.items);
                else if (item.question && item.answer) flatItems.push(item);
            });
        };
        if (json.structure) traverse(json.structure);
        
        faqData = { full: json, flat: flatItems };
        return faqData;
    } catch (e) {
        console.error("FAQ Load Error:", e);
        return null;
    }
}

// --- HELPER: RELEVANCE & PAGE ID ---
function getRelevanceScore(item, currentPage) {
    if (!item.relevant_on || !Array.isArray(item.relevant_on)) return 1;
    const hasCurrent = item.relevant_on.includes(currentPage);
    const hasWildcard = item.relevant_on.includes('*');
    if (hasCurrent && item.relevant_on.length === 1) return 4;
    if (hasCurrent && item.relevant_on.length > 1) return 3;
    if (hasWildcard) return 2;
    return 1;
}

function getCurrentPageId() {
    const path = window.location.pathname;
    if (path === '/' || path === '/index.html') return 'index';
    let clean = path.replace(/^\//, '');
    const parts = clean.split('/');
    let page = parts[0];
    page = page.replace('.html', '');
    return page.toLowerCase();
}

// --- HELPER: SCROLL TO ITEM (Auf der aktuellen Seite) ---
function openAndScrollToFaq(id) {
	const target = document.getElementById(id);
	if (!target) return false; // Nicht auf dieser Seite gefunden

	// 1. Alles andere schließen für Fokus
	document.querySelectorAll('.faq-item.is-open, .faq-item.is-open-inline').forEach(el => {
		el.classList.remove('is-open', 'is-open-inline');
	});
	document.querySelectorAll('.faq-category-card.has-active-content, .faq-category-card.is-expanded-all').forEach(card => {
		card.classList.remove('has-active-content', 'is-expanded-all');
	});

	// 2. Ziel öffnen
	target.classList.add('is-open');

	// 3. Vorfahren suchen und aktivieren (Pfeil nach oben richten)
	let currentParent = target.closest('.faq-category-card');
	while (currentParent) {
		currentParent.classList.add('has-active-content');
		// Eine Ebene höher gehen
		currentParent = currentParent.parentElement ? currentParent.parentElement.closest('.faq-category-card') : null;
	}

	// 4. Hinscrollen
	setTimeout(() => {
		const windowHeight = window.innerHeight;
		// Hier verwende ich deine neuen Metriken (0.95 und 65px)
		if (target.scrollHeight > windowHeight * 0.95) {
			const y = target.getBoundingClientRect().top + window.scrollY - 65;
			window.scrollTo({ top: y, behavior: 'smooth' });
		} else {
			target.scrollIntoView({ behavior: 'smooth', block: 'center' });
		}
	}, 250);

	// 5. Visueller Pulse-Effekt
	target.classList.remove('is-highlighted');
	void target.offsetWidth; // Reflow erzwingen
	target.classList.add('is-highlighted');

	setTimeout(() => {
		target.classList.remove('is-highlighted');
	}, 2000);

	return true;
}

// --- HELPER: OPEN MODAL (Wenn Item nicht auf der Seite ist) ---
function openAnswerModal(item) {
    // 1. Versuch: Gibt es das Template auf der Seite?
    let modal = document.getElementById('faq-overlay-answer');
	
    if (!modal) {
        const tpl = renderTemplate('faq-overlay-answer');
        if (tpl) {
            modal = tpl;
        } else {
            // 2. FALLBACK: Dynamischer Aufbau, falls wir nicht auf der FAQ Seite sind!
            modal = document.createElement('div');
            modal.id = 'faq-overlay-answer';
            // Wir recyceln die Styles des Audio-Recorders für das Overlay
            modal.className = 'recorder-overlay'; 
            
            const modalInner = document.createElement('div');
            modalInner.className = 'recorder-modal faq-answer-modal';
            
            const header = document.createElement('div');
            header.className = 'recorder-header';
            
            const title = document.createElement('h2');
            title.className = 'faq-answer-title';
            
            const closeBtn = document.createElement('button');
            closeBtn.className = 'recorder-close-btn';
            closeBtn.innerHTML = '<i class="fa-solid fa-xmark"></i>';
            
            header.appendChild(title);
            header.appendChild(closeBtn);
            
            const body = document.createElement('div');
            body.className = 'recorder-body';
            
            const contentBox = document.createElement('div');
            contentBox.className = 'faq-answer-content-box';
            
            body.appendChild(contentBox);
            modalInner.appendChild(header);
            modalInner.appendChild(body);
            modal.appendChild(modalInner);
            
            document.body.appendChild(modal);
        }
    }

    // Inhalt befüllen
    modal.querySelector('.faq-answer-title').textContent = item.question;
    const contentBox = modal.querySelector('.faq-answer-content-box');
    contentBox.innerHTML = ''; 
    
    // Baut den Text und die UI-Mockups zusammen
    buildFaqAnswerContent(contentBox, item);

    // Scroll-Sperre für den Hintergrund aktivieren
    setMainContentState(true);

    const closeBtn = modal.querySelector('.recorder-close-btn');
    const close = () => {
        modal.classList.remove('is-visible');
        setTimeout(() => {
            modal.remove();
            // Scroll-Sperre wieder aufheben
            setMainContentState(false);
        }, 300);
    };
    
    closeBtn.onclick = close;
    modal.onclick = (e) => { if (e.target === modal) close(); };

    // Kurzes Timeout für CSS Transition
    setTimeout(() => modal.classList.add('is-visible'), 10);
}


// =========================================================================
// FEATURE 1: SELECTION MODE (Kontext-Hilfe Cursor)
// =========================================================================

function moveFaqCursor(e) {
    if (faqCursorElement) {
        faqCursorElement.style.left = e.clientX + 'px';
        faqCursorElement.style.top = e.clientY + 'px';
    }
}

export async function toggleFaqSelectionMode(event = null) {
    if (isSelectionMode) {
        disableSelectionMode();
        return;
    }
    if (window.getSelection) window.getSelection().removeAllRanges();
    stopMode(); // Beendet Translate/Read Mode falls aktiv
    closeSidebar();

    const data = await ensureFaqData();
    if (!data) { alert(t('faq.load_error') || "FAQ data could not be loaded."); return; }

    isSelectionMode = true;
    document.body.classList.add('faq-selection-active');

	if (window.tippy) {
	    tippy.hideAll({ duration: 0 });
	}
	
	if (document.activeElement && typeof document.activeElement.blur === 'function') {
		document.activeElement.blur(); 
	}

	document.querySelectorAll('[contenteditable="true"]').forEach(el => {
		el.setAttribute('contenteditable', 'false');
		el.dataset.faqRestoredEditable = 'true';
	});

	document.querySelectorAll('input:not([type="hidden"]), textarea, select').forEach(el => {
		if (!el.disabled) {
			el.disabled = true;
			el.dataset.faqRestoredDisabled = 'true';
		}
	});
	
    if (!faqCursorElement) {
        faqCursorElement = document.querySelector('.custom-translate-cursor');
        if (!faqCursorElement) {
            faqCursorElement = document.createElement('div');
            faqCursorElement.className = 'custom-translate-cursor';
            document.body.appendChild(faqCursorElement);
        }
    }
    faqCursorElement.innerHTML = '<i class="fa-solid fa-circle-question"></i>';
    faqCursorElement.style.display = 'block';

    if (event) {
        faqCursorElement.style.left = event.clientX + 'px';
        faqCursorElement.style.top = event.clientY + 'px';
    }

    document.addEventListener('mousemove', moveFaqCursor);

    const menuTrigger = document.getElementById('menu-trigger');
    if (menuTrigger) {
        menuTrigger.innerHTML = '<i class="fa-solid fa-circle-question faq-trigger-icon"></i>';
        menuTrigger.classList.add('is-faq-active'); 
    }

    // Elemente markieren
    let foundCount = 0;
    data.flat.forEach(item => {
        // NEU: Trennung von Mockup und Live-Selektion
        let selectors = [];
        if (item.faq_select) {
            selectors = item.faq_select;
        } else {
            selectors = [...(item.ui_mockup || []), ...(item.highlight_mockup || [])];
		}

		selectors.forEach(sel => {
			// Nur echte DOM-Elemente markieren, keine Mockup-Referenzen (idb:, cache:)
			if (!sel.startsWith('idb:') && !sel.startsWith('cache:')) {
				document.querySelectorAll(sel).forEach(el => {
					// Die Prüfung auf el.offsetParent wurde entfernt!
					el.classList.add('faq-context-highlight');
					el.dataset.faqId = item.id;

					// Fängt Mousedown, Touch & Mausrad ab, BEVOR die Buttons reagieren
					el.addEventListener('mousedown', silenceInteraction, { capture: true, passive: false });
					el.addEventListener('touchstart', silenceInteraction, { capture: true, passive: false });
					el.addEventListener('touchend', silenceInteraction, { capture: true, passive: false });
					el.addEventListener('wheel', silenceInteraction, { capture: true, passive: false });

					foundCount++;
				});
			}
		});
    });

    if (foundCount === 0) {
        alert(t('faq.no_elements') || "No help topics found for elements on this page.");
        disableSelectionMode();
        return;
    }

    document.addEventListener('click', handleFaqClick, { capture: true });
    document.addEventListener('keydown', handleEscKey);
}

function disableSelectionMode() {
    isSelectionMode = false;
	document.body.classList.remove('faq-selection-active');

	document.querySelectorAll('[data-faq-restored-editable="true"]').forEach(el => {
		el.setAttribute('contenteditable', 'true');
		delete el.dataset.faqRestoredEditable;
	});

	document.querySelectorAll('[data-faq-restored-disabled="true"]').forEach(el => {
		el.disabled = false;
		delete el.dataset.faqRestoredDisabled;
	});
	
	document.querySelectorAll('.faq-context-highlight').forEach(el => {
		el.classList.remove('faq-context-highlight');
		delete el.dataset.faqId;

		// NEU: Blocker wieder entfernen, sobald FAQ-Modus beendet wird
		el.removeEventListener('mousedown', silenceInteraction, { capture: true });
		el.removeEventListener('touchstart', silenceInteraction, { capture: true });
		el.removeEventListener('touchend', silenceInteraction, { capture: true });
		el.removeEventListener('wheel', silenceInteraction, { capture: true });
	});

    document.removeEventListener('click', handleFaqClick, { capture: true });
    document.removeEventListener('keydown', handleEscKey);

    if (faqCursorElement) {
        faqCursorElement.style.display = 'none';
    }
    document.removeEventListener('mousemove', moveFaqCursor);

    const menuTrigger = document.getElementById('menu-trigger');
    if (menuTrigger) {
        menuTrigger.innerHTML = '<i class="fa-solid fa-bars"></i>';
        menuTrigger.classList.remove('is-faq-active'); 
    }
}

function handleFaqClick(e) {
	// 1. Klicks auf das Menü (Seitenleiste) erlauben wir
	if (e.target.closest('.sidebar-menu')) return; 

	// 2. DAS IST DER KERN: Stoppt absolut alle anderen Klicks im gesamten DOM!
	// Egal was darunter liegt (Links, Buttons, Toggles), es wird nicht ausgeführt.
	e.preventDefault();
	e.stopPropagation();
	e.stopImmediatePropagation();

	// 3. Prüfen, ob wir auf ein hervorgehobenes FAQ-Element geklickt haben
	const target = e.target.closest('.faq-context-highlight');
	
	if (target) {
		const faqId = target.dataset.faqId;
		const item = faqData.flat.find(i => i.id === faqId);
		if (item) {
			disableSelectionMode();
			openAnswerModal(item);
		}
	} else {
		// Klick ins "Leere" (nicht hervorgehobene Elemente) beendet den Modus einfach
		disableSelectionMode();
	}
}

function handleEscKey(e) {
    if (e.key === 'Escape') disableSelectionMode();
}


// =========================================================================
// FEATURE 2: SEARCH OVERLAY (Morphing Animation)
// =========================================================================

export async function openFaqSearch(options = {}) {
    const originElement = options.originElement || null;

    closeSidebar();
    const data = await ensureFaqData(); 
    if (!data) return;

    setMainContentState(true);

    // 1. Overlay
    const overlay = document.createElement('div');
    overlay.className = 'faq-search-overlay is-floating-backdrop';
    overlay.addEventListener('touchmove', (e) => e.preventDefault(), { passive: false });
    document.body.appendChild(overlay);

    // 2. Container (Hält Wrapper + Results)
    const container = document.createElement('div');
    container.className = 'faq-search-container'; 
    container.style.position = 'fixed';
    container.style.zIndex = '2110';
    container.style.margin = '0';
    container.style.display = 'flex';
    container.style.flexDirection = 'column';
    
    // --- Input Wrapper (Hält Icon + Input stabil zusammen) ---
    const inputWrapper = document.createElement('div');
    inputWrapper.className = 'faq-input-wrapper';
    
    const icon = document.createElement('i');
    icon.className = 'fa-solid fa-magnifying-glass faq-search-icon';
    inputWrapper.appendChild(icon);

    const input = document.createElement('input');
    input.type = window.matchMedia('(max-width: 768px)').matches ? 'search' : 'text';
    input.className = 'faq-search-input';
    input.placeholder = t('faq.search_placeholder') || "Search knowledge base...";
    inputWrapper.appendChild(input);
    
    container.appendChild(inputWrapper);

    const resultsBox = document.createElement('div');
    resultsBox.className = 'faq-search-results';
    
    resultsBox.addEventListener('touchstart', () => {
        if (document.activeElement === input) input.blur();
    }, { passive: true });
    
    container.appendChild(resultsBox);
    document.body.appendChild(container);


    // =========================================================
    // ANIMATION 1: AUFSTEIGEN
    // =========================================================
    
    const isMobile = window.matchMedia('(max-width: 768px)').matches;
    const targetWidth = isMobile ? window.innerWidth * 0.95 : Math.min(window.innerWidth * 0.9, 900);
    const targetLeft = (window.innerWidth - targetWidth) / 2;
    const targetTop = isMobile ? 15 : (window.innerHeight * 0.15);

    let originRect = null;

    if (originElement) {
        originRect = originElement.getBoundingClientRect();
        
        originElement.classList.add('is-hidden-by-overlay');

        // Startposition (Auf Original)
        container.style.top = `${originRect.top}px`;
        container.style.left = `${originRect.left}px`;
        container.style.width = `${originRect.width}px`;
        container.style.height = `${originRect.height}px`; 
        
        void container.offsetWidth; // Reflow

        requestAnimationFrame(() => {
            container.style.transition = 'all 0.35s cubic-bezier(0.16, 1, 0.3, 1)';
            container.style.width = `${targetWidth}px`;
            container.style.left = `${targetLeft}px`;
            container.style.top = `${targetTop}px`;
            container.style.height = '54px'; 
        });

        // Nach Animation: Auto-Height erlauben (für Ergebnisse)
        container.addEventListener('transitionend', () => {
            if(container.style.height === '54px') {
                container.style.height = 'auto';
            }
        }, { once: true });

    } else {
        // Fallback
        container.style.width = `${targetWidth}px`;
        container.style.left = `${targetLeft}px`;
        container.style.top = `${targetTop}px`;
        container.style.opacity = '0';
        container.style.transform = 'scale(0.95)';
        requestAnimationFrame(() => {
            container.style.transition = 'all 0.2s ease-out';
            container.style.opacity = '1';
            container.style.transform = 'scale(1)';
        });
    }
    
    requestAnimationFrame(() => overlay.classList.add('is-active'));
    setTimeout(() => input.focus(), 250);


    // =========================================================
    // SEARCH RESULTS
    // =========================================================

    const renderResults = (hits, highlightTerm = null) => {
        resultsBox.textContent = '';

		if (hits.length > 0 || (hits.length === 0 && highlightTerm)) {
			container.classList.add('has-results');
		} else {
			container.classList.remove('has-results');
		}
		
        if (hits.length === 0) {
            const tpl = getTemplate('tpl-faq-search-no-results');
            if (tpl) {
                const clone = tpl.firstElementChild.cloneNode(true);
                clone.querySelector('p').textContent = highlightTerm
                    ? `${t('faq.no_results') || 'No results found for'} "${highlightTerm}"`
                    : (t('faq.no_relevant_topics') || "No relevant topics found.");
                resultsBox.appendChild(clone);
            }
        } else {
            hits.slice(0, 12).forEach(hit => {
                const tpl = getTemplate('tpl-faq-search-item');
                if (tpl) {
                    const el = tpl.firstElementChild.cloneNode(true);
                    el.querySelector('.faq-search-title').textContent = hit.question;
                    let rawAns = Array.isArray(hit.answer) ? hit.answer.map(a => typeof a === 'object' ? a.text : a).join(" ") : hit.answer;
                    el.querySelector('.faq-search-preview').textContent = rawAns;
                    if (!highlightTerm && hit.isSuggestion) el.style.borderLeft = "3px solid var(--color-primary)";
                    
                    el.onclick = () => {
                        const onPage = openAndScrollToFaq(hit.id);
                        if (onPage) {
                            close();
                        } else {
                            close();
                            setTimeout(() => openAnswerModal(hit), 350);
                        }
                    };
                    resultsBox.appendChild(el);
                }
            });
        }
        
        if (!resultsBox.classList.contains('is-visible')) {
            requestAnimationFrame(() => resultsBox.classList.add('is-visible'));
        }
    };

    const showSuggestions = () => {
        const currentPage = getCurrentPageId();
        const suggestions = data.flat.filter(item => getRelevanceScore(item, currentPage) >= 2);
        suggestions.sort((a, b) => getRelevanceScore(b, currentPage) - getRelevanceScore(a, currentPage));
        suggestions.forEach(s => s.isSuggestion = true);
        if (suggestions.length > 0) renderResults(suggestions);
    };

    setTimeout(() => {
        input.focus();
		showSuggestions();
	}, 380);

	input.oninput = (e) => {
		const term = e.target.value.toLowerCase().trim();
		if (term.length === 0) { showSuggestions(); return; }

		const terms = term.split(/\s+/).filter(t => t.length > 1);

		const scoredHits = data.flat.map(item => {
			let score = 0;
			const qLower = item.question.toLowerCase();
			const aLower = (Array.isArray(item.answer) ? item.answer.map(a => typeof a === 'object' ? a.text : a).join(" ") : (item.answer || "")).toLowerCase();
			const keywordsArray = Array.isArray(item.keywords) ? item.keywords.map(k => k.toLowerCase()) : [];

			// 1. Phrasen-Bonus (Hier entfalten deine Sätze im JSON ihre volle Kraft!)
			// Wenn der Nutzer z.B. "karte speichern" tippt, und deine Phrase lautet "wie kann ich die karte speichern"
			if (keywordsArray.some(k => k.includes(term) || term.includes(k))) {
				score += 50;
			}

			// 2. Volltreffer in der Überschrift/Frage
			if (qLower.includes(term)) {
				score += 30;
			}

			// 3. Einzelwort-Abgleich (Damit auch "speichern karte" funktioniert)
			let matchedTerms = 0;
			terms.forEach(t => {
				if (qLower.includes(t)) { score += 10; matchedTerms++; }
				else if (keywordsArray.some(k => k.includes(t))) { score += 8; matchedTerms++; }
				else if (aLower.includes(t)) { score += 2; matchedTerms++; }
			});

			// Wir werten es als Treffer, wenn der Score hoch genug ist (durch Phrase) 
			// ODER wenn alle eingetippten Einzelwörter irgendwo gefunden wurden.
			const allTermsMatched = (matchedTerms >= terms.length) && terms.length > 0;

			return { item, score, isValidHit: allTermsMatched || score >= 50 };
		});

		// Filtern nach validen Hits, dann nach Score absteigend sortieren
		const hits = scoredHits
			.filter(hit => hit.isValidHit && hit.score > 0)
			.sort((a, b) => b.score - a.score)
			.map(hit => hit.item);

		renderResults(hits, term);
	};


    // =========================================================
    // ANIMATION 2: SCHLIESSEN
    // =========================================================

    const close = () => {
        input.blur();
        resultsBox.classList.remove('is-visible'); 
        
        setTimeout(() => {
            container.style.height = '54px'; 

            if (originElement && originRect) {
                const freshOriginRect = originElement.getBoundingClientRect();

                requestAnimationFrame(() => {
                    container.style.transition = 'all 0.3s cubic-bezier(0.16, 1, 0.3, 1)';
                    container.style.width = `${freshOriginRect.width}px`;
                    container.style.top = `${freshOriginRect.top}px`;
                    container.style.left = `${freshOriginRect.left}px`;
                    
                    overlay.classList.remove('is-active');
                });

                const onTransitionEnd = () => {
                    originElement.classList.remove('is-hidden-by-overlay');
                    requestAnimationFrame(() => {
                        overlay.remove();
                        container.remove();
                        setMainContentState(false);
                    });
                };

                container.addEventListener('transitionend', onTransitionEnd, { once: true });
                setTimeout(() => { if (document.body.contains(container)) onTransitionEnd(); }, 350);

            } else {
                overlay.classList.remove('is-active');
                container.style.opacity = '0';
                container.style.transform = 'scale(0.95)';
                setTimeout(() => {
                    overlay.remove(); container.remove(); setMainContentState(false);
                }, 200);
            }
        }, 100); 
    };

    overlay.onclick = close;

    input.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') { e.preventDefault(); input.blur(); }
        if (e.key === 'Escape') {
            if (input.value.trim() !== '') {
                e.preventDefault(); e.stopPropagation();
                input.value = '';
                showSuggestions();
            } else {
                close();
            }
        }
    });
    
    const escHandler = (e) => { if (e.key === 'Escape') { close(); document.removeEventListener('keydown', escHandler); }};
    document.addEventListener('keydown', escHandler);
}