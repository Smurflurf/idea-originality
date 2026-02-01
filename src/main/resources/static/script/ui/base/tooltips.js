function initializeTippy() {
    tippy('[data-tippy-content]:not([_tippy])', {
        delay: [800, 200],
        theme: 'custom-dark',
        animation: 'shift-away',
		touch: ['hold', 500], 
    });
}

export { initializeTippy };