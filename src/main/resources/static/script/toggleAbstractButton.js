document.addEventListener('DOMContentLoaded', function() {
    
    const allAbstracts = document.querySelectorAll('.expandable-abstract');

    allAbstracts.forEach(abstractElement => {
        if (abstractElement.scrollHeight > abstractElement.clientHeight + 2) {
            const expandButton = document.createElement('span');
            expandButton.className = 'expand-button';
            expandButton.innerHTML = '<i class="fas fa-ellipsis"></i>';
            expandButton.setAttribute('data-tippy-content', 'show full description');
            
            abstractElement.appendChild(expandButton);
            
            expandButton.addEventListener('click', function(event) {
                event.stopPropagation();
                abstractElement.classList.add('expanded');
            });
        }
    });
});