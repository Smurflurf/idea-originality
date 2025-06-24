document.addEventListener('DOMContentLoaded', function() {
    
    const allToggleButtons = document.querySelectorAll('.toggle-json-btn');
    allToggleButtons.forEach(button => {
        button.addEventListener('click', function() {
            const jsonContainer = this.nextElementSibling;
            this.classList.toggle('active');
            jsonContainer.classList.toggle('active');
        });
    });
});