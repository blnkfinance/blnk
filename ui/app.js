document.body.addEventListener('htmx:load', function(evt) {
    // Function to format date
    function formatDate(dateStr) {
        const options = { year: 'numeric', month: 'long', day: 'numeric' };
        return new Date(dateStr).toLocaleDateString(undefined, options);
    }
    // Find all elements with class 'date' and format their content
    document.querySelectorAll('.date').forEach(function(dateElem) {
    const dateText = dateElem.textContent.trim();
    if (dateText) {
    dateElem.textContent = formatDate(dateText);
}
});
});


// Function to remove active class from all tabs
function deactivateAllTabs() {
    document.querySelectorAll('[role="tab"]').forEach(t => t.classList.remove('border-b-2', 'border-primary-600', 'text-primary-400'));
}

// Function to activate a tab
function activateTab(tab) {
    tab.classList.add('border-b-2', 'border-green-600', 'text-green-600');
}

// Add click event listener to all tabs
document.querySelectorAll('[role="tab"]').forEach(tab => {
    tab.addEventListener('click', () => {
        deactivateAllTabs();
        activateTab(tab);
    });
});

// Function to set the active tab based on the current URL
function setActiveTabBasedOnUrl() {
    const currentPath = window.location.pathname;
    document.querySelectorAll('[role="tab"]').forEach(tab => {
        if (tab.getAttribute('href') === currentPath) {
            deactivateAllTabs();
            activateTab(tab);
        }
    });
}


setActiveTabBasedOnUrl()