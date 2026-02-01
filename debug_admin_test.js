// Debug script for testing admin functionality
// Run this in browser console on the task page

console.log('=== Admin Debug Test ===');

// Test 1: Check auth endpoint
fetch('/auth/me')
    .then(r => r.json())
    .then(data => {
        console.log('Auth data:', data);
        console.log('Is admin:', data?.user?.is_admin);
        console.log('User email:', data?.user?.email);
    })
    .catch(e => console.error('Auth error:', e));

// Test 2: Check ensureViewerAdmin function
if (window.ensureViewerAdmin) {
    window.ensureViewerAdmin().then(isAdmin => {
        console.log('ensureViewerAdmin result:', isAdmin);
    });
} else {
    console.log('ensureViewerAdmin function not available');
}

// Test 3: Manual save attempt
if (window.saveDefaultViewerSettings) {
    console.log('saveDefaultViewerSettings function available');
    console.log('To test save: window.saveDefaultViewerSettings("console-test")');
} else {
    console.log('saveDefaultViewerSettings function not available');
}

// Test 4: Check DOM elements
console.log('Z button exists:', !!document.getElementById('viewer-save-default-btn'));
console.log('Debug button exists:', !!document.getElementById('viewer-debug-btn'));
console.log('Viewer overlay exists:', !!document.getElementById('viewer-overlay'));

console.log('=== End Debug Test ===');