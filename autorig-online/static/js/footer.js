/**
 * Reusable Site Footer Component
 * SEO-optimized footer with internal links
 */

const TELEGRAM_ICON_SVG = `<svg viewBox="0 0 24 24" width="20" height="20" fill="currentColor">
    <path d="M11.944 0A12 12 0 0 0 0 12a12 12 0 0 0 12 12 12 12 0 0 0 12-12A12 12 0 0 0 12 0a12 12 0 0 0-.056 0zm4.962 7.224c.1-.002.321.023.465.14a.506.506 0 0 1 .171.325c.016.093.036.306.02.472-.18 1.898-.962 6.502-1.36 8.627-.168.9-.499 1.201-.82 1.23-.696.065-1.225-.46-1.9-.902-1.056-.693-1.653-1.124-2.678-1.8-1.185-.78-.417-1.21.258-1.91.177-.184 3.247-2.977 3.307-3.23.007-.032.014-.15-.056-.212s-.174-.041-.249-.024c-.106.024-1.793 1.14-5.061 3.345-.48.33-.913.49-1.302.48-.428-.008-1.252-.241-1.865-.44-.752-.245-1.349-.374-1.297-.789.027-.216.325-.437.893-.663 3.498-1.524 5.83-2.529 6.998-3.014 3.332-1.386 4.025-1.627 4.476-1.635z"/>
</svg>`;

/**
 * Render the site footer
 * @returns {string} HTML string
 */
function renderSiteFooter() {
    return `
    <footer class="site-footer">
        <div class="container">
            <div class="footer-grid">
                <!-- Brand Column -->
                <div class="footer-brand">
                    <a href="/" class="footer-logo">
                        <img src="/static/images/logo/autorig-logo.png" alt="AutoRig Online" height="60">
                    </a>
                    <p class="footer-tagline" data-i18n="footer_tagline">AI-powered automatic 3D character rigging. Upload your model and get it rigged with animations in minutes.</p>
                </div>
                
                <!-- Services Column -->
                <div class="footer-links">
                    <h4 class="footer-heading" data-i18n="footer_services">Services</h4>
                    <ul>
                        <li><a href="/glb-auto-rig" data-i18n="footer_glb_rig">GLB Auto Rigging</a></li>
                        <li><a href="/fbx-auto-rig" data-i18n="footer_fbx_rig">FBX Auto Rigging</a></li>
                        <li><a href="/obj-auto-rig" data-i18n="footer_obj_rig">OBJ Auto Rigging</a></li>
                        <li><a href="/gallery" data-i18n="nav_gallery">Gallery</a></li>
                    </ul>
                </div>
                
                <!-- Guides Column -->
                <div class="footer-links">
                    <h4 class="footer-heading" data-i18n="footer_guides">Guides</h4>
                    <ul>
                        <li><a href="/mixamo-alternative" data-i18n="footer_mixamo_alt">Mixamo Alternative</a></li>
                        <li><a href="/rig-glb-unity" data-i18n="footer_glb_unity">GLB for Unity</a></li>
                        <li><a href="/rig-fbx-unreal" data-i18n="footer_fbx_unreal">FBX for Unreal</a></li>
                        <li><a href="/glb-vs-fbx" data-i18n="footer_glb_vs_fbx">GLB vs FBX</a></li>
                        <li><a href="/t-pose-vs-a-pose" data-i18n="footer_tpose">T-Pose vs A-Pose</a></li>
                        <li><a href="/animation-retargeting" data-i18n="footer_retargeting">Animation Retargeting</a></li>
                    </ul>
                </div>
                
                <!-- Company Column -->
                <div class="footer-links">
                    <h4 class="footer-heading" data-i18n="footer_company">Company</h4>
                    <ul>
                        <li><a href="/guides" data-i18n="nav_guides">All Guides</a></li>
                        <li><a href="/how-it-works" data-i18n="footer_how_it_works">How It Works</a></li>
                        <li><a href="/faq" data-i18n="footer_faq">FAQ</a></li>
                        <li><a href="/buy-credits" data-i18n="nav_buy">Buy Credits</a></li>
                        <li><a href="/developers" data-i18n="nav_api">API</a></li>
                    </ul>
                </div>
            </div>
            
            <!-- Bottom Bar -->
            <div class="footer-bottom">
                <p class="footer-copyright" data-i18n="footer_copyright">© 2026 AutoRig Online. All rights reserved.</p>
                <div class="footer-social">
                    <a href="https://t.me/autorigonline" target="_blank" rel="noopener noreferrer" class="footer-social-link" title="Telegram">
                        ${TELEGRAM_ICON_SVG}
                        <span>Telegram</span>
                    </a>
                </div>
            </div>
        </div>
    </footer>
    `;
}

// Export for global use
window.SiteFooter = {
    render: renderSiteFooter
};
