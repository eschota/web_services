import unittest
from pathlib import Path


class TaskViewerContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        root = Path(__file__).resolve().parents[2]
        cls.html = (root / "static" / "task.html").read_text(encoding="utf-8")
        cls.split_source = (root / "static" / "js" / "task-split-viewer.js").read_text(encoding="utf-8")

    def test_disabled_modules_are_absent_from_page_runtime(self):
        self.assertNotIn('id="blueprint-viewer-card"', self.html)
        self.assertNotIn('id="idle-ltx-generator-island"', self.html)
        self.assertNotIn('id="idle-ltx-modal"', self.html)
        self.assertNotIn("task-bone-correction-panel.js", self.html)
        self.assertNotIn("task-animation-fitting-panel.js", self.html)
        self.assertNotIn("animal-blueprint-viewer.js", self.html)
        self.assertNotIn("boneCorrectionPanel?.", self.html)

    def test_animation_rail_and_fps_are_inside_viewer_overlay(self):
        overlay_index = self.html.index('id="viewer-overlay"')
        rail_index = self.html.index('id="custom-animations-wrap"')
        fps_index = self.html.index('id="viewer-fps"')
        next_legacy_control_index = self.html.index('id="viewer-channel-wrap"')
        self.assertLess(overlay_index, rail_index)
        self.assertLess(rail_index, fps_index)
        self.assertLess(fps_index, next_legacy_control_index)
        self.assertIn('id="custom-anim-download-only-btn"', self.html[rail_index:fps_index])
        self.assertIn('/static/images/icons/animation-rig-download.svg', self.html[rail_index:fps_index])
        self.assertIn('/static/images/icons/animation-clip-download.svg', self.html[rail_index:fps_index])
        self.assertIn('/static/images/icons/animation-pack-download.svg', self.html[rail_index:fps_index])

    def test_animation_playback_controls_do_not_constrain_category_select(self):
        head_index = self.html.index('<div class="animation-rail-head">')
        head_end_index = self.html.index('</div>', head_index)
        rail_index = self.html.index('id="custom-animations-wrap"')
        controls_index = self.html.index('<div class="animation-rail-controls"', rail_index)
        fps_index = self.html.index('id="viewer-fps"', controls_index)
        play_index = self.html.index('id="anim-play-btn"')
        pause_index = self.html.index('id="anim-pause-btn"')
        self.assertGreater(controls_index, head_end_index)
        self.assertLess(controls_index, fps_index)
        self.assertGreater(play_index, controls_index)
        self.assertGreater(pause_index, controls_index)
        self.assertIn('z-index: 36;', self.html)
        self.assertIn('pointer-events: auto;', self.html)
        self.assertIn('.animation-rail.is-collapsed + .animation-rail-controls', self.html)

    def test_animal_catalog_loads_without_orientation_interaction(self):
        start = self.html.index('updateAnimalVariantsPanel(task) {')
        end = self.html.index('async loadAnimalVariants()', start)
        method = self.html[start:end]
        self.assertIn('if (!this.animationCatalogLoaded)', method)
        self.assertIn('void this.loadAnimationCatalog().catch((error) => {', method)

    def test_split_viewer_has_only_three_views(self):
        self.assertIn("['perspective', 'top', 'front']", self.split_source)
        self.assertNotIn("'left'", self.split_source)

    def test_split_viewer_preserves_staggered_rail_frames(self):
        self.assertIn("preserveDrawingBuffer: true", self.html)
        self.assertIn("secondaryViewportStride: 4", self.html)

    def test_perspective_zoom_is_not_blocked_by_stale_interaction_state(self):
        self.assertIn("splitViewportController?.setInteractionView('perspective');", self.html)
        self.assertIn("if (window.updateZoomInertia) window.updateZoomInertia();", self.html)
        self.assertNotIn("window.updateZoomInertia && splitViewportActiveInteractionView === 'perspective'", self.html)

    def test_manual_rig_buttons_have_no_text_label_nodes(self):
        self.assertNotIn("const label = document.createElement('span');\n                        label.textContent = formatManualRigLabel(key);", self.html)
        self.assertIn("btn.setAttribute('aria-label', formatManualRigLabel(key));", self.html)
        self.assertNotIn('class="manual-rig-restart-title"', self.html)
        self.assertNotIn('id="manual-rig-restart-status"', self.html)
        self.assertNotIn("action.textContent = `Started ${selectedLabel} rig task`;", self.html)

    def test_anonymous_creator_notice_contract(self):
        self.assertIn('id="task-anon-notice-dialog"', self.html)
        self.assertIn("Среднее время ожидания создания рига ~15 минут.", self.html)
        self.assertIn("Авторизуйтесь через Google, чтобы получить однократное уведомление", self.html)
        self.assertIn("anonymousCreatorNoticeDelayMs: 5000", self.html)
        self.assertIn("this.purchaseState?.login_required", self.html)
        self.assertIn("this.purchaseState?.is_owner", self.html)
        self.assertIn("if (event.target === dialog) dialog.close();", self.html)


if __name__ == "__main__":
    unittest.main()
