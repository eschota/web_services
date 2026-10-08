import importlib.util
import pathlib

import cv2
import numpy as np


MODULE = pathlib.Path(__file__).parents[1] / "benchmark_lowres_motion.py"
SPEC = importlib.util.spec_from_file_location("benchmark_lowres_motion", MODULE)
BENCH = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCH)


def test_tri_resize_never_invents_ids(tmp_path):
    # BGR bytes encode the RGB 24-bit integer labels 0, 1, 257 and 65537.
    image = np.array([[[0, 0, 0], [1, 0, 0]], [[1, 1, 0], [1, 0, 1]]], dtype=np.uint8)
    source = tmp_path / "front_tri.png"
    target = tmp_path / "small.png"
    assert cv2.imwrite(str(source), image)
    receipt = BENCH.resize_categorical(source, target, 7, 7, "tri")
    assert receipt["invented_label_count"] == 0
    assert set(BENCH.categorical_values(cv2.imread(str(target)), "tri")) <= set(
        BENCH.categorical_values(image, "tri"))


def test_mask_resize_never_invents_colours(tmp_path):
    image = np.array([[[0, 0, 0], [10, 20, 30]], [[40, 50, 60], [10, 20, 30]]], dtype=np.uint8)
    source = tmp_path / "front_mask.png"
    target = tmp_path / "small.png"
    assert cv2.imwrite(str(source), image)
    receipt = BENCH.resize_categorical(source, target, 9, 9, "mask")
    assert receipt["invented_label_count"] == 0


def test_categorical_resize_accepts_sheet_dimensions(tmp_path):
    image = np.zeros((8, 8), np.uint8)
    image[:, 4:] = 255
    source = tmp_path / "sheet_mask.png"
    target = tmp_path / "small.png"
    assert cv2.imwrite(str(source), image)
    BENCH.resize_categorical(source, target, 12, 10, "mask")
    assert cv2.imread(str(target), cv2.IMREAD_UNCHANGED).shape == (10, 12)


def test_camera_scale_preserves_pixel_to_world_mapping():
    doc = {"views": {"front": {"size": 512, "px_world": 0.01}},
           "sheet": {"tile_px": 512, "tiles": {"front": {"x": 512, "y": 0}}}}
    scaled = BENCH.scaled_cameras(doc, 512, 256)
    assert scaled["views"]["front"]["size"] == [256, 256]
    assert scaled["views"]["front"]["px_world"] == 0.02
    assert scaled["sheet"]["tiles"]["front"] == {"x": 256, "y": 0}
    assert doc["views"]["front"]["px_world"] == 0.01


def test_sample_flow_bilinear_constant_field():
    flow = np.zeros((4, 5, 2), np.float32)
    flow[..., 0] = 2.5
    flow[..., 1] = -1.25
    points = np.array([[0.0, 0.0], [2.2, 1.7], [4.0, 3.0]], np.float32)
    sampled = BENCH.sample_flow(flow, points)
    np.testing.assert_allclose(sampled, [[2.5, -1.25]] * 3, atol=1e-6)


def test_flow_agreement_uses_native_resolution():
    endpoint = [[[0.1, 0.2], [0.3, 0.4]]]
    paths = [[0.5, 1.0]]
    cases = [
        {"tile_px": 256, "probe_endpoints_normalized": endpoint, "probe_path_lengths_normalized": paths},
        {"tile_px": 384, "probe_endpoints_normalized": endpoint, "probe_path_lengths_normalized": paths},
    ]
    BENCH.add_flow_agreement(cases, 384)
    assert cases[0]["agreement_to_native"]["endpoint_drift_native_px_p95"] == 0.0
    assert cases[1]["agreement_to_native"]["path_length_ratio_median"] == 1.0
