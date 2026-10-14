"""Typed /nodes interfaces for the wheeled-vehicle branch of V3."""
from __future__ import annotations

VEHICLE = "vehicle"


def install(catalogue):
    """Extend the existing registry once; leave every other pipeline unchanged."""
    if not any(row["id"] == VEHICLE for row in catalogue.ENTITY_TYPES):
        catalogue.ENTITY_TYPES.append({"id": VEHICLE, "title": "Vehicle session",
                                      "carries": "source-bound vehicle run receipt URL", "icon": "🚙"})
    stages = (
        ("analyze", "Vehicle · analysis", "Identify the vehicle, working side, dimensions and materials."),
        ("prepare", "Vehicle · orthographic 45°/45°", "Prepare and validate a canonical orthographic reference before depth reconstruction."),
        ("remove_wheels", "Vehicle · clean body reference", "Qwen removes wheels, mounted equipment and trunk or bed cargo on the render farm; the original analysis retains their separate identities."),
        ("project_views", "Vehicle · four projections", "Generate wheel-free orthographic top, front, left and back views in one vehicle frame on the render farm."),
        ("depth_views", "Vehicle · four depth maps", "Compute source-bound Z-depth for every wheel-free projection on the render farm."),
        ("fuse_voxels", "Vehicle · fuse and mirror voxels", "Fuse four depth maps into a shared centred voxel grid and mirror the working half across the longitudinal centre plane."),
        ("cluster", "Vehicle · parts", "Source-registered parts and wheel masks with validation."),
        ("voxel", "Vehicle · single-view diagnostic", "Legacy single-view depth preview; does not satisfy the four-view reconstruction contract."),
        ("model", "Vehicle · assembly", "Build the selected parts and shared wheel meshes in one vehicle frame."),
    )
    for stage, title, summary in stages:
        identifier = "vehicle_" + stage
        if not any(row["id"] == identifier for row in catalogue.SERVICES):
            catalogue.SERVICES.append({
                "id": identifier, "title": title, "path": "/nodes?g=VehicleFromImage",
                "api": "/api/vehicle/runs", "status": "planned", "slow": True, "summary": summary,
                "validation_status": "pending_farm_graph_end_to_end",
                "inputs": [{"type": catalogue.IMAGE if stage == "analyze" else VEHICLE,
                            "field": "image" if stage == "analyze" else "vehicle",
                            "required": True, "title": "Source image" if stage == "analyze" else "Vehicle session"}],
                "outputs": [{"type": VEHICLE, "field": "vehicle_url", "title": "Vehicle session"},
                            {"type": catalogue.IMAGE, "field": "preview_url", "title": "Stage preview"}]
                           + ([{"type": catalogue.IMAGE, "field": "prepared_image_url", "title": "Orthographic 45°/45° reference"}]
                              if stage == "prepare" else [])
                           + ([{"type": catalogue.MODEL3D, "field": "model_url", "title": "3D preview"}]
                              if stage in {"voxel", "fuse_voxels", "model"} else []),
            })
        catalogue.PARAMS[identifier] = ([
            {"name": "working_side", "title": "Working side", "type": "select", "default": "auto",
             "options": [{"value": "auto", "title": "Best visible side"},
                         {"value": "left", "title": "Left"}, {"value": "right", "title": "Right"}]},
            {"name": "length_m", "title": "Known length (m)", "type": "number", "default": 0,
             "min": 0, "max": 30, "step": 0.1, "help": "0 uses an explicitly labelled Vision estimate; known lengths are 0.5–30 m."},
        ] if stage == "analyze" else []) + ([
            {"name": "resolution", "title": "Voxel resolution", "type": "select", "default": "192",
             "options": [{"value": str(n), "title": str(n)} for n in (32, 64, 96, 128, 192, 256)],
             "help": "Fixed for this vehicle run; the sparse grid is limited to 150,000 points."},
        ] if stage == "analyze" else []) + ([
            {"name": "part_ids", "title": "Selected part IDs", "type": "text", "default": "",
             "help": "Stable part identifiers from the registered mask receipt, separated by semicolons."},
            {"name": "provider", "title": "Modelling method", "type": "select", "default": "blender",
             "options": [{"value": "blender", "title": "Blender · fitted geometry"},
                         {"value": "farm", "title": "Hunyuan · farm"}]},
            {"name": "polygon_budget", "title": "Polygon budget", "type": "number", "default": 10000,
             "min": 500, "max": 200000, "step": 500},
            {"name": "mode", "title": "Assembly", "type": "select", "default": "split",
             "options": [{"value": "split", "title": "Separate parts"},
                         {"value": "body", "title": "Body and physical wheels"}]},
        ] if stage == "model" else [])


def graph_template():
    return {
        "id": "VehicleFromImage", "title": "VehicleFromImage",
        "summary": "Image → dimensions → reference → Qwen removes wheels, mounted equipment and cargo → top/front/left/back → four Z-depth maps → centred symmetric voxels → registered parts. Executed through render-farm jobs.",
        "graph": {
            "name": "VehicleFromImage",
            "nodes": [
                {"id": "source", "kind": "input", "entity_type": "image", "value": "", "x": 30, "y": 180},
                {"id": "analyze", "kind": "service", "service": "vehicle_analyze", "x": 350, "y": 180,
                 "params": {"working_side": "auto", "length_m": 0, "resolution": 192}},
                {"id": "prepare", "kind": "service", "service": "vehicle_prepare", "x": 680, "y": 180, "params": {}},
                {"id": "remove_wheels", "kind": "service", "service": "vehicle_remove_wheels", "x": 1010, "y": 180, "params": {}},
                {"id": "project_views", "kind": "service", "service": "vehicle_project_views", "x": 1340, "y": 180, "params": {}},
                {"id": "depth_views", "kind": "service", "service": "vehicle_depth_views", "x": 1670, "y": 180, "params": {}},
                {"id": "fuse_voxels", "kind": "service", "service": "vehicle_fuse_voxels", "x": 2000, "y": 180, "params": {}},
                {"id": "cluster", "kind": "service", "service": "vehicle_cluster", "x": 2330, "y": 180, "params": {}},
            ],
            "links": [
                {"from": "source", "output": "value", "to": "analyze", "input": "image"},
                {"from": "analyze", "output": "vehicle_url", "to": "prepare", "input": "vehicle"},
                {"from": "prepare", "output": "vehicle_url", "to": "remove_wheels", "input": "vehicle"},
                {"from": "remove_wheels", "output": "vehicle_url", "to": "project_views", "input": "vehicle"},
                {"from": "project_views", "output": "vehicle_url", "to": "depth_views", "input": "vehicle"},
                {"from": "depth_views", "output": "vehicle_url", "to": "fuse_voxels", "input": "vehicle"},
                {"from": "fuse_voxels", "output": "vehicle_url", "to": "cluster", "input": "vehicle"},
            ],
        },
    }
