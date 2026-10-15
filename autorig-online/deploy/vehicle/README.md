# autorig-vehicle (Vehicles V3, owner orders 2026-10-15)

The vehicle conveyor's own service: `mt/vehicle_v3_service.py` from the MT truth tree
(`/srv/autorig/data/motion_transfer`), uvicorn on `127.0.0.1:8284`, nginx `location ^~ /api/mt/vehicle`.
Separate from autorig-mt so a vehicle release never needs an MT restart (the same pattern as autorig-gen3d).

Install (once):

    sudo install -m 644 autorig-vehicle.service /etc/systemd/system/autorig-vehicle.service
    # paste nginx-location.conf into /etc/nginx/sites-enabled/autorig.online-storage before `location ^~ /api/mt/`
    sudo nginx -t && sudo systemctl reload nginx
    sudo systemctl daemon-reload && sudo systemctl enable --now autorig-vehicle
    curl -s http://127.0.0.1:8284/api/mt/vehicle | head -c 300

A code change: `mt-deploy --mt-file mt/vehicle_v3.py=… --mt-file mt/vehicle_v3_service.py=…` (no `--restart`),
then `sudo systemctl restart autorig-vehicle` (only this unit; a run in flight resumes from its stage files).
