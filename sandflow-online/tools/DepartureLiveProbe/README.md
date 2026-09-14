# Departure protocol canary

Explicitly targets the existing production QA API, with redirects disabled and normal TLS verification. Uses a new private disposable test world, two guest identities, control/state WSS, a **CPU-only 16x16 fixture**, and final-save receipts. It never touches a user's existing world and never claims Unity physics, UI or voice acceptance.

Pass the existing project-local QA authority env-file and project-local output path; credentials/passwords are never printed or written to the receipt. The private canary remains stored for audit; it is not added to the public catalogue. Its last participant saves and disconnects; bounded failure cleanup disposes the sockets.

```powershell
dotnet run --project tools/DepartureLiveProbe -c Release -- .work/qa-session/server.env .work/departure-live.json
```

Set DOTNET_CLI_HOME/NUGET_PACKAGES/TEMP/TMP inside this project before invoking, as in tools/Run-Checks.ps1. The harness has a 60-second overall deadline, 15-second HTTP deadlines, and maintains the elected peer's heartbeat during the per-world departure cooldown.
