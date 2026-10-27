# Deploy Spark Hub on Windows

These scripts run the Spark Hub dashboard and collector on a Windows 10 or 11 computer. The full dashboard needs Python and the monitor server. Other devices on the same trusted network can open it in a browser without installing Python.

## Requirements

- Windows 10 or 11, with Windows PowerShell 5.1 or newer.
- Python 3.8 or newer, available through the Python launcher (`py -3`) or `python.exe` on `PATH`.
- Outbound HTTPS access to the data sources for a refresh. The collector uses only Python's standard library; no `pip` packages are required.

## Install and start

1. Copy the complete `SparkHub` project folder to the Windows host. Keep `index.html`, both Python files, and the current `data.js` and `data.json` together.
2. Open PowerShell in the project folder and run:

   ```powershell
   powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\windows\setup.ps1 -Action Install
   ```

   This checks Python, creates an optional token file, performs an initial data refresh, then installs and starts a per-user Task Scheduler task named **Spark Hub Monitor**. The task starts the dashboard when that Windows user signs in. The scripts do not require administrator rights for this task.

   Wait for the initial refresh to finish and for the script to report that the monitor task has started. Do not open the dashboard at `http://localhost:8090/` before the first refresh completes.

3. On the host, open `http://localhost:8090/`. The monitor is at `http://localhost:8090/#/refresh`.

The monitor listens on all network interfaces. Windows Firewall may block connections from other devices. To allow access on a trusted private network, open **PowerShell as Administrator** and run:

```powershell
New-NetFirewallRule -DisplayName "Spark Hub TCP 8090" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 8090 -Profile Private
```

Find the host's IPv4 address with `ipconfig`, then open `http://<host-LAN-IP>:8090/` on another device on the same network. Keep this port on a trusted network and do not forward it to the public internet.

## Configure refreshes

Use **Data & Refresh → Settings** in the dashboard to enable a daily, weekly, or monthly source refresh. The Windows task keeps the monitor running while that user is signed in. The computer must remain awake and connected to the network for scheduled refreshes to run.

Optional API keys are read from `%APPDATA%\spark-hub\env`, outside the project folder. The installer creates this file with empty values. Add values without spaces around `=` and restart the monitor task for new credentials to take effect:

```text
HF_TOKEN=your_hugging_face_token
AA_API_KEY=your_artificial_analysis_key
GITHUB_TOKEN=your_github_token
```

The keys are optional. Hugging Face and GitHub tokens raise rate limits; the Artificial Analysis key enables its ratings data.

## Manage the deployment

Run these commands from the project folder:

```powershell
# Refresh all sources now
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\windows\setup.ps1 -Action Run

# Refresh selected sources
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\windows\setup.ps1 -Action Run -Sources forum,models

# Check the task and local monitor
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\windows\setup.ps1 -Action Status

# Remove the monitor task (keeps project data and token configuration)
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\windows\setup.ps1 -Action Uninstall
```

The task runs in the signed-in user's session. To run the monitor manually in the foreground instead, use `-Action Start`; press Ctrl+C to stop it. The monitor stores logs and schedule state under `%LOCALAPPDATA%\spark-hub` by default. The dashboard and collector update `data.js`, `data.json`, and `.cache.json` in the project folder, so the Windows user needs write access to that folder.

## Static read-only option

For a read-only display without the monitor API, serve `index.html` and `data.js` together with any static web server. This does not support manual refreshes, run history, or the dashboard's refresh schedule.
