# Spark Hub

Spark Hub is a local dashboard and data collector for the NVIDIA DGX Spark ecosystem. It brings model discovery, Spark-fit details, NVIDIA playbooks and recipes, community discussions, a fit calculator, and useful links into one searchable interface.

The browser displays the latest local dataset. The collector updates that dataset from its configured sources. With the monitor service running, Data & Refresh also provides manual collection, progress, live logs during manual runs, run history, source status, and schedule settings.

## What Spark Hub includes

- **Models:** browse and compare model repositories, quantizations, hardware fit, runtime commands, and available ratings.
- **NVIDIA resources:** explore playbooks and inference recipes.
- **Community tips:** search and filter DGX Spark forum topics.
- **Fit calculator and directory:** estimate memory fit and find related resources.
- **Data & Refresh:** manually fetch selected sources, follow progress and live logs, review run history, and configure an optional schedule.

Ratings from Artificial Analysis require a key. Without one, those ratings may be absent. Spark Hub is an independent community utility and is not affiliated with NVIDIA, Hugging Face, or Artificial Analysis.

## Run Spark Hub on another computer

The full setup can run on a Linux or Windows host. Other computers on the same trusted local network can use it in a browser; they do not need Python or a local copy of the project. For Windows, follow the [Windows deployment instructions](windows/instructions.md). The steps below describe a Linux host.

### 1. Copy the project folder to the Linux host

Copy the complete `SparkHub` folder, including the current `data.js` and `data.json` files, to the host. For example, from the computer that has the folder:

```bash
scp -r ./SparkHub <username>@<spark-host>:~/SparkHub
```

Replace `<username>` and `<spark-host>` with the Linux account and host name or address. You can also copy the folder by USB or another file-transfer method. On the host, open a terminal and enter the folder:

```bash
cd ~/SparkHub
```

### 2. Install the monitor service on the host

The full dashboard and collector need Python 3.8 or newer. Manual collection also uses `flock` from `util-linux`. The service installer uses Linux user systemd.

```bash
chmod +x setup.sh spark_hub_fetch.py spark_hub_server.py install_sparkhub_monitor.sh
./setup.sh --run
./install_sparkhub_monitor.sh 8090
```

The first command performs the initial data refresh. Wait for it to finish before opening `http://localhost:8090/` or installing the monitor. The second command installs and starts the Spark Hub monitor on port 8090. To check the service later:

```bash
systemctl --user status spark-hub-web.service --no-pager
```

### 3. Open Spark Hub from another computer

On the Spark Hub host, find its local network address:

```bash
hostname -I
```

On another computer connected to the same trusted network, open this address in a browser, replacing the placeholder with the host address:

```text
http://<spark-host-LAN-IP>:8090/
```

For example, if the host address is `192.168.1.40`, open `http://192.168.1.40:8090/`. The Data & Refresh page is at `http://<spark-host-LAN-IP>:8090/#/refresh`.

The host must stay powered on with the monitor service running. Its firewall may need to allow incoming TCP connections on port 8090 from your local network. The service listens on the host's network interfaces; keep access on a trusted network and do not forward this port to the public internet. The host—not the browser client—needs network access to the data sources when a collection runs.

### Static, read-only option

For a basic read-only display, `index.html` and `data.js` can be served together by any static web server. This mode does not include the manual-run API, live monitor, run history, or scheduling. The host monitor setup described above is needed for those features.

## Manual and scheduled collection

To run a manual refresh in the dashboard, open **Data & Refresh → Run & Monitor** and select **Run data refresh now**. The live log is shown during a manual run and clears to an idle message when that run finishes. On Linux, run a refresh from the host terminal with:

```bash
./setup.sh --run
```

On Windows, run this from PowerShell in the project folder:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\windows\setup.ps1 -Action Run
```

To schedule collection through the dashboard, open **Data & Refresh → Settings**, select the sources, choose the time and repetition, and save. That dashboard schedule is disabled until you enable and save it. It runs only while the Spark Hub monitor service is active.

On Linux, there is also a legacy cron setup: running `./setup.sh` with no options performs a refresh and installs cron entries for a full refresh every six hours and a forum refresh hourly. Those cron jobs are separate from the schedule in dashboard Settings. Use one scheduling method to avoid duplicate collection.

On Linux, optional source tokens are stored outside the project folder in `~/.config/spark-hub/env` (or the equivalent under `XDG_CONFIG_HOME`). On Windows, they are stored in `%APPDATA%\spark-hub\env`. Do not place credentials in files served by the web server.

## Project files

| File | What it does |
| --- | --- |
| `README.md` | This overview, setup guide, and file reference. |
| `index.html` | The dashboard interface, navigation, About page, search, filters, and tools. |
| `data.js` | Generated browser dataset loaded by `index.html`; copy it to retain the current display snapshot. |
| `data.json` | Generated structured dataset used by the monitor API and collector for the current snapshot and change comparisons. |
| `.cache.json` | Collector cache used for deduplication and comparison continuity. |
| `spark_hub_fetch.py` | Fetches and processes model, playbook, recipe, forum, and optional rating data. |
| `spark_hub_server.py` | Serves the dashboard and provides local status, manual-run, history, source, and schedule APIs. |
| `setup.sh` | Runs collection, reports status, and manages the legacy cron and web-server setup. |
| `install_sparkhub_monitor.sh` | Installs and starts the Linux user service for the dashboard and refresh API. |
| `windows/` | Windows PowerShell deployment script and instructions. |
| `screenshot.md` | Static gallery of screenshots from Spark Hub pages and features. |
| `sparkhub-icon.svg` | Spark Hub brand mark used in the navigation and About page. |
| `favicon.svg` | Browser tab icon. |
| `developer-photo.jpg` | Developer photo shown in the About page credits. |
| `ratings_overrides.json` | Optional local rating corrections keyed by model repository ID. Preserve any overrides you add. |
| `screenshots/` | Screenshots of Spark Hub pages and features. |

`data.js`, `data.json`, and `.cache.json` are generated or maintained by the collector. Keep them when copying an existing installation to preserve its dataset and comparison state; a new collection can regenerate them. The rating overrides are optional user configuration.
