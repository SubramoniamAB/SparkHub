#!/usr/bin/env python3
"""Spark Hub static web server plus a small local refresh-monitor API.

On Linux, manual refreshes use setup.sh --run (which provides flock locking).
On Windows, the API launches the Python collector directly. API binds to the
chosen interface; manual POST requests require a short-lived same-origin token.
"""
import argparse, datetime as dt, json, os, secrets, subprocess, threading, time, calendar, sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parent
STATE = Path(os.environ.get("XDG_STATE_HOME", Path.home()/".local/state")) / "spark-hub"
STATE.mkdir(parents=True, exist_ok=True)
PROGRESS = STATE / "progress.json"
RUN_META = STATE / "api-run.json"
LOG = STATE / "fetch.log"
HISTORY = STATE / "history.json"
SETTINGS = STATE / "settings.json"
SOURCES = {"models", "playbooks", "recipes", "forum", "ratings"}
TOKEN = secrets.token_urlsafe(24)
LOCK = threading.Lock()
PROC = None


def read_json(path, fallback):
    try: return json.loads(path.read_text(encoding="utf-8"))
    except Exception: return fallback


def valid_duration(value):
    """Treat empty and UI placeholder strings as missing rather than truthy data."""
    if value is None:
        return None
    value = str(value).strip()
    return value if value and value not in {"—", "-", "None", "null", "undefined", "duration unavailable"} else None


def duration_between(started, finished=None):
    try:
        start_dt = dt.datetime.fromisoformat(str(started))
        end_dt = dt.datetime.fromisoformat(str(finished)) if finished else dt.datetime.now(start_dt.tzinfo)
        seconds = max(0, int((end_dt - start_dt).total_seconds()))
        hours, rem = divmod(seconds, 3600)
        minutes, secs = divmod(rem, 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    except Exception:
        return None


def normalize_changes(items):
    """Return only changes with concrete details; never fabricate 'Data changed'."""
    if not isinstance(items, list):
        return []
    generic = {"data changed", "change", "changed", "update", "updated", "refresh completed", "source updated", "data updated"}
    result = []
    seen = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        kind = str(item.get("kind") or item.get("type") or "update").strip().lower().replace("_", "-")
        text = str(item.get("text") or item.get("title") or item.get("name") or item.get("id") or "").strip()
        if not text or text.lower().rstrip(".!:") in generic:
            # Recover specific identity from common legacy change payloads only.
            entity = item.get("model_id") or item.get("repo_id") or item.get("slug") or item.get("path")
            if entity:
                text = f"{kind.replace('-', ' ').capitalize()}: {entity}"
            else:
                continue
        url = item.get("url") or item.get("html_url")
        date = item.get("date") or item.get("at") or item.get("updated")
        row = {"kind": kind, "text": text[:500]}
        if isinstance(url, str) and url.startswith(("https://", "http://")):
            row["url"] = url
        if date:
            row["date"] = str(date)[:40]
        key = (row["kind"], row["text"], row.get("url", ""))
        if key not in seen:
            result.append(row); seen.add(key)
    return result


def data_counts(data):
    if not isinstance(data, dict):
        return {"models": 0, "playbooks": 0, "recipes": 0, "forum": 0, "ratings": 0}
    return {"models": len(data.get("models", []) or []), "playbooks": len(data.get("playbooks", []) or []),
            "recipes": len(data.get("recipes", []) or []), "forum": len(data.get("forum", []) or []),
            "ratings": len(data.get("ratings", {}) or {})}


def cron_next():
    try:
        lines = subprocess.check_output(["crontab", "-l"], stderr=subprocess.DEVNULL, text=True).splitlines()
    except Exception:
        return "Not detected (cron unavailable)"
    # Spark Hub full refresh: minute 17, every six hours; forum-only: minute 47 hourly.
    now = dt.datetime.now().astimezone()
    candidates=[]
    for line in lines:
        if "# spark-hub" not in line or line.lstrip().startswith("#"): continue
        fields=line.split()
        if len(fields)<5: continue
        minute,hour=fields[0],fields[1]
        if minute=="17" and hour=="*/6":
            for day in range(0,2):
                for hr in range(0,24,6):
                    candidate=now.replace(hour=hr,minute=17,second=0,microsecond=0)+dt.timedelta(days=day)
                    if candidate>now:candidates.append(candidate)
        elif minute=="47" and hour=="*":
            candidate=now.replace(minute=47,second=0,microsecond=0)
            if candidate<=now:candidate+=dt.timedelta(hours=1)
            candidates.append(candidate)
    return min(candidates).strftime("%a %b %d, %I:%M %p %Z") if candidates else "No Spark Hub cron schedule found"


def default_settings():
    return {"schedule": {"enabled": False, "sources": [], "time": "09:00", "repeat": "daily", "weekday": 0, "monthday": 1}}


def load_settings():
    data = read_json(SETTINGS, default_settings())
    if not isinstance(data, dict): data = default_settings()
    schedule = data.get("schedule") if isinstance(data.get("schedule"), dict) else {}
    defaults = default_settings()["schedule"]
    clean = dict(defaults); clean.update(schedule)
    clean["enabled"] = bool(clean.get("enabled"))
    clean["sources"] = [x for x in clean.get("sources", []) if x in SOURCES] if isinstance(clean.get("sources"), list) else []
    if clean.get("repeat") not in ("daily", "weekly", "monthly"): clean["repeat"] = "daily"
    if not isinstance(clean.get("time"), str) or not __import__("re").fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", clean["time"]): clean["time"] = "09:00"
    try: clean["weekday"] = max(0, min(6, int(clean.get("weekday", 0))))
    except Exception: clean["weekday"] = 0
    try: clean["monthday"] = max(1, min(31, int(clean.get("monthday", 1))))
    except Exception: clean["monthday"] = 1
    data["schedule"] = clean
    return data


def save_settings(data):
    tmp = SETTINGS.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    os.replace(tmp, SETTINGS)


def next_scheduled_datetime(schedule, now=None):
    if not schedule.get("enabled") or not schedule.get("sources"):
        return None
    now = now or dt.datetime.now().astimezone()
    hour, minute = map(int, schedule.get("time", "09:00").split(":"))
    repeat = schedule.get("repeat", "daily")
    for offset in range(0, 370):
        day = now.date() + dt.timedelta(days=offset)
        if repeat == "weekly" and day.weekday() != int(schedule.get("weekday", 0)): continue
        if repeat == "monthly" and day.day != min(int(schedule.get("monthday", 1)), calendar.monthrange(day.year, day.month)[1]): continue
        candidate = dt.datetime.combine(day, dt.time(hour, minute), tzinfo=now.tzinfo)
        if candidate > now: return candidate
    return None


def scheduled_dispatcher():
    # Local clock/config checks only; no website/network polling occurs here.
    while True:
        try:
            settings = load_settings(); schedule = settings["schedule"]
            now = dt.datetime.now().astimezone()
            if schedule.get("enabled") and schedule.get("sources"):
                hour, minute = map(int, schedule.get("time", "09:00").split(":"))
                repeat = schedule.get("repeat", "daily")
                matches = repeat == "daily" or (repeat == "weekly" and now.weekday() == int(schedule.get("weekday", 0)))
                if repeat == "monthly":
                    matches = now.day == min(int(schedule.get("monthday", 1)), calendar.monthrange(now.year, now.month)[1])
                occurrence = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                key = occurrence.strftime("%Y-%m-%dT%H:%M")
                # Fire in the minute containing the configured local time, once only.
                if matches and occurrence <= now < occurrence + dt.timedelta(minutes=1) and schedule.get("last_fired") != key:
                    ok, _message = launch_refresh(schedule["sources"], trigger="scheduled")
                    if ok:
                        schedule["last_fired"] = key
                        settings["schedule"] = schedule
                        save_settings(settings)
        except Exception as exc:
            print(f"schedule dispatcher warning: {exc}", flush=True)
        time.sleep(10)


def remove_legacy_sparkhub_cron():
    """Remove older automatic Spark Hub cron entries; retain unrelated crontab lines."""
    if os.name == "nt":
        return
    try:
        current = subprocess.check_output(["crontab", "-l"], stderr=subprocess.DEVNULL, text=True)
    except Exception:
        return
    kept = [line for line in current.splitlines() if "# spark-hub" not in line.lower()]
    if kept != current.splitlines():
        payload = "\n".join(kept) + ("\n" if kept else "")
        subprocess.run(["crontab", "-"], input=payload, text=True, check=True)


def launch_refresh(sources=None, trigger="manual"):
    """Start one user-requested or settings-scheduled local collection."""
    global PROC
    if not LOCK.acquire(blocking=False): return False, "A refresh is already starting or running"
    if PROC and PROC.poll() is None:
        LOCK.release(); return False, "A refresh is already running"
    try:
        selected = sorted(SOURCES) if sources is None else list(dict.fromkeys(sources))
        if not selected or any(s not in SOURCES for s in selected):
            LOCK.release(); return False, "Choose at least one valid data source"
        try: LOG.write_text("", encoding="utf-8")
        except Exception: pass
        started = dt.datetime.now().astimezone()
        progress = {"stage":0,"total":10,"label":"Starting","detail":"Launching collector","percent":0,"state":"running","started":started.isoformat(timespec="seconds"),"elapsed":"00:00:00"}
        PROGRESS.write_text(json.dumps(progress),encoding="utf-8")
        meta = {"state":"running","started":started.isoformat(timespec="seconds"),"finished":None,"duration":None,"elapsed":"00:00:00","errors":None,"trigger":trigger,"sources":selected}
        RUN_META.write_text(json.dumps(meta),encoding="utf-8")
        env=os.environ.copy(); env["SPARK_HUB_PROGRESS_FILE"]=str(PROGRESS)
        setup = ROOT / "setup.sh"
        if os.name == "nt":
            cmd=[sys.executable,str(ROOT/"spark_hub_fetch.py"),"--out",str(ROOT)]
        else:
            cmd=["/bin/bash",str(setup),"--run"] if setup.exists() else [sys.executable,str(ROOT/"spark_hub_fetch.py"),"--out",str(ROOT)]
        for source in selected: cmd.extend(["--only", source])
        with LOG.open("a",encoding="utf-8") as log_file:
            popen_options={"cwd":str(ROOT),"env":env,"stdout":log_file,"stderr":subprocess.STDOUT}
            if os.name != "nt": popen_options["start_new_session"]=True
            PROC=subprocess.Popen(cmd,**popen_options)
        threading.Thread(target=Handler._wait_run_static,args=(PROC,started),daemon=True).start()
        return True, str(PROC.pid)
    except Exception as exc:
        if LOCK.locked(): LOCK.release()
        meta=read_json(RUN_META,{})
        meta.update({"state":"failed","finished":dt.datetime.now().astimezone().isoformat(timespec="seconds"),"error":str(exc),"errors":1})
        RUN_META.write_text(json.dumps(meta),encoding="utf-8")
        return False, str(exc)



class Handler(SimpleHTTPRequestHandler):
    def __init__(self,*args,**kwargs): super().__init__(*args,directory=str(ROOT),**kwargs)
    def log_message(self, fmt, *args):
        # The collector log is for collector output only; status polling must not
        # turn the live log into a stream of GET /api/status requests.
        if urlparse(getattr(self, "path", "")).path.startswith("/api/"):
            return
        try:
            with LOG.open("a",encoding="utf-8") as f: f.write("[%s] %s\n"%(dt.datetime.now().isoformat(timespec="seconds"),fmt%args))
        except Exception: pass
    def send_json(self, code, data):
        body=json.dumps(data).encode(); self.send_response(code); self.send_header("Content-Type","application/json; charset=utf-8"); self.send_header("Cache-Control","no-store"); self.send_header("Content-Length",str(len(body))); self.end_headers(); self.wfile.write(body)
    def do_OPTIONS(self):
        self.send_response(204); self.send_header("Access-Control-Allow-Methods","GET, POST, OPTIONS"); self.send_header("Access-Control-Allow-Headers","Content-Type, X-Spark-Token"); self.send_header("Access-Control-Allow-Origin","null"); self.end_headers()
    def do_GET(self):
        path=urlparse(self.path).path
        if path=="/api/token": return self.send_json(200,{"token":TOKEN})
        if path=="/api/settings":
            settings=load_settings(); due=next_scheduled_datetime(settings["schedule"])
            return self.send_json(200,{"settings":settings,"next_run":due.isoformat(timespec="minutes") if due else None})
        if path=="/api/history":
            history=read_json(HISTORY, [])
            if not isinstance(history, list): history=[]
            latest=read_json(RUN_META, {})
            if latest and latest.get("finished") and not any(h.get("started")==latest.get("started") for h in history if isinstance(h,dict)):
                history.insert(0, latest)
            # Normalize legacy records on read so old generic placeholders such
            # as "Data changed" do not leak back into the dedicated history UI.
            cleaned=[]
            for entry in history[:100]:
                if not isinstance(entry, dict):
                    continue
                row=dict(entry)
                row["change_items"]=normalize_changes(row.get("change_items", []))
                row["changes"]=len(row["change_items"])
                duration=valid_duration(row.get("duration")) or valid_duration(row.get("elapsed")) or duration_between(row.get("started"), row.get("finished"))
                if duration:
                    row["duration"]=duration; row["elapsed"]=duration
                cleaned.append(row)
            return self.send_json(200, {"history": cleaned})
        if path=="/api/sources":
            data=read_json(ROOT/"data.json", {})
            statuses=data.get("status", {}) if isinstance(data,dict) else {}
            counts=data_counts(data)
            labels={"models":"Hugging Face models", "playbooks":"NVIDIA playbooks (GitHub)", "recipes":"NVIDIA inference recipes", "forum":"NVIDIA DGX Spark forum", "ratings":"Artificial Analysis ratings"}
            sources=[]
            for key,label in labels.items():
                state=statuses.get(key,{}) if isinstance(statuses,dict) else {}
                sources.append({"id":key,"label":label,"ok":bool(state.get("ok")),"at":state.get("at"),"error":state.get("error"),"records":counts.get(key,0)})
            return self.send_json(200, {"sources":sources,"generated":data.get("generated") if isinstance(data,dict) else None})
        if path=="/api/status":
            meta=read_json(RUN_META,{})
            running=bool(PROC and PROC.poll() is None)
            try: lines=LOG.read_text(encoding="utf-8",errors="replace").splitlines()[-140:]
            except Exception: lines=[]
            progress=read_json(PROGRESS,{})
            history=read_json(HISTORY,[])
            if not isinstance(history,list): history=[]
            now=dt.datetime.now().astimezone()
            started = meta.get("started") if isinstance(meta,dict) else None
            finished = meta.get("finished") if isinstance(meta,dict) else None
            if running and progress:
                elapsed=duration_between(started) or valid_duration(meta.get("elapsed")) or "00:00:00"
                progress["started"] = progress.get("started") or started
                progress["elapsed"]=elapsed; meta["elapsed"]=elapsed
                try: PROGRESS.write_text(json.dumps(progress),encoding="utf-8")
                except Exception: pass
            elif meta:
                elapsed=valid_duration(meta.get("duration")) or valid_duration(meta.get("elapsed")) or valid_duration(progress.get("duration")) or valid_duration(progress.get("elapsed")) or duration_between(started, finished) or "—"
                if progress:
                    progress["started"] = progress.get("started") or started
                    progress["elapsed"]=elapsed
                    if meta.get("state"): progress["state"]=meta["state"]
                    if finished: progress["finished"]=finished
                    if valid_duration(meta.get("duration")): progress["duration"]=meta["duration"]
            settings=load_settings(); due=next_scheduled_datetime(settings["schedule"])
            data=read_json(ROOT/"data.json",{})
            # Distinguish the most recent run delta from cumulative historical changes.
            if isinstance(data,dict) and "changes_this_run" in data:
                current_changes=normalize_changes(data.get("changes_this_run"))
            else:
                current_changes=normalize_changes(data.get("changes",[]) if isinstance(data,dict) else [])
            statuses=data.get("status",{}) if isinstance(data,dict) else {}
            selected=(meta or {}).get("sources") or sorted(SOURCES)
            source_errors=sum(1 for source in selected if not statuses.get(source,{}).get("ok",False)) if statuses else 0
            counts=data_counts(data)
            summary={"changes":len(current_changes),"counts":counts,"generated":data.get("generated") if isinstance(data,dict) else None}
            meta_out=meta.copy() if meta and (meta.get("started") or meta.get("state")) else None
            if meta_out and not running:
                actual_duration=valid_duration(meta_out.get("duration")) or duration_between(meta_out.get("started"),meta_out.get("finished"))
                if actual_duration:
                    meta_out["duration"]=actual_duration
                    meta_out["elapsed"]=actual_duration
                    if progress:
                        progress["duration"]=actual_duration
                        progress["elapsed"]=actual_duration
                if not valid_duration(progress.get("elapsed")) and actual_duration:
                    progress["elapsed"]=actual_duration
            if meta_out is not None:
                meta_out["errors"] = meta_out.get("errors", source_errors)
            return self.send_json(200,{"running":running,"progress":progress or None,"last_run":meta_out,"history":history,"next_run":due.strftime("%a %b %d, %I:%M %p %Z") if due else "No schedule configured","settings":settings,"log":lines if running and meta.get("trigger")=="manual" else [],"summary":summary,"changes":current_changes[:200],"source_status":statuses,"counts":counts})
        return super().do_GET()
    def do_POST(self):
        global PROC
        path=urlparse(self.path).path
        if path not in ("/api/run","/api/cancel","/api/settings"): return self.send_json(404,{"error":"Not found"})
        if self.headers.get("X-Spark-Token")!=TOKEN: return self.send_json(403,{"error":"Invalid or missing session token"})
        if path=="/api/settings":
            try:
                length=int(self.headers.get("Content-Length","0")); body=json.loads(self.rfile.read(length) or b"{}")
                schedule=body.get("schedule",{})
                sources=schedule.get("sources",[])
                if not isinstance(sources,list) or any(x not in SOURCES for x in sources): return self.send_json(400,{"error":"Invalid source selection"})
                if schedule.get("repeat") not in ("daily","weekly","monthly"): return self.send_json(400,{"error":"Invalid repetition"})
                if not __import__("re").fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d",str(schedule.get("time",""))): return self.send_json(400,{"error":"Time must use HH:MM"})
                normalized=default_settings(); normalized["schedule"].update(schedule); normalized["schedule"]["sources"]=sources
                if not normalized["schedule"]["enabled"]: normalized["schedule"]["last_fired"]=None
                save_settings(normalized)
                due=next_scheduled_datetime(normalized["schedule"])
                return self.send_json(200,{"ok":True,"settings":normalized,"next_run":due.isoformat(timespec="minutes") if due else None})
            except Exception as exc: return self.send_json(400,{"error":str(exc)})
        if path=="/api/cancel":
            if PROC and PROC.poll() is None:
                meta=read_json(RUN_META,{})
                meta["cancel_requested"]=True
                RUN_META.write_text(json.dumps(meta),encoding="utf-8")
                PROC.terminate(); return self.send_json(200,{"ok":True,"message":"Cancellation requested"})
            return self.send_json(409,{"error":"No active run"})
        try:
            length=int(self.headers.get("Content-Length","0")); body=json.loads(self.rfile.read(length) or b"{}") if length else {}
            sources=body.get("sources")
            if sources is not None and (not isinstance(sources,list) or not sources or any(x not in SOURCES for x in sources)):
                return self.send_json(400,{"error":"sources must be a non-empty list of supported source names"})
        except Exception as exc:
            return self.send_json(400,{"error":f"Invalid request body: {exc}"})
        ok, result = launch_refresh(sources, trigger="manual")
        return self.send_json(202 if ok else 409, {"ok":ok,"pid":result if ok else None,"error":None if ok else result})

    @staticmethod
    def _wait_run_static(proc, started):
        rc=proc.wait(); finished=dt.datetime.now().astimezone(); meta=read_json(RUN_META,{})
        duration=duration_between(started.isoformat(), finished.isoformat()) or "00:00:00"
        data=read_json(ROOT/"data.json",{})
        statuses=data.get("status",{}) if isinstance(data,dict) else {}
        selected=meta.get("sources") or sorted(SOURCES)
        failed_sources=[source for source in selected if statuses and not statuses.get(source,{}).get("ok",False)]
        failed_count=(0 if meta.get("cancel_requested") else len(failed_sources) + (1 if rc else 0))
        state=("cancelled" if meta.get("cancel_requested") else ("failed" if rc else ("completed_with_errors" if failed_sources else "completed")))
        run_changes=normalize_changes(data.get("changes_this_run", data.get("changes",[]))) if isinstance(data,dict) else []
        counts=data_counts(data)
        meta.update({"state":state,"finished":finished.isoformat(timespec="seconds"),"duration":duration,"elapsed":duration,"return_code":rc,"errors":failed_count,"failed_sources":failed_sources,"changes":len(run_changes),"change_items":run_changes[:30],"counts":counts})
        progress=read_json(PROGRESS,{})
        progress.update({"state":state,"elapsed":duration,"duration":duration,"finished":meta["finished"],"started":meta.get("started",started.isoformat(timespec="seconds"))})
        if state == "cancelled":
            progress["detail"] = "Refresh cancelled by user"
        elif rc:
            progress["detail"]=(progress.get("detail","Refresh failed")+f"; process exit {rc}")
        else:
            progress["percent"]=100; progress["stage"]=10
            if state=="completed_with_errors": progress["detail"]="Refresh finished with source errors: "+", ".join(failed_sources)
        PROGRESS.write_text(json.dumps(progress),encoding="utf-8")
        RUN_META.write_text(json.dumps(meta),encoding="utf-8")
        history=read_json(HISTORY,[])
        if not isinstance(history,list): history=[]
        history.insert(0,meta.copy())
        tmp=HISTORY.with_suffix(".tmp"); tmp.write_text(json.dumps(history[:100],indent=2),encoding="utf-8"); os.replace(tmp,HISTORY)
        global PROC
        PROC=None
        if LOCK.locked(): LOCK.release()



def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--port",type=int,default=8090); ap.add_argument("--bind",default="0.0.0.0"); a=ap.parse_args()
    remove_legacy_sparkhub_cron()
    server=ThreadingHTTPServer((a.bind,a.port),Handler)
    threading.Thread(target=scheduled_dispatcher,daemon=True).start()
    print(f"Spark Hub serving {ROOT} with refresh API on {a.bind}:{a.port}",flush=True)
    server.serve_forever()
if __name__=="__main__": main()
