"""
SyncLinkPro — monolithic FastAPI app.
Serves the HTML/JS frontend, REST API, and WebSocket event stream in one process.

Run:   python app.py
Open:  http://localhost:7878
"""
from __future__ import annotations
import asyncio
import os
import secrets
import string
import threading
import webbrowser
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from sync_engine import Engine, SyncPairConfig

ROOT = Path(__file__).parent
STATIC = ROOT / "static"

app = FastAPI(title="SyncLinkPro")


# ---- live event broadcast ----
class Broadcaster:
    def __init__(self):
        self.clients: set[WebSocket] = set()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._logs: dict[str, list] = {}
        self._logs_lock = threading.Lock()

    def bind_loop(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop

    def append_log(self, pair_id: str, level: str, message: str, extra: dict):
        with self._logs_lock:
            lst = self._logs.setdefault(pair_id, [])
            lst.append({
                "ts": datetime.now().isoformat(timespec="seconds"),
                "level": level,
                "message": message,
                **(extra or {}),
            })
            if len(lst) > 1000:
                del lst[: len(lst) - 1000]

    def get_logs(self, pair_id: str, limit: int = 200) -> list:
        with self._logs_lock:
            return list(self._logs.get(pair_id, []))[-limit:]

    def drop_logs(self, pair_id: str):
        with self._logs_lock:
            self._logs.pop(pair_id, None)

    async def _send_all(self, payload: dict):
        dead = []
        for ws in list(self.clients):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for d in dead:
            self.clients.discard(d)

    def push(self, pair_id: str, level: str, message: str, extra: dict):
        # Called from sync-engine threads → must schedule onto asyncio loop.
        # progress + status are transient UI state, not history — don't store
        # them in the per-pair log buffer (otherwise the GET /logs endpoint
        # replays them as a fake "PROGRESS"/"STATUS" log line on refresh).
        if level not in ("progress", "status"):
            self.append_log(pair_id, level, message, extra)
        payload = {"pair_id": pair_id, "level": level, "message": message, **extra}
        if self._loop and self._loop.is_running():
            asyncio.run_coroutine_threadsafe(self._send_all(payload), self._loop)


broadcaster = Broadcaster()
engine = Engine(lambda pid, lvl, msg, extra: broadcaster.push(pid, lvl, msg, extra))

# ---- scheduler ----
scheduler = BackgroundScheduler()
scheduler.start()
_scheduled_jobs: dict[str, str] = {}  # pair_id -> job_id


def _reschedule(pair_id: str):
    old = _scheduled_jobs.pop(pair_id, None)
    if old:
        try:
            scheduler.remove_job(old)
        except Exception:
            pass
    p = engine.pairs.get(pair_id)
    if not p or p.cfg.trigger != "scheduled" or not p.cfg.schedule:
        return
    try:
        trig = CronTrigger.from_crontab(p.cfg.schedule)
        job = scheduler.add_job(
            lambda: engine.manual_sync(pair_id),
            trig,
            id=f"sync-{pair_id}",
            replace_existing=True,
        )
        _scheduled_jobs[pair_id] = job.id
    except Exception as e:
        print(f"[scheduler] bad cron for pair {pair_id}: {e}")


# Schedule existing pairs on boot
for _pid in list(engine.pairs.keys()):
    _reschedule(_pid)


# ---- models ----
class PairCreate(BaseModel):
    name: str
    folder_a: str
    folder_b: str
    # (two-way sync was removed; one-way A→B is the only mode)
    trigger: str = "auto"
    schedule: Optional[str] = None
    ignore_dirs: list[str] = []
    ignore_files: list[str] = []
    ignore_patterns: list[str] = []
    delete_orphans: bool = True
    safety_scan_interval: Optional[int] = None


class PairPatch(BaseModel):
    name: Optional[str] = None
    folder_a: Optional[str] = None
    folder_b: Optional[str] = None
    # (two-way sync was removed; one-way A→B is the only mode)
    trigger: Optional[str] = None
    schedule: Optional[str] = None
    ignore_dirs: Optional[list[str]] = None
    ignore_files: Optional[list[str]] = None
    ignore_patterns: Optional[list[str]] = None
    delete_orphans: Optional[bool] = None
    safety_scan_interval: Optional[int] = None
    paused: Optional[bool] = None


def _id() -> str:
    alphabet = string.ascii_lowercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(10))


def _dangerous_root(rp: Path) -> Optional[str]:
    """Return why a path is too dangerous to sync (drive root / system folder),
    or None if it's a safe ordinary folder."""
    if rp == Path(rp.anchor):  # e.g. C:\ or Z:\
        return "a whole drive root"
    blocked: set[Path] = set()
    for ev in ("SystemRoot", "ProgramFiles", "ProgramFiles(x86)", "ProgramData"):
        v = os.environ.get(ev)
        if v:
            try:
                blocked.add(Path(v).resolve())
            except Exception:
                pass
    try:
        home = Path.home().resolve()
        blocked.add(home)          # the user-profile root itself
        blocked.add(home.parent)   # C:\Users
    except Exception:
        pass
    if rp in blocked:
        return f"a protected system folder ({rp})"
    return None


def _validate_pair(folder_a: str, folder_b: str):
    """Reject dangerous topologies BEFORE a pair can ever sync: missing folders,
    identical/nested pairs, or drive-root/system-folder selections. A bad pair
    here is an 'absurd' way to delete files, so we stop it at the door."""
    a = Path(folder_a).expanduser()
    b = Path(folder_b).expanduser()
    for p in (a, b):
        if not p.exists() or not p.is_dir():
            raise HTTPException(400, f"Folder does not exist: {p}")
    try:
        ra, rb = a.resolve(), b.resolve()
    except Exception:
        raise HTTPException(400, "Could not resolve one of the folders.")
    if ra == rb:
        raise HTTPException(400, "Folder A and Folder B must be different folders.")
    if ra.is_relative_to(rb) or rb.is_relative_to(ra):
        raise HTTPException(400, "Folder A and Folder B must not be nested inside each other.")
    for p in (ra, rb):
        why = _dangerous_root(p)
        if why:
            raise HTTPException(400, f"Refusing to sync {why}: {p}. Pick a specific subfolder instead.")


# ---- routes ----
@app.get("/api/pairs")
def list_pairs():
    return [p.to_dict() for p in engine.pairs.values()]


@app.post("/api/pairs")
def create_pair(body: PairCreate):
    _validate_pair(body.folder_a, body.folder_b)
    cfg = SyncPairConfig(
        id=_id(),
        name=body.name,
        folder_a=str(Path(body.folder_a).expanduser()),
        folder_b=str(Path(body.folder_b).expanduser()),
        trigger=body.trigger,
        schedule=body.schedule,
        ignore_dirs=body.ignore_dirs,
        ignore_files=body.ignore_files,
        ignore_patterns=body.ignore_patterns,
        delete_orphans=True,  # delete propagation is always on (not user-toggleable)
        safety_scan_interval=body.safety_scan_interval,
    )
    pair = engine.add_pair(cfg)
    _reschedule(cfg.id)
    return pair.to_dict()


@app.patch("/api/pairs/{pid}")
def update_pair(pid: str, body: PairPatch):
    if pid not in engine.pairs:
        raise HTTPException(404, "Pair not found")
    patch = {k: v for k, v in body.model_dump().items() if v is not None}
    # Validate path topology whenever either folder is being changed.
    if "folder_a" in patch or "folder_b" in patch:
        cur = engine.pairs[pid].cfg
        _validate_pair(patch.get("folder_a", cur.folder_a), patch.get("folder_b", cur.folder_b))
    patch["delete_orphans"] = True  # always on, regardless of client input
    pair = engine.update_pair(pid, patch)
    _reschedule(pid)
    return pair.to_dict()


@app.delete("/api/pairs/{pid}")
def delete_pair(pid: str):
    if pid not in engine.pairs:
        raise HTTPException(404, "Pair not found")
    old = _scheduled_jobs.pop(pid, None)
    if old:
        try:
            scheduler.remove_job(old)
        except Exception:
            pass
    engine.remove_pair(pid)
    broadcaster.drop_logs(pid)
    return {"ok": True}


@app.post("/api/pairs/{pid}/sync")
def trigger_sync(pid: str):
    if pid not in engine.pairs:
        raise HTTPException(404, "Pair not found")
    engine.manual_sync(pid)
    return {"ok": True}


@app.post("/api/pairs/{pid}/pause")
def pause_pair(pid: str):
    p = engine.pause(pid)
    if not p:
        raise HTTPException(404, "Pair not found")
    return p.to_dict()


@app.post("/api/pairs/{pid}/resume")
def resume_pair(pid: str):
    p = engine.resume(pid)
    if not p:
        raise HTTPException(404, "Pair not found")
    _reschedule(pid)
    return p.to_dict()


@app.get("/api/pairs/{pid}/logs")
def get_logs(pid: str, limit: int = 200):
    if pid not in engine.pairs:
        raise HTTPException(404, "Pair not found")
    return broadcaster.get_logs(pid, limit)


@app.get("/api/browse")
def browse(path: str = ""):
    """List folder contents — used by folder picker."""
    if not path:
        # Default roots (Windows drives + home)
        from string import ascii_uppercase
        drives = []
        for letter in ascii_uppercase:
            p = Path(f"{letter}:/")
            if p.exists():
                drives.append({"name": f"{letter}:\\", "path": f"{letter}:/"})
        return {"path": "", "entries": drives, "is_root": True}
    p = Path(path).expanduser()
    if not p.exists() or not p.is_dir():
        raise HTTPException(400, "Not a valid directory")
    entries = []
    try:
        for item in sorted(p.iterdir(), key=lambda x: (not x.is_dir(), x.name.lower())):
            if item.is_dir():
                entries.append({"name": item.name, "path": str(item).replace("\\", "/")})
    except PermissionError:
        raise HTTPException(403, "Access denied")
    parent = str(p.parent).replace("\\", "/") if p.parent != p else ""
    return {"path": str(p).replace("\\", "/"), "entries": entries, "parent": parent, "is_root": False}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    broadcaster.clients.add(ws)
    try:
        while True:
            await ws.receive_text()  # just keep it open
    except WebSocketDisconnect:
        pass
    finally:
        broadcaster.clients.discard(ws)


@app.get("/")
def root():
    return FileResponse(str(STATIC / "index.html"))


app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")


@app.on_event("startup")
async def on_startup():
    broadcaster.bind_loop(asyncio.get_running_loop())


@app.on_event("shutdown")
def on_shutdown():
    engine.stop_all()
    scheduler.shutdown(wait=False)


def _pid_cmdline(pid: str) -> str:
    """Return the lowercased command-line of a PID, or '' if it can't be read.

    Tries PowerShell's Get-CimInstance first (works on every modern Windows),
    falls back to WMIC for older boxes. Both are read-only — no side effects.
    """
    import subprocess
    try:
        ps = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             f"(Get-CimInstance Win32_Process -Filter 'ProcessId={pid}').CommandLine"],
            capture_output=True, text=True, timeout=4
        )
        out = (ps.stdout or "").strip()
        if out:
            return out.lower()
    except Exception:
        pass
    try:
        wmic = subprocess.run(
            ["wmic", "process", "where", f"ProcessId={pid}", "get", "CommandLine", "/value"],
            capture_output=True, text=True, timeout=4
        )
        return (wmic.stdout or "").lower()
    except Exception:
        return ""


def stop_stale_synclinkpro(port: int):
    """If a stale SyncLinkPro (python running app.py) is on this port, kill
    ONLY it. Anything else is left alone.

    Safety logic: SyncLinkPro's port is read from .env via dotenv_values()
    above — never from os.environ — so Christopher's PORT=7777 can't sneak
    in here. That means whatever is on `port` is either us or an unrelated
    process. We only kill when the process is python AND its command line
    references `app.py` (matches `python app.py`, `python.exe app.py`, full
    paths to either, and Uvicorn reload's child workers)."""
    import subprocess
    try:
        result = subprocess.run(["netstat", "-ano"], capture_output=True, text=True)
        for line in result.stdout.splitlines():
            if f":{port}" not in line or "LISTENING" not in line:
                continue
            pid = line.strip().split()[-1]
            cmdline = _pid_cmdline(pid)
            is_python = "python" in cmdline
            is_app_py = "app.py" in cmdline
            if is_python and is_app_py:
                subprocess.run(["taskkill", "/F", "/PID", pid], capture_output=True)
                print(f"  Stopped stale SyncLinkPro on port {port} (PID {pid})")
                # Give Windows a moment to release the socket — otherwise the
                # very next bind hits TIME_WAIT and we get WinError 10048.
                import time
                for _ in range(20):
                    time.sleep(0.25)
                    probe = subprocess.run(["netstat", "-ano"], capture_output=True, text=True)
                    if not any(f":{port}" in ln and "LISTENING" in ln for ln in probe.stdout.splitlines()):
                        break
            else:
                print(f"  Port {port} is held by PID {pid} (not SyncLinkPro). Leaving it alone.")
    except Exception:
        pass


if __name__ == "__main__":
    import os, sys
    from dotenv import dotenv_values
    # Read PORT ONLY from .env (or fall back to the hardcoded default 7878).
    # We never read os.environ["PORT"] because Christopher exports PORT=7777
    # in some shells; if SyncLinkPro picked that up it would try to bind 7777
    # and (before this fix) kill Christopher's process. dotenv_values returns
    # purely what's parsed from the .env file — os.environ is ignored.
    env_file = dotenv_values(".env")
    port = int(env_file.get("PORT") or 7878)
    stop_stale_synclinkpro(port)
    url = f"http://localhost:{port}"
    # Plain ASCII so Windows cp1252 console doesn't crash on the arrow.
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    print(f"\n  SyncLinkPro  ->  {url}\n")
    try:
        webbrowser.open(url)
    except Exception:
        pass
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")
