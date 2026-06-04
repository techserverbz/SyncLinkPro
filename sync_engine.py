"""
SyncLinkPro — sync engine.
Port of Initial Auto Sync's logic with:
  - State files stored in Documents/autosync/state/ (never in source or destination)
  - Callback-based event emission for web UI
  - Pause/resume support
  - One-way only (A → B). The previous two-way mode was removed; any legacy
    "mode" field in existing pairs.json is ignored on load.
"""
from __future__ import annotations
import os
import time
import json
import shutil
import hashlib
import threading
import fnmatch
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field, asdict
from typing import Callable, Optional

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

STATE_ROOT = Path(os.path.expanduser("~")) / "Documents" / "autosync" / "state"
(STATE_ROOT / "sync-state").mkdir(parents=True, exist_ok=True)
(STATE_ROOT / "logs").mkdir(parents=True, exist_ok=True)

DEBOUNCE_SECONDS = 0.5            # was DEBOUNCE_SECONDS_1WAY; the only mode now
SAFETY_SCAN_INTERVAL = 30         # was SAFETY_SCAN_ONEWAY; the only mode now
DELETE_MAX_RETRIES = 30
DELETE_INITIAL_DELAY = 2.0
SUPPRESS_TTL = 5.0

# Live-watcher delete circuit-breaker: if more than this many deletes are
# propagated within the window, the pair pauses itself and asks for a manual
# resume. Set high enough that ordinary deleting never trips it.
LIVE_DELETE_BURST = 50
LIVE_DELETE_WINDOW = 10.0       # seconds

# Legacy online-marker name. No longer written or required (online-ness is now
# decided by whether a folder root exists and is readable). Kept in the ignore
# set only so any leftover/cloud-resurrected marker file is never treated as a
# real file to sync or delete.
SENTINEL_NAME = ".synclinkpro-online"

DEFAULT_IGNORE_FILES = {
    "desktop.ini", "thumbs.db", ".ds_store",
    SENTINEL_NAME,
}
DEFAULT_IGNORE_PATTERNS = ("~$*", "*.tmp", "*.temp", "*.swp", "*.lock")
DEFAULT_IGNORE_DIRS = {
    ".git", "__pycache__", "node_modules", ".next",
    ".sync-metadata", "dist", ".venv", "venv",
}


def md5_of(path: Path, block: int = 65536) -> Optional[str]:
    try:
        h = hashlib.md5()
        with path.open("rb") as f:
            while chunk := f.read(block):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


def should_ignore(name: str, extra_files: set[str], extra_patterns: tuple[str, ...]) -> bool:
    low = name.lower()
    if low in DEFAULT_IGNORE_FILES or low in extra_files:
        return True
    for pat in DEFAULT_IGNORE_PATTERNS + extra_patterns:
        if fnmatch.fnmatch(low, pat.lower()):
            return True
    return False


def should_ignore_dir(name: str, extra_dirs: set[str]) -> bool:
    return name in DEFAULT_IGNORE_DIRS or name in extra_dirs


class SyncState:
    """Per-pair state: relpath -> {mtime, size, source, updated}."""
    def __init__(self, pair_id: str):
        self.path = STATE_ROOT / "sync-state" / f"{pair_id}.json"
        self.data: dict = {}
        # True when an existing state file failed to parse (corruption). While
        # suspect, the engine refuses to DELETE (only copies/records) until a
        # clean save() re-establishes a trusted baseline — a corrupt/empty state
        # must never be read as "everything was deleted".
        self.suspect = False
        self._lock = threading.Lock()
        self._load()

    def _load(self):
        if self.path.exists():
            try:
                self.data = json.loads(self.path.read_text(encoding="utf-8"))
            except Exception:
                # File exists but won't parse → corruption. Keep empty data but
                # flag as suspect so deletes are suppressed until re-baselined.
                self.data = {}
                self.suspect = True

    def save(self):
        # Snapshot under the lock so we never serialize a dict that another
        # thread (safety scan vs. live watcher) is mutating mid-write.
        with self._lock:
            payload = json.dumps(self.data, indent=2)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(payload, encoding="utf-8")
        tmp.replace(self.path)
        self.suspect = False  # a clean write re-establishes a trusted baseline

    def record(self, relpath: str, mtime: float, size: int, source: str):
        with self._lock:
            self.data[relpath] = {
                "mtime": mtime, "size": size, "source": source,
                "updated": datetime.now().isoformat(timespec="seconds"),
            }

    def forget(self, relpath: str):
        with self._lock:
            self.data.pop(relpath, None)

    def was_known(self, relpath: str) -> bool:
        with self._lock:
            return relpath in self.data

    def known_count(self) -> int:
        with self._lock:
            return len(self.data)

    def known_keys(self) -> list[str]:
        with self._lock:
            return list(self.data.keys())


class SuppressSet:
    """Track paths recently written/deleted by the engine to ignore watcher echoes."""
    def __init__(self, ttl: float = SUPPRESS_TTL):
        self.ttl = ttl
        self._entries: dict[str, float] = {}
        self._lock = threading.Lock()

    def add(self, path: str):
        with self._lock:
            self._entries[path] = time.time()

    def contains(self, path: str) -> bool:
        now = time.time()
        with self._lock:
            self._entries = {k: v for k, v in self._entries.items() if now - v < self.ttl}
            return path in self._entries


class DeleteQueue:
    """Retries deletes that fail because of Google Drive / cloud locks.

    Each queued delete carries an optional ``on_done(success: bool)`` callback so
    the owner can forget sync-state only once the delete *actually* completes
    (and re-enable normal handling if it is ultimately abandoned).
    """
    def __init__(self, logger: Callable[[str, str], None]):
        self.queue: list[tuple[Path, float, int, Optional[Callable[[bool], None]]]] = []
        self._lock = threading.Lock()
        self.logger = logger

    @staticmethod
    def _remove(target: Path):
        if target.is_dir() and not target.is_symlink():
            shutil.rmtree(target)
        elif target.exists():
            target.unlink()

    def attempt_delete(self, target: Path, on_done: Optional[Callable[[bool], None]] = None) -> bool:
        """Try once. Returns True on immediate success (caller handles cleanup);
        on failure the delete is queued and ``on_done`` fires later from tick()."""
        try:
            self._remove(target)
            return True
        except Exception as e:
            self.logger("warn", f"Delete failed (queuing retry): {target} — {e}")
            with self._lock:
                self.queue.append((target, time.time() + DELETE_INITIAL_DELAY, 0, on_done))
            return False

    def tick(self):
        if not self.queue:
            return
        now = time.time()
        pending = []
        callbacks: list[tuple[Callable[[bool], None], bool]] = []
        with self._lock:
            for target, when, tries, on_done in self.queue:
                if now < when:
                    pending.append((target, when, tries, on_done))
                    continue
                try:
                    self._remove(target)
                    self.logger("info", f"Delete succeeded on retry: {target}")
                    if on_done:
                        callbacks.append((on_done, True))
                except Exception as e:
                    tries += 1
                    if tries >= DELETE_MAX_RETRIES:
                        self.logger("error", f"Delete abandoned after {tries} tries: {target} — {e}")
                        if on_done:
                            callbacks.append((on_done, False))
                    else:
                        delay = DELETE_INITIAL_DELAY * min(tries, 4)
                        pending.append((target, now + delay, tries, on_done))
            self.queue = pending
        # Fire callbacks outside the lock to avoid any re-entrancy surprises.
        for cb, ok in callbacks:
            try:
                cb(ok)
            except Exception:
                pass


@dataclass
class SyncPairConfig:
    id: str
    name: str
    folder_a: str
    folder_b: str
    # NOTE: "mode" was removed when two-way sync was retired (one-way A→B only).
    # Legacy "mode" values in pairs.json are stripped in load_pairs() / update_pair().
    trigger: str = "auto"  # manual | auto | scheduled
    schedule: Optional[str] = None  # cron expression
    ignore_dirs: list = field(default_factory=list)
    ignore_files: list = field(default_factory=list)
    ignore_patterns: list = field(default_factory=list)
    delete_orphans: bool = True
    safety_scan_interval: Optional[int] = None  # seconds; None = SAFETY_SCAN_INTERVAL
    paused: bool = False
    created_at: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))
    last_sync: Optional[str] = None


class PairRuntime:
    """A single sync pair. Handles watchers + full scans + state."""
    def __init__(self, cfg: SyncPairConfig, emit: Callable[[str, str, str, dict], None]):
        self.cfg = cfg
        self.emit = emit  # (pair_id, level, message, extra) callback
        self.state = SyncState(cfg.id)
        self.suppress = SuppressSet()
        self.delete_q = DeleteQueue(lambda lvl, msg: self._log(lvl, msg))
        # relpaths whose delete is currently queued/retrying — never re-create
        # or re-queue these while a delete is in flight.
        self._pending_delete: set[str] = set()
        self._scan_lock = threading.Lock()
        self._stop = threading.Event()
        self._observer: Optional[Observer] = None
        self._scan_thread: Optional[threading.Thread] = None
        self._pending_events: dict[str, tuple[str, float]] = {}  # relpath -> (kind, ts)
        self._events_lock = threading.Lock()
        self._debounce_thread: Optional[threading.Thread] = None
        self.status = "idle"  # idle | syncing | error | paused
        # Rolling timestamps of recent live-watcher deletes (circuit breaker).
        self._recent_deletes: list[float] = []

    # ---- logging helpers ----
    def _log(self, level: str, message: str, extra: Optional[dict] = None):
        self.emit(self.cfg.id, level, message, extra or {})

    def _set_status(self, s: str):
        self.status = s
        self.emit(self.cfg.id, "status", s, {})

    def _progress(self, phase: str, percent: int, current: int = 0, total: int = 0):
        self.emit(self.cfg.id, "progress", phase, {"percent": percent, "current": current, "total": total})

    # ---- ignore helpers ----
    def _ignore_file(self, name: str) -> bool:
        return should_ignore(name, set(self.cfg.ignore_files), tuple(self.cfg.ignore_patterns))

    def _ignore_dir(self, name: str) -> bool:
        return should_ignore_dir(name, set(self.cfg.ignore_dirs))

    # ---- online / health detection ----
    def _side_online(self, root: Path) -> bool:
        """A side is 'online' (safe to treat its absences as real deletions) when
        its root EXISTS and is READABLE — even if it happens to be empty. The
        catastrophic case we guard against is the drive being disconnected, which
        shows up as a missing/unreadable root (the path is gone or scandir errors),
        not as a clean empty listing. An empty-but-readable folder is genuinely
        empty, so deletions there are real and may propagate.

        A separate non-aborting backstop in full_sync still refuses to propagate a
        WHOLE-side wipe, so a freak empty read can never mass-delete the other side."""
        try:
            if not root.exists() or not root.is_dir():
                return False
            # Readability probe: a half-mounted / disconnected drive errors here
            # even when exists() returned True.
            with os.scandir(root) as it:
                next(it, None)
            return True
        except Exception:
            return False

    # ---- scan ----
    def _walk(self, root: Path) -> tuple[dict[str, dict], bool]:
        """Return (file_map, had_errors). ``had_errors`` is True if any directory
        could not be listed or any file could not be stat'd — in which case the
        listing is INCOMPLETE and callers must NOT infer deletions from it."""
        out: dict[str, dict] = {}
        had_errors = False
        if not root.exists():
            return out, had_errors

        def _on_walk_error(err: OSError):
            nonlocal had_errors
            had_errors = True
            self._log("warn", f"Read error while scanning {getattr(err, 'filename', root)}: {err}")

        for dirpath, dirnames, filenames in os.walk(root, onerror=_on_walk_error):
            dirnames[:] = [d for d in dirnames if not self._ignore_dir(d)]
            rel_dir = Path(dirpath).relative_to(root).as_posix()
            for fname in filenames:
                if self._ignore_file(fname):
                    continue
                full = Path(dirpath) / fname
                rel = (Path(rel_dir) / fname).as_posix() if rel_dir != "." else fname
                try:
                    st = full.stat()
                    out[rel] = {"mtime": st.st_mtime, "size": st.st_size, "is_dir": False}
                except Exception:
                    # Couldn't stat a file that DOES exist → incomplete read.
                    had_errors = True
                    continue
            # track empty dirs
            if not filenames and not dirnames and rel_dir != ".":
                out[rel_dir + "/"] = {"mtime": 0, "size": -1, "is_dir": True}
        return out, had_errors

    # ---- copy / delete ----
    def _copy(self, src: Path, dst: Path):
        # Suppress the destination BEFORE writing so the watcher's own echo for
        # the write is ignored even while a large copy is in progress.
        self.suppress.add(str(dst))
        dst.parent.mkdir(parents=True, exist_ok=True)
        if src.is_dir():
            dst.mkdir(parents=True, exist_ok=True)
        else:
            shutil.copy2(src, dst)

    def _delete(self, target: Path, rel: str, save: bool = False) -> bool:
        """Delete ``target`` (the mirror of ``rel``) and forget its sync-state.

        State is forgotten only once the delete actually succeeds. If the delete
        has to be queued (file locked / cloud lock), ``rel`` is parked in
        ``_pending_delete`` so a concurrent scan won't copy it back, and the
        state is forgotten later when the retry finally lands.
        """
        self.suppress.add(str(target))

        def _on_done(success: bool, r: str = rel):
            if success:
                self._finish_delete(r, save=True)
            else:
                # Abandoned: stop treating it as in-flight so normal logic can
                # retry on the next scan (state stays 'known' on purpose).
                self._pending_delete.discard(r)

        ok = self.delete_q.attempt_delete(target, on_done=_on_done)
        if ok:
            self._finish_delete(rel, save=save)
        else:
            self._pending_delete.add(rel)
        return ok

    def _finish_delete(self, rel: str, save: bool = True):
        self.state.forget(rel)
        self._pending_delete.discard(rel)
        if save:
            self.state.save()

    def _delete_empty_dir(self, target: Path, rel: str) -> bool:
        """Remove an *empty* directory only — never recurse, so we can't nuke
        files that appeared after the scan. Returns True if removed."""
        self.suppress.add(str(target))
        try:
            if target.exists():
                target.rmdir()  # raises OSError if not empty
            self.state.forget(rel)
            return True
        except OSError:
            return False

    def _register_delete_and_guard(self) -> bool:
        """Record a live deletion; trip a circuit-breaker on a burst. Returns
        False (and PAUSES the pair) if too many deletes happen too fast — the
        signature of something going wrong rather than deliberate editing."""
        now = time.time()
        self._recent_deletes = [t for t in self._recent_deletes if now - t < LIVE_DELETE_WINDOW]
        self._recent_deletes.append(now)
        if len(self._recent_deletes) > LIVE_DELETE_BURST:
            self.cfg.paused = True
            self._set_status("paused")
            self._log("error",
                      f"Delete circuit-breaker tripped (> {LIVE_DELETE_BURST} deletions in "
                      f"{LIVE_DELETE_WINDOW:.0f}s). Pair PAUSED — resume manually after checking.")
            return False
        return True

    def _live_delete_allowed(self, vanished_root: Path, rel: str) -> bool:
        """Gate a live deletion: allow it only when the side that LOST the file
        is genuinely online, the state is trusted, and the burst-breaker is ok.
        A file that 'vanished' because its drive went offline is NOT a deletion."""
        if self.state.suspect:
            self._log("warn", f"Skipping delete of {rel}: sync-state untrusted (corrupt?)")
            return False
        if not self._side_online(vanished_root):
            self._log("warn", f"Skipping delete of {rel}: source folder looks offline")
            return False
        return self._register_delete_and_guard()

    # ---- delete safety preflight ----
    def _preflight(self, a: Path, b: Path, map_a: dict, err_a: bool,
                   map_b: dict, err_b: bool) -> tuple[Optional[str], Optional[str]]:
        """Decide, BEFORE touching anything, whether this scan is trustworthy.

        Returns ``(abort, block_deletes)``:
        - ``abort``: a message → do nothing at all (a side is offline/unreadable;
          acting on it could wipe the live data). Applies to manual too.
        - ``block_deletes``: a message → still copy/record, but DO NOT delete
          (the listing is incomplete or the baseline is untrusted).

        The point: a *real* deletion (folder online, readable, file genuinely
        gone) propagates; an *absurd* one (drive offline, partial read, corrupt
        state) never does."""
        if not self._side_online(a):
            return (f"folder A ({a}) is offline or unreadable (the drive may be "
                    f"disconnected) — refusing to sync to avoid deleting real files", None)
        if not self._side_online(b):
            return (f"folder B ({b}) is offline or unreadable (the drive may be "
                    f"disconnected) — refusing to sync to avoid deleting real files", None)
        if err_a or err_b:
            return (None, "read errors during scan — skipping deletions this pass "
                          "(copies still applied; some files were unreadable)")
        if self.state.suspect:
            return (None, "sync-state file was unreadable/corrupt — skipping deletions "
                          "until a clean baseline is re-established")
        return (None, None)

    # ---- full scan (both modes) ----
    def full_sync(self, reason: str = "manual"):
        if self.cfg.paused:
            self._log("warn", f"Skipping sync — pair is paused (reason: {reason})")
            return
        if not self._scan_lock.acquire(blocking=False):
            self._log("warn", f"Scan already running; ignoring trigger: {reason}")
            return
        try:
            self._set_status("syncing")
            self._log("info", f"Full sync started ({reason})")
            a = Path(self.cfg.folder_a)
            b = Path(self.cfg.folder_b)

            self._progress("Scanning folder A", 5)
            map_a, err_a = self._walk(a)
            self._progress("Scanning folder B", 25)
            map_b, err_b = self._walk(b)
            all_paths = sorted(set(map_a) | set(map_b))
            total = len(all_paths)

            # Trust check BEFORE any mutation. We never auto-create a vanished
            # destination — a missing/unreadable folder means the drive is
            # OFFLINE, so we refuse rather than mirror its emptiness as deletions.
            abort, block_deletes = self._preflight(a, b, map_a, err_a, map_b, err_b)
            if abort:
                self._log("error", f"Aborting sync: {abort}")
                self._set_status("error")
                return
            if block_deletes:
                self._log("warn", block_deletes)
            delete_ok = self.cfg.delete_orphans and not block_deletes

            # Non-aborting wipe backstop: if a WHOLE side reads empty while the
            # other still has files (and we have a baseline), don't propagate that
            # as a mass-delete — copy/keep instead. Real one-off deletions still
            # flow through the live watcher; this only shields the periodic scan
            # from a freak empty read. The sync continues (no error), just no deletes.
            if delete_ok and self.state.known_count() > 0 and \
                    ((not map_a and map_b) or (not map_b and map_a)):
                empty = "A" if not map_a else "B"
                self._log("warn", f"folder {empty} read as empty while the other has files — "
                                  f"not propagating deletions this pass (keeping/copying instead).")
                delete_ok = False

            self._progress(f"Comparing {total} files", 40, 0, total)
            copies = dels = skips = errs = 0
            for idx, rel in enumerate(all_paths):
                if idx and total and idx % max(50, total // 40) == 0:
                    pct = 40 + int((idx / total) * 55) if total else 95
                    self._progress(f"Syncing ({idx}/{total})", pct, idx, total)
                try:
                    in_a = rel in map_a
                    in_b = rel in map_b
                    src_path_a = a / rel
                    src_path_b = b / rel
                    # Handle (empty) directory markers — one-way: A is source.
                    if rel.endswith("/"):
                        if rel in self._pending_delete:
                            continue
                        actual_rel = rel.rstrip("/")
                        pb = b / actual_rel
                        if in_a:
                            if not in_b:
                                pb.mkdir(parents=True, exist_ok=True)
                                copies += 1
                            if not self.state.was_known(rel):
                                self.state.record(rel, 0, -1, "a")
                        elif delete_ok:  # in B only → not in source
                            if self._delete_empty_dir(pb, rel):
                                dels += 1
                        continue

                    # A delete for this path is in flight (queued retry) — don't
                    # recreate it or re-handle until that delete resolves.
                    if rel in self._pending_delete:
                        continue

                    if in_a and not in_b:
                        # present only in A → copy to B (one-way: A is source)
                        self._copy(src_path_a, src_path_b)
                        self.state.record(rel, map_a[rel]["mtime"], map_a[rel]["size"], "a")
                        copies += 1
                    elif in_b and not in_a:
                        # present only in B → B is mirror, missing in source → delete B
                        if delete_ok:
                            self._delete(src_path_b, rel)
                            dels += 1
                    else:  # in both — compare
                        ma = map_a[rel]["mtime"]
                        mb = map_b[rel]["mtime"]
                        sa = map_a[rel]["size"]
                        sb = map_b[rel]["size"]
                        if sa == sb and abs(ma - mb) < 1.0:
                            # Identical → record as known so a future deletion of
                            # this file is recognised as a delete (and propagated)
                            # rather than mistaken for a brand-new file (copied back).
                            if not self.state.was_known(rel):
                                self.state.record(rel, ma, sa, "both")
                            skips += 1
                            continue
                        if sa == sb:
                            # same size, different mtime → verify with hash
                            ha = md5_of(src_path_a)
                            hb = md5_of(src_path_b)
                            if ha and hb and ha == hb:
                                # content identical — touch state, skip copy
                                newer_mtime = max(ma, mb)
                                self.state.record(rel, newer_mtime, sa, "both")
                                skips += 1
                                continue
                        # One-way: A is the source of truth, always overwrite B.
                        self._copy(src_path_a, src_path_b)
                        self.state.record(rel, ma, sa, "a")
                        copies += 1
                except Exception as e:
                    errs += 1
                    self._log("error", f"Sync error for {rel}: {e}")
            self.state.save()
            self.cfg.last_sync = datetime.now().isoformat(timespec="seconds")
            self._progress("Done", 100, total, total)
            self._log("info", f"Full sync done — copied: {copies}, deleted: {dels}, skipped: {skips}, errors: {errs}")
            self._set_status("idle" if errs == 0 else "error")
        finally:
            self._scan_lock.release()

    # ---- event-driven sync ----
    def _queue_event(self, relpath: str, kind: str):
        with self._events_lock:
            self._pending_events[relpath] = (kind, time.time())

    def _debounce_loop(self, stop_event):
        interval = DEBOUNCE_SECONDS
        while not stop_event.is_set():
            time.sleep(0.25)
            now = time.time()
            due: list[tuple[str, str]] = []
            with self._events_lock:
                for rel, (kind, ts) in list(self._pending_events.items()):
                    if now - ts >= interval:
                        due.append((rel, kind))
                        self._pending_events.pop(rel, None)
            for rel, kind in due:
                try:
                    self._handle_single_event(rel, kind)
                except Exception as e:
                    self._log("error", f"Live sync error ({rel}): {e}")
            self.delete_q.tick()

    def _handle_single_event(self, rel: str, kind: str):
        if self.cfg.paused:
            return
        a = Path(self.cfg.folder_a)
        b = Path(self.cfg.folder_b)
        src_a = a / rel
        src_b = b / rel
        # A delete for this path is queued/retrying — don't recreate it.
        if rel in self._pending_delete:
            return
        a_exists = src_a.exists()
        b_exists = src_b.exists()
        # One-way only: A is source, B mirrors A.
        if a_exists:
            self._copy(src_a, src_b)
            try:
                st = src_a.stat()
                self.state.record(rel, st.st_mtime, st.st_size, "a")
            except Exception:
                pass
        else:
            if self.cfg.delete_orphans and b_exists:
                # File gone from source A → delete mirror only if A is online.
                if self._live_delete_allowed(a, rel):
                    self._delete(src_b, rel)
        self.state.save()
        self.cfg.last_sync = datetime.now().isoformat(timespec="seconds")

    # ---- watchdog handlers ----
    def _make_handler(self, root: Path):
        pair = self
        class H(FileSystemEventHandler):
            def _rel(self, p: str) -> Optional[str]:
                try:
                    rp = Path(p).relative_to(root).as_posix()
                except Exception:
                    return None
                if not rp or rp == ".":
                    return None
                # drop ignored
                parts = rp.split("/")
                if any(pair._ignore_dir(x) for x in parts[:-1]):
                    return None
                if pair._ignore_file(parts[-1]):
                    return None
                return rp

            def on_any_event(self, event):
                if event.is_directory:
                    return  # directories handled via file events or full scan
                if pair.suppress.contains(event.src_path):
                    return
                rel = self._rel(event.src_path)
                if rel is None:
                    return
                pair._queue_event(rel, event.event_type)
        return H()

    # ---- safety scan ----
    def _safety_scan_loop(self, stop_event):
        interval = self.cfg.safety_scan_interval if self.cfg.safety_scan_interval and self.cfg.safety_scan_interval > 0 else SAFETY_SCAN_INTERVAL
        while not stop_event.is_set():
            # chunk the sleep so we respond to stop quickly
            for _ in range(interval):
                if stop_event.is_set():
                    return
                time.sleep(1)
            if self.cfg.paused:
                continue
            # quiet full scan — no noisy log unless changes
            try:
                self.full_sync(reason="safety-scan")
            except Exception as e:
                self._log("error", f"Safety scan error: {e}")

    # ---- lifecycle ----
    def start(self):
        if self.cfg.trigger != "auto":
            self._log("info", f"Pair configured for {self.cfg.trigger} trigger — watcher NOT started.")
            return
        if self._observer is not None:
            return
        self._stop = threading.Event()  # fresh event for this run
        stop_event = self._stop  # capture by closure — old threads kept ref to the OLD (set) event
        self._observer = Observer()
        # One-way: watch the source (A) only — B is a passive mirror.
        self._observer.schedule(self._make_handler(Path(self.cfg.folder_a)), self.cfg.folder_a, recursive=True)
        self._observer.start()
        self._debounce_thread = threading.Thread(target=self._debounce_loop, args=(stop_event,), daemon=True)
        self._debounce_thread.start()
        self._scan_thread = threading.Thread(target=self._safety_scan_loop, args=(stop_event,), daemon=True)
        self._scan_thread.start()
        self._log("info", "Live watcher started (one-way A → B).")

    def stop(self):
        self._stop.set()
        if self._observer:
            self._observer.stop()
            try:
                self._observer.join(timeout=2)
            except Exception:
                pass
            self._observer = None
        self._log("info", "Pair stopped.")

    def to_dict(self) -> dict:
        d = asdict(self.cfg)
        d["status"] = self.status
        return d


# ------------- Engine manager + persistence -------------
PAIRS_FILE = STATE_ROOT / "pairs.json"


class Engine:
    def __init__(self, emit: Callable[[str, str, str, dict], None]):
        self.emit = emit
        self.pairs: dict[str, PairRuntime] = {}
        self._logs: dict[str, list] = {}  # pair_id -> list (capped)
        self._logs_lock = threading.Lock()
        self.load_pairs()

    def load_pairs(self):
        if not PAIRS_FILE.exists():
            return
        try:
            raw = json.loads(PAIRS_FILE.read_text(encoding="utf-8"))
            for cfg_dict in raw:
                # Migration: 'mode' was removed when two-way was retired.
                # Strip it so dataclass construction doesn't fail and so any
                # legacy "twoway" pairs run as one-way going forward.
                cfg_dict.pop("mode", None)
                cfg = SyncPairConfig(**cfg_dict)
                pair = PairRuntime(cfg, self.emit)
                self.pairs[cfg.id] = pair
                if cfg.trigger == "auto" and not cfg.paused:
                    pair.start()
        except Exception as e:
            print(f"[engine] failed to load pairs: {e}")

    def save_pairs(self):
        data = [asdict(p.cfg) for p in self.pairs.values()]
        tmp = PAIRS_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
        tmp.replace(PAIRS_FILE)

    def add_pair(self, cfg: SyncPairConfig) -> PairRuntime:
        self.pairs[cfg.id] = PairRuntime(cfg, self.emit)
        self.save_pairs()
        if cfg.trigger == "auto" and not cfg.paused:
            self.pairs[cfg.id].start()
        return self.pairs[cfg.id]

    def remove_pair(self, pid: str):
        p = self.pairs.pop(pid, None)
        if p:
            p.stop()
        self.save_pairs()

    def update_pair(self, pid: str, patch: dict):
        p = self.pairs.get(pid)
        if not p:
            return None
        p.stop()
        cfg_dict = asdict(p.cfg)
        cfg_dict.update(patch)
        # Migration: ignore any legacy 'mode' key coming from old clients/state.
        cfg_dict.pop("mode", None)
        new_cfg = SyncPairConfig(**cfg_dict)
        p.cfg = new_cfg
        if new_cfg.trigger == "auto" and not new_cfg.paused:
            p.start()
        self.save_pairs()
        return p

    def pause(self, pid: str):
        p = self.pairs.get(pid)
        if not p:
            return None
        p.cfg.paused = True
        p.stop()
        self.save_pairs()
        return p

    def resume(self, pid: str):
        p = self.pairs.get(pid)
        if not p:
            return None
        p.cfg.paused = False
        if p.cfg.trigger == "auto":
            p.start()
        self.save_pairs()
        return p

    def manual_sync(self, pid: str):
        p = self.pairs.get(pid)
        if not p:
            return None
        threading.Thread(target=p.full_sync, args=("manual",), daemon=True).start()
        return p

    def append_log(self, pid: str, level: str, message: str, extra: dict):
        with self._logs_lock:
            lst = self._logs.setdefault(pid, [])
            lst.append({
                "ts": datetime.now().isoformat(timespec="seconds"),
                "level": level,
                "message": message,
                **(extra or {}),
            })
            if len(lst) > 1000:
                del lst[: len(lst) - 1000]

    def get_logs(self, pid: str, limit: int = 200) -> list:
        with self._logs_lock:
            return list(self._logs.get(pid, []))[-limit:]

    def stop_all(self):
        for p in self.pairs.values():
            p.stop()
