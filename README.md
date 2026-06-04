# SyncLinkPro

Monolithic Python sync tool. Combines **Initial Auto Sync**'s battle-tested engine (MD5 hash verification, Google Drive lock retry, echo suppression, state-based delete detection, periodic safety scan) with **SyncLink**'s polished UX (web UI, manual / auto / scheduled triggers, live logs, progress).

## Key design choices

- **No Electron** — single FastAPI process, runs in your browser on `http://localhost:7878`.
- **State files never pollute source/destination.** All sync state lives in `%USERPROFILE%\Documents\autosync\state\`:
  - `pairs.json` — all sync pair configs
  - `sync-state/{pair-id}.json` — per-pair mtime/size/origin tracking
- **Live sync** via watchdog file events with debouncing + echo suppression.
- **Safety net** — periodic full scan every 30s catches anything watchers miss.

## Run

```bash
pip install -r requirements.txt
python app.py
```

Or double-click `start.bat`. Opens your browser automatically.

## Where things live

| Data | Location |
|---|---|
| Pair configs | `~/Documents/autosync/state/pairs.json` |
| Per-pair sync state | `~/Documents/autosync/state/sync-state/{id}.json` |
| Logs (in-memory, ephemeral) | RAM only; streams to UI via WebSocket |

## What it handles

- One-way sync (A → B). Folder A is the source of truth; B is a mirror.
- Manual, auto (real-time), scheduled (cron) triggers
- File system locks (retry queue with exponential backoff)
- Watcher echo loops (SuppressSet)
- Partial writes (debounce)
- Same-size different-mtime conflicts (MD5 tiebreaker)
- State-aware delete propagation (knows "was synced, now gone")
- Empty directories
- Custom ignore dirs / patterns
