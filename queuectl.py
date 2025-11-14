#!/usr/bin/env python3
"""
queuectl.py - Minimal job queue CLI with SQLite persistence, workers, retries, DLQ.

Usage examples:
  ./queuectl.py enqueue '{"id":"job1","command":"echo Hello","max_retries":3}'
  ./queuectl.py worker start --count 2
  ./queuectl.py status
  ./queuectl.py list --state pending
  ./queuectl.py dlq list
  ./queuectl.py dlq retry job1
  ./queuectl.py config set backoff_base 2
"""

import argparse
import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta
from datetime import datetime, timezone
from multiprocessing import Process
from pathlib import Path

DB_PATH = os.environ.get("QUEUECTL_DB", "queuectl.db")
PID_FILE = os.environ.get("QUEUECTL_PIDFILE", "/tmp/queuectl_workers.pid")
DEFAULT_BACKOFF_BASE = 2
DEFAULT_MAX_RETRIES = 3
POLL_INTERVAL = 1.0  # seconds

# -----------------------
# DB helpers / schema
# -----------------------
def now_iso():
    return datetime.now(timezone.utc).isoformat()

def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=10, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    return conn

def init_db():
    conn = get_conn()
    c = conn.cursor()

    # executescript() cannot accept parameters — must run as plain SQL
    c.executescript("""
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        command TEXT NOT NULL,
        state TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        max_retries INTEGER NOT NULL DEFAULT 3,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        next_run INTEGER NULL,
        last_error TEXT NULL,
        worker TEXT NULL
    );

    CREATE TABLE IF NOT EXISTS config (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    """)

    # Now insert defaults separately (with parameters)
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO config(key, value) VALUES ('backoff_base', ?)",
                (str(DEFAULT_BACKOFF_BASE),))
    cur.execute("INSERT OR IGNORE INTO config(key, value) VALUES ('default_max_retries', ?)",
                (str(DEFAULT_MAX_RETRIES),))

    conn.commit()
    conn.close()


# -----------------------
# Job operations
# -----------------------
def enqueue_job(job_json):
    """
    job_json: dict or json-string with fields like id, command, max_retries (optional)
    """
    if isinstance(job_json, str):
        job = json.loads(job_json)
    else:
        job = job_json
    jid = job.get("id") or str(uuid.uuid4())
    command = job["command"]
    attempts = 0
    max_retries = int(job.get("max_retries") or get_config("default_max_retries") or DEFAULT_MAX_RETRIES)
    now = now_iso()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO jobs(id, command, state, attempts, max_retries, created_at, updated_at, next_run) VALUES (?, ?, 'pending', ?, ?, ?, ?, NULL)",
        (jid, command, attempts, max_retries, now, now),
    )
    conn.commit()
    conn.close()
    print(f"Enqueued {jid}")

def _claim_one_job(worker_name):
    """
    Atomically claim one pending job whose next_run <= now (or next_run IS NULL)
    Returns row dict or None.
    """
    conn = get_conn()
    cur = conn.cursor()
    now_ts = int(time.time())
    try:
        # Use a transaction to atomically pick and mark processing.
        cur.execute("BEGIN IMMEDIATE;")
        # subquery returns an id or NULL; the update will only affect that row
        cur.execute("""
            UPDATE jobs SET state='processing', worker=?, updated_at=?, last_error=NULL
            WHERE id = (
                SELECT id FROM jobs
                WHERE state='pending' AND (next_run IS NULL OR next_run <= ?)
                ORDER BY created_at LIMIT 1
            );
        """, (worker_name, now_iso(), now_ts))
        if cur.rowcount == 0:
            conn.commit()
            return None
        # get the job we just claimed
        cur.execute("SELECT * FROM jobs WHERE worker = ? AND state='processing' ORDER BY updated_at DESC LIMIT 1", (worker_name,))
        row = cur.fetchone()
        conn.commit()
        return row
    except sqlite3.OperationalError as e:
        conn.rollback()
        return None
    finally:
        conn.close()

def mark_job_completed(jid):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE jobs SET state='completed', updated_at=? WHERE id=?", (now_iso(), jid))
    conn.commit()
    conn.close()

def mark_job_failed(jid, last_error, attempts, max_retries, base):
    """
    attempts is already incremented.
    if attempts > max_retries -> dead
    else set next_run = now + base**attempts
    """
    conn = get_conn()
    cur = conn.cursor()
    if attempts > max_retries:
        cur.execute("UPDATE jobs SET state='dead', updated_at=?, last_error=? WHERE id=?", (now_iso(), last_error, jid))
        print(f"[{jid}] moved to DLQ (dead)")
    else:
        delay = int(base ** attempts)
        next_run_ts = int(time.time()) + delay
        cur.execute("UPDATE jobs SET state='pending', attempts=?, updated_at=?, next_run=?, last_error=? WHERE id=?",
                    (attempts, now_iso(), next_run_ts, last_error, jid))
        print(f"[{jid}] will retry in {delay}s (attempt {attempts}/{max_retries})")
    conn.commit()
    conn.close()

def list_jobs(state=None):
    conn = get_conn()
    cur = conn.cursor()
    if state:
        cur.execute("SELECT * FROM jobs WHERE state = ? ORDER BY created_at", (state,))
    else:
        cur.execute("SELECT * FROM jobs ORDER BY created_at")
    rows = cur.fetchall()
    conn.close()
    return rows

# -----------------------
# Config helpers
# -----------------------
def set_config(key, value):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO config(key, value) VALUES (?, ?)", (key, str(value)))
    conn.commit()
    conn.close()
    print(f"config {key} = {value}")

def get_config(key):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT value FROM config WHERE key=?", (key,))
    r = cur.fetchone()
    conn.close()
    return r["value"] if r else None

# -----------------------
# Worker process
# -----------------------
def worker_loop(name, stop_event):
    # read config
    base = float(get_config("backoff_base") or DEFAULT_BACKOFF_BASE)
    print(f"[worker {name}] started (backoff_base={base})")
    while not stop_event.is_set():
        row = _claim_one_job(name)
        if not row:
            time.sleep(POLL_INTERVAL)
            continue
        jid = row["id"]
        command = row["command"]
        attempts = int(row["attempts"])
        max_retries = int(row["max_retries"])
        # execute command (shell=True to support commands like echo, sleep)
        print(f"[{name}] executing job {jid}: {command!r}")
        try:
            proc = subprocess.run(command, shell=True)
            exit_code = proc.returncode
            if exit_code == 0:
                print(f"[{jid}] success")
                mark_job_completed(jid)
            else:
                attempts += 1
                err = f"exit_code:{exit_code}"
                mark_job_failed(jid, err, attempts, max_retries, base)
        except Exception as e:
            attempts += 1
            mark_job_failed(jid, str(e), attempts, max_retries, base)
    print(f"[worker {name}] shutting down gracefully")

# -----------------------
# Worker management (start/stop)
# -----------------------
def start_workers(count):
    # we will spawn processes that run this same script in a 'worker-runner' mode
    # and write their PIDs into PID_FILE for later stopping
    ps = []
    for i in range(count):
        p = Process(target=_spawned_worker_main, args=(i,))
        p.start()
        ps.append(p.pid)
        print(f"Started worker pid={p.pid}")
    # write pidfile
    Path(PID_FILE).write_text("\n".join(str(p) for p in ps))
    print(f"Written worker pids to {PID_FILE}")

def _spawned_worker_main(idx):
    # in child process
    stop_event = threading.Event()

    def _sigterm(_s, _f):
        stop_event.set()

    signal.signal(signal.SIGTERM, _sigterm)
    signal.signal(signal.SIGINT, _sigterm)
    worker_loop(f"w-{idx}-{os.getpid()}", stop_event)

def stop_workers():
    if not os.path.exists(PID_FILE):
        print("No pidfile found - no workers appear to be running.")
        return
    pids = [int(line.strip()) for line in Path(PID_FILE).read_text().splitlines() if line.strip()]
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
            print(f"Sent SIGTERM to {pid}")
        except ProcessLookupError:
            print(f"Process {pid} not found")
    # remove pidfile
    try:
        os.remove(PID_FILE)
    except FileNotFoundError:
        pass

# -----------------------
# Status / utilities
# -----------------------
def status():
    rows = list_jobs()
    counts = {}
    for r in rows:
        counts[r["state"]] = counts.get(r["state"], 0) + 1
    print("Job counts by state:")
    for k in ["pending", "processing", "completed", "failed", "dead"]:
        print(f"  {k}: {counts.get(k,0)}")
    # workers
    if os.path.exists(PID_FILE):
        pids = [line.strip() for line in Path(PID_FILE).read_text().splitlines() if line.strip()]
        print(f"Active workers (from pidfile {PID_FILE}): {len(pids)}")
    else:
        print("Active workers: 0")

# -----------------------
# DLQ operations (dead jobs)
# -----------------------
def dlq_list():
    rows = list_jobs(state="dead")
    if not rows:
        print("DLQ empty")
        return
    for r in rows:
        print(f"{r['id']} | command={r['command']} | attempts={r['attempts']} | updated_at={r['updated_at']} | last_error={r['last_error']}")

def dlq_retry(jid):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM jobs WHERE id=? AND state='dead'", (jid,))
    r = cur.fetchone()
    if not r:
        print("No such dead job:", jid)
        conn.close()
        return
    cur.execute("UPDATE jobs SET state='pending', attempts=0, updated_at=?, next_run=NULL, last_error=NULL WHERE id=?", (now_iso(), jid))
    conn.commit()
    conn.close()
    print(f"Retried {jid} -> pending")

# -----------------------
# CLI parsing
# -----------------------
def main():
    init_db()
    parser = argparse.ArgumentParser(prog="queuectl", description="QueueCTL - simple job queue CLI")
    sub = parser.add_subparsers(dest="cmd")

    # enqueue
    p = sub.add_parser("enqueue", help="enqueue job (pass JSON string)")
    p.add_argument("job_json", help='job as JSON string, e.g. \'{"id":"job1","command":"sleep 2"}\'')

    # worker start / stop
    wp = sub.add_parser("worker", help="worker management")
    wsub = wp.add_subparsers(dest="wcmd")
    ws = wsub.add_parser("start", help="start workers")
    ws.add_argument("--count", type=int, default=1)
    wstop = wsub.add_parser("stop", help="stop workers gracefully")

    # status
    sub.add_parser("status", help="show job counts and running workers")

    # list jobs
    lp = sub.add_parser("list", help="list jobs")
    lp.add_argument("--state", choices=["pending","processing","completed","dead","failed"], help="filter by state")

    # dlq
    dp = sub.add_parser("dlq", help="dead letter queue")
    dsub = dp.add_subparsers(dest="dcmd")
    dsub.add_parser("list", help="list DLQ")
    dretry = dsub.add_parser("retry", help="retry job from DLQ")
    dretry.add_argument("id")

    # config
    cp = sub.add_parser("config", help="config get/set")
    csub = cp.add_subparsers(dest="ccmd")
    cset = csub.add_parser("set", help="set config")
    cset.add_argument("key")
    cset.add_argument("value")
    cget = csub.add_parser("get", help="get config")
    cget.add_argument("key")

    args = parser.parse_args()

    if args.cmd == "enqueue":
        enqueue_job(args.job_json)
    elif args.cmd == "worker":
        if args.wcmd == "start":
            start_workers(args.count)
        elif args.wcmd == "stop":
            stop_workers()
        else:
            parser.print_help()
    elif args.cmd == "status":
        status()
    elif args.cmd == "list":
        rows = list_jobs(args.state)
        for r in rows:
            nr = r["next_run"]
            nr_human = datetime.utcfromtimestamp(nr).isoformat()+"Z" if nr else ""
            print(f"{r['id']} | {r['state']} | cmd={r['command']} | attempts={r['attempts']}/{r['max_retries']} | next_run={nr_human}")
    elif args.cmd == "dlq":
        if args.dcmd == "list":
            dlq_list()
        elif args.dcmd == "retry":
            dlq_retry(args.id)
        else:
            parser.print_help()
    elif args.cmd == "config":
        if args.ccmd == "set":
            set_config(args.key, args.value)
        elif args.ccmd == "get":
            v = get_config(args.key)
            print(v if v is not None else "(not set)")
        else:
            parser.print_help()
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
