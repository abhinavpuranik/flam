import argparse
import json
import signal
import sqlite3
import subprocess
import sys
import threading
import time 
import uuid
import os 
from datetime import datetime, timedelta 
from multiprocessing import Process
from pathlib import Path
DB_PATH  = os.environ.get("QUEUECTL_DB", "queuectl.db")
PID_FILE = os.environ.get("QUEUECTL_PIDFILE", "/tmp/queuectl_workers.pid")
DEFAULT_BACKOFF_BASE = 2
DEFAULT_MAX_RETRIES = 3
POLL_INTERVAL = 1.0

def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=10, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    return conn

def init_db():
    conn = get_conn()
    c = conn.cursor()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS jobs (
        id TEXT PRIMARY KEY,
        command TEXT NOT NULL,
        state TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        max_retries INTEGER NOT NULL DEFAULT ?,
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
    """, (DEFAULT_MAX_RETRIES,))

    cur = conn.cursor()
    
    
