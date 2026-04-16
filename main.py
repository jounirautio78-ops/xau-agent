import json
import sqlite3
from datetime import datetime
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI(title="Paul Railway Backend v2")

DB_PATH = "paul_railway.db"


def now_iso():
    return datetime.now().isoformat()


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS planner_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        zone_id TEXT,
        direction TEXT,
        grade TEXT,
        entry_low REAL,
        entry_high REAL,
        invalidation REAL,
        payload_json TEXT,
        status TEXT DEFAULT 'pending',
        created_at TEXT,
        processed_at TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS webhook_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        message_type TEXT,
        payload_json TEXT,
        accepted INTEGER,
        reason TEXT,
        created_at TEXT
    )
    """)

    conn.commit()
    conn.close()


def log_webhook(message_type: str, payload: dict, accepted: bool, reason: str = ""):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO webhook_log (
            message_type, payload_json, accepted, reason, created_at
        ) VALUES (?, ?, ?, ?, ?)
    """, (
        message_type,
        json.dumps(payload, ensure_ascii=False),
        1 if accepted else 0,
        reason,
        now_iso(),
    ))
    conn.commit()
    conn.close()


@app.get("/")
def root():
    return {"ok": True, "service": "paul_railway_backend_v2"}


@app.post("/webhook")
async def webhook(req: Request):
    try:
        data = await req.json()
    except Exception:
        return JSONResponse({"ok": False, "reason": "invalid_json"}, status_code=400)

    message_type = str(data.get("message_type", "")).strip()

    # We only care about Paul here
    if message_type != "new_zone":
        log_webhook(message_type, data, False, "ignored_message_type")
        return {"ok": True, "zones": 0, "daily_plan_sent": False, "reason": "ignored_message_type"}

    zone_id = str(data.get("zone_id", "")).strip()
    direction = str(data.get("direction", "")).strip().lower()
    grade = str(data.get("grade", "")).strip().upper()

    try:
        entry_low = float(data.get("entry_low"))
        entry_high = float(data.get("entry_high"))
        invalidation = float(data.get("invalidation"))
    except Exception:
        log_webhook(message_type, data, False, "bad_price_fields")
        return {"ok": True, "zones": 0, "daily_plan_sent": False, "reason": "bad_price_fields"}

    if not zone_id:
        log_webhook(message_type, data, False, "missing_zone_id")
        return {"ok": True, "zones": 0, "daily_plan_sent": False, "reason": "missing_zone_id"}

    if direction not in ("buy", "sell"):
        log_webhook(message_type, data, False, "bad_direction")
        return {"ok": True, "zones": 0, "daily_plan_sent": False, "reason": "bad_direction"}

    if grade not in ("SNIPER", "OK", "RISKY"):
        log_webhook(message_type, data, False, "bad_grade")
        return {"ok": True, "zones": 0, "daily_plan_sent": False, "reason": "bad_grade"}

    conn = get_conn()
    cur = conn.cursor()

    # Avoid duplicates
    cur.execute("""
        SELECT id
        FROM planner_queue
        WHERE zone_id = ?
          AND status IN ('pending', 'processed')
        ORDER BY id DESC
        LIMIT 1
    """, (zone_id,))
    existing = cur.fetchone()

    if existing:
        conn.close()
        log_webhook(message_type, data, False, "duplicate_zone_id")
        return {"ok": True, "zones": 0, "daily_plan_sent": False, "reason": "duplicate_zone_id"}

    cur.execute("""
        INSERT INTO planner_queue (
            zone_id, direction, grade, entry_low, entry_high, invalidation,
            payload_json, status, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)
    """, (
        zone_id,
        direction,
        grade,
        entry_low,
        entry_high,
        invalidation,
        json.dumps(data, ensure_ascii=False),
        now_iso(),
    ))

    conn.commit()
    conn.close()

    log_webhook(message_type, data, True, "queued")
    return {"ok": True, "zones": 1, "daily_plan_sent": True, "zone_id": zone_id}


@app.get("/next_planner_signal")
def next_planner_signal():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT *
        FROM planner_queue
        WHERE status = 'pending'
        ORDER BY id ASC
        LIMIT 1
    """)
    row = cur.fetchone()
    conn.close()

    if not row:
        return {"status": "empty"}

    signal = {
        "id": row["id"],
        "zone_id": row["zone_id"],
        "direction": row["direction"],
        "grade": row["grade"],
        "entry_low": row["entry_low"],
        "entry_high": row["entry_high"],
        "invalidation": row["invalidation"],
    }

    return {"status": "ok", "signal": signal}


@app.post("/ack_planner_signal/{signal_id}")
def ack_planner_signal(signal_id: int):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        UPDATE planner_queue
        SET status = 'processed',
            processed_at = ?
        WHERE id = ?
    """, (now_iso(), signal_id))

    conn.commit()
    conn.close()

    return {"status": "processed", "signal_id": signal_id}


@app.get("/report")
def report():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) AS cnt FROM planner_queue")
    total = cur.fetchone()["cnt"]

    cur.execute("""
        SELECT status, COUNT(*) AS cnt
        FROM planner_queue
        GROUP BY status
        ORDER BY status
    """)
    by_status = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT zone_id, direction, grade, status, created_at, processed_at
        FROM planner_queue
        ORDER BY id DESC
        LIMIT 50
    """)
    latest = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT message_type, accepted, reason, created_at
        FROM webhook_log
        ORDER BY id DESC
        LIMIT 50
    """)
    webhook_log = [dict(r) for r in cur.fetchall()]

    conn.close()

    return {
        "ok": True,
        "db_path": DB_PATH,
        "planner_total": total,
        "by_status": by_status,
        "latest_signals": latest,
        "webhook_log": webhook_log,
    }


init_db()
