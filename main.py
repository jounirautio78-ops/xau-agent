import json
import sqlite3
from datetime import datetime
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

app = FastAPI(title="Paul Railway Backend v3")

DB_PATH = "paul_railway_v3.db"

DEFAULT_BIAS = {
    "h4": "bearish",
    "h1": "bearish",
    "daily_bias": "strong_bearish",
    "direction": "sell_only"
}


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

    cur.execute("""
    CREATE TABLE IF NOT EXISTS paul_bias (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        h4 TEXT,
        h1 TEXT,
        daily_bias TEXT,
        direction TEXT,
        updated_at TEXT
    )
    """)

    cur.execute("""
        INSERT INTO paul_bias (id, h4, h1, daily_bias, direction, updated_at)
        VALUES (1, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO NOTHING
    """, (
        DEFAULT_BIAS["h4"],
        DEFAULT_BIAS["h1"],
        DEFAULT_BIAS["daily_bias"],
        DEFAULT_BIAS["direction"],
        now_iso(),
    ))

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


def get_bias_row():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT h4, h1, daily_bias, direction, updated_at FROM paul_bias WHERE id = 1")
    row = cur.fetchone()
    conn.close()
    if not row:
        return dict(DEFAULT_BIAS)
    return dict(row)


@app.get("/")
def root():
    return {"ok": True, "service": "paul_railway_backend_v3"}


@app.get("/paul_bias")
def paul_bias():
    row = get_bias_row()
    return {"ok": True, "bias": row}


@app.post("/set_paul_bias")
async def set_paul_bias(req: Request):
    try:
        data = await req.json()
    except Exception:
        return JSONResponse({"ok": False, "reason": "invalid_json"}, status_code=400)

    h4 = str(data.get("h4", "")).strip().lower()
    h1 = str(data.get("h1", "")).strip().lower()
    daily_bias = str(data.get("daily_bias", "")).strip().lower()
    direction = str(data.get("direction", "")).strip().lower()

    if h4 not in {"bullish", "bearish"}:
        return JSONResponse({"ok": False, "reason": "bad_h4"}, status_code=400)
    if h1 not in {"bullish", "bearish"}:
        return JSONResponse({"ok": False, "reason": "bad_h1"}, status_code=400)
    if daily_bias not in {"bullish", "bearish", "strong_bullish", "strong_bearish"}:
        return JSONResponse({"ok": False, "reason": "bad_daily_bias"}, status_code=400)
    if direction not in {"buy_only", "sell_only", "both"}:
        return JSONResponse({"ok": False, "reason": "bad_direction"}, status_code=400)

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO paul_bias (id, h4, h1, daily_bias, direction, updated_at)
        VALUES (1, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            h4 = excluded.h4,
            h1 = excluded.h1,
            daily_bias = excluded.daily_bias,
            direction = excluded.direction,
            updated_at = excluded.updated_at
    """, (h4, h1, daily_bias, direction, now_iso()))
    conn.commit()
    conn.close()

    return {"ok": True, "bias": get_bias_row()}


@app.post("/webhook")
async def webhook(req: Request):
    try:
        data = await req.json()
    except Exception:
        return JSONResponse({"ok": False, "reason": "invalid_json"}, status_code=400)

    message_type = str(data.get("message_type", "")).strip()

    if message_type == "bias_update":
        h4 = str(data.get("h4", "")).strip().lower()
        h1 = str(data.get("h1", "")).strip().lower()
        daily_bias = str(data.get("daily_bias", "")).strip().lower()
        direction = str(data.get("direction", "")).strip().lower()

        if h4 not in {"bullish", "bearish"} or h1 not in {"bullish", "bearish"} or daily_bias not in {"bullish", "bearish", "strong_bullish", "strong_bearish"} or direction not in {"buy_only", "sell_only", "both"}:
            log_webhook(message_type, data, False, "bad_bias_payload")
            return {"ok": True, "updated": False, "reason": "bad_bias_payload"}

        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO paul_bias (id, h4, h1, daily_bias, direction, updated_at)
            VALUES (1, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                h4 = excluded.h4,
                h1 = excluded.h1,
                daily_bias = excluded.daily_bias,
                direction = excluded.direction,
                updated_at = excluded.updated_at
        """, (h4, h1, daily_bias, direction, now_iso()))
        conn.commit()
        conn.close()

        log_webhook(message_type, data, True, "bias_updated")
        return {"ok": True, "updated": True, "bias": get_bias_row()}

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
        "bias": get_bias_row(),
    }


init_db()
