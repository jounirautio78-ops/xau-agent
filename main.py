from fastapi import FastAPI, Request
import requests
import os
import sqlite3
import json
from datetime import datetime
from zoneinfo import ZoneInfo

app = FastAPI()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

TZ = ZoneInfo("Europe/Madrid")
DB_PATH = os.getenv("TRADE_LOG_DB", "trade_logger.db")

state = {
    "date": None,
    "symbol": "XAUUSD",
    "bias": {
        "h4": "bearish",
        "h1": "bearish",
        "daily_bias": "strong_bearish",
    },
    "zones": [],
    "daily_plan_sent": False,
}


# =========================
# Helpers
# =========================
def clean(value, fallback="N/A"):
    if value is None:
        return fallback
    text = str(value).strip()
    if text == "" or text.lower() == "na":
        return fallback
    return text


def to_float(value):
    try:
        return float(value)
    except Exception:
        return None


def fmt_price(value):
    try:
        return str(int(round(float(value))))
    except Exception:
        return "N/A"


def fmt_range(low, high):
    return f"{fmt_price(low)} - {fmt_price(high)}"


def now_iso():
    return datetime.now(TZ).isoformat()


def send_telegram_message(text: str):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram skipped: BOT_TOKEN or CHAT_ID missing")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
    }
    response = requests.post(url, json=payload, timeout=15)
    response.raise_for_status()


# =========================
# SQLite helpers
# =========================
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS zones (
        zone_id TEXT PRIMARY KEY,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        plan_date TEXT,
        symbol TEXT,
        direction TEXT,
        grade TEXT,
        tier INTEGER,
        tier_label TEXT,
        setup_name TEXT,
        entry_low REAL,
        entry_high REAL,
        entry_mid REAL,
        sl_distance INTEGER,
        sl_price REAL,
        tp1 REAL,
        tp2 REAL,
        tp3 REAL,
        tp4 REAL,
        tp5 REAL,
        suggested_entries INTEGER,
        score REAL,
        countertrend INTEGER DEFAULT 0,
        invalidation_rule TEXT,
        status TEXT,
        activation_time TEXT,
        cancel_time TEXT,
        cancel_reason TEXT,
        notes TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_time TEXT NOT NULL,
        zone_id TEXT,
        message_type TEXT,
        symbol TEXT,
        direction TEXT,
        status_after TEXT,
        payload_json TEXT,
        FOREIGN KEY(zone_id) REFERENCES zones(zone_id)
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS planner_execution_signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL,
        zone_id TEXT NOT NULL,
        action TEXT NOT NULL,
        symbol TEXT,
        direction TEXT,
        tier INTEGER,
        tier_label TEXT,
        entry_low REAL,
        entry_high REAL,
        sl_price REAL,
        tp1 REAL,
        tp2 REAL,
        tp3 REAL,
        tp4 REAL,
        tp5 REAL,
        suggested_entries INTEGER,
        status TEXT NOT NULL DEFAULT 'pending',
        payload_json TEXT
    )
    """)

    # Map v2 tables
    cur.execute("""
    CREATE TABLE IF NOT EXISTS planner_bias_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        daily_score REAL,
        h4_score REAL,
        h1_score REAL,
        composite_score REAL,
        composite_label TEXT,
        payload_json TEXT,
        created_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS planner_candidates (
        candidate_id TEXT PRIMARY KEY,
        direction TEXT NOT NULL,
        candidate_type TEXT NOT NULL,
        timeframe_origin TEXT NOT NULL,
        entry_low REAL NOT NULL,
        entry_high REAL NOT NULL,
        invalidation REAL NOT NULL,
        target_1 REAL,
        target_2 REAL,
        freshness TEXT,
        status TEXT NOT NULL,
        score REAL,
        tier TEXT,
        bias_alignment TEXT,
        score_breakdown_json TEXT,
        notes TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS planner_candidate_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        candidate_id TEXT NOT NULL,
        event_type TEXT NOT NULL,
        old_status TEXT,
        new_status TEXT,
        reason TEXT,
        payload_json TEXT,
        created_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS planner_map_state (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        best_sell_1_id TEXT,
        best_buy_1_id TEXT,
        updated_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS planner_execution_queue_v2 (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        candidate_id TEXT NOT NULL,
        action TEXT NOT NULL,
        symbol TEXT,
        direction TEXT,
        entry_low REAL,
        entry_high REAL,
        invalidation REAL,
        target_1 REAL,
        target_2 REAL,
        tier TEXT,
        score REAL,
        status TEXT NOT NULL DEFAULT 'pending',
        payload_json TEXT,
        created_at TEXT NOT NULL,
        processed_at TEXT
    )
    """)

    conn.commit()
    conn.close()


def log_event(zone_id, message_type, symbol, direction, status_after, payload_json):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO events (
            event_time, zone_id, message_type, symbol, direction, status_after, payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        now_iso(),
        zone_id,
        message_type,
        symbol,
        direction,
        status_after,
        payload_json,
    ))
    conn.commit()
    conn.close()


def enqueue_planner_execution_signal(
    zone_id,
    action,
    symbol,
    direction,
    tier,
    tier_label,
    entry_low,
    entry_high,
    sl_price,
    tp1,
    tp2,
    tp3,
    tp4,
    tp5,
    suggested_entries,
    payload_json
):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO planner_execution_signals (
            created_at, zone_id, action, symbol, direction, tier, tier_label,
            entry_low, entry_high, sl_price, tp1, tp2, tp3, tp4, tp5,
            suggested_entries, status, payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
    """, (
        now_iso(),
        zone_id,
        action,
        symbol,
        direction,
        tier,
        tier_label,
        entry_low,
        entry_high,
        sl_price,
        tp1,
        tp2,
        tp3,
        tp4,
        tp5,
        suggested_entries,
        payload_json,
    ))
    conn.commit()
    conn.close()


def upsert_zone(zone: dict):
    conn = get_conn()
    cur = conn.cursor()

    entry_mid = None
    if zone.get("entry_low") is not None and zone.get("entry_high") is not None:
        entry_mid = (zone["entry_low"] + zone["entry_high"]) / 2.0

    cur.execute("""
        INSERT INTO zones (
            zone_id, created_at, updated_at, plan_date, symbol, direction, grade, tier, tier_label,
            setup_name, entry_low, entry_high, entry_mid, sl_distance, sl_price,
            tp1, tp2, tp3, tp4, tp5, suggested_entries, score, countertrend,
            invalidation_rule, status, activation_time, cancel_time, cancel_reason, notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(zone_id) DO UPDATE SET
            updated_at=excluded.updated_at,
            plan_date=excluded.plan_date,
            symbol=excluded.symbol,
            direction=excluded.direction,
            grade=excluded.grade,
            tier=excluded.tier,
            tier_label=excluded.tier_label,
            setup_name=excluded.setup_name,
            entry_low=excluded.entry_low,
            entry_high=excluded.entry_high,
            entry_mid=excluded.entry_mid,
            sl_distance=excluded.sl_distance,
            sl_price=excluded.sl_price,
            tp1=excluded.tp1,
            tp2=excluded.tp2,
            tp3=excluded.tp3,
            tp4=excluded.tp4,
            tp5=excluded.tp5,
            suggested_entries=excluded.suggested_entries,
            score=excluded.score,
            countertrend=excluded.countertrend,
            invalidation_rule=excluded.invalidation_rule,
            status=excluded.status,
            activation_time=excluded.activation_time,
            cancel_time=excluded.cancel_time,
            cancel_reason=excluded.cancel_reason,
            notes=excluded.notes
    """, (
        zone["zone_id"],
        zone.get("created_at", now_iso()),
        now_iso(),
        zone.get("plan_date"),
        zone.get("symbol", state["symbol"]),
        zone.get("direction"),
        zone.get("grade"),
        zone.get("tier"),
        zone.get("tier_label"),
        zone.get("setup_name"),
        zone.get("entry_low"),
        zone.get("entry_high"),
        entry_mid,
        zone.get("sl_distance"),
        zone.get("sl_price"),
        zone.get("tp1"),
        zone.get("tp2"),
        zone.get("tp3"),
        zone.get("tp4"),
        zone.get("tp5"),
        zone.get("suggested_entries"),
        zone.get("score"),
        1 if zone.get("countertrend", False) else 0,
        zone.get("invalidation_rule"),
        zone.get("status"),
        zone.get("activation_time"),
        zone.get("cancel_time"),
        zone.get("cancel_reason"),
        zone.get("notes"),
    ))

    conn.commit()
    conn.close()


def update_zone_status(zone_id, new_status, cancel_reason=None):
    conn = get_conn()
    cur = conn.cursor()

    if new_status == "active":
        cur.execute("""
            UPDATE zones
            SET status = ?, updated_at = ?, activation_time = COALESCE(activation_time, ?)
            WHERE zone_id = ?
        """, (new_status, now_iso(), now_iso(), zone_id))
    elif new_status == "cancelled":
        cur.execute("""
            UPDATE zones
            SET status = ?, updated_at = ?, cancel_time = ?, cancel_reason = ?
            WHERE zone_id = ?
        """, (new_status, now_iso(), now_iso(), cancel_reason, zone_id))
    else:
        cur.execute("""
            UPDATE zones
            SET status = ?, updated_at = ?
            WHERE zone_id = ?
        """, (new_status, now_iso(), zone_id))

    conn.commit()
    conn.close()


def get_zone_by_id(zone_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM zones WHERE zone_id = ?", (zone_id,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


def get_best_zone_from_db(direction: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM zones
        WHERE direction = ? AND status IN ('planned', 'active')
        ORDER BY tier ASC, score DESC, updated_at DESC
        LIMIT 1
    """, (direction,))
    row = cur.fetchone()
    conn.close()
    return dict(row) if row else None


# =========================
# Planner logic (existing)
# =========================
def reset_day_if_needed():
    today = datetime.now(TZ).date().isoformat()
    if state["date"] != today:
        state["date"] = today
        state["zones"] = []
        state["daily_plan_sent"] = False


def normalize_sl_distance(structural_distance: float):
    for allowed in (10, 15, 20):
        if structural_distance <= allowed:
            return allowed
    return None


def map_grade_to_tier(grade: str):
    grade = (grade or "").upper()
    if grade == "SNIPER":
        return 1, "Tier 1", 5
    if grade == "OK":
        return 2, "Tier 2", 4
    if grade == "RISKY":
        return 3, "Tier 3", 3
    return None, None, None


def enrich_zone(data: dict):
    direction = clean(data.get("direction")).lower()
    grade = clean(data.get("grade"))
    tier, tier_label, suggested_entries = map_grade_to_tier(grade)
    if tier is None:
        return None

    entry_low = to_float(data.get("entry_low"))
    entry_high = to_float(data.get("entry_high"))
    invalidation = to_float(data.get("invalidation"))

    if entry_low is None or entry_high is None or invalidation is None:
        return None

    entry_mid = (entry_low + entry_high) / 2.0
    structural_distance = abs(invalidation - entry_mid)
    sl_distance = normalize_sl_distance(structural_distance)
    if sl_distance is None:
        return None

    if direction == "sell":
        sl_price = entry_mid + sl_distance
        tp1 = entry_mid - sl_distance * 1.0
        tp2 = entry_mid - sl_distance * 1.5
        tp3 = entry_mid - sl_distance * 2.0
        tp4 = entry_mid - sl_distance * 2.5
        tp5 = entry_mid - sl_distance * 3.0
    elif direction == "buy":
        sl_price = entry_mid - sl_distance
        tp1 = entry_mid + sl_distance * 1.0
        tp2 = entry_mid + sl_distance * 1.5
        tp3 = entry_mid + sl_distance * 2.0
        tp4 = entry_mid + sl_distance * 2.5
        tp5 = entry_mid + sl_distance * 3.0
    else:
        return None

    score_map = {"SNIPER": 8.5, "OK": 7.0, "RISKY": 5.5}
    score = score_map.get(grade.upper(), 5.0)

    zone = {
        "zone_id": clean(data.get("zone_id", f"{direction}_{len(state['zones']) + 1}_{state['date']}")),
        "created_at": now_iso(),
        "plan_date": state["date"],
        "symbol": state["symbol"],
        "direction": direction,
        "grade": grade.upper(),
        "tier": tier,
        "tier_label": tier_label,
        "setup_name": clean(data.get("setup_name", f"{direction.upper()} zone")),
        "entry_low": round(entry_low, 3),
        "entry_high": round(entry_high, 3),
        "sl_distance": sl_distance,
        "sl_price": round(sl_price, 3),
        "tp1": round(tp1, 3),
        "tp2": round(tp2, 3),
        "tp3": round(tp3, 3),
        "tp4": round(tp4, 3),
        "tp5": round(tp5, 3),
        "suggested_entries": suggested_entries,
        "score": score,
        "countertrend": False,
        "status": "planned",
        "invalidation_rule": f"H1 invalidation at {fmt_price(invalidation)}",
        "activation_time": None,
        "cancel_time": None,
        "cancel_reason": None,
        "notes": None,
    }
    return zone


def rank_and_select_zones():
    sells = [z for z in state["zones"] if z["direction"] == "sell" and z["status"] in ("planned", "active")]
    buys = [z for z in state["zones"] if z["direction"] == "buy" and z["status"] in ("planned", "active")]

    sells.sort(key=lambda z: (z["tier"], -z["score"]))
    buys.sort(key=lambda z: (z["tier"], -z["score"]))

    return sells[:2], buys[:2]


def format_zone(zone):
    return (
        f"{zone['direction'].capitalize()} Zone — {zone['tier_label']}\n"
        f"Entry: {fmt_range(zone['entry_low'], zone['entry_high'])}\n"
        f"SL: {zone['sl_distance']} ({fmt_price(zone['sl_price'])})\n"
        f"TP1: {fmt_price(zone['tp1'])}\n"
        f"TP2: {fmt_price(zone['tp2'])}\n"
        f"TP3: {fmt_price(zone['tp3'])}\n"
        f"TP4: {fmt_price(zone['tp4'])}\n"
        f"TP5: {fmt_price(zone['tp5'])}\n"
        f"Suggested entries: {zone['suggested_entries']}"
    )


def send_daily_plan_if_needed():
    now = datetime.now(TZ)
    if state["daily_plan_sent"]:
        return
    if now.hour < 7:
        return

    sells, buys = rank_and_select_zones()

    if not sells and not buys:
        return

    parts = [
        "Gold Daily Plan",
        f"Date: {state['date']}",
        f"Bias: {state['bias']['daily_bias'].replace('_', ' ').title()}",
        "",
    ]

    for zone in sells:
        parts.append(format_zone(zone))
        parts.append("")

    for zone in buys:
        parts.append(format_zone(zone))
        parts.append("")

    send_telegram_message("\n".join(parts).strip())
    state["daily_plan_sent"] = True


# =========================
# Planner Map v2 helpers
# =========================
def tier_from_score(score: float):
    if score >= 13:
        return "A"
    if score >= 10:
        return "B"
    if score >= 7:
        return "C"
    return "WEAK"


def bias_label_from_score(score: float):
    if score >= 1.25:
        return "strong_bullish"
    if score >= 0.4:
        return "bullish"
    if score <= -1.25:
        return "strong_bearish"
    if score <= -0.4:
        return "bearish"
    return "neutral"


def clamp(value, low, high):
    return max(low, min(high, value))


def approx_equal(a, b, tol=3.0):
    if a is None or b is None:
        return False
    return abs(float(a) - float(b)) <= tol


def get_or_create_map_state():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM planner_map_state WHERE id = 1")
    row = cur.fetchone()

    if not row:
        cur.execute("""
            INSERT INTO planner_map_state (id, best_sell_1_id, best_buy_1_id, updated_at)
            VALUES (1, NULL, NULL, ?)
        """, (now_iso(),))
        conn.commit()
        cur.execute("SELECT * FROM planner_map_state WHERE id = 1")
        row = cur.fetchone()

    conn.close()
    return dict(row)


def save_map_state(best_sell_1_id, best_buy_1_id):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO planner_map_state (id, best_sell_1_id, best_buy_1_id, updated_at)
        VALUES (1, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            best_sell_1_id=excluded.best_sell_1_id,
            best_buy_1_id=excluded.best_buy_1_id,
            updated_at=excluded.updated_at
    """, (best_sell_1_id, best_buy_1_id, now_iso()))
    conn.commit()
    conn.close()


def save_bias_snapshot(snapshot: dict):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO planner_bias_snapshots (
            daily_score, h4_score, h1_score, composite_score,
            composite_label, payload_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        snapshot["daily_score"],
        snapshot["h4_score"],
        snapshot["h1_score"],
        snapshot["composite_score"],
        snapshot["composite_label"],
        json.dumps(snapshot),
        now_iso(),
    ))
    conn.commit()
    conn.close()


def compute_bias_snapshot():
    daily_map = {
        "strong_bearish": -2,
        "bearish": -1,
        "neutral": 0,
        "bullish": 1,
        "strong_bullish": 2,
    }
    simple_map = {
        "bearish": -1,
        "neutral": 0,
        "bullish": 1,
    }

    daily_score = daily_map.get(clean(state["bias"].get("daily_bias"), "neutral").lower(), 0)
    h4_score = simple_map.get(clean(state["bias"].get("h4"), "neutral").lower(), 0)
    h1_score = simple_map.get(clean(state["bias"].get("h1"), "neutral").lower(), 0)

    composite_score = round(daily_score * 0.4 + h4_score * 0.35 + h1_score * 0.25, 2)
    composite_label = bias_label_from_score(composite_score)

    return {
        "daily_score": daily_score,
        "h4_score": h4_score,
        "h1_score": h1_score,
        "composite_score": composite_score,
        "composite_label": composite_label,
        "updated_at": now_iso(),
    }


def candidate_exists(candidate_id: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM planner_candidates WHERE candidate_id = ?", (candidate_id,))
    row = cur.fetchone()
    conn.close()
    return row is not None


def get_candidate(candidate_id: str):
    if not candidate_id:
        return None
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT * FROM planner_candidates WHERE candidate_id = ?", (candidate_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    item = dict(row)
    try:
        item["score_breakdown"] = json.loads(item.get("score_breakdown_json") or "{}")
    except Exception:
        item["score_breakdown"] = {}
    return item


def get_latest_candidate(direction: str, candidate_type: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM planner_candidates
        WHERE direction = ? AND candidate_type = ?
        ORDER BY updated_at DESC
        LIMIT 1
    """, (direction, candidate_type))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    item = dict(row)
    try:
        item["score_breakdown"] = json.loads(item.get("score_breakdown_json") or "{}")
    except Exception:
        item["score_breakdown"] = {}
    return item


def upsert_candidate(candidate: dict):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO planner_candidates (
            candidate_id, direction, candidate_type, timeframe_origin,
            entry_low, entry_high, invalidation, target_1, target_2,
            freshness, status, score, tier, bias_alignment,
            score_breakdown_json, notes, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(candidate_id) DO UPDATE SET
            direction=excluded.direction,
            candidate_type=excluded.candidate_type,
            timeframe_origin=excluded.timeframe_origin,
            entry_low=excluded.entry_low,
            entry_high=excluded.entry_high,
            invalidation=excluded.invalidation,
            target_1=excluded.target_1,
            target_2=excluded.target_2,
            freshness=excluded.freshness,
            status=excluded.status,
            score=excluded.score,
            tier=excluded.tier,
            bias_alignment=excluded.bias_alignment,
            score_breakdown_json=excluded.score_breakdown_json,
            notes=excluded.notes,
            updated_at=excluded.updated_at
    """, (
        candidate["candidate_id"],
        candidate["direction"],
        candidate["candidate_type"],
        candidate["timeframe_origin"],
        candidate["entry_low"],
        candidate["entry_high"],
        candidate["invalidation"],
        candidate.get("target_1"),
        candidate.get("target_2"),
        candidate.get("freshness", "fresh"),
        candidate.get("status", "watch"),
        candidate.get("score", 0),
        candidate.get("tier", "UNRATED"),
        candidate.get("bias_alignment", "neutral"),
        json.dumps(candidate.get("score_breakdown", {})),
        candidate.get("notes", ""),
        candidate.get("created_at", now_iso()),
        candidate.get("updated_at", now_iso()),
    ))
    conn.commit()
    conn.close()


def log_candidate_event(candidate_id, event_type, old_status, new_status, reason="", payload=None):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO planner_candidate_events (
            candidate_id, event_type, old_status, new_status, reason, payload_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        candidate_id,
        event_type,
        old_status,
        new_status,
        reason,
        json.dumps(payload or {}),
        now_iso(),
    ))
    conn.commit()
    conn.close()


def enqueue_execution_delta(candidate: dict, action: str, reason: str = ""):
    payload = {
        "candidate_id": candidate["candidate_id"],
        "action": action,
        "symbol": state["symbol"],
        "direction": candidate["direction"],
        "entry_low": candidate["entry_low"],
        "entry_high": candidate["entry_high"],
        "invalidation": candidate["invalidation"],
        "target_1": candidate.get("target_1"),
        "target_2": candidate.get("target_2"),
        "tier": candidate.get("tier"),
        "score": candidate.get("score"),
        "candidate_type": candidate.get("candidate_type"),
        "reason": reason,
    }

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO planner_execution_queue_v2 (
            candidate_id, action, symbol, direction, entry_low, entry_high,
            invalidation, target_1, target_2, tier, score, status,
            payload_json, created_at, processed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, NULL)
    """, (
        candidate["candidate_id"],
        action,
        state["symbol"],
        candidate["direction"],
        candidate["entry_low"],
        candidate["entry_high"],
        candidate["invalidation"],
        candidate.get("target_1"),
        candidate.get("target_2"),
        candidate.get("tier"),
        candidate.get("score"),
        json.dumps(payload),
        now_iso(),
    ))
    conn.commit()
    conn.close()
    return payload


def score_candidate(candidate: dict, bias_snapshot: dict):
    composite = bias_snapshot["composite_score"]
    direction = candidate["direction"]

    if direction == "sell":
        bias_alignment = 3 if composite <= -1.25 else 2 if composite <= -0.4 else 1 if composite < 0.4 else 0
    else:
        bias_alignment = 3 if composite >= 1.25 else 2 if composite >= 0.4 else 1 if composite > -0.4 else 0

    width = max(0.0, float(candidate["entry_high"]) - float(candidate["entry_low"]))
    structure_quality = 3 if width <= 8 else 2 if width <= 15 else 1

    zone_mid = (float(candidate["entry_low"]) + float(candidate["entry_high"])) / 2.0
    tp1 = float(candidate["target_1"]) if candidate.get("target_1") is not None else zone_mid
    sl = float(candidate["invalidation"])
    reward = abs(zone_mid - tp1)
    risk = abs(sl - zone_mid)
    rr = reward / risk if risk > 0 else 0

    location_quality = 3 if rr >= 1.8 else 2 if rr >= 1.2 else 1 if rr >= 0.8 else 0
    freshness = 2 if candidate.get("freshness") == "fresh" else 1 if candidate.get("freshness") == "tested_once" else 0
    liquidity_context = 1
    distance_quality = 2

    total_score = float(bias_alignment + structure_quality + location_quality + freshness + liquidity_context + distance_quality)

    candidate["score_breakdown"] = {
        "bias_alignment": bias_alignment,
        "structure_quality": structure_quality,
        "location_quality": location_quality,
        "freshness": freshness,
        "liquidity_context": liquidity_context,
        "distance_quality": distance_quality,
    }
    candidate["score"] = total_score
    candidate["tier"] = tier_from_score(total_score)

    if bias_alignment >= 2:
        candidate["bias_alignment"] = "aligned"
    elif bias_alignment == 1:
        candidate["bias_alignment"] = "partially_aligned"
    else:
        candidate["bias_alignment"] = "counter_bias"

    return candidate


def is_candidate_executable(candidate: dict):
    return candidate.get("tier") in ("A", "B") and candidate.get("freshness") != "stale"


def same_candidate(a: dict | None, b: dict | None):
    if not a or not b:
        return False
    return (
        a.get("direction") == b.get("direction")
        and a.get("candidate_type") == b.get("candidate_type")
        and approx_equal(a.get("entry_low"), b.get("entry_low"), 3.0)
        and approx_equal(a.get("entry_high"), b.get("entry_high"), 3.0)
        and approx_equal(a.get("invalidation"), b.get("invalidation"), 3.0)
    )


def zone_to_continuation_candidate(zone: dict, direction: str):
    if not zone:
        return None

    created_at = zone.get("created_at") or now_iso()
    zone_id = zone["zone_id"]

    return {
        "candidate_id": f"{direction.upper()}_CONT_M15_{zone_id}",
        "direction": direction,
        "candidate_type": f"continuation_{direction}",
        "timeframe_origin": "M15",
        "entry_low": float(zone["entry_low"]),
        "entry_high": float(zone["entry_high"]),
        "invalidation": float(zone["sl_price"]),
        "target_1": float(zone["tp1"]) if zone.get("tp1") is not None else None,
        "target_2": float(zone["tp2"]) if zone.get("tp2") is not None else None,
        "freshness": "fresh" if zone.get("status") == "planned" else "tested_once",
        "status": "watch",
        "score": 0,
        "tier": "UNRATED",
        "bias_alignment": "unknown",
        "score_breakdown": {},
        "notes": f"Derived from zone {zone_id}",
        "created_at": created_at,
        "updated_at": now_iso(),
    }


def generate_continuation_sell(bias_snapshot: dict):
    zone = get_best_zone_from_db("sell")
    return zone_to_continuation_candidate(zone, "sell")


def generate_continuation_buy(bias_snapshot: dict):
    zone = get_best_zone_from_db("buy")
    return zone_to_continuation_candidate(zone, "buy")


def run_planner_map_cycle():
    bias = compute_bias_snapshot()
    save_bias_snapshot(bias)

    old_state = get_or_create_map_state()
    old_best_sell = get_candidate(old_state.get("best_sell_1_id"))
    old_best_buy = get_candidate(old_state.get("best_buy_1_id"))

    new_sell = generate_continuation_sell(bias)
    new_buy = generate_continuation_buy(bias)

    if new_sell:
        if candidate_exists(new_sell["candidate_id"]):
            existing = get_candidate(new_sell["candidate_id"])
            new_sell["created_at"] = existing["created_at"]
        new_sell = score_candidate(new_sell, bias)
        new_sell["status"] = "armed" if is_candidate_executable(new_sell) else "watch"
        upsert_candidate(new_sell)
        log_candidate_event(
            new_sell["candidate_id"],
            "candidate_upserted",
            old_best_sell["status"] if old_best_sell and old_best_sell.get("candidate_id") == new_sell["candidate_id"] else None,
            new_sell["status"],
            "planner_map_cycle",
            new_sell,
        )

    if new_buy:
        if candidate_exists(new_buy["candidate_id"]):
            existing = get_candidate(new_buy["candidate_id"])
            new_buy["created_at"] = existing["created_at"]
        new_buy = score_candidate(new_buy, bias)
        new_buy["status"] = "armed" if is_candidate_executable(new_buy) else "watch"
        upsert_candidate(new_buy)
        log_candidate_event(
            new_buy["candidate_id"],
            "candidate_upserted",
            old_best_buy["status"] if old_best_buy and old_best_buy.get("candidate_id") == new_buy["candidate_id"] else None,
            new_buy["status"],
            "planner_map_cycle",
            new_buy,
        )

    deltas = []

    # sell side
    if old_best_sell and (not new_sell or not same_candidate(old_best_sell, new_sell)):
        payload = enqueue_execution_delta(old_best_sell, "cancel_candidate", "replaced_or_missing")
        deltas.append(payload)
        log_candidate_event(
            old_best_sell["candidate_id"],
            "candidate_cancelled",
            old_best_sell.get("status"),
            "cancelled",
            "replaced_or_missing",
            old_best_sell,
        )

    if new_sell and is_candidate_executable(new_sell) and (not old_best_sell or not same_candidate(old_best_sell, new_sell)):
        payload = enqueue_execution_delta(new_sell, "arm_candidate", "new_or_replacement")
        deltas.append(payload)
        log_candidate_event(
            new_sell["candidate_id"],
            "candidate_armed",
            old_best_sell.get("status") if old_best_sell else None,
            "armed",
            "new_or_replacement",
            new_sell,
        )

    # buy side
    if old_best_buy and (not new_buy or not same_candidate(old_best_buy, new_buy)):
        payload = enqueue_execution_delta(old_best_buy, "cancel_candidate", "replaced_or_missing")
        deltas.append(payload)
        log_candidate_event(
            old_best_buy["candidate_id"],
            "candidate_cancelled",
            old_best_buy.get("status"),
            "cancelled",
            "replaced_or_missing",
            old_best_buy,
        )

    if new_buy and is_candidate_executable(new_buy) and (not old_best_buy or not same_candidate(old_best_buy, new_buy)):
        payload = enqueue_execution_delta(new_buy, "arm_candidate", "new_or_replacement")
        deltas.append(payload)
        log_candidate_event(
            new_buy["candidate_id"],
            "candidate_armed",
            old_best_buy.get("status") if old_best_buy else None,
            "armed",
            "new_or_replacement",
            new_buy,
        )

    save_map_state(
        new_sell["candidate_id"] if new_sell else None,
        new_buy["candidate_id"] if new_buy else None,
    )

    return {
        "ok": True,
        "bias": bias,
        "best_sell_1": new_sell,
        "best_buy_1": new_buy,
        "deltas": deltas,
        "updated_at": now_iso(),
    }


# =========================
# App lifecycle
# =========================
@app.on_event("startup")
def startup_event():
    init_db()
    reset_day_if_needed()


@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/health")
def health():
    return {
        "status": "ok",
        "date": state["date"],
        "zones_in_memory": len(state["zones"]),
        "daily_plan_sent": state["daily_plan_sent"],
        "db_path": DB_PATH,
    }


@app.post("/webhook")
async def webhook(request: Request):
    reset_day_if_needed()
    data = await request.json()
    message_type = clean(data.get("message_type", "")).lower()
    symbol = clean(data.get("symbol", state["symbol"]))
    direction = clean(data.get("direction", "")).lower()
    zone_id = clean(data.get("zone_id", ""))
    payload_str = json.dumps(data)

    if message_type == "new_zone":
        zone = enrich_zone(data)
        if zone:
            state["zones"].append(zone)
            upsert_zone(zone)
            log_event(
                zone_id=zone["zone_id"],
                message_type="new_zone",
                symbol=symbol,
                direction=zone["direction"],
                status_after="planned",
                payload_json=payload_str,
            )

            enqueue_planner_execution_signal(
                zone_id=zone["zone_id"],
                action="place_zone",
                symbol=zone["symbol"],
                direction=zone["direction"],
                tier=zone["tier"],
                tier_label=zone["tier_label"],
                entry_low=zone["entry_low"],
                entry_high=zone["entry_high"],
                sl_price=zone["sl_price"],
                tp1=zone["tp1"],
                tp2=zone["tp2"],
                tp3=zone["tp3"],
                tp4=zone["tp4"],
                tp5=zone["tp5"],
                suggested_entries=zone["suggested_entries"],
                payload_json=json.dumps(zone),
            )

            send_daily_plan_if_needed()

    elif message_type == "zone_active":
        matched_zone = None

        for zone in reversed(state["zones"]):
            if zone_id != "N/A":
                if zone["zone_id"] != zone_id:
                    continue
            else:
                if zone["direction"] != direction or zone["status"] != "planned":
                    continue

            if zone["status"] == "planned":
                zone["status"] = "active"
                zone["activation_time"] = now_iso()
                matched_zone = zone
                break

        if matched_zone:
            update_zone_status(matched_zone["zone_id"], "active")
            log_event(
                zone_id=matched_zone["zone_id"],
                message_type="zone_active",
                symbol=symbol,
                direction=matched_zone["direction"],
                status_after="active",
                payload_json=payload_str,
            )
            send_telegram_message(
                f"Gold Update\n"
                f"{matched_zone['direction'].capitalize()} Zone active\n\n"
                f"Entry: {fmt_range(matched_zone['entry_low'], matched_zone['entry_high'])}\n"
                f"SL: {matched_zone['sl_distance']} ({fmt_price(matched_zone['sl_price'])})\n"
                f"TP1: {fmt_price(matched_zone['tp1'])}\n"
                f"TP2: {fmt_price(matched_zone['tp2'])}\n"
                f"TP3: {fmt_price(matched_zone['tp3'])}\n"
                f"TP4: {fmt_price(matched_zone['tp4'])}\n"
                f"TP5: {fmt_price(matched_zone['tp5'])}\n"
                f"Tier: {matched_zone['tier_label']}"
            )

       elif message_type == "zone_cancel":
        matched_zone = None
        reason = clean(data.get("reason", "unknown"))

        for zone in reversed(state["zones"]):
            if zone_id != "N/A":
                if zone["zone_id"] != zone_id:
                    continue
            else:
                if zone["direction"] != direction or zone["status"] not in ("planned", "active"):
                    continue

            if zone["status"] in ("planned", "active"):
                zone["status"] = "cancelled"
                zone["cancel_time"] = now_iso()
                zone["cancel_reason"] = reason
                matched_zone = zone
                break

        if matched_zone:
            update_zone_status(matched_zone["zone_id"], "cancelled", reason)

            log_event(
                zone_id=matched_zone["zone_id"],
                message_type="zone_cancel",
                symbol=symbol,
                direction=matched_zone["direction"],
                status_after="cancelled",
                payload_json=payload_str,
            )

            # 🔴 1. CANCEL pending orders
            enqueue_planner_execution_signal(
                zone_id=matched_zone["zone_id"],
                action="cancel_zone",
                symbol=matched_zone["symbol"],
                direction=matched_zone["direction"],
                tier=matched_zone["tier"],
                tier_label=matched_zone["tier_label"],
                entry_low=matched_zone["entry_low"],
                entry_high=matched_zone["entry_high"],
                sl_price=matched_zone["sl_price"],
                tp1=matched_zone["tp1"],
                tp2=matched_zone["tp2"],
                tp3=matched_zone["tp3"],
                tp4=matched_zone["tp4"],
                tp5=matched_zone["tp5"],
                suggested_entries=matched_zone["suggested_entries"],
                payload_json=payload_str,
            )

            # 🔴 2. CLOSE open positions
            enqueue_planner_execution_signal(
                zone_id=matched_zone["zone_id"],
                action="close_zone",
                symbol=matched_zone["symbol"],
                direction=matched_zone["direction"],
                tier=matched_zone["tier"],
                tier_label=matched_zone["tier_label"],
                entry_low=matched_zone["entry_low"],
                entry_high=matched_zone["entry_high"],
                sl_price=matched_zone["sl_price"],
                tp1=matched_zone["tp1"],
                tp2=matched_zone["tp2"],
                tp3=matched_zone["tp3"],
                tp4=matched_zone["tp4"],
                tp5=matched_zone["tp5"],
                suggested_entries=matched_zone["suggested_entries"],
                payload_json=payload_str,
            )

            send_telegram_message(
                f"Cancel {matched_zone['direction'].capitalize()} Zone\n"
                f"Reason: {reason}"
            )
    


# =========================
# Existing planner execution endpoints
# =========================
@app.get("/next_planner_signal")
def next_planner_signal():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM planner_execution_signals
        WHERE status = 'pending'
        ORDER BY id ASC
        LIMIT 1
    """)
    row = cur.fetchone()
    conn.close()

    if not row:
        return {"status": "empty"}

    return {
        "status": "ok",
        "signal": dict(row)
    }


@app.post("/ack_planner_signal/{signal_id}")
def ack_planner_signal(signal_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE planner_execution_signals
        SET status = 'processed'
        WHERE id = ? AND status = 'pending'
    """, (signal_id,))
    conn.commit()
    updated = cur.rowcount
    conn.close()

    if updated == 0:
        return {"status": "not_found_or_already_processed"}

    return {"status": "processed", "signal_id": signal_id}


@app.get("/planner_execution_report")
def planner_execution_report():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) AS cnt FROM planner_execution_signals")
    total = cur.fetchone()["cnt"]

    cur.execute("SELECT COUNT(*) AS cnt FROM planner_execution_signals WHERE status = 'pending'")
    pending = cur.fetchone()["cnt"]

    cur.execute("SELECT COUNT(*) AS cnt FROM planner_execution_signals WHERE status = 'processed'")
    processed = cur.fetchone()["cnt"]

    cur.execute("""
        SELECT action, COUNT(*) AS cnt
        FROM planner_execution_signals
        GROUP BY action
    """)
    by_action = {row["action"]: row["cnt"] for row in cur.fetchall()}

    conn.close()

    return {
        "total_signals": total,
        "pending_signals": pending,
        "processed_signals": processed,
        "by_action": by_action,
    }


# =========================
# Planner Map v2 endpoints
# =========================
@app.post("/planner_map_cycle")
def planner_map_cycle():
    return run_planner_map_cycle()


@app.get("/planner_map_state")
def planner_map_state():
    state_row = get_or_create_map_state()
    return {
        "state": state_row,
        "best_sell_1": get_candidate(state_row.get("best_sell_1_id")),
        "best_buy_1": get_candidate(state_row.get("best_buy_1_id")),
    }


@app.get("/planner_map_report")
def planner_map_report():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) AS cnt FROM planner_bias_snapshots")
    bias_snapshots = cur.fetchone()["cnt"]

    cur.execute("SELECT COUNT(*) AS cnt FROM planner_candidates")
    candidates_total = cur.fetchone()["cnt"]

    cur.execute("""
        SELECT status, COUNT(*) AS cnt
        FROM planner_candidates
        GROUP BY status
    """)
    by_status = {row["status"]: row["cnt"] for row in cur.fetchall()}

    cur.execute("""
        SELECT direction, COUNT(*) AS cnt
        FROM planner_candidates
        GROUP BY direction
    """)
    by_direction = {row["direction"]: row["cnt"] for row in cur.fetchall()}

    cur.execute("""
        SELECT action, COUNT(*) AS cnt
        FROM planner_execution_queue_v2
        GROUP BY action
    """)
    queue_by_action = {row["action"]: row["cnt"] for row in cur.fetchall()}

    cur.execute("""
        SELECT status, COUNT(*) AS cnt
        FROM planner_execution_queue_v2
        GROUP BY status
    """)
    queue_by_status = {row["status"]: row["cnt"] for row in cur.fetchall()}

    conn.close()

    return {
        "bias_snapshots": bias_snapshots,
        "candidates_total": candidates_total,
        "candidates_by_status": by_status,
        "candidates_by_direction": by_direction,
        "queue_v2_by_action": queue_by_action,
        "queue_v2_by_status": queue_by_status,
    }


@app.get("/next_planner_signal_v2")
def next_planner_signal_v2():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM planner_execution_queue_v2
        WHERE status = 'pending'
        ORDER BY id ASC
        LIMIT 1
    """)
    row = cur.fetchone()
    conn.close()

    if not row:
        return {"status": "empty"}

    item = dict(row)
    try:
        item["payload"] = json.loads(item.get("payload_json") or "{}")
    except Exception:
        item["payload"] = {}

    return {"status": "ok", "signal": item}


@app.post("/ack_planner_signal_v2/{signal_id}")
def ack_planner_signal_v2(signal_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE planner_execution_queue_v2
        SET status = 'processed', processed_at = ?
        WHERE id = ? AND status = 'pending'
    """, (now_iso(), signal_id))
    conn.commit()
    updated = cur.rowcount
    conn.close()

    if updated == 0:
        return {"status": "not_found_or_already_processed"}

    return {"status": "processed", "signal_id": signal_id}


@app.get("/planner_candidates")
def planner_candidates():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM planner_candidates
        ORDER BY updated_at DESC
        LIMIT 50
    """)
    rows = cur.fetchall()
    conn.close()

    items = []
    for row in rows:
        item = dict(row)
        try:
            item["score_breakdown"] = json.loads(item.get("score_breakdown_json") or "{}")
        except Exception:
            item["score_breakdown"] = {}
        items.append(item)

    return {"items": items}


@app.get("/report")
def report():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) as cnt FROM zones")
    total_zones = cur.fetchone()["cnt"]

    cur.execute("SELECT COUNT(*) as cnt FROM zones WHERE status = 'active'")
    active_zones = cur.fetchone()["cnt"]

    cur.execute("SELECT COUNT(*) as cnt FROM zones WHERE status = 'cancelled'")
    cancelled_zones = cur.fetchone()["cnt"]

    cur.execute("""
        SELECT grade, COUNT(*) as cnt
        FROM zones
        GROUP BY grade
    """)
    grade_stats = {row["grade"]: row["cnt"] for row in cur.fetchall()}

    cur.execute("""
        SELECT direction, COUNT(*) as cnt
        FROM zones
        GROUP BY direction
    """)
    direction_stats = {row["direction"]: row["cnt"] for row in cur.fetchall()}

    conn.close()

    activation_rate = 0
    if total_zones > 0:
        activation_rate = round(active_zones / total_zones * 100, 2)

    return {
        "total_zones": total_zones,
        "active_zones": active_zones,
        "cancelled_zones": cancelled_zones,
        "activation_rate_percent": activation_rate,
        "by_grade": grade_stats,
        "by_direction": direction_stats,
    }
