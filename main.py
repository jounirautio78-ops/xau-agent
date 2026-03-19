from fastapi import FastAPI, Request
import requests
import os
import sqlite3
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
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
    }
    response = requests.post(url, json=payload, timeout=15)
    response.raise_for_status()


# =========================
# SQLite logger
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


# =========================
# Planner logic
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
    payload_str = str(data)

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
            send_telegram_message(
                f"Cancel {matched_zone['direction'].capitalize()} Zone\n"
                f"Reason: {reason}"
            )

    elif message_type == "debug_ping":
        # Ei lähetetä Telegramiin debug-spämmiä, mutta voidaan halutessa logata eventti.
        log_event(
            zone_id=None,
            message_type="debug_ping",
            symbol=symbol,
            direction=direction,
            status_after="debug",
            payload_json=payload_str,
        )

    return {
        "ok": True,
        "zones": len(state["zones"]),
        "daily_plan_sent": state["daily_plan_sent"],
    }
@app.get("/report")
def report():
    conn = get_conn()
    cur = conn.cursor()

    # Total zones
    cur.execute("SELECT COUNT(*) as cnt FROM zones")
    total_zones = cur.fetchone()["cnt"]

    # Active
    cur.execute("SELECT COUNT(*) as cnt FROM zones WHERE status = 'active'")
    active_zones = cur.fetchone()["cnt"]

    # Cancelled
    cur.execute("SELECT COUNT(*) as cnt FROM zones WHERE status = 'cancelled'")
    cancelled_zones = cur.fetchone()["cnt"]

    # By grade
    cur.execute("""
        SELECT grade, COUNT(*) as cnt
        FROM zones
        GROUP BY grade
    """)
    grade_stats = {row["grade"]: row["cnt"] for row in cur.fetchall()}

    # By direction
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
