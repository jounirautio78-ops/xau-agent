from fastapi import FastAPI, Request
import requests
import os
from datetime import datetime
from zoneinfo import ZoneInfo

app = FastAPI()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

TZ = ZoneInfo("Europe/Madrid")

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


def send_telegram_message(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
    }
    response = requests.post(url, json=payload, timeout=15)
    response.raise_for_status()


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
        "direction": direction,
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


@app.get("/")
def root():
    return {"status": "ok"}


@app.post("/webhook")
async def webhook(request: Request):
    reset_day_if_needed()
    data = await request.json()

    message_type = clean(data.get("message_type", "")).lower()

    if message_type == "new_zone":
        zone = enrich_zone(data)
        if zone:
            state["zones"].append(zone)
            send_daily_plan_if_needed()

    elif message_type == "zone_active":
        zone_id = clean(data.get("zone_id", ""))
        direction = clean(data.get("direction", "")).lower()

        for zone in reversed(state["zones"]):
            if zone_id != "N/A":
                if zone["zone_id"] != zone_id:
                    continue
            else:
                if zone["direction"] != direction or zone["status"] != "planned":
                    continue

            if zone["status"] == "planned":
                zone["status"] = "active"
                send_telegram_message(
                    f"Gold Update\n"
                    f"{direction.capitalize()} Zone active\n\n"
                    f"Entry: {fmt_range(zone['entry_low'], zone['entry_high'])}\n"
                    f"SL: {zone['sl_distance']} ({fmt_price(zone['sl_price'])})\n"
                    f"TP1: {fmt_price(zone['tp1'])}\n"
                    f"TP2: {fmt_price(zone['tp2'])}\n"
                    f"TP3: {fmt_price(zone['tp3'])}\n"
                    f"TP4: {fmt_price(zone['tp4'])}\n"
                    f"TP5: {fmt_price(zone['tp5'])}\n"
                    f"Tier: {zone['tier_label']}"
                )
                break

    elif message_type == "zone_cancel":
        zone_id = clean(data.get("zone_id", ""))
        direction = clean(data.get("direction", "")).lower()
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
                send_telegram_message(
                    f"Cancel {direction.capitalize()} Zone\n"
                    f"Reason: {reason}"
                )
                break

    elif message_type == "debug_ping":
        # Ei lähetetä Telegramiin debug-spämmiä.
        pass

    return {
        "ok": True,
        "zones": len(state["zones"]),
        "daily_plan_sent": state["daily_plan_sent"],
    }
