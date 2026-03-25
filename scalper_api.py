from fastapi import FastAPI, Request
import json
import os
from datetime import datetime
from zoneinfo import ZoneInfo

app = FastAPI()

TZ = ZoneInfo("Europe/Madrid")
QUEUE_FILE = "scalp_queue.json"


# =========================
# Helpers
# =========================
def now_iso():
    return datetime.now(TZ).isoformat()


def safe_write_to_queue(data):
    try:
        with open(QUEUE_FILE, "a") as f:
            f.write(json.dumps(data) + "\n")
    except Exception as e:
        print("QUEUE WRITE ERROR:", e)


# =========================
# Health
# =========================
@app.get("/")
def root():
    return {"status": "scalper_api_running"}


@app.get("/health")
def health():
    return {"status": "ok", "time": now_iso()}


# =========================
# SCALPER WEBHOOK
# =========================
@app.post("/scalper")
async def scalper_webhook(request: Request):
    try:
        data = await request.json()
        print("INCOMING SCALP:", data)

        # basic validation
        if data.get("message_type") != "scalp_signal":
            return {"status": "ignored_not_scalp"}

        direction = data.get("direction")
        symbol = data.get("symbol")

        if direction not in ["buy", "sell"]:
            return {"status": "invalid_direction"}

        if not symbol:
            return {"status": "missing_symbol"}

        # enrich data
        data["received_at"] = now_iso()

        # write to queue
        safe_write_to_queue(data)

        print("SCALP SIGNAL QUEUED")

        return {"status": "scalp_signal_queued"}

    except Exception as e:
        print("ERROR:", e)
        return {"status": "error", "message": str(e)}
