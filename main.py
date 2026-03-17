from fastapi import FastAPI, Request
import requests
import os

app = FastAPI()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")


def clean(value, fallback="N/A"):
    if value is None:
        return fallback
    text = str(value).strip()
    if text == "" or text.lower() == "na":
        return fallback
    return text


def send_telegram_message(text: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text
    }
    response = requests.post(url, json=payload, timeout=15)
    response.raise_for_status()


@app.get("/")
def root():
    return {"status": "ok"}


@app.post("/webhook")
async def webhook(request: Request):
    data = await request.json()

    signal = clean(data.get("signal"))
    symbol = clean(data.get("symbol", "XAUUSD"))
    tf = clean(data.get("tf"))
    setup = clean(data.get("setup"))
    grade = clean(data.get("grade"))
    score = clean(data.get("score"))

    entry_low = clean(data.get("entry_low"))
    entry_high = clean(data.get("entry_high"))
    sl = clean(data.get("sl"))
    tp1 = clean(data.get("tp1"))
    tp2 = clean(data.get("tp2"))
    tp3 = clean(data.get("tp3"))
    tp4 = clean(data.get("tp4"))

    title = f"{symbol} {signal}"

    msg = (
        f"{title}\n"
        f"Setup: {setup}\n"
        f"Grade: {grade}\n"
        f"Score: {score}\n"
        f"TF: {tf}\n\n"
        f"Entry: {entry_low} - {entry_high}\n"
        f"SL: {sl}\n"
        f"TP1: {tp1}\n"
        f"TP2: {tp2}\n"
        f"TP3: {tp3}\n"
        f"TP4: {tp4}"
    )

    send_telegram_message(msg)

    return {"ok": True, "received": data}
