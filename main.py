from fastapi import FastAPI, Request
import requests
import os

app = FastAPI()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

@app.post("/webhook")
async def webhook(request: Request):
    data = await request.json()

    signal = data.get("signal", "N/A")
    price = data.get("price", "N/A")

    msg = f"XAU SIGNAL 🚨\nSignal: {signal}\nPrice: {price}"

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    requests.post(url, json={
        "chat_id": CHAT_ID,
        "text": msg
    })

    return {"ok": True}
