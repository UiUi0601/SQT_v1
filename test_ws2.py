import asyncio
import websockets
import json
import time
import hmac
import hashlib
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")

async def test_ws():
    url = "wss://testnet.binance.vision/ws-api/v3"
    async with websockets.connect(url) as ws:
        ts = int(time.time() * 1000)
        payload = f"apiKey={API_KEY}&timestamp={ts}"
        sig = hmac.new(API_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
        req = {
            "id": "1",
            "method": "userDataStream.start",
            "params": {
                "apiKey": API_KEY,
                "timestamp": ts,
                "signature": sig
            }
        }
        await ws.send(json.dumps(req))
        res = await ws.recv()
        print(f"Start Response: {res}")

asyncio.run(test_ws())
