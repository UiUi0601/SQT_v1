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
    url = "wss://testnet.binance.vision/ws"
    print(f"Connecting to {url}")
    async with websockets.connect(url) as ws:
        print("Connected.")
        
        timestamp = int(time.time() * 1000)
        payload = f"apiKey={API_KEY}&timestamp={timestamp}"
        signature = hmac.new(API_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
        
        req = {
            "id": "123",
            "method": "userDataStream.start",
            "params": {
                "apiKey": API_KEY,
                "timestamp": timestamp,
                "signature": signature
            }
        }
        print(f"Sending: {req}")
        await ws.send(json.dumps(req))
        
        res = await ws.recv()
        print(f"Response: {res}")
        
        req2 = {
            "id": "124",
            "method": "userDataStream.subscribe.signature",
            "params": {
                "apiKey": API_KEY,
                "timestamp": timestamp,
                "signature": signature
            }
        }
        print(f"Sending: {req2}")
        await ws.send(json.dumps(req2))
        
        res2 = await ws.recv()
        print(f"Response2: {res2}")

asyncio.run(test_ws())
