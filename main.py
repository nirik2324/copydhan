"""
CopyDhan v3 — FastAPI Backend
==============================
What's new in v3:
  - Real order placement via brokers.py (Dhan, Zerodha, Angel One, Upstox)
  - Follower registry: add/remove/update followers via API
  - Per-follower API credentials stored in memory (never logged)
  - Copy engine: fires orders to all active followers on every TRADED signal
  - Copy results broadcast to frontend in real-time
  - Settings API: max lots, slippage, index filter, exchange filter
"""

import asyncio
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from brokers import copy_trade_to_all

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("copydhan")

DHAN_WS_URL = "wss://api-order-update.dhan.co"
STATIC_DIR  = Path(__file__).parent / "static"

app = FastAPI(title="CopyDhan", version="3.0.0", docs_url="/api/docs")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

LOT_SIZES = {"NIFTY": 50, "BANKNIFTY": 15, "FINNIFTY": 40, "SENSEX": 10, "BANKEX": 15}

# ── Global State ──────────────────────────────────────────────────────────────
class AppState:
    # Dhan master WS
    dhan_ws = None
    status: str = "DISCONNECTED"
    client_id: str = ""
    order_count: int = 0
    copy_count: int = 0
    last_order_ts: Optional[str] = None
    _task: Optional[asyncio.Task] = None
    # Browser clients
    frontend_clients: set = set()
    # Follower registry: dict keyed by follower id
    followers: dict = {}
    # Settings
    settings: dict = {
        "max_lots": 5,
        "copy_trigger": "TRADED",
        "exchange_filter": "BOTH",     # BOTH / NSE / BSE
        "index_filter": "ALL",          # ALL / NIFTY / BANKNIFTY / FINNIFTY / SENSEX / BANKEX
        "order_type": "MARKET",
        "stop_mirror": True,
        "pause_on_dd": False,
        "dd_pct": 15,
    }

state = AppState()

# ── Pydantic models ───────────────────────────────────────────────────────────

class ConnectRequest(BaseModel):
    client_id: str
    access_token: str

class FollowerRequest(BaseModel):
    id: str
    name: str
    broker: str                         # dhan / zerodha / angel / upstox
    client_id: str
    lots: int = 1
    active: bool = True
    tags: list[str] = []
    billing: str = "free"
    sub_fee: float = 0
    comm_pct: float = 0
    # Credentials (broker-specific)
    access_token: str = ""
    api_key: str = ""                   # Zerodha / Angel
    angel_jwt_token: str = ""           # Angel One JWT
    angel_client_code: str = ""         # Angel One client code
    dhan_security_id: str = ""          # Dhan scrip security ID (per instrument)
    angel_symbol_token: str = ""        # Angel scrip token (per instrument)
    upstox_instrument_key: str = ""     # Upstox instrument key (per instrument)

class SettingsRequest(BaseModel):
    max_lots: Optional[int] = None
    exchange_filter: Optional[str] = None
    index_filter: Optional[str] = None
    order_type: Optional[str] = None
    stop_mirror: Optional[bool] = None
    pause_on_dd: Optional[bool] = None
    dd_pct: Optional[int] = None

# ── Helpers ───────────────────────────────────────────────────────────────────

def ist_now():
    return datetime.now().strftime("%H:%M:%S")

async def broadcast(payload: dict):
    if not state.frontend_clients:
        return
    msg = json.dumps(payload)
    dead = set()
    for ws in state.frontend_clients:
        try:
            await ws.send_text(msg)
        except Exception:
            dead.add(ws)
    state.frontend_clients -= dead

async def set_status(new_status: str, detail: str = ""):
    state.status = new_status
    log.info(f"Status → {new_status}  {detail}")
    await broadcast({"type": "status", "status": new_status, "detail": detail, "ts": ist_now()})

def parse_dhan_order(msg: dict) -> Optional[dict]:
    """Extract and normalise fields from a Dhan order_alert message."""
    try:
        d = msg.get("Data", {})
        sym = (d.get("Symbol") or "").upper()
        index = "NIFTY"
        for name in ["BANKNIFTY", "FINNIFTY", "SENSEX", "BANKEX", "NIFTY"]:
            if name in sym:
                index = name
                break
        lot_sz = LOT_SIZES.get(index, 50)
        qty    = d.get("Quantity", lot_sz)
        lots   = max(1, round(qty / lot_sz))
        expiry_raw = d.get("ExpiryDate", "")
        try:
            expiry = datetime.strptime(expiry_raw[:10], "%Y-%m-%d").strftime("%d-%b").upper()
        except Exception:
            expiry = "—"
        return {
            "id":          d.get("OrderNo") or d.get("ExchOrderNo") or f"ORD-{int(datetime.now().timestamp())}",
            "exchOrderNo": d.get("ExchOrderNo"),
            "securityId":  d.get("SecurityId", ""),   # passed to Dhan follower orders
            "index":       index,
            "symbol":      d.get("Symbol", sym),
            "exchange":    d.get("Exchange", "NSE"),
            "optType":     d.get("OptType", "CE"),
            "side":        "BUY" if d.get("TxnType") == "B" else "SELL",
            "strike":      d.get("StrikePrice", 0),
            "expiry":      expiry,
            "lots":        lots,
            "qty":         qty,
            "price":       d.get("Price", 0.0),
            "tradedPrice": d.get("TradedPrice", 0.0),
            "status":      d.get("Status", ""),
            "lotSize":     lot_sz,
            "ts":          ist_now(),
        }
    except Exception as e:
        log.warning(f"parse_dhan_order error: {e}")
        return None

def should_copy(trade: dict) -> bool:
    """Apply settings filters to decide if this trade should be copied."""
    s = state.settings
    if s["exchange_filter"] != "BOTH" and trade.get("exchange") != s["exchange_filter"]:
        return False
    if s["index_filter"] != "ALL" and trade.get("index") != s["index_filter"]:
        return False
    return True

def eligible_followers(trade: dict) -> list:
    """Return followers that are active and interested in this index."""
    result = []
    for f in state.followers.values():
        if not f.get("active"):
            continue
        tags = f.get("tags", [])
        if tags and trade.get("index") not in tags:
            continue
        # Cap lots at settings max
        f_copy = dict(f)
        f_copy["lots"] = min(f.get("lots", 1), state.settings["max_lots"])
        result.append(f_copy)
    return result

# ── Dhan WebSocket Loop ───────────────────────────────────────────────────────

async def dhan_loop(client_id: str, access_token: str):
    retry = 0
    while retry < 20:
        try:
            await set_status("CONNECTING")
            async with websockets.connect(
                DHAN_WS_URL, ping_interval=20, ping_timeout=10, close_timeout=5
            ) as ws:
                state.dhan_ws = ws
                retry = 0

                await set_status("AUTHENTICATING")
                await ws.send(json.dumps({
                    "LoginReq": {"MsgCode": 42, "ClientId": client_id, "Token": access_token},
                    "UserType": "SELF",
                }))

                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    t = msg.get("Type", "")

                    if t == "connection" or msg.get("MsgCode") == 43:
                        await set_status("CONNECTED", f"Client {client_id}")
                        continue

                    if t == "auth_failed" or msg.get("status") == "failed":
                        await set_status("AUTH_FAILED", "Invalid credentials")
                        return

                    if t == "order_alert":
                        d = msg.get("Data", {})
                        seg = d.get("Segment", "")
                        opt = d.get("OptType", "")
                        # Only NSE FNO (D) and BSE FNO (F) options
                        if seg not in ("D", "F") or opt not in ("CE", "PE"):
                            continue

                        state.order_count += 1
                        state.last_order_ts = ist_now()

                        trade = parse_dhan_order(msg)
                        if not trade:
                            continue

                        log.info(f"#{state.order_count} {trade['side']} {trade['symbol']} {trade['status']}")

                        # Forward raw signal to frontend
                        await broadcast({
                            "type": "order_alert",
                            "data": msg,
                            "trade": trade,
                            "ts": ist_now(),
                            "count": state.order_count,
                        })

                        # ── COPY ENGINE ──────────────────────────────────────
                        if trade["status"] == "TRADED" and should_copy(trade):
                            followers = eligible_followers(trade)
                            if followers:
                                log.info(f"Copying {trade['symbol']} → {len(followers)} followers")
                                asyncio.create_task(
                                    copy_trade_to_all(followers, trade, broadcast)
                                )
                                state.copy_count += 1

        except websockets.exceptions.ConnectionClosedError as e:
            if e.code == 805:
                await set_status("MAX_CONN", "Max 5 WS connections on Dhan")
                return
            await set_status("DISCONNECTED")
        except Exception as e:
            log.error(f"WS error: {e}")
            await set_status("ERROR", str(e))
        finally:
            state.dhan_ws = None

        delay = min(2 ** (retry + 1), 30)
        log.info(f"Reconnecting in {delay}s…")
        await broadcast({"type": "reconnecting", "delay": delay, "ts": ist_now()})
        await asyncio.sleep(delay)
        retry += 1

    await set_status("ERROR", "Max retries exhausted")

# ── Startup: auto-connect if env vars set ─────────────────────────────────────

@app.on_event("startup")
async def startup():
    cid = os.getenv("DHAN_CLIENT_ID", "")
    tok = os.getenv("DHAN_ACCESS_TOKEN", "")
    if cid and tok:
        log.info(f"Auto-connecting via env vars (client {cid})")
        state.client_id = cid
        state._task = asyncio.create_task(dhan_loop(cid, tok))

# ── REST — Master connection ──────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {
        "ok": True, "status": state.status, "client_id": state.client_id,
        "order_count": state.order_count, "copy_count": state.copy_count,
        "followers": len(state.followers),
        "active_followers": sum(1 for f in state.followers.values() if f.get("active")),
        "frontend_clients": len(state.frontend_clients),
        "last_order": state.last_order_ts,
    }

@app.post("/api/connect")
async def connect(req: ConnectRequest):
    if state.status in ("CONNECTING", "AUTHENTICATING", "CONNECTED"):
        return {"ok": False, "message": f"Already {state.status}"}
    if state._task and not state._task.done():
        state._task.cancel()
    state.client_id = req.client_id
    state.order_count = 0
    state._task = asyncio.create_task(dhan_loop(req.client_id, req.access_token))
    return {"ok": True, "message": "Connecting…"}

@app.post("/api/disconnect")
async def disconnect_route():
    if state._task and not state._task.done():
        state._task.cancel()
    if state.dhan_ws:
        await state.dhan_ws.close()
    state.dhan_ws = None
    await set_status("DISCONNECTED", "Manual disconnect")
    return {"ok": True}

@app.get("/api/status")
def get_status():
    return {
        "status": state.status, "client_id": state.client_id,
        "order_count": state.order_count, "copy_count": state.copy_count,
        "last_order": state.last_order_ts,
    }

# ── REST — Follower management ────────────────────────────────────────────────

@app.get("/api/followers")
def list_followers():
    # Return followers but mask sensitive credentials
    safe = []
    for f in state.followers.values():
        fc = dict(f)
        for k in ("access_token","api_key","angel_jwt_token","angel_client_code"):
            if fc.get(k):
                fc[k] = "****"
        safe.append(fc)
    return {"ok": True, "followers": safe}

@app.post("/api/followers")
async def add_follower(req: FollowerRequest):
    state.followers[req.id] = req.dict()
    log.info(f"Follower added: {req.id} [{req.broker}] {req.name}")
    await broadcast({"type": "follower_added", "follower_id": req.id, "name": req.name, "broker": req.broker, "ts": ist_now()})
    return {"ok": True, "message": f"Follower {req.name} added"}

@app.put("/api/followers/{follower_id}")
async def update_follower(follower_id: str, req: FollowerRequest):
    if follower_id not in state.followers:
        return JSONResponse({"ok": False, "message": "Follower not found"}, status_code=404)
    # Preserve existing credentials if not provided in update
    existing = state.followers[follower_id]
    updated  = req.dict()
    for cred_key in ("access_token","api_key","angel_jwt_token","angel_client_code"):
        if not updated.get(cred_key):
            updated[cred_key] = existing.get(cred_key, "")
    state.followers[follower_id] = updated
    log.info(f"Follower updated: {follower_id}")
    await broadcast({"type": "follower_updated", "follower_id": follower_id, "ts": ist_now()})
    return {"ok": True, "message": "Follower updated"}

@app.delete("/api/followers/{follower_id}")
async def delete_follower(follower_id: str):
    if follower_id not in state.followers:
        return JSONResponse({"ok": False, "message": "Follower not found"}, status_code=404)
    name = state.followers[follower_id].get("name", follower_id)
    del state.followers[follower_id]
    log.info(f"Follower removed: {follower_id}")
    await broadcast({"type": "follower_removed", "follower_id": follower_id, "ts": ist_now()})
    return {"ok": True, "message": f"Follower {name} removed"}

@app.patch("/api/followers/{follower_id}/toggle")
async def toggle_follower(follower_id: str):
    if follower_id not in state.followers:
        return JSONResponse({"ok": False, "message": "Follower not found"}, status_code=404)
    f = state.followers[follower_id]
    f["active"] = not f.get("active", True)
    await broadcast({"type": "follower_toggled", "follower_id": follower_id, "active": f["active"], "ts": ist_now()})
    return {"ok": True, "active": f["active"]}

# ── REST — Settings ───────────────────────────────────────────────────────────

@app.get("/api/settings")
def get_settings():
    return {"ok": True, "settings": state.settings}

@app.post("/api/settings")
async def update_settings(req: SettingsRequest):
    updates = {k: v for k, v in req.dict().items() if v is not None}
    state.settings.update(updates)
    log.info(f"Settings updated: {updates}")
    await broadcast({"type": "settings_updated", "settings": state.settings, "ts": ist_now()})
    return {"ok": True, "settings": state.settings}

# ── Browser WebSocket ─────────────────────────────────────────────────────────

@app.websocket("/ws")
async def frontend_ws(websocket: WebSocket):
    await websocket.accept()
    state.frontend_clients.add(websocket)
    log.info(f"Browser connected (total: {len(state.frontend_clients)})")

    # Send full current state on connect
    await websocket.send_text(json.dumps({
        "type": "init",
        "status": state.status,
        "order_count": state.order_count,
        "copy_count": state.copy_count,
        "settings": state.settings,
        "ts": ist_now(),
    }))

    try:
        while True:
            data = await websocket.receive_text()
            msg  = json.loads(data)
            if msg.get("type") == "ping":
                await websocket.send_text(json.dumps({"type": "pong", "ts": ist_now()}))
    except WebSocketDisconnect:
        pass
    except Exception as e:
        log.warning(f"Browser WS error: {e}")
    finally:
        state.frontend_clients.discard(websocket)
        log.info(f"Browser disconnected (total: {len(state.frontend_clients)})")

# ── Serve React Static Build ──────────────────────────────────────────────────

if (STATIC_DIR / "assets").exists():
    app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="assets")

@app.get("/{full_path:path}", include_in_schema=False)
async def serve_spa(full_path: str):
    index = STATIC_DIR / "index.html"
    if index.exists():
        return FileResponse(index)
    return JSONResponse({"error": "Frontend not built yet"}, status_code=503)
