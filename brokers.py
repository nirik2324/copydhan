"""
CopyDhan — Broker Order Execution Engine
=========================================
Handles real order placement for:
  - Dhan      (REST API v2)
  - Zerodha   (Kite Connect)
  - Angel One (SmartAPI)
  - Upstox    (API v3)

Each broker has:
  - A credentials model
  - A place_order() async function
  - An error normaliser that returns a standard result dict

Standard result dict:
  {
    "ok": True/False,
    "order_id": "...",    # on success
    "error": "...",       # on failure
    "broker": "dhan",
    "follower_id": "fl1",
  }
"""

import asyncio
import logging
from datetime import datetime

import httpx

log = logging.getLogger("copydhan.brokers")

# ── Helpers ──────────────────────────────────────────────────────────────────

def ist_now():
    return datetime.now().strftime("%H:%M:%S")

def nse_symbol_to_zerodha(index: str, strike: int, opt_type: str, expiry_str: str) -> str:
    """
    Convert our internal symbol to Zerodha tradingsymbol format.
    e.g. NIFTY + 24200 + CE + "17-APR" → "NIFTY2441724200CE"
    Zerodha format: {INDEX}{YY}{MON_SHORT}{STRIKE}{CE/PE}
    """
    try:
        dt = datetime.strptime(expiry_str, "%d-%b")
        yy = datetime.now().strftime("%y")
        mon = dt.strftime("%b").upper()[:3]
        # For weekly expiry Zerodha uses numeric month for non-monthly expiries
        # Simplified: use full format that works for most cases
        return f"{index}{yy}{mon}{strike}{opt_type}"
    except Exception:
        return f"{index}{strike}{opt_type}"

def upstox_instrument_key(index: str, strike: int, opt_type: str, expiry_str: str, exchange: str) -> str:
    """
    Upstox instrument key format:
    NSE_FO|NIFTY24APR2024200CE  (example)
    """
    try:
        dt = datetime.strptime(expiry_str, "%d-%b")
        yy = datetime.now().strftime("%Y")
        mon = dt.strftime("%b").upper()
        day = dt.strftime("%d")
        prefix = "NSE_FO" if exchange == "NSE" else "BSE_FO"
        return f"{prefix}|{index}{yy}{mon}{day}{strike}{opt_type}"
    except Exception:
        return f"NSE_FO|{index}{strike}{opt_type}"


# ═══════════════════════════════════════════════════════════════════════════
#  DHAN BROKER
# ═══════════════════════════════════════════════════════════════════════════

async def dhan_place_order(
    *,
    follower_id: str,
    client_id: str,
    access_token: str,
    index: str,
    exchange: str,       # NSE / BSE
    symbol: str,         # e.g. NIFTY2441724200CE
    security_id: str,    # Dhan scrip master ID (required)
    txn_type: str,       # BUY / SELL
    quantity: int,
    order_type: str = "MARKET",
    price: float = 0.0,
    product: str = "INTRADAY",
) -> dict:
    """
    Place order via Dhan REST API v2.
    Docs: https://dhanhq.co/docs/v2/orders/
    """
    seg_map = {"NSE": "NSE_FNO", "BSE": "BSE_FNO"}
    url = "https://api.dhan.co/v2/orders"
    headers = {
        "access-token": access_token,
        "client-id": client_id,
        "Content-Type": "application/json",
    }
    payload = {
        "dhanClientId": client_id,
        "transactionType": txn_type,           # BUY / SELL
        "exchangeSegment": seg_map.get(exchange, "NSE_FNO"),
        "productType": product,                 # INTRADAY / CNC / MARGIN
        "orderType": order_type,                # MARKET / LIMIT
        "validity": "DAY",
        "tradingSymbol": symbol,
        "securityId": security_id,
        "quantity": quantity,
        "price": price if order_type == "LIMIT" else 0,
        "triggerPrice": 0,
        "disclosedQuantity": 0,
        "afterMarketOrder": False,
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(url, headers=headers, json=payload)
            data = r.json()
            if r.status_code == 200 and data.get("orderId"):
                log.info(f"[Dhan] ✓ {follower_id}  order {data['orderId']}")
                return {"ok": True, "order_id": str(data["orderId"]), "broker": "dhan", "follower_id": follower_id}
            else:
                err = data.get("errorMessage") or data.get("remarks") or str(data)
                log.error(f"[Dhan] ✗ {follower_id}  {err}")
                return {"ok": False, "error": err, "broker": "dhan", "follower_id": follower_id}
    except Exception as e:
        log.error(f"[Dhan] Exception {follower_id}: {e}")
        return {"ok": False, "error": str(e), "broker": "dhan", "follower_id": follower_id}


# ═══════════════════════════════════════════════════════════════════════════
#  ZERODHA (KITE CONNECT)
# ═══════════════════════════════════════════════════════════════════════════

async def zerodha_place_order(
    *,
    follower_id: str,
    api_key: str,
    access_token: str,
    index: str,
    exchange: str,
    trading_symbol: str,   # e.g. NIFTY2441724200CE
    txn_type: str,         # BUY / SELL
    quantity: int,
    order_type: str = "MARKET",
    price: float = 0.0,
    product: str = "MIS",  # MIS=intraday, NRML=overnight
) -> dict:
    """
    Place order via Zerodha Kite Connect API.
    Docs: https://kite.trade/docs/connect/v3/orders/
    """
    url = "https://api.kite.trade/orders/regular"
    headers = {
        "X-Kite-Version": "3",
        "Authorization": f"token {api_key}:{access_token}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    exch = "NFO" if exchange == "NSE" else "BFO"
    data = {
        "tradingsymbol": trading_symbol,
        "exchange": exch,
        "transaction_type": txn_type,
        "order_type": order_type,
        "quantity": str(quantity),
        "product": product,
        "validity": "DAY",
    }
    if order_type == "LIMIT":
        data["price"] = str(price)

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(url, headers=headers, data=data)
            resp = r.json()
            if r.status_code == 200 and resp.get("status") == "success":
                oid = resp.get("data", {}).get("order_id", "")
                log.info(f"[Zerodha] ✓ {follower_id}  order {oid}")
                return {"ok": True, "order_id": str(oid), "broker": "zerodha", "follower_id": follower_id}
            else:
                err = resp.get("message") or str(resp)
                log.error(f"[Zerodha] ✗ {follower_id}  {err}")
                return {"ok": False, "error": err, "broker": "zerodha", "follower_id": follower_id}
    except Exception as e:
        log.error(f"[Zerodha] Exception {follower_id}: {e}")
        return {"ok": False, "error": str(e), "broker": "zerodha", "follower_id": follower_id}


# ═══════════════════════════════════════════════════════════════════════════
#  ANGEL ONE (SMARTAPI)
# ═══════════════════════════════════════════════════════════════════════════

async def angel_place_order(
    *,
    follower_id: str,
    api_key: str,
    jwt_token: str,
    client_code: str,
    index: str,
    exchange: str,
    trading_symbol: str,   # e.g. NIFTY24APR2442400CE
    symbol_token: str,     # Angel One instrument token (from scrip master)
    txn_type: str,         # BUY / SELL
    quantity: int,
    order_type: str = "MARKET",
    price: float = 0.0,
    product: str = "INTRADAY",
) -> dict:
    """
    Place order via Angel One SmartAPI.
    Docs: https://smartapi.angelbroking.com/docs
    """
    url = "https://apiconnect.angelbroking.com/rest/secure/angelbroking/order/v1/placeOrder"
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": "127.0.0.1",
        "X-ClientPublicIP": "127.0.0.1",
        "X-MACAddress": "00:00:00:00:00:00",
        "X-PrivateKey": api_key,
    }
    exch = "NFO" if exchange == "NSE" else "BFO"
    payload = {
        "variety": "NORMAL",
        "tradingsymbol": trading_symbol,
        "symboltoken": symbol_token,
        "transactiontype": txn_type,
        "exchange": exch,
        "ordertype": order_type,
        "producttype": product,
        "duration": "DAY",
        "quantity": str(quantity),
        "price": str(price) if order_type == "LIMIT" else "0",
        "squareoff": "0",
        "stoploss": "0",
        "trailingStopLoss": "0",
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(url, headers=headers, json=payload)
            resp = r.json()
            if resp.get("status") and resp.get("data", {}).get("orderid"):
                oid = resp["data"]["orderid"]
                log.info(f"[Angel] ✓ {follower_id}  order {oid}")
                return {"ok": True, "order_id": str(oid), "broker": "angel", "follower_id": follower_id}
            else:
                err = resp.get("message") or str(resp)
                log.error(f"[Angel] ✗ {follower_id}  {err}")
                return {"ok": False, "error": err, "broker": "angel", "follower_id": follower_id}
    except Exception as e:
        log.error(f"[Angel] Exception {follower_id}: {e}")
        return {"ok": False, "error": str(e), "broker": "angel", "follower_id": follower_id}


# ═══════════════════════════════════════════════════════════════════════════
#  UPSTOX (API v3)
# ═══════════════════════════════════════════════════════════════════════════

async def upstox_place_order(
    *,
    follower_id: str,
    access_token: str,
    index: str,
    exchange: str,
    instrument_key: str,   # e.g. NSE_FO|NIFTY24APR2024200CE
    txn_type: str,         # BUY / SELL
    quantity: int,
    order_type: str = "MARKET",
    price: float = 0.0,
    product: str = "I",    # I=Intraday, D=Delivery
) -> dict:
    """
    Place order via Upstox API v3.
    Docs: https://upstox.com/developer/api-documentation/
    """
    url = "https://api.upstox.com/v3/order/place"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {
        "quantity": quantity,
        "product": product,
        "validity": "DAY",
        "price": price if order_type == "LIMIT" else 0,
        "instrument_token": instrument_key,
        "order_type": order_type,
        "transaction_type": txn_type,
        "disclosed_quantity": 0,
        "trigger_price": 0,
        "is_amo": False,
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(url, headers=headers, json=payload)
            resp = r.json()
            if resp.get("status") == "success" and resp.get("data", {}).get("order_id"):
                oid = resp["data"]["order_id"]
                log.info(f"[Upstox] ✓ {follower_id}  order {oid}")
                return {"ok": True, "order_id": str(oid), "broker": "upstox", "follower_id": follower_id}
            else:
                err = resp.get("errors") or resp.get("message") or str(resp)
                log.error(f"[Upstox] ✗ {follower_id}  {err}")
                return {"ok": False, "error": str(err), "broker": "upstox", "follower_id": follower_id}
    except Exception as e:
        log.error(f"[Upstox] Exception {follower_id}: {e}")
        return {"ok": False, "error": str(e), "broker": "upstox", "follower_id": follower_id}


# ═══════════════════════════════════════════════════════════════════════════
#  UNIFIED DISPATCHER
# ═══════════════════════════════════════════════════════════════════════════

async def place_order_for_follower(follower: dict, trade: dict) -> dict:
    """
    Main entry point. Called once per follower when a TRADED signal arrives.

    follower dict keys expected:
      id, broker, clientId, lots, lotSize,
      apiKey, accessToken, [angelJwtToken, angelClientCode, dhanSecurityId, angelSymbolToken, upstoxInstrumentKey]

    trade dict keys expected (parsed from Dhan order_alert):
      index, exchange, optType, side, strike, expiry, lots, lotSize, symbol
    """
    broker  = follower.get("broker", "")
    fid     = follower.get("id", "?")
    lots    = follower.get("lots", 1)
    lot_sz  = trade.get("lotSize", 50)
    qty     = lots * lot_sz
    side    = trade.get("side", "BUY")          # BUY / SELL
    index   = trade.get("index", "NIFTY")
    exch    = trade.get("exchange", "NSE")
    strike  = trade.get("strike", 0)
    opt     = trade.get("optType", "CE")
    expiry  = trade.get("expiry", "")
    price   = trade.get("tradedPrice", 0.0)
    symbol  = trade.get("symbol", f"{index}{strike}{opt}")

    log.info(f"Placing order → {fid} [{broker}]  {side} {qty}qty  {symbol}")

    if broker == "dhan":
        # securityId comes from the master Dhan order_alert — no manual entry needed.
        # The master signal includes SecurityId in its Data payload automatically.
        security_id = (
            trade.get("securityId")
            or follower.get("dhanSecurityId", "")
            or follower.get("dhan_security_id", "")
        )
        return await dhan_place_order(
            follower_id=fid,
            client_id=follower.get("clientId", "") or follower.get("client_id", ""),
            access_token=follower.get("accessToken", "") or follower.get("access_token", ""),
            index=index,
            exchange=exch,
            symbol=symbol,
            security_id=security_id,
            txn_type=side,
            quantity=qty,
            order_type="MARKET",
            product="INTRADAY",
        )

    elif broker == "zerodha":
        zs = nse_symbol_to_zerodha(index, strike, opt, expiry)
        return await zerodha_place_order(
            follower_id=fid,
            api_key=follower.get("apiKey", ""),
            access_token=follower.get("accessToken", ""),
            index=index,
            exchange=exch,
            trading_symbol=zs,
            txn_type=side,
            quantity=qty,
            order_type="MARKET",
            product="MIS",
        )

    elif broker == "angel":
        return await angel_place_order(
            follower_id=fid,
            api_key=follower.get("apiKey", ""),
            jwt_token=follower.get("angelJwtToken", ""),
            client_code=follower.get("angelClientCode", ""),
            index=index,
            exchange=exch,
            trading_symbol=symbol,
            symbol_token=follower.get("angelSymbolToken", ""),
            txn_type=side,
            quantity=qty,
            order_type="MARKET",
            product="INTRADAY",
        )

    elif broker == "upstox":
        ik = follower.get("upstoxInstrumentKey") or upstox_instrument_key(index, strike, opt, expiry, exch)
        return await upstox_place_order(
            follower_id=fid,
            access_token=follower.get("accessToken", ""),
            index=index,
            exchange=exch,
            instrument_key=ik,
            txn_type=side,
            quantity=qty,
            order_type="MARKET",
            product="I",
        )

    else:
        return {"ok": False, "error": f"Unknown broker: {broker}", "broker": broker, "follower_id": fid}


async def copy_trade_to_all(followers: list[dict], trade: dict, broadcast_fn) -> list[dict]:
    """
    Fire off order placement to all active followers concurrently.
    Returns list of results and broadcasts each to frontend.
    """
    if not followers:
        return []

    tasks = [place_order_for_follower(f, trade) for f in followers]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    output = []
    for f, res in zip(followers, results):
        if isinstance(res, Exception):
            res = {"ok": False, "error": str(res), "broker": f.get("broker","?"), "follower_id": f.get("id","?")}
        output.append(res)
        # Broadcast copy result to frontend
        await broadcast_fn({
            "type": "copy_result",
            "follower_id": res.get("follower_id"),
            "broker": res.get("broker"),
            "ok": res.get("ok"),
            "order_id": res.get("order_id"),
            "error": res.get("error"),
            "trade_symbol": trade.get("symbol"),
            "side": trade.get("side"),
            "ts": ist_now(),
        })

    ok_count  = sum(1 for r in output if r.get("ok"))
    err_count = len(output) - ok_count
    log.info(f"Copy complete: {ok_count} success, {err_count} failed  [{trade.get('symbol')}]")
    return output
