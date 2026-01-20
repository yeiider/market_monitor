import os
import asyncio
import datetime
import json
import random
from collections import defaultdict
from fastapi import FastAPI, Query, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from influxdb_client import InfluxDBClient
from typing import List, Optional, Dict, Any
from pydantic import BaseModel

app = FastAPI(title="Market Monitor Pro API", version="3.0")

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Config
INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")

def get_influx_client():
    return InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)

def get_iso_now():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

# --- WebSocket Infrastructure V2.0 ---

class WebSocketHub:
    def __init__(self):
        # Mapea channel_id -> Lista de WebSockets
        self.channels: Dict[str, List[WebSocket]] = defaultdict(list)
        # Mapea WebSocket -> Lista de channel_ids (para desconexión rápida)
        self.socket_channels: Dict[WebSocket, List[str]] = defaultdict(list)

    async def connect(self, websocket: WebSocket, channel: str):
        if websocket not in self.channels[channel]:
            self.channels[channel].append(websocket)
            self.socket_channels[websocket].append(channel)

    def disconnect(self, websocket: WebSocket, channel: str = None):
        if channel:
            if websocket in self.channels[channel]:
                self.channels[channel].remove(websocket)
                if websocket in self.socket_channels:
                    self.socket_channels[websocket].remove(channel)
        else:
            # Desconectar de TODOS los canales
            if websocket in self.socket_channels:
                for ch in self.socket_channels[websocket]:
                    if websocket in self.channels[ch]:
                        self.channels[ch].remove(websocket)
                del self.socket_channels[websocket]

    async def broadcast(self, channel: str, message: dict):
        if channel in self.channels:
            for connection in list(self.channels[channel]):
                try:
                    await connection.send_json(message)
                except Exception:
                    pass
    
    def get_active_symbols(self):
        return [ch.replace("symbol_", "") for ch in self.channels.keys() if ch.startswith("symbol_") and self.channels[ch]]

hub = WebSocketHub()

# --- Logic Core ---
async def fetch_stock_snapshots(limit_last_min=10):
    """Trae el último estado de todas las acciones"""
    client = get_influx_client()
    query_api = client.query_api()
    flux_query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -{limit_last_min}m)
      |> filter(fn: (r) => r["_measurement"] == "stock_price")
      |> last()
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> group()
    '''
    try:
        result = await asyncio.to_thread(query_api.query, org=INFLUX_ORG, query=flux_query)
        stocks = []
        for table in result:
            for r in table.records:
                stock = {k: v for k, v in r.values.items() if k not in ['result', 'table', '_start', '_stop', '_measurement']}
                # Normalizar
                stock['price'] = float(stock.get('price', 0))
                stock['beta'] = float(stock.get('beta', 0))
                stock['market_cap'] = int(stock.get('market_cap', 0))
                stock['volume'] = float(stock.get('volume', 0))
                stock['dividend'] = float(stock.get('dividend', 0))
                stock['company_name'] = stock.get('company_name', 'N/A')
                stock['symbol'] = stock.get('symbol', 'N/A')
                stock['sector'] = stock.get('sector', 'Unknown')
                
                # Campos calculados simulados (en prod vendrían de DB)
                stock['change_percent'] = round(random.uniform(-2.0, 2.0), 2)
                stock['bid'] = round(stock['price'] * 0.999, 2)
                stock['ask'] = round(stock['price'] * 1.001, 2)
                
                stocks.append(stock)
        return stocks
    except Exception as e:
        print(f"Error fetching snapshots: {e}")
        return []
    finally:
        client.close()

async def get_symbol_history(symbol: str, interval_str: str = "1m", limit: int = 100):
    client = get_influx_client()
    query_api = client.query_api()
    window = "1m"
    start = "-2h"
    if interval_str == "5m": window = "5m"; start = "-6h"
    if interval_str == "15m": window = "15m"; start = "-24h"
    if interval_str == "1h": window = "1h"; start = "-7d"
    
    query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: {start})
      |> filter(fn: (r) => r["_measurement"] == "stock_price" and r["symbol"] == "{symbol}")
      |> filter(fn: (r) => r["_field"] == "price")
      |> aggregateWindow(every: {window}, fn: mean, createEmpty: false)
      |> limit(n: {limit})
    '''
    try:
        result = await asyncio.to_thread(query_api.query, org=INFLUX_ORG, query=query)
        data = []
        for table in result:
            for r in table.records:
                val = r.get_value()
                if val:
                    data.append({
                        "time": r.get_time().isoformat(),
                        "close": round(val, 2),
                        "open": round(val * 0.99, 2), 
                        "high": round(val * 1.01, 2), 
                        "low": round(val * 0.98, 2),  
                        "volume": 0 
                    })
        return data
    except Exception:
        return []
    finally:
        client.close()

# --- REST Endpoints (V3.0 Spec) ---

@app.get("/api/dashboard")
async def get_dashboard():
    stocks = await fetch_stock_snapshots()
    
    bulls = sum(1 for s in stocks if s.get('change_percent', 0) > 0)
    bears = len(stocks) - bulls
    
    top_rec = {}
    if stocks:
        top_rec_stock = max(stocks, key=lambda x: x.get('volume', 0))
        top_rec = {
            "symbol": top_rec_stock.get('symbol'),
            "company_name": top_rec_stock.get('company_name', 'Unknown'),
            "price": top_rec_stock.get('price'),
            "change_percent": top_rec_stock.get('change_percent'),
            "signal": "STRONG BUY",
            "strategy": "Volume Leader",
            "reason": "High volume activity detected",
            "confidence": 85,
            "risk_level": "medium"
        }

    return {
        "last_updated": get_iso_now(),
        "market_status": "open",
        "summary": {
            "total_stocks_analyzed": len(stocks),
            "bullish_signals": bulls,
            "bearish_signals": bears,
            "neutral": 0
        },
        "top_recommendation": top_rec,
        "market_sentiment": {
            "score": 65,
            "label": "Bullish",
            "description": "Mercado con tendencia alcista moderada"
        }
    }

@app.get("/api/recommendations")
async def get_recommendations(limit: int = 10, strategy: str = "all"):
    stocks = await fetch_stock_snapshots()
    sorted_stocks = sorted(stocks, key=lambda x: x.get('dividend', 0), reverse=True)[:limit]
    
    recs = []
    for i, s in enumerate(sorted_stocks):
        price = s.get('price', 1)
        div = s.get('dividend', 0)
        yield_pct = (div / price * 100) if price > 0 else 0
        
        recs.append({
            "rank": i + 1,
            "symbol": s.get('symbol'),
            "company_name": s.get('company_name'),
            "price": s.get('price'),
            "change_percent": s.get('change_percent'),
            "change_5m": 0.5,
            "change_1h": 1.1,
            "change_24h": 2.3,
            "volume": s.get('volume'),
            "signal": "BUY",
            "strategy": "Value Investing" if yield_pct > 2 else "Growth",
            "confidence": 80,
            "risk_level": "low" if s.get('beta', 1) < 1 else "high",
            "reasons": ["Good fundamentals", "Market trend"],
            "target_price": price * 1.1,
            "stop_loss": price * 0.9
        })
    
    return {
        "generated_at": get_iso_now(),
        "recommendations": recs
    }

@app.get("/api/gainers")
async def get_gainers(period: str = "24h", limit: int = 10):
    stocks = await fetch_stock_snapshots()
    # Sort by change percent DESC
    sorted_stocks = sorted(stocks, key=lambda x: x.get('change_percent', 0), reverse=True)[:limit]
    
    gainers = []
    for i, s in enumerate(sorted_stocks):
        gainers.append({
            "rank": i+1,
            "symbol": s.get('symbol'),
            "company_name": s.get('company_name'),
            "price": s.get('price'),
            "change_percent": s.get('change_percent'),
            "change_value": 1.2,
            "previous_price": s.get('price') * 0.95,
            "volume": s.get('volume'),
            "avg_volume": s.get('volume'),
            "volume_ratio": 1.5,
            "sector": s.get('sector'),
            "high_24h": s.get('price') * 1.05,
            "low_24h": s.get('price') * 0.98
        })
        
    return {
        "period": period,
        "generated_at": get_iso_now(),
        "gainers": gainers
    }

@app.get("/api/losers")
async def get_losers(period: str = "24h", limit: int = 10):
    stocks = await fetch_stock_snapshots()
    # Sort by change percent ASC
    sorted_stocks = sorted(stocks, key=lambda x: x.get('change_percent', 0))[:limit]
    
    losers = []
    for i, s in enumerate(sorted_stocks):
        losers.append({
            "rank": i+1,
            "symbol": s.get('symbol'),
            "company_name": s.get('company_name'),
            "price": s.get('price'),
            "change_percent": s.get('change_percent'),
            "change_value": -0.5,
            "previous_price": s.get('price') * 1.03,
            "volume": s.get('volume'),
            "sector": s.get('sector'),
            "is_opportunity": True,
            "opportunity_reason": "Oversold"
        })
        
    return {
        "period": period,
        "generated_at": get_iso_now(),
        "losers": losers
    }

@app.get("/api/alerts")
async def get_alerts(type: str = "all", limit: int = 50):
    stocks = await fetch_stock_snapshots()
    alerts = []
    for i, s in enumerate(stocks[:5]): 
        alerts.append({
            "id": f"alert-{100+i}",
            "timestamp": get_iso_now(),
            "type": "buy" if i % 2 == 0 else "warning",
            "priority": "high",
            "symbol": s.get('symbol'),
            "company_name": s.get('company_name'),
            "title": "Movement Detected",
            "message": f"{s.get('symbol')} is moving significantly.",
            "price_at_alert": s.get('price'),
            "current_price": s.get('price'),
            "change_since_alert": 0.0,
            "action_recommended": "Monitor",
            "expires_at": (datetime.datetime.utcnow() + datetime.timedelta(hours=1)).isoformat() + "Z"
        })
    
    return {
        "alerts": alerts,
        "unread_count": len(alerts),
        "total_today": 120
    }

@app.get("/api/analysis/growth")
async def get_growth_analysis():
    stocks = await fetch_stock_snapshots()
    growth_stocks = [s for s in stocks if s.get('beta', 0) > 1.2][:5]
    
    data = []
    for s in growth_stocks:
        data.append({
            "symbol": s.get('symbol'),
            "company_name": s.get('company_name'),
            "price": s.get('price'),
            "change_5m": 1.2,
            "change_1h": 2.4,
            "change_24h": 5.1,
            "volume": s.get('volume'),
            "volume_change": 15,
            "signal": "STRONG BUY",
            "strength": 88,
            "trend": "accelerating",
            "entry_point": s.get('price') * 0.99,
            "target": s.get('price') * 1.15,
            "stop_loss": s.get('price') * 0.95
        })

    return {
        "strategy": "Growth Momentum",
        "description": "Acciones con fuerte momentum",
        "generated_at": get_iso_now(),
        "count": len(data),
        "data": data
    }

@app.get("/api/analysis/value")
async def get_value_analysis():
    stocks = await fetch_stock_snapshots()
    value_stocks = [s for s in stocks if s.get('beta', 0) < 1.0][:5]
    
    data = []
    for s in value_stocks:
        price = s.get('price', 1)
        div = s.get('dividend', 0)
        yield_pct = (div / price * 100) if price > 0 else 0
        
        data.append({
            "symbol": s.get('symbol'),
            "company_name": s.get('company_name'),
            "price": s.get('price'),
            "dividend_yield": round(yield_pct, 2),
            "pe_ratio": 15.5,
            "beta": s.get('beta'),
            "market_cap": s.get('market_cap'),
            "sector": s.get('sector'),
            "signal": "BUY",
            "undervalued_percent": 15,
            "fair_value": price * 1.15,
            "reasons": ["Safe haven", "Dividends"]
        })

    return {
        "strategy": "Value Investing",
        "description": "Acciones infravaloradas",
        "generated_at": get_iso_now(),
        "count": len(data),
        "data": data
    }

@app.get("/api/market/overview")
def get_market_overview():
    return {
        "timestamp": get_iso_now(),
        "market_status": "open",
        "next_event": "Market closes in 4h",
        "indices": [
            { "name": "S&P 500", "value": 4850.25, "change_percent": 0.85 },
            { "name": "NASDAQ", "value": 15234.50, "change_percent": 1.2 },
            { "name": "DOW", "value": 38500.00, "change_percent": 0.45 }
        ],
        "sectors": [
            { "name": "Technology", "change_percent": 1.5, "trending": "up" },
            { "name": "Healthcare", "change_percent": -0.3, "trending": "down" },
        ],
        "summary": {
            "advancing": 320,
            "declining": 180,
            "unchanged": 50,
            "new_highs": 25,
            "new_lows": 8
        }
    }

@app.get("/api/stocks/{symbol}")
async def get_stock_detail(symbol: str, range: str = Query("24h")):
    stocks = await fetch_stock_snapshots()
    info = next((s for s in stocks if s['symbol'] == symbol), {})
    history = await get_symbol_history(symbol, "1h" if range=="7d" else "15m", 100)
    
    if not info and not history:
        raise HTTPException(status_code=404, detail="Symbol not found")

    price = info.get('price', 0)
    return {
        "symbol": symbol,
        "company_name": info.get('company_name', 'N/A'),
        "sector": info.get('sector', 'Unknown'),
        "industry": info.get('industry', 'Unknown'),
        "current": {
            "price": price,
            "change_percent": info.get('change_percent', 0),
            "volume": info.get('volume', 0),
            "market_cap": info.get('market_cap', 0),
            "beta": info.get('beta', 0),
            "dividend_yield": info.get('dividend', 0)
        },
        "statistics": {
            "high_24h": round(price * 1.05, 2),
            "low_24h": round(price * 0.95, 2),
            "avg_volume": info.get('volume', 0)
        },
        "signals": {
            "overall": "BUY",
            "sentiment": "BULLISH"
        },
        "history": history
    }

# --- Background Tasks ---

async def task_broadcast_heartbeat():
    while True:
        msg = {"type": "heartbeat", "timestamp": get_iso_now()}
        await hub.broadcast("live", msg)
        await asyncio.sleep(30)

async def task_broadcast_live_pulse():
    while True:
        msg = {
            "type": "market_pulse",
            "timestamp": get_iso_now(),
            "data": {
                "sentiment": "bullish",
                "sentiment_score": random.randint(60, 80),
                "active_stocks": 500,
                "market_status": "open"
            }
        }
        await hub.broadcast("live", msg)
        await asyncio.sleep(10)

async def task_broadcast_prices():
    while True:
        stocks = await fetch_stock_snapshots(limit_last_min=2)
        if stocks:
            data = []
            for s in stocks:
                data.append({
                    "symbol": s.get('symbol'),
                    "price": s.get('price'),
                    "change": round(s.get('price') * (s.get('change_percent')/100), 2),
                    "change_percent": s.get('change_percent'),
                    "volume": s.get('volume'),
                    "bid": s.get('bid'),
                    "ask": s.get('ask')
                })
            
            msg = {
                "type": "price_batch",
                "timestamp": get_iso_now(),
                "data": data
            }
            await hub.broadcast("prices", msg)
        await asyncio.sleep(2)

async def task_broadcast_movers():
    while True:
        stocks = await fetch_stock_snapshots(limit_last_min=5)
        if stocks:
            sorted_stocks = sorted(stocks, key=lambda x: x.get('change_percent', 0))
            losers = sorted_stocks[:10]
            gainers = sorted_stocks[-10:][::-1]
            
            g_msg = {
                "type": "gainers_update",
                "timestamp": get_iso_now(),
                "period": "today",
                "data": [{"rank": i+1, "symbol": s['symbol'], "price": s['price'], "change_percent": s['change_percent']} for i, s in enumerate(gainers)]
            }
            await hub.broadcast("gainers", g_msg)

            l_msg = {
                "type": "losers_update",
                "timestamp": get_iso_now(),
                "period": "today",
                "data": [{"rank": i+1, "symbol": s['symbol'], "price": s['price'], "change_percent": s['change_percent']} for i, s in enumerate(losers)]
            }
            await hub.broadcast("losers", l_msg)
            
        await asyncio.sleep(5)

async def task_broadcast_symbol_details():
    while True:
        active_symbols = hub.get_active_symbols()
        if active_symbols:
            stocks = await fetch_stock_snapshots(limit_last_min=2)
            stock_map = {s['symbol']: s for s in stocks}
            
            for sym in active_symbols:
                if sym in stock_map:
                    s = stock_map[sym]
                    msg = {
                        "type": "symbol_tick",
                        "timestamp": get_iso_now(),
                        "symbol": sym,
                        "data": {
                            "price": s['price'],
                            "change_percent": s['change_percent'],
                            "volume": s['volume'],
                            "bid": s['bid'],
                            "ask": s['ask']
                        }
                    }
                    await hub.broadcast(f"symbol_{sym}", msg)
                    
        await asyncio.sleep(1)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(task_broadcast_heartbeat())
    asyncio.create_task(task_broadcast_live_pulse())
    asyncio.create_task(task_broadcast_prices())
    asyncio.create_task(task_broadcast_movers())
    asyncio.create_task(task_broadcast_symbol_details())

# --- WebSocket Endpoints ---

async def handle_websocket_connection(websocket: WebSocket, channel_name: str):
    await websocket.accept()
    await hub.connect(websocket, channel_name)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                if msg.get("type") == "ping":
                    await websocket.send_json({"type": "pong"})
            except:
                pass
    except WebSocketDisconnect:
        hub.disconnect(websocket, channel_name)

@app.websocket("/ws/live")
async def ws_live(websocket: WebSocket):
    await handle_websocket_connection(websocket, "live")

@app.websocket("/ws/prices")
async def ws_prices(websocket: WebSocket):
    await handle_websocket_connection(websocket, "prices")

@app.websocket("/ws/gainers")
async def ws_gainers(websocket: WebSocket):
    await handle_websocket_connection(websocket, "gainers")

@app.websocket("/ws/losers")
async def ws_losers(websocket: WebSocket):
    await handle_websocket_connection(websocket, "losers")

@app.websocket("/ws/sectors")
async def ws_sectors(websocket: WebSocket):
    await handle_websocket_connection(websocket, "sectors")

@app.websocket("/ws/recommendations")
async def ws_recs(websocket: WebSocket):
    await handle_websocket_connection(websocket, "recommendations")

@app.websocket("/ws/symbol/{symbol}")
async def ws_symbol(websocket: WebSocket, symbol: str):
    channel = f"symbol_{symbol}"
    await websocket.accept()
    await hub.connect(websocket, channel)
    try:
        while True:
            raw_msg = await websocket.receive_text()
            try:
                payload = json.loads(raw_msg)
                action = payload.get("action")
                
                if action == "get_history":
                    interval = payload.get("interval", "1m")
                    limit = payload.get("limit", 100)
                    hist_data = await get_symbol_history(symbol, interval, limit)
                    await websocket.send_json({
                        "type": "symbol_history",
                        "symbol": symbol,
                        "interval": interval,
                        "data": hist_data
                    })
            except json.JSONDecodeError:
                pass
    except WebSocketDisconnect:
        hub.disconnect(websocket, channel)
