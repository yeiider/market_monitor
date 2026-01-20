import os
import asyncio
import datetime
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

# --- WebSocket Infrastructure ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        # Evitar bloqueos si hay muchos clientes
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                pass # Manejado por disconnect

manager = ConnectionManager()

# --- Helper Functions ---
def get_iso_now():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

# --- Logic Core ---
async def fetch_stock_snapshots():
    """Trae el último estado de todas las acciones (últimos 10 min)"""
    client = get_influx_client()
    query_api = client.query_api()
    flux_query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -10m)
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
                stock = {k: v for k, v in r.values.items() if k not in ['result', 'table', '_start', '_stop', '_time', '_measurement']}
                # Normalizar
                stock['price'] = float(stock.get('price', 0))
                stock['beta'] = float(stock.get('beta', 0))
                stock['market_cap'] = int(stock.get('market_cap', 0))
                stock['volume'] = float(stock.get('volume', 0))
                stock['dividend'] = float(stock.get('dividend', 0))
                stocks.append(stock)
        return stocks
    except Exception as e:
        print(f"Error fetching snapshots: {e}")
        return []
    finally:
        client.close()

# --- Endpoints ---

@app.get("/api/dashboard")
async def get_dashboard():
    stocks = await fetch_stock_snapshots()
    
    # Simple logic to determine market sentiment
    bulls = sum(1 for s in stocks if s.get('price', 0) > 100) # Mock condition, real would use change %
    bears = len(stocks) - bulls # Simplification
    
    # Mocking top recommendation logic here (real logic would be more complex)
    top_rec = {}
    if stocks:
        top_rec_stock = max(stocks, key=lambda x: x.get('volume', 0))
        top_rec = {
            "symbol": top_rec_stock.get('symbol'),
            "company_name": top_rec_stock.get('company_name', 'Unknown'),
            "price": top_rec_stock.get('price'),
            "change_percent": 2.5, # Placeholder as we need history for real change
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
    # Mocking advanced recommendation engine consolidation
    stocks = await fetch_stock_snapshots()
    recs = []
    
    # Sort by some metric, e.g., low beta for safe, high beta for growth
    # This is a basic implementation to satisfy the schema
    sorted_stocks = sorted(stocks, key=lambda x: x.get('dividend', 0), reverse=True)[:limit]
    
    for i, s in enumerate(sorted_stocks):
        price = s.get('price', 1)
        div = s.get('dividend', 0)
        yield_pct = (div / price * 100) if price > 0 else 0
        
        recs.append({
            "rank": i + 1,
            "symbol": s.get('symbol'),
            "company_name": s.get('company_name', 'N/A'),
            "price": s.get('price'),
            "change_percent": 1.2, # Would need real calc
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
    # In a real app, this would query Influx for % change over period
    # Returning mock structure compatible with spec
    stocks = await fetch_stock_snapshots()
    return {
        "period": period,
        "generated_at": get_iso_now(),
        "gainers": [
            {
                "rank": i+1,
                "symbol": s.get('symbol'),
                "company_name": s.get('company_name'),
                "price": s.get('price'),
                "change_percent": 5.4 - (i*0.2), # Mock
                "change_value": 1.2,
                "previous_price": s.get('price') * 0.95,
                "volume": s.get('volume'),
                "avg_volume": s.get('volume'), # Placeholder
                "volume_ratio": 1.5,
                "sector": s.get('sector'),
                "high_24h": s.get('price') * 1.05,
                "low_24h": s.get('price') * 0.98
            } for i, s in enumerate(stocks[:limit])
        ]
    }

@app.get("/api/losers")
async def get_losers(period: str = "24h", limit: int = 10):
    stocks = await fetch_stock_snapshots()
    # Mocking losers from the bottom of the list or reversing
    reversed_stocks = stocks[::-1][:limit]
    return {
        "period": period,
        "generated_at": get_iso_now(),
        "losers": [
            {
                "rank": i+1,
                "symbol": s.get('symbol'),
                "company_name": s.get('company_name'),
                "price": s.get('price'),
                "change_percent": -3.2 - (i*0.1), # Mock
                "change_value": -0.5,
                "previous_price": s.get('price') * 1.03,
                "volume": s.get('volume'),
                "sector": s.get('sector'),
                "is_opportunity": True,
                "opportunity_reason": "Oversold"
            } for i, s in enumerate(reversed_stocks)
        ]
    }

@app.get("/api/alerts")
async def get_alerts(type: str = "all", limit: int = 50):
    # Generating Alerts based on current data
    stocks = await fetch_stock_snapshots()
    alerts = []
    for i, s in enumerate(stocks[:5]): # Generate 5 alerts
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
    # Similar to existing logic but updated response format
    # Reusing existing logic but wrapping in spec response
    stocks = await fetch_stock_snapshots()
    # Mock filtering for growth
    growth_stocks = [s for s in stocks if s.get('beta', 0) > 1.2][:5]
    
    data = []
    for s in growth_stocks:
        data.append({
            "symbol": s.get('symbol'),
            "company_name": s.get('company_name'),
            "price": s.get('price'),
            "change_5m": 1.2, # Mock
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
            "pe_ratio": 15.5, # Placeholder
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

@app.get("/api/stocks/{symbol}")
def get_stock_detail(symbol: str, range: str = Query("24h")):
    client = get_influx_client()
    query_api = client.query_api()
    
    # ... (Logic from previous implementation but updated response format)
    # Reusing query logic
    ranges = {
        "1h": {"start": "-1h", "window": "1m"},
        "24h": {"start": "-24h", "window": "15m"},
        "7d": {"start": "-7d", "window": "1h"},
        "30d": {"start": "-30d", "window": "6h"}
    }
    config = ranges.get(range, ranges["24h"])

    history_query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: {config["start"]})
      |> filter(fn: (r) => r["_measurement"] == "stock_price" and r["symbol"] == "{symbol}")
      |> filter(fn: (r) => r["_field"] == "price" or r["_field"] == "volume")
      |> aggregateWindow(every: {config["window"]}, fn: mean, createEmpty: false)
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''
    
    last_query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -24h)
      |> filter(fn: (r) => r["_measurement"] == "stock_price" and r["symbol"] == "{symbol}")
      |> last()
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    '''

    try:
        history_result = query_api.query(org=INFLUX_ORG, query=history_query)
        last_result = query_api.query(org=INFLUX_ORG, query=last_query)
        
        history = []
        for table in history_result:
            for r in table.records:
                history.append({
                    "time": r.get_time().isoformat(),
                    "price": round(r.values.get("price", 0), 2),
                    "volume": int(r.values.get("volume", 0))
                })
        
        info = {}
        for table in last_result:
            for r in table.records:
                info = r.values
        
        if not info and not history:
            raise HTTPException(status_code=404, detail="Symbol not found")

        price = float(info.get('price', 0))
        
        return {
            "symbol": symbol,
            "company_name": info.get('company_name', 'N/A'),
            "sector": info.get('sector', 'Unknown'),
            "industry": info.get('industry', 'Unknown'),
            "current": {
                "price": round(price, 2),
                "change_percent": 0.0, # Need open price to calc
                "change_value": 0.0,
                "volume": info.get('volume', 0),
                "market_cap": info.get('market_cap', 0),
                "beta": info.get('beta', 0),
                "dividend_yield": info.get('dividend', 0)
            },
            "statistics": {
                "high_24h": round(price * 1.05, 2), # Mock
                "low_24h": round(price * 0.95, 2),
                "high_52w": round(price * 1.2, 2),
                "low_52w": round(price * 0.8, 2),
                "avg_volume": info.get('volume', 0)
            },
            "signals": {
                "overall": "BUY",
                "technical": "BUY",
                "fundamental": "HOLD",
                "sentiment": "BULLISH"
            },
            "history": history
        }
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        client.close()

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

# --- Background Task ---
async def broadcast_market_updates():
    while True:
        if manager.active_connections:
            stocks = await fetch_stock_snapshots()
            if stocks:
                # Update format to match spec: {type: "price_update", data: {...}}
                # Emitting one per stock might be too much, usually bulk is better or selective
                # Spec shows "type": "price_update" with SINGLE stock object in "data".
                # We will emit updates for the top 5 movers to save bandwidth or all
                
                for s in stocks[:5]:
                     msg = {
                         "type": "price_update",
                         "data": {
                             "symbol": s.get("symbol"),
                             "price": s.get("price"),
                             "change_percent": 0.5, # Mock
                             "volume": s.get("volume"),
                             "timestamp": get_iso_now()
                         }
                     }
                     await manager.broadcast(msg)
                     
        await asyncio.sleep(5)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(broadcast_market_updates())

@app.websocket("/ws/live")
async def websocket_endpoint(websocket: WebSocket):
    print(f"WS: Connect {websocket.client}")
    try:
        await manager.connect(websocket)
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            manager.disconnect(websocket)
    except Exception as e:
        print(f"WS Error: {e}")
