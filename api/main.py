import os
import asyncio
from fastapi import FastAPI, Query, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from influxdb_client import InfluxDBClient
from typing import List, Optional
from pydantic import BaseModel

app = FastAPI(title="Market Monitor Pro API", version="2.1")

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite cualquier origen (Frontend en Vercel, Localhost, etc.)
    allow_credentials=True,
    allow_methods=["*"],  # Permite todos los métodos (GET, POST, OPTIONS...)
    allow_headers=["*"],  # Permite todos los headers
)

# Config
INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")

def get_influx_client():
    return InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)

class Stock(BaseModel):
    symbol: str
    price: float
    company_name: str
    sector: str = "Unknown"
    industry: str = "Unknown"
    beta: float = 0.0
    market_cap: int = 0
    dividend_yield: float = 0.0
    volume: float = 0.0

# --- WebSocket Infrastructure ---

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                # Si falla el envío, asumimos desconexión en el próximo ciclo o dejamos que disconnect lo maneje
                pass

manager = ConnectionManager()

async def get_latest_market_data():
    """Consulta InfluxDB para obtener los datos más recientes de todas las acciones."""
    client = get_influx_client()
    query_api = client.query_api()
    
    # Consulta optimizada para traer el ÚLTIMO punto de cada acción en los últimos 2 minutos
    flux_query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -2m)
      |> filter(fn: (r) => r["_measurement"] == "stock_price")
      |> last()
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> group() 
    '''
    
    try:
        # Ejecutar en thread pool para no bloquear el loop async
        result = await asyncio.to_thread(query_api.query, org=INFLUX_ORG, query=flux_query)
        stocks = []
        for table in result:
            for r in table.records:
                price = r.values.get("price", 0)
                div = r.values.get("dividend", 0)
                div_yield = (div / price * 100) if price > 0 else 0
                
                stocks.append({
                    "symbol": r.values.get("symbol"),
                    "price": round(price, 2),
                    "change_percent": 0.0, # (Opcional: Calcular cambio vs apertura si se desea)
                    "volume": r.values.get("volume", 0),
                    "timestamp": r.values.get("_time").isoformat()
                })
        return stocks
    except Exception as e:
        print(f"WS Error reading Influx: {e}")
        return []
    finally:
        client.close()

async def broadcast_market_updates():
    """Tarea de fondo que envía actualizaciones cada 5 segundos."""
    while True:
        if manager.active_connections:
            data = await get_latest_market_data()
            if data:
                await manager.broadcast({"type": "market_update", "data": data})
        await asyncio.sleep(5)

@app.on_event("startup")
async def startup_event():
    # Iniciar el broadcaster en background
    asyncio.create_task(broadcast_market_updates())

@app.websocket("/ws/live-market")
async def websocket_endpoint(websocket: WebSocket):
    print(f"WS: New connection request from {websocket.client}")
    try:
        await manager.connect(websocket)
        print("WS: Connection accepted")
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            print("WS: Client disconnected")
            manager.disconnect(websocket)
        except Exception as e:
            print(f"WS: Error inside loop: {e}")
            manager.disconnect(websocket)
    except Exception as e:
        print(f"WS: Handshake/Connect failed: {e}")

# --- Standard Endpoints ---

@app.get("/")
def health():
    return {"status": "online", "version": "2.1 Real-Time", "service": "Market Monitor API"}

@app.get("/stocks", response_model=List[dict])
def list_stocks(
    page: int = 1,
    limit: int = 20,
    sector: Optional[str] = None,
    min_market_cap: Optional[int] = None
):
    """
    Lista paginada de acciones con sus últimos valores conocidos.
    Soporta filtrado por sector y capitalización de mercado.
    """
    client = get_influx_client()
    query_api = client.query_api()
    offset = (page - 1) * limit

    # Filtros dinámicos
    filters = '|> filter(fn: (r) => r["_measurement"] == "stock_price")'
    if sector:
        filters += f' |> filter(fn: (r) => r["tag_sector"] == "{sector}")' # Correction: In Flux, tags are accessed directly or via r["tagname"] if pivoting? 
        # Actually in pivot, tags are columns. But let's check how we stored it. 
        # In collector: .tag("sector", ...)
        # So after pivot, "sector" is a column.
        # But filter usually happens BEFORE pivot for efficiency.
        # If filtering before pivot: r.sector == "Technology"
    
    # Re-writing filter logic safely for before pivot:
    pre_pivot_filters = ''
    if sector:
        pre_pivot_filters += f' |> filter(fn: (r) => r["sector"] == "{sector}")'

    # Market Cap is a FIELD, so we must filter AFTER pivot
    
    flux_query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -10m)
      |> filter(fn: (r) => r["_measurement"] == "stock_price")
      {pre_pivot_filters}
      |> last()
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> group()
      {f'|> filter(fn: (r) => r["market_cap"] >= {min_market_cap})' if min_market_cap else ''}
      |> sort(columns: ["market_cap"], desc: true)
      |> limit(n: {limit}, offset: {offset})
    '''

    try:
        result = query_api.query(org=INFLUX_ORG, query=flux_query)
        stocks = []
        for table in result:
            for r in table.records:
                price = r.values.get("price", 0)
                div = r.values.get("dividend", 0)
                div_yield = (div / price * 100) if price > 0 else 0

                stocks.append({
                    "symbol": r.values.get("symbol"),
                    "company_name": r.values.get("company_name", "N/A"),
                    "price": round(price, 2),
                    "sector": r.values.get("sector", "Unknown"),
                    "industry": r.values.get("industry", "Unknown"),
                    "beta": r.values.get("beta", 0),
                    "market_cap": r.values.get("market_cap", 0),
                    "dividend_yield": round(div_yield, 2),
                    "volume": r.values.get("volume", 0)
                })
        return stocks
    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        client.close()

@app.get("/stocks/{symbol}")
def get_stock_detail(
    symbol: str, 
    range: str = Query("24h", description="Rango de tiempo: 1h, 24h, 7d")
):
    """
    Obtiene detalle histórico y métricas actuales de una acción específica.
    """
    client = get_influx_client()
    query_api = client.query_api()
    
    # Mapeo de rangos a ventanas de agregación
    ranges = {
        "1h": {"start": "-1h", "window": "1m"},
        "24h": {"start": "-24h", "window": "15m"},
        "7d": {"start": "-7d", "window": "1h"},
        "30d": {"start": "-30d", "window": "6h"}
    }
    config = ranges.get(range, ranges["24h"])
    
    # 1. Query para historial (Gráfica)
    history_query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: {config["start"]})
      |> filter(fn: (r) => r["_measurement"] == "stock_price" and r["symbol"] == "{symbol}")
      |> filter(fn: (r) => r["_field"] == "price")
      |> aggregateWindow(every: {config["window"]}, fn: mean, createEmpty: false)
      |> yield(name: "history")
    '''
    
    # 2. Query para último estado (Info actual)
    last_query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -24h)
      |> filter(fn: (r) => r["_measurement"] == "stock_price" and r["symbol"] == "{symbol}")
      |> last()
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> yield(name: "last")
    '''

    try:
        # Ejecutamos ambas queries
        # Nota: En producción idealmente se harían en paralelo o en una sola llamada multiscript
        history_result = query_api.query(org=INFLUX_ORG, query=history_query)
        last_result = query_api.query(org=INFLUX_ORG, query=last_query)
        
        # Procesar Historial
        history = []
        stats = {"min": float('inf'), "max": float('-inf'), "count": 0}
        
        for table in history_result:
            for r in table.records:
                val = r.get_value()
                if val is not None:
                    history.append({
                        "time": r.get_time().isoformat(),
                        "price": round(val, 2)
                    })
                    # Calc stats simple
                    if val < stats["min"]: stats["min"] = val
                    if val > stats["max"]: stats["max"] = val
                    stats["count"] += 1
        
        if stats["min"] == float('inf'): stats["min"] = 0
        if stats["max"] == float('-inf'): stats["max"] = 0

        # Procesar Último dato
        info = {}
        for table in last_result:
            for r in table.records:
                info = {
                    "symbol": r.values.get("symbol"),
                    "price": round(r.values.get("price", 0), 2),
                    "sector": r.values.get("sector", "Unknown"),
                    "industry": r.values.get("industry", "Unknown"),
                    "market_cap": r.values.get("market_cap", 0),
                    "beta": r.values.get("beta", 0),
                    "company_name": r.values.get("company_name", "N/A")
                }

        if not info and not history:
            raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found")

        return {
            "symbol": symbol,
            "range": range,
            "info": info,
            "statistics": {
                "min_price": round(stats["min"], 2),
                "max_price": round(stats["max"], 2),
                "data_points": stats["count"]
            },
            "history": history
        }

    except Exception as e:
        print(f"Error fetching detail: {e}")
        # Si es un 404 lanzado arriba, lo re-lanzamos
        if isinstance(e, HTTPException): raise e
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        client.close()

@app.get("/analysis/value")
def get_value_picks(min_dividend_yield: float = 2.0, max_beta: float = 1.5):
    """
    Estrategia VALUE: Encuentra acciones 'seguras' con buenos dividendos y volatilidad controlada.
    """
    client = get_influx_client()
    query_api = client.query_api()
    
    flux_query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -10m)
      |> filter(fn: (r) => r["_measurement"] == "stock_price")
      |> last()
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> group()
      |> map(fn: (r) => ({{
          r with
          div_yield: if r.price > 0.0 then (r.dividend / r.price) * 100.0 else 0.0
      }}))
      |> filter(fn: (r) => r.div_yield >= {min_dividend_yield} and r.beta <= {max_beta})
      |> sort(columns: ["div_yield"], desc: true)
      |> limit(n: 20)
    '''
    
    try:
        result = query_api.query(org=INFLUX_ORG, query=flux_query)
        picks = []
        for table in result:
            for r in table.records:
                picks.append({
                    "symbol": r["symbol"],
                    "strategy": "Value Investing",
                    "price": round(r["price"], 2),
                    "dividend_yield": round(r["div_yield"], 2),
                    "beta": r.values.get("beta", 0),
                    "reason": "High Yield & Low Volatility"
                })
        return {"strategy": "Value Picks", "count": len(picks), "data": picks}
    finally:
        client.close()

@app.get("/analysis/growth")
def get_growth_picks(min_momentum: float = 3.0):
    """
    Estrategia GROWTH: Acciones con alto momentum (subida rápida) y alto volumen.
    """
    client = get_influx_client()
    query_api = client.query_api()
    
    flux_query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -5m)
      |> filter(fn: (r) => r["_measurement"] == "stock_price" and r["_field"] == "price")
      |> group(columns: ["symbol"])
      |> sort(columns: ["_time"], desc: false)
      |> reduce(
          identity: {{first: 0.0, last: 0.0, count: 0}},
          fn: (r, accumulator) => ({{
            first: if accumulator.count == 0 then r._value else accumulator.first,
            last: r._value,
            count: accumulator.count + 1
          }})
      )
      |> map(fn: (r) => ({{
          symbol: r.symbol,
          price: r.last,
          change_percent: if r.first > 0.0 then ((r.last - r.first) / r.first) * 100.0 else 0.0
      }}))
      |> filter(fn: (r) => r.change_percent >= {min_momentum})
      |> sort(columns: ["change_percent"], desc: true)
      |> limit(n: 20)
    '''
    
    try:
        result = query_api.query(org=INFLUX_ORG, query=flux_query)
        picks = []
        for table in result:
            for r in table.records:
                picks.append({
                    "symbol": r["symbol"],
                    "strategy": "Aggressive Growth",
                    "price": round(r["price"], 2),
                    "change_5m": f"{round(r['change_percent'], 2)}%",
                    "signal": "STRONG BUY"
                })
        return {"strategy": "Growth Momentum", "count": len(picks), "data": picks}
    finally:
        client.close()
