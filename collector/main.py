import os
import time
import requests
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Configuración desde variables de entorno
API_KEY = os.getenv("FMP_API_KEY")
INFLUX_URL = os.getenv("INFLUX_URL")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN")
INFLUX_ORG = os.getenv("INFLUX_ORG")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET")
INTERVAL = int(os.getenv("INTERVAL_SECONDS", 30))

# URL con tus filtros validados (Tech, NASDAQ, >$10, etc.)
# Nota: Quitamos el "limit" o lo ponemos muy alto para traer todo lo que cumpla
URL = (
    f"https://financialmodelingprep.com/stable/company-screener?apikey={API_KEY}"
    "&priceMoreThan=10"
    "&marketCapMoreThan=1000000"
    "&sector=Technology"
    "&exchange=NASDAQ"
    "&isEtf=false"
    "&isFund=false"
    "&isActivelyTrading=true"
    "&limit=4000"
)

def fetch_and_store():
    client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    print(f"📡 [Collector] Consultando FMP Screener...", flush=True)
    print(f"📡 [Collector] Consultando FMP API-TOKEN... "+ API_KEY, flush=True)
    try:
        start_time = time.time()
        response = requests.get(URL, timeout=10)
        
        if response.status_code != 200:
            print(f"⚠️ Error API FMP: {response.status_code} - {response.text}", flush=True)
            return

        data = response.json()
        points = []
        
        for item in data:
            symbol = item.get('symbol')
            price = item.get('price')
            
            if symbol and price:
                # Guardamos: Precio crudo + Tags útiles para filtrar luego
                p = Point("stock_price") \
                    .tag("symbol", symbol) \
                    .tag("industry", item.get('industry', 'Unknown')) \
                    .tag("sector", item.get('sector', 'Unknown')) \
                    .field("price", float(price)) \
                    .field("volume", float(item.get('volume', 0))) \
                    .field("beta", float(item.get('beta', 0) or 0)) \
                    .field("market_cap", int(item.get('marketCap', 0) or 0)) \
                    .field("dividend", float(item.get('lastAnnualDividend', 0) or 0)) \
                    .field("company_name", item.get('companyName', 'Unknown'))
                points.append(p)
        
        if points:
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
            elapsed = time.time() - start_time
            print(f"✅ [Collector] {len(points)} acciones guardadas en {elapsed:.2f}s", flush=True)
        else:
            print("⚠️ [Collector] La lista llegó vacía.", flush=True)

    except Exception as e:
        print(f"❌ [Collector] Error crítico: {e}", flush=True)
    finally:
        client.close()

if __name__ == "__main__":
    print(f"🚀 Iniciando Collector cada {INTERVAL} segundos...", flush=True)
    while True:
        fetch_and_store()
        time.sleep(INTERVAL)
