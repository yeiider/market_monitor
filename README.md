# 🦈 Market Monitor Pro - Developer Documentation

Bienvenido a la documentación técnica de **Market Monitor Pro**. Este servicio expone datos financieros en tiempo real y métricas de inversión avanzadas a través de HTTP y WebSockets.

## 📡 WebSocket API (Real-Time)

Conéctate aquí para recibir actualizaciones de precios "Push" (sin polling).

**URL**: `ws://<HOST>:8000/ws/live-market`

### Flujo de Conexión
1.  El cliente establece conexión.
2.  El servidor acepta la conexión inmediatamente.
3.  El servidor enviará mensajes JSON automáticamente cuando detecte cambios en InfluxDB (cada ~5s).

### Formato de Mensaje (Server -> Client)
```json
{
    "type": "market_update",
    "data": [
        {
            "symbol": "NVDA",
            "price": 179.23,
            "change_percent": 0.0,
            "volume": 140065053,
            "timestamp": "2024-05-20T10:30:00Z"
        },
        ...
    ]
}
```

---

## ⚡️ HTTP API (REST)

### 1. Listado de Acciones
Obtén el estado actual del mercado con paginación y filtros.

`GET /stocks`

| Parámetro | Tipo | Default | Descripción |
| :--- | :--- | :--- | :--- |
| `page` | `int` | `1` | Número de página. |
| `limit` | `int` | `20` | Resultados por página. |
| `sector` | `string` | `null` | Filtra por sector (ej. `Technology`). |
| `min_market_cap` | `int` | `null` | Mínima capitalización de mercado. |

**Ejemplo:**
`GET /stocks?sector=Technology&page=1`

### 2. Estrategia "Value Investing"
Encuentra oportunidades de bajo riesgo y alto dividendo.

`GET /analysis/value`

| Parámetro | Tipo | Default | Descripción |
| :--- | :--- | :--- | :--- |
| `min_dividend_yield` | `float` | `2.0` | Mínimo retorno por dividendo (%). |
| `max_beta` | `float` | `1.5` | Máxima volatilidad permitida. |

### 3. Estrategia "Growth Momentum"
Detecta acciones con movimientos agresivos a corto plazo (Day Trading).

`GET /analysis/growth`

| Parámetro | Tipo | Default | Descripción |
| :--- | :--- | :--- | :--- |
| `min_momentum` | `float` | `3.0` | Mínimo % de subida en los últimos 5 min. |

---

## 🛠 Instalación y Despliegue

### Requisitos
- Docker & Docker Compose

### Iniciar Servicio
```bash
# 1. Configurar variables de entorno
cp .env.example .env

# 2. Levantar servicios
docker-compose up -d --build
```

### Probar Cliente Web
Abre el archivo `WS_TEST_CLIENT.html` en tu navegador para ver la demo en tiempo real.
