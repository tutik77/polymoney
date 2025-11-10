Polymarket Top-500 Closed Positions Ingest

Quick start

1) Bring up Postgres + Adminer:

```bash
docker compose up -d
```

2) Create a `.env` (optional; defaults are fine). Suggested content:

```bash
POSTGRES_USER=polymoney
POSTGRES_PASSWORD=polymoney
POSTGRES_DB=polymoney
DATABASE_URL=postgresql+asyncpg://polymoney:polymoney@localhost:5432/polymoney
POLYMARKET_BASE_URL=https://polymarket.com
REQUEST_TIMEOUT_SECONDS=20
MAX_CONCURRENCY=8
REQUESTS_PER_SECOND=2
```

3) Install deps and run a single ingest iteration:

```bash
python -m pip install -r requirements.txt
python -m src.polymoney.ingest
```

Notes

- The HTTP client is a skeleton; wire it to the public JSON endpoints that power the profile "Closed" tab and leaderboard, or share the endpoints and I will complete it.
- The database schema is created automatically on first run.
- Adminer is available on http://localhost:8080 (System: PostgreSQL, Server: db, user/pass from env).


Trading (CLOB) quick start

1) Set env variables (add to `.env`):

```bash
# Required for trading
POLY_PRIVATE_KEY=0xYOUR_PRIVATE_KEY # used to sign orders (keep it secret!)

# Optional overrides
POLYMARKET_CLOB_HOST=https://clob.polymarket.com
POLY_CHAIN_ID=137
POLY_PROXY_WALLET=0xYourProxyOrFunderAddress
```

2) Install deps (includes py-clob-client):

```bash
python -m pip install -r requirements.txt
```

3) Place an order via CLI:

```bash
python -m src.polymoney.place_order_cli <OUTCOME_TOKEN_ID> <buy|sell> <PRICE> <SIZE> [--tif GTC]
# example:
python -m src.polymoney.place_order_cli 123456789012345678901234567890123456789012345678901234567890 buy 0.55 10 --tif GTC
```

Signing basics

- Orders are signed locally with your private key (EIP-712 typed data) and submitted to Polymarket's CLOB API.
- Never commit or share `POLY_PRIVATE_KEY`. Prefer `.env` and secrets management in production.
- Ensure your account has USDC balance and required allowances on the target chain.


FastAPI API (optional)

1) Install deps:

```bash
python -m pip install -r requirements.txt
```

2) Run the API server:

```bash
python -m src.polymoney.api.main
# or with uvicorn directly
# uvicorn src.polymoney.api.main:app --host 0.0.0.0 --port 8000 --reload
```

3) Browse docs at `http://localhost:8000/docs`.

Available endpoints (non-exhaustive):

- GET `/health` — health check
- GET `/users/{address}` — user info
- GET `/users/{address}/closed-positions` — latest closed positions from DB
- GET `/users/{address}/active-positions` — active positions from DB
- GET `/users/{address}/activities` — recent activities from DB
- POST `/admin/ingest/once` — schedule one ingest run (top leaderboard)
- POST `/admin/activities/follow/{address}` — start following a user's activities (background)
- DELETE `/admin/activities/follow/{address}` — stop following
- POST `/trading/orders` — place a limit order via CLOB

Realtime simulation API

- POST `/sim/realtime/start` — body: `{ users: ["0x..."], sim_user?: "id", initial_cash?, poll_interval?, slippage_bps?, max_trade_size? }`
- DELETE `/sim/realtime/stop/{sim_user}/{address}` — остановить задачу по паре (sim_user, адрес)
- GET `/sim/realtime` — список активных задач (ключи вида `simUser:address`)
- GET `/sim/realtime/portfolio` — снимки портфелей в памяти (per leader)
- GET `/sim/realtime/portfolio/{address}` — снимок по адресу (для `sim_user=default`)
- GET `/sim/db/{sim_user}/portfolio` — агрегированный глобальный портфель (учитывает всех лидеров; стартовый кэш учитывается один раз)

Docker usage

1) Build and start all services (db, adminer, api):

```bash
docker compose up -d --build
```

2) API docs: `http://localhost:8000/docs`

3) Adminer: `http://localhost:8080` (System: PostgreSQL, Server: db, user/pass from .env)

Note: If you have a local `.env` with `DATABASE_URL=...localhost...`, it does not affect the API container now (compose sets a container-internal `DATABASE_URL` pointing to `db`). For local (non-docker) runs, keep `localhost`; for docker, the API uses `db` hostname.

Elasticsearch (optional)

- By default Elasticsearch/Kibana are disabled and logging goes to stdout.
- To run WITH Elasticsearch+Kibana and enable ES logging:

```bash
docker compose -f docker-compose.yml -f docker-compose.es.yml up -d --build
```

Then open Kibana: `http://localhost:5601`, Elasticsearch: `http://localhost:9200`.

Lightweight logs with UI

- File logs with rotation are enabled by default to `/app/logs/<service>.log` (inside containers). Tune via env:
  - `LOG_TO_FILE=true|false` (default true)
  - `LOG_DIR=/app/logs`
  - `LOG_FILE_MAX_MB=5`
  - `LOG_FILE_BACKUP_COUNT=3`
- Simple web UI for Docker logs via Dozzle:
  - Compose already includes `dozzle` (port `9999`).
  - Open `http://localhost:9999` to browse container logs in a UI.
- Tail logs via API (served by the `api` container, reading from shared `app_logs` volume):
  - `GET /logs/tail?service=api&lines=200`
  - Services: `api`, `celery_worker`, `celery_beat`

Use .env with Docker

1) Скопируй шаблон и отредактируй:

```bash
cp env.example .env
# укажи свои значения (например, REQUESTS_PER_SECOND, MAX_CONCURRENCY, POLY_PRIVATE_KEY)
```

2) Compose пробросит переменные внутрь контейнера `api` через `env_file: .env`. Значения из `environment:` в compose имеют приоритет над `.env` (например, `DATABASE_URL` внутри контейнера принудительно указывает на `db`).

HTTP client rate limits

- Переменные (можно переопределить в `.env`):
  - `REQUESTS_PER_SECOND` (стартовая), по умолчанию 4
  - `MIN_REQUESTS_PER_SECOND` минимум 1
  - `MAX_REQUESTS_PER_SECOND` максимум 6
  - `ADAPT_UP_SUCCESSES` успешных запросов до повышения RPS, по умолчанию 12
  - `ADAPT_DOWN_FACTOR` множитель снижения при 429, по умолчанию 0.8
  - `ADAPT_UP_FACTOR` множитель роста, по умолчанию 1.2

Клиент не опускает RPS ниже 1 и адаптирует скорость при 429.

Activities fetch (pagination)

- `ACTIVITIES_PAGE_SIZE` — размер страницы, по умолчанию 100
- `ACTIVITIES_MAX_PAGES_PER_POLL` — максимум страниц за один опрос (защита от потери событий при всплесках), по умолчанию 10


Dry-run simulator (closed positions)

1) Ensure DB and deps are ready:

```bash
python -m pip install -r requirements.txt
docker compose up -d db adminer
```

2) Run the simulator for top 10 users with a $10,000,000 bankroll (refresh ingest by default):

```bash
python -m src.polymoney.simulate_dry --top-n 10 --initial-cash 10000000
```

3) To reuse existing DB without fetching fresh data:

```bash
python -m src.polymoney.simulate_dry --no-refresh
```

The script prints a JSON summary with total and per-user PnL. It computes net PnL from closed positions as:

- quantity × (exit_avg_price − entry_avg_price) − fees_total

Assumptions: large bankroll (no capital constraints), replicate 1:1 amounts, ignore slippage.

Realtime dry-run (activities with live quotes)

1) Следим за пользователем/пользователями и копируем их сделки в симуляции по актуальным котировкам:

```bash
python -m src.polymoney.simulate_realtime --user 0xUSER1 --user 0xUSER2 --initial-cash 10000000 \
  --slippage-bps 0 --max-trade-size 1000
```

Параметры:

- slippage-bps: опционально, наценка/скидка к live-цене (по умолчанию 0)
- max-trade-size: ограничение размера копируемой сделки (по акциям)

Логика: зеркалим количество акций лидера (size из события) и исполняем по текущей live цене. Сумма сделки при этом может отличаться от суммы лидера (из‑за другой цены). Продажи ограничены текущими остатками. Live-цена берётся напрямую из CLOB best bid/ask по `asset` (tokenId); при недоступности — fallback к цене из события. Поддерживается список `--user` (можно указывать несколько флагов).

Дополнительно: симулятор поддерживает глобальный портфель на уровне `sim_user` (стартовый кэш учитывается один раз), а также хранит позиции и сделки по каждому лидеру отдельно для аналитики.

