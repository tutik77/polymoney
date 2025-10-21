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


