# Settlement Logic - Backup для реализации в старой версии

## Дата создания: 2025-11-29

## Описание
Это документация новой логики settlement, которая определяет исход ставок по `condition_id` и `token_id`.
Этот файл создан для сохранения логики перед откатом проекта к старому коммиту.

---

## Ключевая идея

**НОВАЯ УЛУЧШЕННАЯ ЛОГИКА SETTLEMENT:**

Вместо проверки закрытых позиций лидера (ненадежно!), мы напрямую получаем данные о резолюции рынков из Gamma API.

### Алгоритм:

1. **Находим активные позиции** где `end_date <= NOW()`
2. **Группируем по `condition_id`** (НЕ по лидеру!)
3. **Для каждого `condition_id`** запрашиваем данные рынка из Gamma API
4. **Получаем фактический исход резолюции** из `market.tokens[].price`
5. **Сеттлим позиции** на основе цены токена:
   - `price = 1.0` → Выиграл
   - `price = 0.0` → Проиграл
   - `price = другое значение` → Частичный исход (редко)
6. **Фоллбэк**: принудительный сеттлмент просроченных позиций по `avg_cost`

---

## 1. Основной метод settlement

**Файл:** `src/services/settlement.py`

**Функция:** `settle_resolved_positions()`

```python
async def settle_resolved_positions(
    sim_user: str,
    *,
    force_settle_after_days: int = 3,
    fetch_limit: int = 1000,
) -> SettlementResult:
    """
    Settle resolved positions for a simulator using DIRECT market resolution data.

    NEW IMPROVED LOGIC:
    1. Find SimActivePosition where end_date <= NOW()
    2. Group by condition_id (NOT leader!)
    3. For each condition_id, fetch market data from Gamma API
    4. Get actual resolution outcome from market.tokens[].price
    5. Settle positions based on token price (1.0 = won, 0.0 = lost)
    6. Fallback: force settle expired positions at avg_cost

    This is MUCH MORE RELIABLE than checking leader's closed positions!
    """
    log = structlog.get_logger()
    now = datetime.now(timezone.utc)
    force_settle_threshold = now - timedelta(days=force_settle_after_days)

    settled_positions: List[SettledPosition] = []
    total_pnl = 0.0
    total_cash_change = 0.0

    async with session_scope() as session:
        # Получаем активные позиции, у которых прошла end_date
        stmt = select(SimActivePosition).where(
            SimActivePosition.sim_user == sim_user,
            SimActivePosition.end_date.isnot(None),
            SimActivePosition.end_date <= now,
        )
        result = await session.execute(stmt)
        active_positions = list(result.scalars().all())

        if not active_positions:
            return SettlementResult(
                sim_user=sim_user,
                settled_count=0,
                total_pnl=0.0,
                total_cash_change=0.0,
                positions=[],
            )

        log.info("settlement_start", sim_user=sim_user, positions=len(active_positions))

        # Группируем позиции по condition_id для эффективного batch-резолюшена
        positions_by_condition: Dict[str, List[SimActivePosition]] = {}
        positions_without_condition: List[SimActivePosition] = []
        
        for pos in active_positions:
            cond_id = pos.condition_id
            if not cond_id:
                positions_without_condition.append(pos)
                continue
            if cond_id not in positions_by_condition:
                positions_by_condition[cond_id] = []
            positions_by_condition[cond_id].append(pos)

        # Предупреждаем о позициях без condition_id
        if positions_without_condition:
            log.warning(
                "settlement_no_condition",
                count=len(positions_without_condition),
                sim_user=sim_user,
            )

        # Обрабатываем позиции каждого condition_id
        async with PolymarketClient() as client:
            for condition_id, cond_positions in positions_by_condition.items():
                # Запрашиваем детали рынка из Gamma API (ПРЯМЫЕ данные резолюции!)
                market_data = await client.fetch_market_by_condition_id(condition_id)

                if not market_data:
                    log.debug(
                        "settlement_market_not_found",
                        condition_id=condition_id[:32],
                        count=len(cond_positions),
                    )
                    # Фоллбэк: принудительный сеттлмент просроченных позиций
                    for pos in cond_positions:
                        if pos.end_date and pos.end_date < force_settle_threshold:
                            await _settle_single_position(
                                session=session,
                                pos=pos,
                                payout=float(pos.avg_cost),
                                settlement_type="expired_no_market",
                                now=now,
                                sim_user=sim_user,
                                settled_positions=settled_positions,
                            )
                            total_pnl += (float(pos.avg_cost) - float(pos.avg_cost)) * float(pos.quantity)
                            total_cash_change += float(pos.avg_cost) * float(pos.quantity)
                    continue

                # Извлекаем данные резолюции из рынка
                tokens = market_data.get("tokens", [])
                is_closed = market_data.get("closed", False)
                
                # Строим маппинг: token_id -> price
                token_prices: Dict[str, float] = {}
                for token in tokens:
                    token_id = token.get("token_id")
                    price = token.get("price")
                    if token_id and price is not None:
                        try:
                            token_prices[str(token_id)] = float(price)
                        except (ValueError, TypeError):
                            pass

                # Обрабатываем каждую позицию для этого condition
                for pos in cond_positions:
                    asset = pos.asset
                    quantity = float(pos.quantity)
                    avg_cost = float(pos.avg_cost)
                    end_date = pos.end_date

                    payout: Optional[float] = None
                    settlement_type: str = ""

                    # Проверяем, есть ли у этого asset резолвенутая цена
                    if asset in token_prices:
                        price = token_prices[asset]
                        # Резолвенутые рынки имеют price = 1.0 (выиграл) или 0.0 (проиграл)
                        if abs(price - 1.0) < 0.01:
                            payout = 1.0
                            settlement_type = "resolved_won"
                        elif abs(price) < 0.01:
                            payout = 0.0
                            settlement_type = "resolved_lost"
                        elif is_closed:
                            # Рынок закрыт, но цена не ровно 0 или 1
                            # Может быть частичный исход или edge case
                            payout = price
                            settlement_type = "resolved_partial"
                    
                    # Фоллбэк: принудительный сеттлмент если слишком старая
                    if payout is None and end_date and end_date < force_settle_threshold:
                        payout = avg_cost
                        settlement_type = "expired"
                    
                    # Пропускаем, если не готово к сеттлменту
                    if payout is None:
                        continue

                    # Сеттлим эту позицию
                    await _settle_single_position(
                        session=session,
                        pos=pos,
                        payout=payout,
                        settlement_type=settlement_type,
                        now=now,
                        sim_user=sim_user,
                        settled_positions=settled_positions,
                    )
                    
                    redemption_cash = payout * quantity
                    realized_pnl = (payout - avg_cost) * quantity
                    total_pnl += realized_pnl
                    total_cash_change += redemption_cash

    result = SettlementResult(
        sim_user=sim_user,
        settled_count=len(settled_positions),
        total_pnl=total_pnl,
        total_cash_change=total_cash_change,
        positions=settled_positions,
    )

    log.info(
        "settlement_done",
        sim_user=sim_user,
        count=result.settled_count,
        pnl=total_pnl,
        cash=total_cash_change,
    )

    return result
```

---

## 2. Polymarket Client - получение данных рынка

**Файл:** `src/polymarket_client.py`

### Метод 1: `fetch_market_by_condition_id()`

Это ключевой метод для получения данных резолюции!

```python
async def fetch_market_by_condition_id(
    self, condition_id: str, *, fetch_prices: bool = True
) -> Optional[Dict[str, Any]]:
    """
    Fetch market details by condition_id from Gamma API.
    
    Returns market object with fields including:
    - tokens: array of outcome tokens (with prices if fetch_prices=True)
    - active: whether market is active
    - closed: whether market is closed
    - umaResolutionStatus: resolution status from UMA Oracle
    - outcomes: array of outcome names
    
    This is useful for determining market resolution outcomes directly.
    
    Args:
        condition_id: The condition ID (CTF condition ID)
        fetch_prices: If True, fetch current prices for all tokens (default: True)
        
    Returns:
        Market object dict or None if not found
    """
    gamma_api = "https://gamma-api.polymarket.com"
    url = f"{gamma_api}/markets"
    params = {"condition_ids": condition_id, "closed": "true"}
    
    try:
        data = await self._get_json(url, params=params)
        # Response is an array of markets
        if isinstance(data, list) and len(data) > 0:
            market = data[0]
            
            # Parse JSON strings that Gamma API returns
            # NOTE: Gamma API returns these as JSON-encoded STRINGS, not arrays!
            clob_token_ids_str = market.get("clobTokenIds", "[]")
            outcomes_str = market.get("outcomes", "[]")
            outcome_prices_str = market.get("outcomePrices", "[]")
            
            try:
                clob_token_ids = orjson.loads(clob_token_ids_str)
                outcomes = orjson.loads(outcomes_str)
                outcome_prices = orjson.loads(outcome_prices_str)
            except Exception:
                # Fallback if parsing fails
                clob_token_ids = []
                outcomes = []
                outcome_prices = []
            
            # Build tokens array from the parsed data
            tokens = []
            for i, token_id in enumerate(clob_token_ids):
                token_data = {
                    "token_id": token_id,
                    "outcome": outcomes[i] if i < len(outcomes) else f"Outcome {i+1}",
                }
                
                # Add price from outcomePrices if available
                if i < len(outcome_prices):
                    try:
                        token_data["price"] = float(outcome_prices[i])
                    except (ValueError, TypeError):
                        token_data["price"] = None
                
                # Optionally fetch live price from CLOB API (more accurate but slower)
                if fetch_prices and token_data.get("price") is None:
                    live_price = await self.fetch_token_price(token_id)
                    token_data["price"] = live_price
                
                tokens.append(token_data)
            
            # Inject the properly formatted tokens array and outcomes
            market["tokens"] = tokens
            market["outcomes"] = outcomes  # parsed array, not string
            
            return market
        return None
    except Exception as e:
        real_e = e
        if isinstance(e, RetryError):
            real_e = e.last_attempt.exception()
        
        self._log.warning(
            "fetch_market_error",
            condition_id=condition_id[:32],
            error=str(real_e)[:200],
        )
        return None
```

### Метод 2: `fetch_token_price()`

Дополнительный метод для получения цены конкретного токена:

```python
async def fetch_token_price(self, token_id: str) -> Optional[float]:
    """
    Fetch current price for a specific outcome token.
    
    This uses the CLOB API /price endpoint which returns the last traded price.
    For resolved markets, this will be 1.0 (winning outcome) or 0.0 (losing outcome).
    
    Args:
        token_id: The outcome token ID (asset ID)
        
    Returns:
        Current price as float or None if unavailable
    """
    url = f"{self._clob_api}/price"
    params = {"token_id": token_id}
    
    try:
        data = await self._get_json(url, params=params)
        if not isinstance(data, dict):
            return None
            
        price = data.get("price")
        if price is not None:
            return float(price)
        return None
    except Exception as e:
        real_e = e
        if isinstance(e, RetryError):
            real_e = e.last_attempt.exception()
        
        err_msg = str(real_e)
        status = None
        if isinstance(real_e, aiohttp.ClientResponseError):
            status = real_e.status
            err_msg = f"Http {status}: {real_e.message}"
        
        # 404 is expected for closed/removed tokens - log as debug
        log_level = self._log.debug if status == 404 else self._log.warning
        log_level(
            "token_price_error",
            token=token_id[:20],
            error=err_msg,
            status=status,
        )
        return None
```

---

## 3. Тестовый эндпоинт для проверки

**Файл:** `src/api/routers/test.py`

**Эндпоинт:** `GET /test/market/{condition_id}`

Этот эндпоинт позволяет вручную проверить резолюцию рынка и статус ставки:

```python
@router.get(
    "/test/market/{condition_id}",
    response_model=MarketResolutionResponse,
    summary="Get market resolution by condition_id",
    description="""
    Fetch market details and resolution status from Gamma API.
    
    Optional: Provide `asset_id` (token_id) to check if a specific bet won or lost.
    
    This endpoint shows:
    - Market question
    - Whether market is closed/active
    - All outcome tokens with their current prices
    - **User Bet Status**: WON/LOST if asset_id is provided
    
    **Use this to check if a market has been resolved!**
    
    Example condition_id: 0x1234567890abcdef...
    """,
)
async def get_market_resolution(
    condition_id: str,
    asset_id: Optional[str] = Query(None, description="Token ID of your bet to check status")
) -> MarketResolutionResponse:
    """Get market resolution information."""
    async with PolymarketClient() as client:
        market = await client.fetch_market_by_condition_id(condition_id)
        
        if not market:
            raise HTTPException(
                status_code=404,
                detail=f"Market with condition_id '{condition_id}' not found"
            )
        
        # Process tokens
        tokens_info: List[TokenInfo] = []
        raw_tokens = market.get("tokens", [])
        
        user_bet_status = None
        
        for token in raw_tokens:
            token_id = token.get("token_id")
            price = token.get("price")
            outcome = token.get("outcome", "Unknown")
            
            winner = None
            if price is not None:
                price_float = float(price)
                # Определяем победителя по цене токена
                if abs(price_float - 1.0) < 0.01:
                    winner = True  # Выиграл
                elif abs(price_float) < 0.01:
                    winner = False  # Проиграл
            
            tokens_info.append(
                TokenInfo(
                    token_id=token_id or "",
                    outcome=outcome,
                    price=float(price) if price is not None else None,
                    winner=winner,
                )
            )
            
            # Проверяем, это ли asset пользователя
            if asset_id and token_id == asset_id:
                status = "PENDING"
                payout = float(price) if price is not None else 0.0
                
                if winner is True:
                    status = "WON"
                    payout = 1.0
                elif winner is False:
                    status = "LOST"
                    payout = 0.0
                
                user_bet_status = UserBetStatus(
                    token_id=token_id,
                    outcome_name=outcome,
                    status=status,
                    payout=payout
                )
        
        return MarketResolutionResponse(
            condition_id=market.get("condition_id", condition_id),
            question=market.get("question", "N/A"),
            closed=market.get("closed", False),
            active=market.get("active", True),
            umaResolutionStatus=market.get("umaResolutionStatus"),
            tokens=tokens_info,
            user_bet_status=user_bet_status,
        )
```

---

## 4. Структуры данных

### SettledPosition

```python
@dataclass
class SettledPosition:
    """Details of a settled position."""

    asset: str
    leader_address: str
    quantity: float
    avg_cost: float
    payout: float
    realized_pnl: float
    settlement_type: str  # "resolved_won", "resolved_lost", "expired", etc.
    end_date: Optional[datetime] = None
    title: Optional[str] = None
```

### SettlementResult

```python
@dataclass
class SettlementResult:
    """Result of settlement operation."""

    sim_user: str
    settled_count: int
    total_pnl: float
    total_cash_change: float
    positions: List[SettledPosition]
```

---

## 5. Ключевые моменты реализации

### 5.1 Gamma API эндпоинты:

- **Base URL:** `https://gamma-api.polymarket.com`
- **Endpoint:** `GET /markets?condition_ids={condition_id}&closed=true`

### 5.2 Важные поля в ответе Gamma API:

- `clobTokenIds` - JSON-строка с массивом token_id
- `outcomes` - JSON-строка с массивом названий исходов
- `outcomePrices` - JSON-строка с массивом цен (для резолвенутых рынков)
- `closed` - булево, закрыт ли рынок
- `active` - булево, активен ли рынок
- `umaResolutionStatus` - статус резолюции от UMA Oracle

⚠️ **ВАЖНО:** Gamma API возвращает эти поля как JSON-строки, их нужно парсить с помощью `orjson.loads()`!

### 5.3 Логика определения победителя:

```python
if abs(price - 1.0) < 0.01:
    # Выиграл - получает 1.0 за каждый токен
    payout = 1.0
    settlement_type = "resolved_won"
elif abs(price) < 0.01:
    # Проиграл - получает 0.0
    payout = 0.0
    settlement_type = "resolved_lost"
elif is_closed:
    # Частичный исход (редкий случай)
    payout = price
    settlement_type = "resolved_partial"
```

### 5.4 Фоллбэк для старых позиций:

Если позиция старше `force_settle_after_days` (по умолчанию 3 дня), она принудительно сеттлится:

```python
if payout is None and end_date and end_date < force_settle_threshold:
    payout = avg_cost  # Возврат средней стоимости
    settlement_type = "expired"
```

---

## 6. Примеры использования

### 6.1 Проверка резолюции рынка через API:

```bash
# Без asset_id - показывает все исходы
GET /test/market/0x123abc...

# С asset_id - показывает статус конкретной ставки
GET /test/market/0x123abc...?asset_id=21742633143463906290569050155826241533067272736897614950488156847949938836455
```

### 6.2 Запуск settlement:

```python
from src.services.settlement import settle_resolved_positions

result = await settle_resolved_positions(
    sim_user="default",
    force_settle_after_days=3
)

print(f"Settled {result.settled_count} positions")
print(f"Total PNL: {result.total_pnl}")
print(f"Cash change: {result.total_cash_change}")
```

---

## 7. Зависимости

### Необходимые импорты:

```python
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
import structlog
from sqlalchemy import select
import orjson
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError
```

### Модели базы данных:

- `SimActivePosition` - активные позиции
  - Поля: `sim_user`, `asset`, `condition_id`, `quantity`, `avg_cost`, `end_date`, `leader_address`, `title`
- `SimClosedPosition` - закрытые позиции
- `SimGlobalPortfolio` - глобальное портфолио симулятора

---

## 8. Чеклист для реализации в старой версии

- [ ] Добавить в `PolymarketClient`:
  - [ ] `fetch_market_by_condition_id()` метод
  - [ ] `fetch_token_price()` метод
- [ ] Обновить `settlement.py`:
  - [ ] Новая логика `settle_resolved_positions()`
  - [ ] Группировка по `condition_id`
  - [ ] Использование `fetch_market_by_condition_id()`
  - [ ] Определение payout по token price
- [ ] Добавить тестовый эндпоинт (опционально):
  - [ ] `GET /test/market/{condition_id}`
  - [ ] Схемы `MarketResolutionResponse`, `TokenInfo`, `UserBetStatus`
- [ ] Проверить что в модели `SimActivePosition` есть поле `condition_id`
- [ ] Тестирование:
  - [ ] Проверить резолюцию выигранной позиции
  - [ ] Проверить резолюцию проигранной позиции
  - [ ] Проверить фоллбэк для старых позиций

---

## 9. Отличия от старой логики

### Старая логика (ненадежная):
- Полагалась на закрытые позиции лидера
- Требовала парсинг activities лидера
- Не работала, если лидер удалял данные или ставка была сделана не лидером

### Новая логика (надежная):
- ✅ Прямой запрос к Gamma API
- ✅ Независимость от действий лидера
- ✅ Группировка по `condition_id` для эффективности
- ✅ Надежное определение исхода по `token.price`
- ✅ Фоллбэк для edge cases

---

## Заметки

- Эта логика была протестирована и работает корректно
- Gamma API - официальный API Polymarket для получения данных рынков
- CLOB API используется как фоллбэк для получения live цен
- Логика учитывает edge cases (частичные исходы, устаревшие позиции)

---

Конец документа.
