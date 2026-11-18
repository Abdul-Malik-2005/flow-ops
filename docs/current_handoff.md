# FlowOps — Текущий статус (Current Handoff)

**Актуальный сервис:** `services/order` (блоки 1–5 **закрыты**)
**Статус:** 147 тестов зелёные (115 unit/API + 32 интеграционных на реальном Postgres), `ruff check`/`ruff format --check` чисто, `alembic upgrade head` + `check` → "No new upgrade operations detected" (`c7d2e5f13a08`), `downgrade -1` + `upgrade` проверены.
**Следующая задача:** блок 6 — реконсилер «зависших» `pending` (повторный `reserve` по `order_id`; Inventory идемпотентен). `services/inventory` и `services/identity` **не трогать**.

---

## Что готово (блок 5: сага + API + идемпотентность + пагинация)
- **Конфиг:** `RESERVATION_STALE_AFTER_SECONDS` с `model_validator` (`>= 2 * INVENTORY_TIMEOUT_SECONDS`); иначе lease ничего не доказывает. Есть в `.env.example` и `compose.yaml`.
- **Домен (`app/domain/`):** `OrderReason` (cancellation/failure наборы не пересекаются), таблица `ALLOWED_TRANSITIONS` (`reserved` без исходящих рёбер), `normalize_lines()` (схлопывание дублей + детерминированный порядок), `canonical_request_hash()`, `ensure_transition/reason`.
- **Схемы:** `extra="forbid"` на входных моделях (`user_id` в теле → 422), `MAX_ORDER_LINES=100`; ответ не отдаёт `user_id`, `idempotency_key`, `request_hash`.
- **БД:** колонка `orders.reservation_started_at` + обратимая миграция `c7d2e5f13a08` (nullable, `NULL` = «попытки вызова не было»).
- **Клиент Inventory (`app/clients/inventory.py`):** `InventoryClientPort` + httpx-реализация; один `httpx.AsyncClient` на процесс в `app.state` (создаётся/закрывается в `lifespan`), DI через `get_inventory_client`. Маппинг: 201/200→`reserved`, 409→`rejected`, 404→`item_not_found`, 400/422→`invalid_request`, 429/5xx/timeout/битый 2xx→`unavailable`. **Клиент не бросает исключений на бизнес-ответы.**
- **Pricing (`app/services/pricing.py`):** `PricingPort` + `NullPricing`; `total_amount()` → `None`, если хотя бы одна строка не оценена (никаких частичных сумм).
- **Репозиторий (`app/repositories/order.py`):** `OrderCursor`, `CancelStatus`, `SqlOrderRepository`; идемпотентность — `pg_insert(...).on_conflict_do_nothing(index_where=...)` (без savepoint, сессия остаётся живой); переходы — compare-and-set `UPDATE ... WHERE status = :expected`; отмена — под lease (`reservation_started_at IS NULL OR < now() - make_interval(secs => :x)`). **Репозиторий никогда не коммитит** — транзакциями владеет сервис.
- **Сервис (`app/services/order.py`):** сага `TX1 (insert pending + history) → COMMIT → HTTP reserve → TX2 (применить исход + lease)`; повтор с тем же `Idempotency-Key` и тем же телом → 200 + исходный заказ; с другим телом → 409.
- **API (`/api/v1/orders`):** POST (201/200/202/409/422/502), GET list (keyset, opaque base64url-курсор, битый → 400), GET one (404 и для чужого), POST `{id}/cancel` (200/404/409, `reason` в теле → 422). Роутер подключён в `app/main.py` под `/api/v1`, `/health` и `/ready` остались в корне.

## Тесты

- **115 unit/API:** `test_health` (4), `test_security` (8), `test_deps` (5), `test_config` (4), `test_domain` (29), `test_schemas` (10), `test_inventory_client` (16, `MockTransport`), `test_pricing` (5), `test_pagination` (9), `test_orders_api` (25, `FakeOrderService` + `dependency_overrides`).
- **32 интеграционных** (`tests/integration/`): 18 репозиторий + 14 сага (включая доказательство, что TX1 закоммичен **до** HTTP-вызова, и кейс «отмена выиграла гонку»).
- **Изоляция:** `conftest.py` — дроп схемы → `alembic upgrade head` → `TRUNCATE` перед каждым тестом; guard на имя БД (`_test_db`) и `REQUIRE_INTEGRATION_TESTS=1` → hard fail вместо тихого skip.
- Локально без `TEST_DATABASE_URL`: 115 passed, 32 skipped (skip, не error).

## CI (`.github/workflows/ci.yml`, job `order`)

`ruff format --check` → `ruff check` → `alembic upgrade head` → `alembic check` → `pytest -v` → `docker build`; добавлен `REQUIRE_INTEGRATION_TESTS: "1"`.

## План следующего шага (блок 6: реконсилер)

1. `app/services/reconciler.py` — выбирает `pending` старше `RESERVATION_STALE_AFTER_SECONDS` (partial-индекс `(created_at) WHERE status='pending'` уже есть), повторяет `reserve` по тому же `order_id`, применяет исход как TX2.
2. Пачка через `FOR UPDATE SKIP LOCKED`: два инстанса не сделают один и тот же вызов.
3. Логи/метрики: `pending` старше N минут → `error` с `order_id` (симптом «Inventory лежит»).
4. Тесты: восстановление после timeout и после «reserve применился, TX2 — нет».

**Вопросы к согласованию:**
1. Реконсилер как отдельный процесс (entrypoint + `asyncio.sleep`) или `POST /internal/reconcile` под cron? (Влияет на деплой и на необходимость сервисного токена.)
2. Сколько попыток и когда переводим `pending → failed` (`reservation_unavailable`)? Нужна ли отдельная причина `reservation_expired`?
3. Outbox в этом же блоке или отдельным? RabbitMQ в `compose.yaml` пока нет.

## Команды

```
# из корня репозитория
docker compose up -d order-db

cd services/order
poetry run alembic upgrade head && poetry run alembic check
poetry run pytest -q        # без БД: 115 passed, 32 skipped
# полный прогон:
$env:TEST_DATABASE_URL='postgresql+asyncpg://order_user:dev_password@localhost:5434/order_test_db'
$env:REQUIRE_INTEGRATION_TESTS='1'; poetry run pytest -q   # 147 passed
poetry run ruff check . && poetry run ruff format --check .
```

## Подводные камни

- **Отмена не освобождает резерв:** у Inventory нет `release`; если reserve успел после отмены, резерв остаётся (логируем `error`, заказ не воскрешаем). Ограничение проекта, не баг.
- `failed` и `cancelled` — терминальные: повторная отмена → 409 (договорённость API).
- Lease сравнивается **в SQL** (`now() - make_interval(secs => :x)`), не в Python: часы приложения и БД расходятся.
- `HTTP_422_UNPROCESSABLE_ENTITY` в Starlette устарела → использовать `HTTP_422_UNPROCESSABLE_CONTENT`.
- В новом FastAPI роутеры подключаются лениво (`_IncludedRouter`): обход `app.routes` показывает не всё, пути проверять через `app.openapi()["paths"]`.
- Windows: API по `127.0.0.1:8002`, не `localhost` (IPv6). `order-db` (:5434), `order-api` (:8002).
- Partial-индексы содержат литерал `'pending'` в `WHERE`; новый статус в enum — миграция в два шага (`ALTER TYPE ... ADD VALUE` вне транзакции).
- Env-переменные PowerShell живут между командами одной сессии: перед «локальным» прогоном сбрасывать `REQUIRE_INTEGRATION_TESTS`.

