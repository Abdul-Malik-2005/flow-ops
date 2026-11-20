# FlowOps — Текущий статус (Current Handoff)

**Актуальный сервис:** `services/order` — блоки 1–7 закрыты, **блок 8 (RabbitMQ) закрыт полностью**: транспорт → консьюмер-журнал → флип публикации → наблюдаемость → переигрывание DLQ.
**Статус:** 401 passed / 0 skipped (314 unit/API + 87 интеграционных) при `REQUIRE_INTEGRATION_TESTS=1` и `ORDER_RABBITMQ_IT=1` на живых Postgres + RabbitMQ; без флагов RabbitMQ-модули скипаются целиком. `ruff check`/`ruff format --check` чисто (96 файлов). Alembic head `a7c9e1f4b2d6`; `docker compose config --quiet` валиден.
**Следующая задача:** блок 9 — выбрать между межсервисным потребителем (Notification) и эксплуатационной зрелостью Order. `services/inventory` **не трогать**: журнал внутри Order задуман как read-side и остаётся.

---

## Что готово

### Блоки 1–6 (сжато)
- Сага `order-first`: TX1 (`INSERT pending` + history) → COMMIT → HTTP `reserve` → TX2 (исход + lease); идемпотентность по `Idempotency-Key` + телу (200 / 409); недоступность Inventory → 202, заказ остаётся `pending`.
- Lease `reservation_started_at` защищает отмену от гонки с reserve; реконсилер — claim-by-lease (`FOR UPDATE SKIP LOCKED`), HTTP вне транзакции, TX2 = CAS `WHERE status='pending'`.
- API `/api/v1/orders`: POST, GET list (keyset), GET one, POST `{id}/cancel`; access-токен проверяется локально, `user_id` только из `sub`.

### Блок 7 (outbox) — три тезиса
1. **Событие пишется в той же транзакции**, что и смена статуса; наружу его выносит `python -m app.outbox_relay`: TX1 claim (`FOR UPDATE SKIP LOCKED`, `attempts += 1`, lease) → публикация **вне транзакции** → TX2 `mark_published` (CAS) → TX3 `count_exhausted`. At-least-once, дедуп по `event_id`; `RelayReport` — единственная наблюдаемость.
2. **Политика цикла:** дренаж без сна только пока `published >= claimed * 0.5`; падение прохода → экспоненциальный `pass_backoff` (потолок 300 с); dead letters читаются лишь когда счёт мог измениться, ERROR — на рост числа.
3. **Рантайм:** `app/runtime.py` общий для воркера, relay и консьюмера (лог, `signal.signal` → `asyncio.Event`, `sleep_or_stop`); compose-сервис `order-outbox-relay` — тот же образ, `stop_grace_period: 35s`, после флипа зависит от `order-db` и `rabbitmq`.

### Блок 8, шаг 3 (RabbitMQ-транспорт) — три тезиса
1. **Второй адаптер за тем же портом:** `RabbitMQEventPublisher` (`aio-pika 10.2.3`, `connect_robust`) включается `OUTBOX_PUBLISHER=rabbitmq` + `RABBITMQ_URL`; код-дефолт `log` остаётся для запусков без инфраструктуры (pytest, локальный `--once`).
2. **Контракт публикации:** durable topic exchange `OUTBOX_EXCHANGE` (`order.events`), routing key = `event_type`, `delivery_mode=PERSISTENT` + confirms + `mandatory=True` + `on_return_raises=True`; `Nack`/`Reject`/`Basic.Return` → `EventNotDelivered` → строка остаётся `published_at IS NULL`.
3. **Инфраструктура:** `rabbitmq:4-management-alpine` (healthcheck `rabbitmq-diagnostics -q ping`, порты только на localhost); 18 unit-тестов на фейках с инъекцией `connect`; живой брокер — `tests/integration/test_rabbitmq_publisher.py`.

### Блок 8, шаг 4 (консьюмер-журнал) — три тезиса
1. **Схема:** `order_event_projections` (append-only), PK = `event_id` (= AMQP `message_id` = `outbox_events.id`), `event_type varchar(64)`, `occurred_at timestamptz NULL`, `received_at server_default now()`, индекс `(aggregate_id, occurred_at)`, **без FK** на `orders`/`outbox_events`. Миграция `a7c9e1f4b2d6` (down `f4b8c1d2e903`), цикл `upgrade → check → downgrade -1 → upgrade → check` проверен.
2. **Репозиторий:** `ProjectionRepositoryPort` + `SqlProjectionRepository.record(...) -> bool` — один `pg_insert(...).on_conflict_do_nothing(index_elements=[event_id]).returning(event_id)`; `False` = повтор, `True` = вставили. Никогда не коммитит, не делает `SELECT`-then-`INSERT` (race) и не ловит `IntegrityError` (aborted-транзакция).
3. **Обработчик:** `OrderEventProjector.handle(message) -> bool` — чистый, без сессии/ack/брокера. Allow-list = `OrderEventType`; неизвестный тип → `UnsupportedEventTypeError`, битый конверт → `MalformedEventError` (оба — `EventHandlingError` → DLQ). Лишние поля конверта игнорируются (forward compatibility), `occurred_at: null` допустим, naive timestamp отвергается; ошибки разбора не доходят до БД.

### Блок 8, шаг 5 (рантайм, флип, наблюдаемость, replay) — три тезиса
1. **Рантайм:** `python -m app.event_consumer`, compose-сервис `order-event-consumer` (без портов, `depends_on` order-db + rabbitmq, `stop_grace_period: 15s`). Объявляет durable **quorum** очередь, DLX + DLQ, allow-list биндингов из `OrderEventType`; exchange переобъявляется идемпотентно. `commit` строго до `ack`; дубликат тоже ack; `EventHandlingError` → `nack(requeue=False)` → DLQ; временная ошибка → `nack(requeue=True)` + прерываемая пауза. Предел доставок проверяется по заголовку брокера `x-acquired-count`, потому что RabbitMQ 4 **не применяет** `x-delivery-limit` к явному requeue.
2. **Флип:** в compose `OUTBOX_PUBLISHER=rabbitmq` и `depends_on: rabbitmq`; код-дефолт `log` сохранён, чтобы API не требовал брокер. E2E `tests/integration/test_outbox_to_journal.py`: outbox → релей → брокер → консьюмер → журнал, `published_at` ставится только после confirm.
3. **Наблюдаемость и replay:** периодический `consumer report` (`processed/duplicates/retried/refused/dead_lettered`, per-event логи в DEBUG, `report(force=True)` на SIGTERM). `python -m app.dlq_replay --limit N`: publish с confirm + `mandatory=True` → только потом ack; routing key из `event_type`; заголовки брокера не копируются; коды выхода 0 / 2 (осталось человеку) / 1 (publish отказал).

## Тесты (314 unit/API + 87 интеграционных)

- **Unit:** новые — `test_event_consumer_entrypoint` 24, `test_rabbitmq_consumer` 11, `test_dlq_replay` 8, `test_event_projection` 23, `test_projection_repository` 2, `test_config` 32; остальные — домен, API, publisher/relay, клиент Inventory и т.д.
- **Интеграционные:** заказы 24, сага 14, outbox-репозиторий 12, эмиссия 8, relay 6, реконсилер 6, журнал проекций 3, консьюмер 5, outbox→журнал 1, DLQ replay 2, RabbitMQ publisher 3, relay→брокер 3.
- Без `TEST_DATABASE_URL` интеграционные тихо скипаются; `REQUIRE_INTEGRATION_TESTS=1` превращает skip в падение. RabbitMQ-модули — отдельный гейт `ORDER_RABBITMQ_IT=1` + `RABBITMQ_URL`. `order_event_projections` включена в `TRUNCATE`.

## Ключевые файлы блока 8

```
services/order/app/publishers/{rabbitmq,envelope,log}.py, app/consumers/rabbitmq.py
services/order/app/event_consumer.py                 # commit→ack, лимит доставок, report
services/order/app/dlq_replay.py                     # ручное переигрывание DLQ
services/order/app/services/event_projection.py      # чистый обработчик + EventHandlingError
services/order/app/repositories/projection.py        # ON CONFLICT adapter
services/order/app/models/, migrations/versions/a7c9e1f4b2d6_*.py
services/order/app/core/config.py, .env.example, compose.yaml
contracts/order-events.schema.json, docs/architecture.md (блок 8)
```

## План следующего шага (блок 9)

- **A. Межсервисный потребитель Notification** (крупный блок, рекомендуется): новый сервис `services/notification` со своей БД, durable очередь `notification.order-events`, allow-list биндингов, дедуп по `event_id`, DLX/DLQ, собственный аналог журнала. Order не меняется вовсе — очередь объявляет её владелец. Это первая настоящая проверка контракта между сервисами.
- **B. Эксплуатационная зрелость Order** (дешевле, без нового сервиса): health/readiness консьюмера (время последней успешной обработки), `--once` для smoke, healthcheck для `order-event-consumer`, алерты по `refused/dead_lettered`, structured logs.
- **C. Версионирование контракта:** `schema_version` в конверте + тесты совместимости до появления второго потребителя.

**Вопросы к согласованию:**
1) следующий блок: A (Notification) или B (зрелость Order)?
2) если A: Notification сразу с собственной БД (миграции, compose) или сначала тонкий консьюмер без БД?
3) нужен ли `schema_version` (C) до второго потребителя или после?
4) DLQ replay: оставить ручным CLI или добавить расписание/операторскую обёртку?

## Команды

```
# из корня репозитория
docker compose up -d order-db rabbitmq

cd services/order
poetry run alembic upgrade head && poetry run alembic check
poetry run ruff check . ; poetry run ruff format --check .
$env:TEST_DATABASE_URL='postgresql+asyncpg://order_user:dev_password@localhost:5434/order_test_db'
$env:ORDER_RABBITMQ_IT='1'; $env:RABBITMQ_URL='amqp://flowops:dev_password@localhost:5672/'
$env:REQUIRE_INTEGRATION_TESTS='1'; poetry run pytest -q      # 401 passed
poetry run python -m app.dlq_replay --limit 100               # операция, не тест
```

`TEST_DATABASE_URL` в `.env` **нет** (только в `.env.example`) — задавать переменной окружения явно, иначе интеграционные тихо скипнутся.

## Подводные камни

- **`commit` строго до `ack`;** ack дубликата — успех; консьюмер никогда не публикует; детерминированная ошибка → DLQ, временная → requeue; `services/inventory` не трогать.
- **`x-delivery-limit` не применяется к явному `nack(requeue=True)`** (проверено: 1368 доставок, DLQ пуст). Рабочий счётчик — `x-acquired-count`; на последней разрешённой доставке рантайм делает `nack(requeue=False)`.
- **`passive` declare на уже использованном канале отдаёт устаревшую глубину** (same channel 0, другое соединение 1) — в тестах читать свежим соединением.
- **`mandatory=True` без очереди = ошибка публикации:** флип сделан только после появления консьюмера; код-дефолт `log` — для запусков без брокера.
- **`x-death`/`x-acquired-count` нельзя копировать при replay** — новый консьюмер увидит исчерпанный бюджет и сразу отправит сообщение в DLQ.
- **`PublishError` — подкласс `DeliveryError`:** `except PublishError` идёт первым; оба нельзя собрать без AMQP-фреймов — в тестах только через `pamqp`. `connect_robust` не переигрывает упавшие публикации.
- **BLE001:** ruff требует `exc_info=True` у `logger.*` внутри `except Exception` (иначе «do not catch blind exception») — проверено на `dlq_replay._close`.
- **`requires-python = ">=3.14,<4"`**; `OUTBOX_LEASE_SECONDS >= 2 * OUTBOX_RELAY_INTERVAL_SECONDS`; `expire_on_commit=False`.
- **Документы править только редактором** (иначе BOM/CRLF); интеграционные тесты не параллелить на одной `*_test_db` (`DROP SCHEMA` + `alembic upgrade head` на сессию).
- Windows: `signal.signal`, не `loop.add_signal_handler`; порты :5434/:8002/:5672/:15672 (localhost); relay не зависит от `order-api`. Репозитории **никогда не коммитят**; partial-индексы содержат `'pending'`; `HTTP_422_UNPROCESSABLE_CONTENT`; маршруты через `app.openapi()["paths"]`; длинный вывод pytest читать через файл.
