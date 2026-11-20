# FlowOps — Текущий статус (Current Handoff)

**Актуальные сервисы:** `services/order` (блоки 1–8) и `services/notification` (блок 9) — **блок 9 закрыт полностью**: межсервисный консьюмер → sender → SMTP/Mailpit → `schema_version` → сквозной E2E → DLQ-replay → `--once`/readiness.
**Статус:** Order **409 passed / 0 skipped**; Notification **154 passed / 0 skipped** (живые Postgres + RabbitMQ + Mailpit). `ruff check`/`format --check` чисто (Order 96, Notification 51 файл). Alembic: Order `a7c9e1f4b2d6`, Notification `c2d8e3a5b914`; `docker compose config --quiet` валиден.
**Git:** `main` = `208653c`, всё запушено. `services/inventory` **не трогать**.
**Следующая задача:** новый блок — Catalog (прайсинг) или дописать Identity (см. план).

---

## Что готово (сжато)

### Блоки 1–8 (Order) — 3 тезиса
1. **Сага `order-first`:** TX1 (`INSERT pending` + history) → commit → HTTP `reserve` → TX2 (исход + lease); идемпотентность `Idempotency-Key`+тело (200/409); недоступность Inventory → 202/pending; реконсилер claim-by-lease (`FOR UPDATE SKIP LOCKED`).
2. **Outbox + RabbitMQ:** событие пишется в транзакции смены статуса; `python -m app.outbox_relay` = claim → publish вне tx (durable topic `order.events`, routing key = `event_type`, confirms + `mandatory`) → `mark_published` CAS; at-least-once, дедуп по `event_id`. `python -m app.event_consumer` журналирует (`order_event_projections`), commit→ack, `EventHandlingError`→DLQ, предел по `x-acquired-count`.
3. **Версионирование:** `schema_version` в конверте (`SCHEMA_VERSION=1` в `app/domain/events.py`), общий контракт `contracts/order-events.schema.json`; контрактные тесты пинят ключи/enum/версию.

### Блок 9 (Notification) — 3 тезиса
1. **Консьюмер (`python -m app.event_consumer`):** новый сервис со своей БД, inbox-паттерн (`notification_inbox`, PK=`event_id`, `ON CONFLICT DO NOTHING`), своя durable очередь `notification.order-events` + свой DLX/DLQ, allow-list биндингов; `commit→ack`, дубль=успех, `EventHandlingError`→DLQ, transient→requeue. Order не менялся.
2. **Sender (`python -m app.sender`):** claim (`FOR UPDATE SKIP LOCKED`, `attempts+=1`, lease) → compose+resolve+send **вне** tx → `mark_sent` CAS; порт `NotificationSenderPort`; composer (шаблоны по типу), `RecipientResolverPort` (`user_id→email`, dev `LocalRecipientResolver`), SMTP (`aiosmtplib`, dev Mailpit), `SENDER_TRANSPORT=log|smtp`; `event_id` = `Message-ID`.
3. **Контракт/операции:** `schema_version` обязателен (int≥1, higher толерится); сквозной E2E `scripts/e2e_order_to_notification.py`; DLQ-replay **в свою очередь** через default exchange; `--once` drain + `idle_s` heartbeat.

## Тесты
- **Order:** 409 passed (unit/API + интеграционные: заказы, сага, outbox, relay, реконсилер, консьюмер, DLQ replay, publisher, outbox→журнал).
- **Notification:** 154 passed (unit: config/handler/repo/consumer/entrypoint/sender/composer/resolver/smtp/dlq_replay; интеграционные: inbox repo, consumer E2E, sender, SMTP→Mailpit, DLQ replay; контракт).
- Гейты: `REQUIRE_INTEGRATION_TESTS=1`; RabbitMQ `ORDER_RABBITMQ_IT=1` / `NOTIFICATION_RABBITMQ_IT=1` + `RABBITMQ_URL`; SMTP `NOTIFICATION_SMTP_IT=1` + `SMTP_HOST/PORT`. Без флагов интеграционные скипаются.

## Ключевые файлы блока 9

```
services/notification/app/{event_consumer,sender,dlq_replay,runtime}.py
services/notification/app/consumers/rabbitmq.py
services/notification/app/repositories/inbox.py
services/notification/app/services/{event_handler,notification_sender,recipient_resolver}.py
services/notification/app/domain/{events,notifications}.py, senders/{log,smtp}.py
services/notification/migrations/versions/{b1e4f7a2c803,c2d8e3a5b914}_*.py
scripts/e2e_order_to_notification.py, docker/notification/, compose.yaml (mailpit + notification-*)
contracts/order-events.schema.json (schema_version)
```

## План следующего шага

- **A. Catalog** (прайсинг): новый сервис; Order получает цены (`PricingPort` вместо `NullPricing`), `total_amount` перестаёт быть NULL. Крупный блок.
- **B. Дописать Identity:** завершить auth, затем заменить `LocalRecipientResolver` на HTTP-резолвер (`user_id→email` из Identity). Разблокирует настоящую доставку по адресу.

**Вопросы к согласованию:**
1) следующий блок: A (Catalog) или B (Identity)?
2) если Catalog: цены тянуть синхронно из Order (`PricingPort` → HTTP) или событием/кэшем?
3) если Identity: достаточно ли JWKS-эндпоинта вместо раздачи публичного ключа через `.env`?
4) `IdentityRecipientResolver`: синхронный HTTP на каждое письмо или кэш адресов?

## Команды

```
# из корня
docker compose up -d                      # стек: dbs, rabbitmq, mailpit, api, consumer, sender, relay
cd services/order && poetry run alembic upgrade head && poetry run alembic check

# Order
$env:TEST_DATABASE_URL='postgresql+asyncpg://order_user:dev_password@localhost:5434/order_test_db'
$env:ORDER_RABBITMQ_IT='1'; $env:RABBITMQ_URL='amqp://flowops:dev_password@localhost:5672/'
$env:REQUIRE_INTEGRATION_TESTS='1'; poetry run pytest -q      # 409 passed

# Notification
cd ../notification && poetry run alembic upgrade head && poetry run alembic check
$env:TEST_DATABASE_URL='postgresql+asyncpg://notification_user:dev_password@localhost:5435/notification_test_db'
$env:NOTIFICATION_RABBITMQ_IT='1'; $env:NOTIFICATION_SMTP_IT='1'; $env:SMTP_HOST='localhost'; $env:SMTP_PORT='1025'
$env:REQUIRE_INTEGRATION_TESTS='1'; poetry run pytest -q      # 154 passed
poetry run python -m app.event_consumer --once                # smoke: drain + heartbeat
poetry run python -m app.sender --once                        # smoke: отправка
poetry run python -m app.dlq_replay --limit 100               # операция
cd ../../services/order && poetry run python ../../scripts/e2e_order_to_notification.py  # сквозной E2E
```
`TEST_DATABASE_URL` в `.env` нет — задавать переменной. Mailpit UI: http://127.0.0.1:8025.

## Подводные камни
- **`commit` строго до `ack`;** дубль=успех; консьюмер никогда не публикует; детерминированная ошибка → DLQ, временная → requeue; `services/inventory` не трогать.
- **`x-delivery-limit` не применяется к явному `nack(requeue=True)`** — рабочий счётчик `x-acquired-count`; на последней доставке рантайм делает `nack(requeue=False)`.
- **`passive` declare на занятом канале — устаревшая глубина** → читать свежим соединением (тесты и `--once` drain).
- **`x-death`/`x-acquired-count` не копировать при replay**; consumer-side DLQ-replay идёт в **свою очередь** (default exchange), не в чужой `order.events`.
- **Пересобирать образы после кода/миграций:** устаревший order-образ падает на `alembic upgrade` («Can't locate revision»), а релей без `schema_version` → Notification уходит в DLQ.
- **`schema_version`:** требовать наличие/int≥1, higher толерится; breaking — новый тип события; контрактный тест пинит `SCHEMA_VERSION == schema.const`.
- **SMTP:** `aiosmtplib` с ленивым переиспользуемым соединением; `Message-ID = event_id`; Mailpit — только dev; `smtp` требует `SMTP_HOST`+`SMTP_FROM` (валидатор).
- **`SENDER_LEASE_SECONDS >= 2 * SENDER_INTERVAL_SECONDS`**; `requires-python >=3.14`; документы только редактором (BOM/CRLF); интеграционные не параллелить на одной `*_test_db` (`DROP SCHEMA`+upgrade на сессию).
- Windows: `signal.signal`; порты :5432–:5435, :8000/:8001/:8002, :5672/:15672, :1025/:8025 (localhost). Репозитории не коммитят; partial-индексы содержат литералы статусов.

