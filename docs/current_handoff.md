# FlowOps — Текущий статус (Current Handoff)

**Актуальный сервис:** `services/order` (блоки 1–6 **закрыты**)
**Статус:** 189 тестов зелёные (145 unit/API + 44 интеграционных на реальном Postgres), `ruff check`/`ruff format --check` чисто, `alembic check` → "No new upgrade operations detected" (head `d3f8a1b6c204`, downgrade/upgrade проверены), `docker compose config` валиден, `python -m app.worker --once` печатает отчёт.
**Следующая задача:** блок 7 (предложение — публикация событий/outbox, см. «План следующего шага»). `services/inventory` и `services/identity` **не трогать**.

---

## Что готово

### Блоки 1–5 (сжато)
- Сага `order-first`: TX1 (`INSERT pending` + history) → COMMIT → HTTP `reserve` → TX2 (исход + lease). Повтор с тем же `Idempotency-Key` и телом → 200, с другим телом → 409. Недоступность Inventory → **202**, заказ остаётся `pending`.
- Lease `reservation_started_at` защищает отмену от гонки с reserve; сравнение времени **в SQL**; валидатор `RESERVATION_STALE_AFTER_SECONDS >= 2 * INVENTORY_TIMEOUT_SECONDS`.
- API `/api/v1/orders`: POST, GET list (keyset-курсор), GET one (404 и для чужого), POST `{id}/cancel`; access-токен проверяется локально, `user_id` только из `sub`.

### Блок 6 (реконсилер) — три тезиса
1. **Claim-by-lease:** один короткий `UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED)` двигает `reservation_started_at=now()` и `reservation_attempts += 1`, затем commit; HTTP-вызов вне транзакции, TX2 — отдельная транзакция на заказ (CAS `WHERE status='pending'`).
2. **Бюджет попыток:** `attempts > RESERVATION_MAX_ATTEMPTS` (инкремент в claim идёт *до* вызова) → `failed`/`reservation_unavailable` **без** финального HTTP. Маппинг исходов общий с сагой — `app/services/reservation.py`.
3. **Процесс:** `python -m app.worker` (`--once`) из того же образа, compose-сервис `order-reconciler`; SIGTERM/SIGINT доделывают пачку; исключение прохода логируется, цикл продолжается; `engine.dispose()` в `finally`; `app.main` не импортируется (JWT-ключ воркеру не нужен).

## Тесты (189)

- **145 unit/API:** `test_domain` 30, `test_orders_api` 25, `test_inventory_client` 16, `test_config` 12, `test_reconciler` 11, `test_schemas` 10, `test_pagination` 9, `test_security` 8, `test_worker` 7, `test_deps` 5, `test_pricing` 5, `test_health` 4, `test_reservation` 3.
- **44 интеграционных:** репозиторий 24 (claim по lease, два сеанса `SKIP LOCKED` без двойного захвата, `count_pending_older_than`), сага 14, реконсилер 6: потерянный ответ, потерянный TX2 с повтором того же `reservation_id`, свежий lease не трогается, отмена выиграла гонку, исчерпанный бюджет, `rejected` на ретрае.
- Без `TEST_DATABASE_URL` — 145 passed, 44 skipped; с `REQUIRE_INTEGRATION_TESTS=1` тихий skip становится падением.

## Ключевые файлы блока 6

```
services/order/app/worker.py               # build_reconciler, run_once/run_forever, log_report, --once
services/order/app/services/reconciler.py  # process_batch, ReconcileReport, per-order try/except
services/order/app/services/reservation.py # FAILURE_REASONS, apply_terminal_outcome, fail_exhausted_reservation
services/order/app/repositories/order.py   # claim_stale_pending, count_pending_older_than, clear_lease
services/order/tests/{test_worker,test_reconciler,test_reservation}.py
services/order/tests/integration/test_reconciler.py
services/order/tests/integration/helpers.py # insert_order, history_of, age_lease, set_reservation_attempts
compose.yaml                                # order-reconciler: тот же образ, без портов/healthcheck/secrets
```

## План следующего шага (блок 7 — публикация событий, черновик)

1. Транспорт: outbox в БД Order + relay (нет RabbitMQ в `compose.yaml`) vs прямой publish (теряет событие при падении между TX2 и отправкой).
2. Таблица `outbox_events` (`id`, `aggregate_id`, `event_type`, `payload jsonb`, `occurred_at`, `published_at NULL`), partial-индекс по неопубликованным; запись в **той же** транзакции, что и смена статуса.
3. Relay переиспользует каркас воркера (claim пачкой, `SKIP LOCKED`, батч, отчёт) — новый паттерн не вводим.
4. События: `order.reserved`, `order.failed`, `order.cancelled`. Обязательный тест: `reserved` уходит ровно один раз, включая путь реконсилера.

**Вопросы к согласованию:**
1. Relay внутри `app.worker` или отдельный `python -m app.outbox_relay` (второй контейнер, независимые интервалы и пачки)?
2. Кто потребитель — Notification/Payment напрямую или будущий брокер? От этого зависит контракт `event_type`/payload.
3. `published_at` батчем или `DELETE` опубликованных (рост таблицы vs потеря истории)?

## Команды

```
# из корня репозитория
docker compose up -d order-db

cd services/order
poetry run alembic upgrade head && poetry run alembic check
poetry run pytest -q                     # без БД: 145 passed, 44 skipped
$env:TEST_DATABASE_URL='postgresql+asyncpg://order_user:dev_password@localhost:5434/order_test_db'
$env:REQUIRE_INTEGRATION_TESTS='1'; poetry run pytest -q    # 189 passed
poetry run ruff check . ; poetry run ruff format --check .
$env:DATABASE_URL='postgresql+asyncpg://order_user:dev_password@localhost:5434/order_db'
poetry run python -m app.worker --once   # ReconcileReport(claimed=..., ...)
```

## Подводные камни

- **`RESERVATION_STALE_AFTER_SECONDS` у API и воркера обязан совпадать** (`.env.example`, `compose.yaml`): иначе один процесс работает со строкой, которую второй считает занятой.
- Лимит попыток может скрыть реально созданный резерв: у Inventory нет `release`, `failed` терминален — чистится вручную (в логе `order_id` + `reservation_id`).
- Падение между claim и TX2 = −1 попытка и лишнее окно lease задержки; восстанавливается само (поэтому счётчик — в `reservation_attempts`, а не отдельный «успешных вызовов»).
- `attempts > max_attempts`, а не `>=`: при `>=` и `max=1` ретрая не было бы вообще.
- Отмена не освобождает резерв (нет `release`); отмена, выигравшая гонку, не откатывается — `error`-лог.
- Lease сравнивается **в SQL** (`now() - make_interval(secs => :x)`): часы приложения и БД расходятся.
- Репозиторий **никогда не коммитит**; claim требует `synchronize_session=False` (SQL-функцию в Python не вычислить).
- `HTTP_422_UNPROCESSABLE_ENTITY` устарела → `HTTP_422_UNPROCESSABLE_CONTENT`; роутеры FastAPI ленивые, пути проверять через `app.openapi()["paths"]`.
- Partial-индексы содержат литерал `'pending'`; новый статус в enum — миграция в два шага (`ALTER TYPE ... ADD VALUE` вне транзакции).
- Windows: `order-db` :5434, `order-api` :8002 (`127.0.0.1`, не `localhost`); env в PowerShell живут до конца сессии — перед «локальным» прогоном сбрасывать `REQUIRE_INTEGRATION_TESTS`.
- Ruff сортирует `Reconciler, ReconcileReport` против алфавита человека (регистр) — импорты править `ruff check --fix`, а не вручную.
- В compose нет `alembic upgrade` у воркера: схемой владеет API (один мигратор). На чистой БД воркер до миграции API будет логировать ошибки проходов — это ожидаемо.
