# FlowOps — Архитектура и технические решения

## 1. Общая архитектура платформы
- **Домен:** Микросервисная платформа заказов, склада, оплат и уведомлений.
- **Порядок разработки:** Inventory (готов) → Identity (в работе) → Order → Catalog → Payment → Notification.
- **Изоляция данных:** Каждый сервис владеет своей БД PostgreSQL. Прямые запросы в чужие таблицы запрещены.
- **Взаимодействие:** Order использует async-оркестрацию через RabbitMQ + Transactional Outbox. Синхронные межсервисные вызовы (например, Order → Inventory) используют отдельный сервисный токен/заголовок, а не пользовательский JWT.
- **Ограничения:** Без Kafka, Kubernetes и Service Mesh.

---

## 2. Инфраструктурные правила и грабли (Lessons Learned)
- **Poetry:** Версия в `Dockerfile` (`ENV POETRY_VERSION=2.4.3`) должна строго совпадать с локальной. Любое изменение `pyproject.toml` требует `poetry lock`.
- **SQLAlchemy + PostgreSQL Enum:** Всегда использовать `StrEnum` и передавать `values_callable=lambda x: [e.value for e in x]` в `SAEnum`, иначе SQLAlchemy сохранит `.name` (`ADMIN`) вместо `.value` (`admin`).
- **Alembic `downgrade`:** Автогенерация не создаёт `DROP TYPE` для нативных enum. Дописывать вручную `sa.Enum(name='<type>').drop(op.get_bind(), checkfirst=False)` строго **после** `op.drop_table(...)`.
- **Pydantic Settings в тестах:** `pydantic-settings` ищет `.env` от `cwd`. В `tests/conftest.py` обязательно задавать `os.environ.setdefault(...)` **до** импорта модулей `app.*`.
- **Кэширование ключей в тестах:** Функции загрузки ключей обернуты в `lru_cache`. В фикстурах тестов обязательно вызывать `.cache_clear()` до и после теста.
- **Ruff:** В `pyproject.toml` исключать `migrations/versions` через `extend-exclude`. Использовать `datetime.UTC` (правило `UP017`).

---

## 3. Архитектурные решения: Inventory Service (Завершён, не менять)
- **Слои:** API → Service → Repository → Models. Репозитории не делают `commit`/`rollback` — границами транзакций управляет сервисный слой.
- **Конкурентность и идемпотентность:** Идемпотентность по `order_id` через PostgreSQL advisory lock + `SELECT ... FOR UPDATE` на строках остатков.
- **Модели:** `Inventory` (составной PK), `Reservation` (`order_id` unique), `ReservationItem` (composite FK).

---

## 4. Архитектурные решения: Identity Service
- **Токены (RS256):**
  - Access JWT (TTL 15 мин): claims `sub, role, type="access", exp, iat, jti` + `kid` в header.
  - Refresh JWT (TTL 30 дней): claims `sub, type="refresh", exp, iat, jti` + `kid` в header (без `role`, при обновлении роль всегда читается из БД).
- **Безопасность:**
  - Пароли хешируются через `Argon2id` (t=3, m=64 MiB, p=4).
  - При логине с несуществующим email выполняется проверка против dummy-хеша для выравнивания времени ответа.
  - Попытки входа (`LoginAttempt`) записываются в отдельной транзакции до выброса исключения 401/403, чтобы аудит не откатывался.
  - При обнаружении повторного использования уже отозванного refresh-токена (reuse) отзываются **все** refresh-токены пользователя.
  - При старте приложения (`lifespan`) выполняется fail-fast проверка наличия PEM-ключей (кроме `ENVIRONMENT=testing`).
- **Схема БД Identity:**
  - `users`: `id` (UUID PK), `email` (unique, lower-case), `password_hash`, `role` (`user_role` enum), `is_active`, `created_at`, `updated_at`.
  - `refresh_tokens`: `jti` (UUID PK), `user_id` (FK `ON DELETE CASCADE`), `expires_at`, `revoked_at`, `replaced_by_jti` (без FK, audit-поле), `created_at`.
  - `login_attempts`: `id` (UUID PK), `user_id` (nullable FK `ON DELETE SET NULL`), `email`, `success`, `ip`, `created_at`.

---

## 5. Архитектурные решения: Order Service
- **Сага `order-first`:** `pending → reserved`; отмена разрешена только из `pending` (у Inventory нет `release`/`confirm`, сервис заморожен). `order_id` генерируется приложением **до** вызова Inventory — резерв идемпотентен по `order_id`, поэтому `pending` восстановим.
- **Схема БД Order:**
  - `orders`: `id` (UUID PK, из приложения), `user_id` (**без FK** — граница сервиса), `status` (`order_status` enum), `currency` (varchar(3) + CHECK ISO), `total_amount`/`request_hash`/`idempotency_key`/`inventory_reservation_id` nullable, `created_at`, `updated_at`.
  - `order_items`: FK `order_id` `ON DELETE CASCADE`, `quantity > 0`, `UNIQUE(order_id, product_id, warehouse_id)` (он же индекс по `order_id`), цены nullable.
  - `order_status_history`: append-only аудит, `from_status` nullable, `reason` varchar(64) (набор причин валидируется доменом, не БД).
- **Индексы `orders`:** partial unique `(user_id, idempotency_key) WHERE idempotency_key IS NOT NULL`; `(user_id, created_at DESC)`; partial `(created_at) WHERE status = 'pending'` для реконсилера.
- **Деньги:** только `Decimal` + `numeric(12,2)`; `total_amount = NULL` означает «цена неизвестна» (Catalog не существует), `0.00` запрещено как ложное утверждение.
- **Транзакционные границы саги (order-first):** TX1 — `INSERT pending` + запись истории, **commit**; затем HTTP-вызов `reserve`; TX2 — применить исход и `reservation_started_at`. Держать транзакцию БД открытой на время сетевого вызова нельзя (блокировки + пул), поэтому «неизвестный исход» — легальное состояние: заказ остаётся `pending`, клиент получает `202`, дальше его доводит реконсилер.
- **Идемпотентность создания:** ключ — заголовок `Idempotency-Key` (≤ 255), дедуп по partial-unique `(user_id, idempotency_key)`; `INSERT ... ON CONFLICT DO NOTHING` возвращает 0 строк → сервис делает `rollback` и читает существующий заказ: совпал `request_hash` → `200` + исходный заказ, не совпал → `409`. Хеш считается по канонизированному телу (дубли строк схлопнуты, порядок фиксирован, цены не участвуют).
- **Lease против гонки «cancel vs reserve»:** `reservation_started_at` — «вызов, возможно, в полёте». Отмена разрешена только если lease пуст или старше `RESERVATION_STALE_AFTER_SECONDS` (валидатор требует `>= 2 * INVENTORY_TIMEOUT_SECONDS`), сравнение времени — в SQL (`now() - make_interval(...)`), а не в Python. Отмена, выигравшая гонку, не откатывается: заказ не воскрешается, orphaned reservation уходит в лог как `error`.
- **Клиент Inventory — единственная точка правды о статусах:** клиент **никогда не бросает** исключений на бизнес-ответы, а возвращает типизированный исход (`reserved`/`rejected`/`item_not_found`/`invalid_request`/`unavailable`); маппинг в HTTP делает API-слой. Транспортные сбои (`timeout`, `5xx`, `429`, битый `2xx`) не отменяют заказ — иначе клиент получал бы «отменено» из-за сетевого чиха.
- **Пагинация списка:** keyset по `(created_at, id)` (не `offset`), курсор — opaque base64url `created_at|order_id`, naive-таймстамп отвергается (`400`), запрос тянет `limit + 1` строку, чтобы отдать `next_cursor` без второго COUNT.
- **Карта ошибок API (Order):** один словарь `_ERROR_HTTP_STATUS` (доменное исключение → код): `OrderNotFound`→404 (и для чужого заказа — существование не подтверждаем), `OrderNotCancellable`/`OrderStateChanged`/`IdempotencyConflict`/`InsufficientStock`→409, `InventoryItemNotFound`/`InvalidOrder`→422, `InvalidCursor`→400, `InventoryRejected`→502. Транспортная недоступность Inventory — **не** ошибка: `202` + заказ остаётся `pending`.
- **Аутентификация API (Order):** access-токен проверяется **локально** (`kid` → подпись → `type == "access"` → `sub`), без вызова Identity и без таблицы `users`: `Principal(user_id, role)` — это только клеймы. Следствие: смена роли применяется со следующим access-токеном (TTL 15 мин) — осознанный компромисс «нет сетевого вызова на каждый запрос». `user_id` всегда берётся из `sub` через DI (`CurrentUserId`), **никогда** из тела запроса (закрывает IDOR на уровне зависимостей).
- **Семантика 401 в Order:** отсутствие заголовка, не-Bearer схема и любая ошибка токена → `401` + `WWW-Authenticate: Bearer`. Нужен `HTTPBearer(auto_error=False)`, иначе FastAPI отдаёт `403`. `detail` единый и неинформативный (причина остаётся в цепочке исключений/логах) — чтобы не давать оракул «истёк / битая подпись / чужой kid».
- **Грабли (проверено на практике):**
  - В Order принят `MetaData(naming_convention=...)`. Явный `name=` у `UniqueConstraint`/`Index` **перебивает** convention — для генерации имён имя не указывается.
  - `Index("order_id", "created_at")` трактует первую строку как **имя индекса**; нужен `Index(None, "order_id", "created_at")`.
  - Partial-индексы содержат литерал ('pending') в `WHERE` — при переименовании статуса индекс надо пересоздать той же миграцией.
  - Идемпотентный `INSERT` делать через `pg_insert(...).on_conflict_do_nothing(index_where=<тот же предикат>)`, а не `try/except IntegrityError`: пойманная ошибка оставляет сессию в состоянии aborted-транзакции, и «прочитать существующую строку для replay» в ней уже нельзя.
  - `HTTP_422_UNPROCESSABLE_ENTITY` в Starlette помечена deprecated → `HTTP_422_UNPROCESSABLE_CONTENT` (иначе warning в каждом прогоне тестов).
  - Новый FastAPI подключает роутеры лениво (в `app.routes` лежит `_IncludedRouter` без `path`): для проверки маршрутов использовать `app.openapi()["paths"]`, а не обход `app.routes`.
  - Миграция с native enum: `postgresql.ENUM(..., create_type=False)` + явный `.create(bind, checkfirst=True)` (иначе второе `create_table` пытается создать тип повторно), а в `downgrade` — `DROP TYPE` **после** `drop_table`.
  - `ALTER TYPE ... ADD VALUE` нельзя использовать в той же транзакции ⇒ добавление статуса делается миграцией в два шага.
  - Контроль расхождений моделей и миграций: `alembic check` (в CI — после `upgrade head`).

---

## 6. Бэклог технических решений (на будущие сервисы)
- **JWKS-эндпоинт:** Внедрить при появлении следующих сервисов, чтобы не раздавать публичный ключ через `.env`.
- **Rate Limiting:** Добавить защиту `/auth/login` (sliding window / token bucket) после внедрения Redis.
- **Маскирование 422 ошибок:** Настроить кастомный обработчик `RequestValidationError`, чтобы вычищать поле `input` с паролями из ответов и логов.
- **Стратегия миграций B:** При переходе на несколько реплик API за Nginx вынести `alembic upgrade head` в отдельный one-off контейнер.
