# FlowOps — техническая передача проекта (Handoff)

**Актуально на:** конец Шага 2 Identity Service (модели + миграция + strategy A в Docker).
**Репозиторий:** https://github.com/Abdul-Malik-2005/flow-ops
**Наставник:** senior Python Backend архитектор. Общение на русском, уровень Middle+, без базовых объяснений FastAPI. Работать небольшими этапами, не переходить к следующему, пока текущий не проверен. Давать полный код файла с точным путём. Не менять архитектурные решения молча. **Предлагать не «проще», а надёжнее** — с учётом корректности при сбоях, совместимости миграций, безопасности, предсказуемости в проде.

---

## 1. Идея и стек

FlowOps — портфолио-проект Middle+ Python Backend: микросервисная платформа
заказов/склада/оплат/уведомлений. Каждый сервис владеет своей БД; прямой доступ
к чужим таблицам запрещён.

**Стек:** Python 3.14+, FastAPI, Pydantic/pydantic-settings, PostgreSQL 17,
SQLAlchemy 2.x async, asyncpg, Alembic, Poetry 2.4.3, Docker/Compose,
pytest/pytest-asyncio, Ruff, HTTPX. RabbitMQ/Redis/Nginx/Prometheus — по мере
появления сервисов, не все внедрены. Kafka/K8s/Service Mesh не нужны.
Saga/CQRS не обязательны.

**Порядок реализации:** Inventory (завершён) → Identity → Order → Catalog →
Payment → Notification. Order — вариант Б (async-оркестрация через RabbitMQ +
transactional outbox).

---

## 2. Состояние по сервисам

### Inventory Service — ЗАВЕРШЁН, НЕ ПЕРЕПИСЫВАТЬ

- 16 тестов passed, Docker, CI, Alembic.
- Владеет складскими остатками и резервациями. Идемпотентен по `order_id`.
- Advisory lock по `order_id` + `SELECT FOR UPDATE` по складским строкам.
- Endpoint: `POST /api/v1/reservations/`. `GET /health`.
- Слои: API → Service → Repository → Models/Schemas. Repository не делает
  commit/rollback — транзакцией управляет Service.
- Модели: `Inventory` (составной PK), `Reservation` (order_id unique, enum
  status), `ReservationItem` (FK cascade, composite FK).
- Не переписывать без конкретной причины/бага/нового требования.

### Identity Service — ШАГИ 1 И 2 ЗАКРЫТЫ

**Шаг 1 — каркас (закрыт):**

- `services/identity/` — отдельный Poetry-проект (`package-mode = false`,
  Poetry 2.4.3, Python 3.14+, версии зависимостей 1-в-1 с Inventory).
- Структура: `app/{api/v1,core,db,models,repositories,schemas,services}`,
  `migrations/`, `alembic.ini`, `Dockerfile`, `.dockerignore`, `.env.example`,
  `.gitignore`.
- `app/core/config.py` — `Settings(BaseSettings)` с полями:
  `database_url`, `test_database_url`, `debug`, `environment`,
  **JWT-заготовки** (`jwt_algorithm="RS256"`, `jwt_kid="identity-1"`,
  `jwt_private_key_path=""`, `jwt_public_key_path=""`,
  `access_token_ttl_seconds=900`, `refresh_token_ttl_seconds=2592000`).
  JWT-поля используются начиная с Шага 3.
- `app/db/base.py` — `Base(DeclarativeBase)`.
- `app/db/session.py` — `create_async_engine(pool_pre_ping=True)`,
  `async_sessionmaker(expire_on_commit=False)`, `get_session()`.
- `app/main.py` — `FastAPI(title="FlowOps Identity Service", lifespan=lifespan)`,
  `include_router(api_v1_router, prefix="/api/v1")`, `GET /health`.
- `app/api/v1/router.py` — пустой `APIRouter()`, наполнится на Шаге 4.
- Alembic: `alembic.ini` (URL перезаписывается `env.py` из
  `settings.database_url`), `migrations/env.py` (async-режим,
  `compare_type=True`, `from app import models  # noqa: F401`),
  `migrations/script.py.mako`, `migrations/README`, `migrations/versions/`.
- `pyproject.toml` содержит `[tool.ruff] extend-exclude = ["migrations/versions"]`.
- Dockerfile: multi-stage, `python:3.14-slim`, `ENV POETRY_VERSION=2.4.3`,
  non-root `app`,
  `CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]`.
- `compose.yaml` (корень): `identity-db` (postgres:17-alpine, **5433:5432**,
  volume `identity_postgres_data`, healthcheck), `identity-api` (build из
  `services/identity`, **8001:8000**, depends_on `identity-db` service_healthy,
  healthcheck через `/health`).
- CI (`.github/workflows/ci.yml`): workflow `CI`, два параллельных job —
  `inventory` и `identity` (`poetry install --with dev`,
  `ruff format --check`, `ruff check`, `docker build -t flowops-identity:ci .`).
  Pytest в job `identity` — на Шаге 5.

**Шаг 2 — модели + миграция + strategy A (закрыт):**

- `app/models/user.py` — `User` + `UserRole(StrEnum)`, `SAEnum(...,
  values_callable=lambda x: [e.value for e in x])`, нативный PG enum
  `user_role`; `id` UUID (`default=uuid4` + `server_default=gen_random_uuid()`);
  `email String(320) unique`; `password_hash String(255)`;
  `role` default `USER` + `server_default='user'`; `is_active` default `True`
  + `server_default=true`; `created_at`/`updated_at` `server_default=func.now()`,
  `updated_at.onupdate=func.now()`.
- `app/models/refresh_token.py` — `RefreshToken`, PK `jti` UUID, FK
  `user_id→users.id ON DELETE CASCADE`, индекс `ix_refresh_tokens_user_id`,
  `expires_at`, `revoked_at nullable`, `replaced_by_jti nullable` (без FK —
  историческое audit-поле), `created_at`.
- `app/models/login_attempt.py` — `LoginAttempt`, `id` UUID, FK
  `user_id→users.id ON DELETE SET NULL` (nullable), `email String(320)`,
  `success`, `ip String(45)`, `created_at`, индекс
  `ix_login_attempts_email_created_at (email, created_at)`.
- `app/models/__init__.py` — импорты всех трёх + `__all__`.
- Миграция `57bb9f3333c4_create_identity_tables.py`:
  - enum `user_role` = `'admin', 'user'` (строчные, благодаря `values_callable`);
  - три таблицы, FK с `ondelete` (CASCADE / SET NULL), индексы, `server_default`;
  - `downgrade` содержит `sa.Enum(name='user_role').drop(op.get_bind(),
    checkfirst=False)` **после** `op.drop_table('users')` — дописано вручную,
    потому что Alembic не генерирует `DROP TYPE` автоматически.
- `compose.yaml`: `identity-api.command: sh -c "alembic upgrade head &&
  uvicorn app.main:app --host 0.0.0.0 --port 8000"` (strategy A).
- Проверено: `upgrade → downgrade → upgrade` чисто; `\dT+ user_role` =
  `admin, user`; smoke-вставка даёт `role=user`; 4 контейнера healthy;
  оба `/health` отвечают; миграция применяется при старте `identity-api`;
  коммит `identity: models + initial migration (step 2)` сделан.

**Известные особенности (не баги):**

- `sa.Enum` по умолчанию сохраняет `.name` членов Python-enum (`'ADMIN'`), а
  не `.value` (`'admin'`). Фикс — `values_callable=lambda x: [e.value for e in x]`.
  Если добавляется новый enum — не забыть `values_callable`.
- Alembic `autogenerate` не создаёт `DROP TYPE` в `downgrade` для нативных
  enum. Дописывать вручную: `sa.Enum(name='<type>').drop(op.get_bind(),
  checkfirst=False)` **после** `drop_table`, который использует тип.
- `alembic autogenerate` может терять `ondelete` у FK в некоторых версиях —
  проверять глазами.
- `StrEnum` (Python 3.11+) предпочтительнее `(str, enum.Enum)` для строковых
  enum.
- Alembic `autogenerate` иногда не видит модели, если `migrations/env.py`
  пуст/не импортирует `models` — проверять `Base.metadata.tables.keys()`
  после добавления моделей.

**Ещё не сделано (Шаги 3–5):**

- Безопасность: Argon2id, JWT (RS256), ключи, `get_current_user`, роли.
- Схемы, репозитории, сервисный слой, эндпоинты (`/auth/register`,
  `/auth/login`, `/auth/refresh`, `/auth/logout`, `/auth/me`, `/users`).
- Тесты (минимум 10: unit + integration).
- `/ready` с проверкой БД — открытый вопрос, не блокирует.
- Метрики Prometheus, Redis-кэш, Nginx — по мере появления остальных сервисов.

---

## 3. Принятые архитектурные решения по Identity

1. Аутентификация — отдельный Identity Service со своей БД.
2. Access + refresh. Access TTL 15 мин, refresh 30 дней (в конфиге).
3. Подпись **RS256**. Приватный ключ только в Identity (.env/секрет, не в Git).
   Публичный ключ проверяющим — через .env на первом этапе; JWKS позже.
   В заголовке токена **kid** (задел под ротацию).
4. Access claims: `sub, role, type=access, exp, iat, jti, kid`.
5. Refresh claims: `sub, type=refresh, exp, iat, jti, kid` (без role —
   роль читается из БД при refresh).
6. Роль — `UserRole(StrEnum)` (`admin`/`user`) в `User` (не отдельная таблица).
   В access — claim `role` (снимок на момент выпуска).
7. Пароли — только хеш **Argon2id** (`argon2-cffi`). В логи/ответы не попадают.
8. `register` → 201 + пользователь, БЕЗ токенов.
9. `login` → 200 + access/refresh. Неверные данные → 401 без раскрытия
   существования email. `is_active=false` → 403. Каждая попытка пишется
   в `LoginAttempt`.
10. `refresh` → 200 + новая пара. **Ротация:** старый refresh `revoked_at`,
    `replaced_by_jti = новый jti`. Истёкший/отозванный/неизвестный → 401.
    Повторное использование использованного refresh → 401 + отзыв всей цепочки
    refresh-токенов пользователя.
11. `logout` → 204, отзывает ТОЛЬКО переданный refresh (текущая сессия).
12. `GET /me` — по access. `GET /api/v1/users` — только admin.
13. Межсервисные вызовы (Order → Inventory) — НЕ пользовательский токен,
    а отдельный сервисный заголовок/токен (фиксируется в контракте Order).
14. Порядок реализации Identity: каркас → модели+миграция → безопасность
    (хеш, ключи, выпуск/проверка токенов) → схемы/репозитории/сервис →
    эндпоинты+зависимости → тесты → Docker/CI.

### Модели (реализованы на Шаге 2)

**User**

- `id` UUID PK (`default=uuid4` + `server_default=gen_random_uuid()`);
- `email String(320) unique`, нормализация lower-case в сервисе;
- `password_hash String(255)`;
- `role UserRole` (нативный PG enum `user_role`), default `user`;
- `is_active bool`, default `true`;
- `created_at`, `updated_at` — timezone-aware, `server_default=func.now()`,
  `updated_at.onupdate=func.now()`.

**RefreshToken**

- `jti` UUID PK;
- `user_id` FK → `users.id` `ON DELETE CASCADE`, индекс
  `ix_refresh_tokens_user_id`;
- `expires_at`;
- `revoked_at` nullable;
- `replaced_by_jti` nullable (без FK — историческое audit-поле);
- `created_at`.

**LoginAttempt**

- `id` UUID PK;
- `user_id` nullable FK → `users.id` `ON DELETE SET NULL`;
- `email String(320)`;
- `success bool`;
- `ip String(45)` nullable;
- `created_at`;
- индекс `ix_login_attempts_email_created_at (email, created_at)`.

### Эндпоинты (план, наполнение на Шаге 4)

- `POST /api/v1/auth/register` → 201 + user (`id, email, role, is_active,
  created_at`); дубликат email → 409; валидация → 422.
- `POST /api/v1/auth/login` → 200 + `access_token, refresh_token,
  token_type=bearer`; неверные данные → 401; `is_active=false` → 403.
- `POST /api/v1/auth/refresh` → 200 + новая пара; ошибки → 401
  (+отзыв цепочки при повторе использованного refresh).
- `POST /api/v1/auth/logout` → 204; отзыв текущего refresh.
- `GET /api/v1/auth/me` → 200 + текущий user; требует access.
- `GET /api/v1/users` → 200 + список; только admin.
- `GET /health` → 200 `{"status":"healthy"}` (liveness).

---

## 4. Структура репозитория (актуальная)
flow_ops/
├── compose.yaml
├── .github/
│ └── workflows/
│ └── ci.yml
├── docs/
│ └── FLOWOPS_HANDOFF.md
└── services/
├── inventory/
│ ├── .dockerignore
│ ├── .env
│ ├── .gitignore
│ ├── alembic.ini
│ ├── check_db.py
│ ├── Dockerfile
│ ├── poetry.lock
│ ├── pyproject.toml
│ ├── migrations/
│ │ ├── env.py
│ │ ├── README
│ │ ├── script.py.mako
│ │ └── versions/
│ │ └── ddfe927be72e_create_inventory_tables.py
│ ├── app/
│ │ ├── main.py
│ │ ├── init.py
│ │ ├── api/v1/router.py
│ │ ├── api/v1/endpoints/reservations.py
│ │ ├── core/config.py
│ │ ├── db/{base.py, session.py}
│ │ ├── models/{inventory.py, reservation.py, reservation_item.py}
│ │ ├── repositories/{inventory.py, reservation.py}
│ │ ├── schemas/{inventory.py, reservation.py}
│ │ └── services/reservation.py
│ └── tests/
│ ├── conftest.py
│ ├── test_database.py
│ ├── test_reservations.py
│ └── test_reservations_api.py
└── identity/
├── .dockerignore
├── .env # локальный, в .gitignore
├── .env.example
├── .gitignore
├── alembic.ini
├── Dockerfile # ENV POETRY_VERSION=2.4.3
├── poetry.lock # сгенерирован Poetry 2.4.3
├── pyproject.toml # + [tool.ruff] extend-exclude = ["migrations/versions"]
├── migrations/
│ ├── env.py
│ ├── README
│ ├── script.py.mako
│ └── versions/
│ └── 57bb9f3333c4_create_identity_tables.py
├── app/
│ ├── init.py
│ ├── main.py
│ ├── api/
│ │ └── v1/
│ │ ├── init.py
│ │ └── router.py
│ ├── core/
│ │ ├── init.py
│ │ └── config.py
│ ├── db/
│ │ ├── init.py
│ │ ├── base.py
│ │ └── session.py
│ ├── models/
│ │ ├── init.py
│ │ ├── user.py
│ │ ├── refresh_token.py
│ │ └── login_attempt.py
│ ├── repositories/
│ │ └── init.py
│ ├── schemas/
│ │ └── init.py
│ └── services/
│ └── init.py
└── tests/ # появится на Шаге 5

text

Генерируемые каталоги (`.pytest_cache/`, `.ruff_cache/`, `__pycache__/`,
`.venv/`) — не часть архитектуры, в репозиторий не входят.

В `services/inventory/` есть пустой каталог `alembic/` — не трогать без проверки,
фактическая конфигурация миграций в `migrations/` и `alembic.ini`.

---

## 5. Обязательные правила проекта

1. **Версия Poetry должна совпадать.** Версия в Dockerfile
   (`ENV POETRY_VERSION=2.4.3`) должна точно совпадать с версией Poetry,
   которой локально генерируется `poetry.lock`. Иначе `docker compose build`
   падает с `pyproject.toml changed significantly since poetry.lock was last
   generated`.
2. **Любое изменение `pyproject.toml` требует обновления lock.** Даже изменение
   `[tool.*]` → `poetry lock` → коммит `pyproject.toml` + `poetry.lock` вместе.
3. **Миграции в Docker — стратегия A.** В `compose.yaml` для `identity-api`:
   `command: sh -c "alembic upgrade head && uvicorn app.main:app --host 0.0.0.0
   --port 8000"`. При переходе к Nginx/балансировке — стратегия B (отдельный
   one-off `identity-migrate` + `depends_on: service_completed_successfully`).
4. **Не менять архитектурные решения молча.** Если видишь проблему — объясни
   и предложи варианты.
5. **Работать небольшими шагами.** Не переходить дальше, пока текущий шаг
   не реализован и не проверен.
6. **Предупреждать о росте контекста.** Если переписка разрастается, заранее
   предупредить и подготовить handoff.
7. **Inventory не переписывать** без конкретной причины/бага/нового требования.
8. **Секреты не коммитить.** `.env` локальный, в `.gitignore`. В Git только
   `.env.example`. JWT-ключи — локально в `.env`, в GitHub Secrets для CI,
   Docker secrets в production.
9. **`ruff check .`** — обязателен в каждом сервисе и в CI.
   `ruff format --check` — по мере готовности файлов (для Identity включён
   в CI сразу; `migrations/versions/` исключён из ruff через `extend-exclude`).
10. **Не использовать destructive-команды** (`docker compose down -v`,
    удаление volumes) без предупреждения — volumes содержат данные PostgreSQL.
11. **Коммит после каждой проверенной части (A/B/C):**
    `git add -A && git commit -m "<scope>: <что сделано> (step N)"`.
    Причина: в Шаге 1 был потерян `migrations/env.py` (оказался пустым),
    восстанавливали вручную. С коммитом — `git checkout -- <file>` и всё.
    Коммит — не «для красоты», а страховка от потери и точка отката.
12. **Предлагать надёжное, а не простое.** При выборе между «проще
    реализовать» и «надёжнее в эксплуатации» — выбирать надёжное. Учитывать:
    корректность при сбоях (crash, timeout, race, retry), совместимость
    миграций (upgrade/downgrade), безопасность (утечки, enumeration, timing),
    предсказуемость в проде. Если «простое» и «надёжное» расходятся —
    объяснить трейд-офф и рекомендовать надёжное.

---

## 6. План Identity: Шаги 3–5

### Шаг 2 — модели + миграция + strategy A — ЗАКРЫТ

См. раздел 2 (Identity, Шаг 2). Реализовано, проверено, закоммичено.

### Шаг 3 — безопасность (СЛЕДУЮЩИЙ)

- Зависимости: `argon2-cffi`, `pyjwt`, `cryptography` → `poetry lock` → коммит.
- `app/core/security.py`:
  - `hash_password` / `verify_password` (Argon2id);
  - `load_private_key` / `load_public_key` (RS256, пути из `Settings`);
  - `create_access_token(sub, role, kid)` → `{sub, role, type="access", exp,
    iat, jti, kid}`;
  - `create_refresh_token(sub, jti, kid)` → `{sub, type="refresh", exp, iat,
    jti, kid}` (без role);
  - `decode_token(token, expected_type)` — проверка подписи, exp, type;
    возврат payload или исключение.
- Генерация ключей (dev): скрипт/инструкция, ключи в
  `services/identity/keys/` (в `.gitignore`).
- **Проверка:** unit-тесты (argon2 round-trip, wrong password; JWT
  encode/decode, expiry, wrong kid, wrong type). Пока без эндпоинтов.

### Шаг 4 — схемы, репозитории, сервис, эндпоинты, зависимости

- `app/schemas/auth.py`, `app/schemas/user.py`.
- `app/repositories/user.py`, `refresh_token.py`, `login_attempt.py`.
- `app/services/auth.py` (register, login, refresh с ротацией и отзывом
  цепочки, logout), `app/services/user.py` (get/list).
- `app/api/deps.py`: `get_current_user` (Bearer access, type=access, загрузка
  user из БД), `require_admin`.
- `app/api/v1/endpoints/auth.py`, `users.py`; наполнить `router.py`.
- Обработка ошибок: 401/403/409/422, без утечки внутренних деталей.
- **Проверка:** Swagger, ручной прогон register→login→refresh→logout→me,
  повторный refresh с использованным токеном → 401 + отзыв цепочки.

### Шаг 5 — тесты + финализация

- `services/identity/tests/` — минимум 10 тестов (unit + integration), моки,
  фикстуры (БД, клиент, admin/user).
- CI job `identity`: добавить `pytest -v`.
- Обновить handoff.

---

## 7. Открытые вопросы на будущее (не блокируют)

- `/ready` с проверкой БД — когда вводить.
- JWKS-эндпоинт и kid-ротация — когда.
- Дублировать проверяющий слой JWT в каждом сервисе или выносить в общий пакет.
- Контракт Order ↔ Inventory (endpoint, payload, события, timeout/отказ,
  идемпотентность создания заказа, компенсация/release reservation).
- Transactional outbox в Order: схема, публикация, retry/DLQ.
- Стратегия миграций B (one-off `identity-migrate`) — при переходе к Nginx.
- В Inventory: политика повторного `order_id` с отличающимся payload
  (вариант A — первый побеждает, вариант B — 409). Не менять без контракта
  Order.
- В Inventory: подтверждение/освобождение резерваций пока не реализованы
  как бизнес-операции; enum статусов уже содержит `RESERVED`, `CONFIRMED`,
  `RELEASED` — жизненный цикл проектировать вместе с Order.
- В Inventory: `GET /health` — liveness без проверки БД; `/ready` при
  необходимости.

---

## 8. Полезные команды

Из корня (PowerShell):

```powershell
tree /F /A
git status
git log -1 --oneline
docker compose config
docker compose up -d --build
docker compose ps
docker compose logs -f identity-api
docker compose logs -f inventory-api
```

Из services/identity:

```powershell
poetry install
poetry lock
poetry run ruff check .
poetry run ruff check . --fix
poetry run ruff format .
poetry run ruff format --check .
poetry run alembic current
poetry run alembic upgrade head
poetry run alembic downgrade -1
poetry run alembic revision --autogenerate -m "message"
poetry run pytest -v   # после Шага 5
poetry check --lock
```
Из services/inventory — аналогично (см. предыдущую версию handoff).

Swagger: http://localhost:8000/docs (Inventory),
http://localhost:8001/docs (Identity). Health:
http://localhost:8000/health, http://localhost:8001/health.


## 9. Текущая точка старта

- **Шаг 1 Identity закрыт:** каркас, БД, Alembic, Docker, CI — зелёные.
- **Шаг 2 Identity закрыт:** модели `User` / `RefreshToken` / `LoginAttempt`, миграция `57bb9f3333c4`, strategy A в compose, цикл `upgrade/downgrade/upgrade` чистый, коммит сделан.
- **Следующий шаг — Шаг 3 Identity:** безопасность (Argon2id, RS256-ключи, выпуск/проверка JWT). См. раздел 6.
- **Inventory не трогать.**
- **До начала кода по Шагу 3:** согласовать план (раздел 6) и ключевые решения (параметры Argon2, формат ключей PEM, хранение dev-ключей, тестирование в CI без реальных ключей).

---

## 10. Инструкция для первого ответа после передачи

1. Кратко подтверди понимание идеи **FlowOps** и текущего статуса (**Inventory** готов, по **Identity** закрыты Шаги 1 и 2).
2. Зафиксируй правило, что **Inventory** не переписывается и не затрагивается.
3. Обозначь следующий этап — **Шаг 3 Identity** (безопасность: Argon2id, RS256, JWT).
4. Предложи начать с согласования плана Шага 3 (раздел 6) и принятия архитектурных решений до написания кода (параметры Argon2, формат PEM-ключей, расположение dev-ключей, генерация/моки ключей для CI).
5. Задавай строго **один** действительно необходимый вопрос за раз.
6. Не пересказывай всю документацию и не давай базовых вводных по FastAPI.
7. Следи за объёмом контекста: если диалог начнёт разрастаться — предупреди заранее и подготовь обновлённый handoff-файл.
