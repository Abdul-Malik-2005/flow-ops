# FlowOps — техническая передача проекта (Handoff)

**Актуально на:** Шаг 3 Identity Service, части A и B закрыты (безопасность + тесты безопасности); Часть C (CI + Docker-сборка) — следующий шаг.
**Репозиторий:** https://github.com/Abdul-Malik-2005/flow-ops
**Наставник:** senior Python Backend архитектор. Общение на русском, уровень Middle+, без базовых объяснений FastAPI. Работать небольшими этапами, не переходить к следующему, пока текущий не проверен. Давать полный код файла с точным путём. Не менять архитектурные решения молча. **Предлагать не «проще», а надёжнее** — с учётом корректности при сбоях, совместимости миграций, безопасности, предсказуемости в проде.

---

## 1. Идея и стек

FlowOps — портфолио-проект Middle+ Python Backend: микросервисная платформа
заказов/склада/оплат/уведомлений. Каждый сервис владеет своей БД; прямой доступ
к чужим таблицам запрещён.

**Стек:** Python 3.14+, FastAPI, Pydantic/pydantic-settings, PostgreSQL 17,
SQLAlchemy 2.x async, asyncpg, Alembic, Poetry 2.4.3, Docker/Compose,
pytest/pytest-asyncio, Ruff, HTTPX, argon2-cffi, pyjwt, cryptography.
RabbitMQ/Redis/Nginx/Prometheus — по мере появления сервисов, не все внедрены.
Kafka/K8s/Service Mesh не нужны. Saga/CQRS не обязательны.

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

### Identity Service — ШАГИ 1 И 2 ЗАКРЫТЫ, ШАГ 3 (A + B) ЗАКРЫТ

**Шаг 1 — каркас (закрыт):**

- `services/identity/` — отдельный Poetry-проект (`package-mode = false`,
  Poetry 2.4.3, Python 3.14+, версии зависимостей 1-в-1 с Inventory).
- Структура: `app/{api/v1,core,db,models,repositories,schemas,services}`,
  `migrations/`, `alembic.ini`, `Dockerfile`, `.dockerignore`, `.env.example`,
  `.gitignore`.
- `app/core/config.py` — `Settings(BaseSettings)` с полями:
  `database_url`, `test_database_url`, `debug`, `environment`,
  JWT-поля (`jwt_algorithm="RS256"`, `jwt_kid="identity-1"`,
  `jwt_private_key_path=""`, `jwt_public_key_path=""`,
  `access_token_ttl_seconds=900`, `refresh_token_ttl_seconds=2592000`).
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
  Pytest в job `identity` — добавляется на Шаге 3, часть C (тесты
  безопасности уже есть — 13 passed локально).

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

**Шаг 3, часть A — безопасность (закрыта):**

- Зависимости добавлены в `pyproject.toml` → `poetry.lock`:
  `argon2-cffi (~25.1)`, `pyjwt (~2.10)`, `cryptography (~46.0)`.
- `scripts/gen_keys.py` — генерация RSA 2048 PEM:
  `keys/jwt_private.pem` (PKCS#8, без пароля) + `keys/jwt_public.pem`
  (SubjectPublicKeyInfo). Отказывается перезаписывать без `--force`.
  `keys/` добавлен в `.gitignore`.
- `.env.example` и локальный `.env`: пути
  `JWT_PRIVATE_KEY_PATH=./keys/jwt_private.pem`,
  `JWT_PUBLIC_KEY_PATH=./keys/jwt_public.pem`, `JWT_ALGORITHM=RS256`,
  `JWT_KID=identity-1`, `ACCESS_TOKEN_TTL_SECONDS=900`,
  `REFRESH_TOKEN_TTL_SECONDS=2592000`.
- `app/core/security.py`:
  - Argon2id (`PasswordHasher`, t=3, m=64 MiB, p=4, hash_len=32, salt_len=16,
    `type=Type.ID` — OWASP-параметры, зафиксированы явно);
  - `hash_password` / `verify_password` (ловит `VerifyMismatchError` и
    `InvalidHashError`, возврат `False`, не бросает);
  - `_private_key()` / `_public_key()` — `lru_cache(maxsize=1)`,
    `Path(...).resolve()`, `FileNotFoundError` с понятным сообщением
    («запусти `scripts/gen_keys.py`»);
  - `create_access_token(sub: UUID, role: UserRole)` — claims
    `sub, role, type="access", iat, exp, jti`; `kid` в **header**;
  - `create_refresh_token(sub: UUID, jti: UUID)` — claims
    `sub, type="refresh", iat, exp, jti`; **без role**; `jti` передаётся
    извне (сервис на Шаге 4 запишет тот же `jti` в БД до `INSERT`);
  - `decode_token(token, expected_type: Literal["access","refresh"])` —
    проверка подписи RS256, `options={"require": ["exp","iat","sub","type","jti"]}`,
    проверка `type == expected_type`, бросает `InvalidTokenError` (собственное
    исключение, не `jwt.InvalidTokenError`);
  - `algorithms=[settings.jwt_algorithm]` явно — защита от `alg=none` и
    подмены RS256→HS256;
  - `iat`/`exp` — `datetime.now(UTC)`, не `utcnow()`.
- Проверено: импорт `security` без ключей (ключи нужны только при первом
  вызове `_private_key`), smoke-тесты argon2 round-trip и JWT encode/decode,
  `ruff check` и `ruff format --check` чисто.
- Коммит `identity: security — argon2id, RS256 keys, jwt (step 3, part A)`.

**Шаг 3, часть B — тесты безопасности (закрыта):**

- `tests/__init__.py` — пустой пакет.
- `tests/conftest.py`:
  - `os.environ.setdefault("DATABASE_URL", ...)` **до** импорта `app.*`
    (иначе `Settings()` падает `ValidationError` при `pytest`, когда `.env`
    не найден в cwd; в CI `.env` нет вообще);
  - `DATABASE_URL`, `DEBUG`, `ENVIRONMENT` — фиктивные значения для тестов;
  - фикстура `jwt_keys(tmp_path, monkeypatch)` — генерирует RSA 2048 в
    `tmp_path`, подменяет `settings.jwt_private_key_path` /
    `settings.jwt_public_key_path`, делает `security._private_key.cache_clear()`
    и `security._public_key.cache_clear()` **до и после** теста (иначе
    `lru_cache` на процесс подтянет ключ предыдущего теста);
  - фикстуры `user_id`, `refresh_jti`, `access_token`, `refresh_token`.
- `tests/test_security.py` — **13 тестов** (все passed):
  argon2 round-trip, wrong password, malformed hash, salted (два хеша
  одного пароля разные); access claims, `kid`/`alg` в header; refresh
  claims без role; type mismatch (refresh↔access); подпись чужим ключом;
  мусорный токен; истёкший токен; отсутствующий обязательный claim.
- Урок: тест, вызывающий `decode_token`/`create_*_token`/`_private_key`/
  `_public_key`, **обязан** иметь `jwt_keys` в сигнатуре (явно или
  транзитивно через фикстуру-токен). `test_garbage_token_is_rejected` сначала
  падал `FileNotFoundError` — забыл `jwt_keys`.
- Коммит `identity: security tests — argon2id + jwt (step 3, part B)`.

**Ещё не сделано (Шаг 3, часть C; Шаги 4–5):**

- **Шаг 3, часть C:** CI job `identity` — добавить `pytest -v` (сейчас
  только `ruff format --check` + `ruff check` + `docker build`);
  проверить `docker compose build identity-api` с новыми зависимостями
  (argon2-cffi, pyjwt, cryptography) — образ собирается; обновить handoff.
- **Шаг 4:** схемы, репозитории, сервисный слой, `get_current_user`,
  `require_admin`, эндпоинты (`/auth/register`, `/auth/login`,
  `/auth/refresh`, `/auth/logout`, `/auth/me`, `/users`).
- **Шаг 5:** тесты бизнес-логики (register/login/refresh/logout/me —
  unit + integration), минимум 10 помимо 13 тестов безопасности.
- `/ready` с проверкой БД — открытый вопрос, не блокирует.
- Метрики Prometheus, Redis-кэш, Nginx — по мере появления остальных сервисов.

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
- `pydantic-settings` резолвит `env_file=".env"` от cwd процесса, не от
  расположения `config.py`. Если `pytest` запущен из корня репозитория
  (или `.env` нет в cwd), `Settings()` падает `ValidationError: database_url
  Field required`. Фикс: `os.environ.setdefault(...)` **до** импорта `app.*`
  в `tests/conftest.py`.
- Ruff `UP017` требует `datetime.UTC` вместо `timezone.utc` (Python 3.11+).
  В `security.py` — `from datetime import UTC, datetime, timedelta` и
  `datetime.now(UTC)`.
- `ruff format check` (без `--`) — не команда. Правильно:
  `ruff format --check .` (флаг `--check`, не подкоманда `check`).
- `lru_cache` на `_private_key`/`_public_key` кеширует ключ **на процесс**.
  В тестах — `cache_clear()` до и после фикстуры `jwt_keys`, иначе второй
  тест увидит ключ первого.

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
├── .gitignore # + keys/
├── alembic.ini
├── Dockerfile # ENV POETRY_VERSION=2.4.3
├── poetry.lock # сгенерирован Poetry 2.4.3
├── pyproject.toml # + [tool.ruff] extend-exclude = ["migrations/versions"]
├── keys/ # локальные PEM-ключи, в .gitignore
│ ├── jwt_private.pem
│ └── jwt_public.pem
├── scripts/
│ └── gen_keys.py
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
│ │ ├── config.py
│ │ └── security.py
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
└── tests/
├── init.py
├── conftest.py
└── test_security.py

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
   Docker secrets в production. `keys/` — в `.gitignore`.
9. **`ruff check .`** — обязателен в каждом сервисе и в CI.
   `ruff format --check .` — по мере готовности файлов (для Identity включён
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
13. **Тесты — часть шага, а не отдельный Шаг 5.** Если на шаге появился код,
    который можно проверить unit-тестом (безопасность, схемы, сервисы) —
    тесты пишутся **на этом же шаге**. Шаг 5 — только интеграционные тесты
    и финализация.

---

## 6. План Identity: Шаги 3 (часть C), 4, 5

### Шаг 3, часть A — безопасность — ЗАКРЫТА

См. раздел 2 (Identity, Шаг 3, часть A). Реализовано, проверено, закоммичено.

### Шаг 3, часть B — тесты безопасности — ЗАКРЫТА

См. раздел 2 (Identity, Шаг 3, часть B). 13 тестов passed, закоммичено.

### Шаг 3, часть C — CI + Docker-сборка (СЛЕДУЮЩАЯ)

- `.github/workflows/ci.yml`, job `identity`: добавить шаг
  `- name: Run tests` / `run: poetry run pytest -v` между `ruff` и
  `docker build` (по образцу job `inventory`). Плюс `ruff format --check .`
  и `ruff check .` уже есть.
- Проверить `docker compose build identity-api` — образ собирается с
  `argon2-cffi`, `pyjwt`, `cryptography`; `poetry.lock` консистентен
  (Poetry 2.4.3 в Dockerfile = локальная версия).
- Опционально: поднять `identity-db` + `identity-api`, проверить, что
  миграция применяется при старте (не должно быть регрессий от Шага 2).
- Коммит `identity: ci — pytest in identity job (step 3, part C)`.
- Обновить handoff (Шаг 3 закрыт).

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
- Тесты на схемы/сервисы/эндпоинты — часть Шага 4 (unit + integration
  с моками). Шаг 5 — только оставшиеся интеграционные и финализация.

### Шаг 5 — тесты + финализация

- `services/identity/tests/` — минимум 10 тестов помимо 13 тестов
  безопасности (unit + integration).
- CI job `identity`: убедиться, что `pytest -v` зелёный (добавлен на
  Шаге 3, часть C).
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
- Rate limiting на `/auth/login` (Redis, скользящее окно или token bucket) —
  когда появится Redis; сейчас `LoginAttempt` пишется, но не блокирует.
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
Из services/identity:

powershell
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
poetry run pytest -v
poetry run pytest tests/test_security.py -v
poetry run python scripts/gen_keys.py
poetry run python scripts/gen_keys.py --force
poetry check --lock
Из services/inventory — аналогично (см. предыдущую версию handoff).

Swagger: http://localhost:8000/docs (Inventory),
http://localhost:8001/docs (Identity). Health:
http://localhost:8000/health, http://localhost:8001/health.

9. Текущая точка старта
Шаг 1 Identity закрыт: каркас, БД, Alembic, Docker, CI — зелёные.

Шаг 2 Identity закрыт: модели User/RefreshToken/LoginAttempt,
миграция 57bb9f3333c4, strategy A в compose, цикл
upgrade/downgrade/upgrade чистый, коммит сделан.

Шаг 3 Identity, части A и B закрыты:

часть A: app/core/security.py (Argon2id + RS256 JWT + загрузка
ключей), scripts/gen_keys.py, keys/ в .gitignore, pyproject.toml

poetry.lock с новыми зависимостями; коммит identity: security — argon2id, RS256 keys, jwt (step 3, part A);

часть B: tests/conftest.py (env до импорта app.*, фикстура
jwt_keys с lru_cache.clear()), tests/test_security.py — 13 тестов,
все passed; коммит identity: security tests — argon2id + jwt (step 3, part B).

Следующий шаг — Шаг 3, часть C: добавить pytest -v в job identity
в .github/workflows/ci.yml; проверить docker compose build identity-api
с новыми зависимостями; обновить handoff (Шаг 3 закрыт). См. раздел 6.

Inventory не трогать.

Перед началом Шага 4 (схемы/репозитории/сервис/эндпоинты) — согласовать
план и решения (формы Pydantic, обработка ошибок, стратегия ротации
refresh, отзыв цепочки, rate limiting на login — последнее опционально).

10. Инструкция для первого ответа после передачи
Кратко подтверди понимание идеи FlowOps и текущего статуса
(Inventory готов; Identity — Шаги 1 и 2 закрыты, Шаг 3 части A и B
закрыты, часть C — следующий шаг).

Зафиксируй правило, что Inventory не переписывается и не затрагивается.

Обозначь следующий этап — Шаг 3, часть C Identity: pytest -v в CI
job identity, проверка docker compose build identity-api с новыми
зависимостями, обновление handoff.

Предложи начать с согласования плана части C (раздел 6) и проверки,
что локальные тесты (13) и ruff зелёные перед правкой ci.yml.

Задавай строго один действительно необходимый вопрос за раз.

Не пересказывай всю документацию и не давай базовых вводных по FastAPI.

Следи за объёмом контекста: если диалог начнёт разрастаться — предупреди
заранее и подготовь обновлённый handoff-файл.