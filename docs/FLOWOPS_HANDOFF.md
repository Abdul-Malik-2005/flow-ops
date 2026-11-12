# FlowOps --- техническая передача проекта (Handoff)

**Актуально на:** конец Шага 1 Identity Service (каркас Identity поднят,
CI зелёный, Docker собирается).\
**Репозиторий:** https://github.com/Abdul-Malik-2005/flow-ops\
**Наставник:** senior Python Backend архитектор.

Общение на русском, уровень Middle+, без базовых объяснений FastAPI.
Работать небольшими этапами, не переходить к следующему, пока текущий не
проверен. Давать полный код файла с точным путём. Не менять
архитектурные решения молча.

------------------------------------------------------------------------

## 1. Идея и стек

FlowOps --- портфолио-проект Middle+ Python Backend: микросервисная
платформа заказов, склада, оплат и уведомлений. Каждый сервис владеет
собственной БД; прямой доступ к чужим таблицам запрещён.

**Стек:** Python 3.14+, FastAPI, Pydantic/pydantic-settings, PostgreSQL
17, SQLAlchemy 2.x async, asyncpg, Alembic, Poetry 2.4.3,
Docker/Compose, pytest/pytest-asyncio, Ruff, HTTPX.

RabbitMQ, Redis, Nginx и Prometheus внедряются по мере появления
сервисов, а не заранее все сразу. Kafka, Kubernetes и Service Mesh не
входят в текущий план. Saga/CQRS не обязательны --- применять только при
обоснованной необходимости.

**Порядок реализации:** Inventory (завершён) → Identity → Order →
Catalog → Payment → Notification.

Order строится по варианту B: асинхронная оркестрация через RabbitMQ и
transactional outbox.

------------------------------------------------------------------------

## 2. Состояние по сервисам

### Inventory Service --- ЗАВЕРШЁН, НЕ ПЕРЕПИСЫВАТЬ

- 16 тестов passed, Docker, CI, Alembic.
- Владеет складскими остатками и резервациями.
- Идемпотентен по `order_id`.
- Использует advisory lock по `order_id` и `SELECT FOR UPDATE` по
  складским строкам.
- Endpoint: `POST /api/v1/reservations/`.
- Health endpoint: `GET /health`.
- Подробности реализации находятся в предыдущей версии handoff (разделы
  про Inventory).

### Identity Service --- ШАГ 1 ЗАКРЫТ (каркас)

Реализовано:

- `services/identity/` как отдельный Poetry-проект
  (`package-mode = false`, Poetry 2.4.3, Python 3.14+, версии
  зависимостей 1-в-1 с Inventory).

- Структура:
  `app/{api/v1,core,db,models,repositories,schemas,services}`,
  `migrations/`, `alembic.ini`, `Dockerfile`, `.dockerignore`,
  `.env.example`, `.gitignore`.

- `app/core/config.py` --- `Settings(BaseSettings)` с полями:

  - `database_url`
  - `test_database_url`
  - `debug`
  - `environment`
  - JWT-заготовки:
    - `jwt_algorithm="RS256"`
    - `jwt_kid="identity-1"`
    - `jwt_private_key_path=""`
    - `jwt_public_key_path=""`
    - `access_token_ttl_seconds=900`
    - `refresh_token_ttl_seconds=2592000`

  JWT-поля на Шаге 1 ещё не используются --- это задел под Шаг 3.

- `app/db/base.py` --- `Base(DeclarativeBase)`.

- `app/db/session.py` --- `create_async_engine`,
  `async_sessionmaker(expire_on_commit=False)`, `get_session()`.

- `app/main.py` ---
  `FastAPI(title="FlowOps Identity Service", lifespan=lifespan)`,
  `include_router(api_v1_router, prefix="/api/v1")`, `GET /health`.

- `app/api/v1/router.py` --- пустой `APIRouter()`, наполнится на Шаге 4.

- Alembic:

  - `alembic.ini` --- URL в ini перезаписывается `env.py` из
    `settings.database_url`;
  - `migrations/env.py` --- async-режим, `compare_type=True`,
    `from app import models  # noqa: F401`;
  - `migrations/script.py.mako`, `migrations/README`;
  - `migrations/versions/` пока пусто.

- `pyproject.toml` содержит
  `[tool.ruff] extend-exclude = ["migrations/versions"]` ---
  сгенерированные миграции не линтуются, `env.py` линтуется.

- Dockerfile: multi-stage, `python:3.14-slim`,
  `ENV POETRY_VERSION=2.4.3`, non-root пользователь `app`.

- Команда запуска Dockerfile:
  `uvicorn app.main:app --host 0.0.0.0 --port 8000`.

- Корневой `compose.yaml`:

  - `identity-db`: `postgres:17-alpine`, наружный порт `5433:5432`,
    volume `identity_postgres_data`, healthcheck;
  - `identity-api`: build из `services/identity`, наружный порт
    `8001:8000`, зависит от `identity-db` со статусом `service_healthy`,
    имеет healthcheck через `/health`.

- CI (`.github/workflows/ci.yml`): workflow `CI`, два параллельных job:

  - `inventory` --- без изменений;
  - `identity` --- PostgreSQL service `identity_test_db`,
    `poetry install --with dev`, `ruff format --check`, `ruff check`,
    `docker build -t flowops-identity:ci .`.

- Pytest в job `identity` пока отсутствует --- добавить на Шаге 5.

**Проверено на Шаге 1:**

- `poetry install` --- без ошибок, lock сгенерирован Poetry 2.4.3.
- `alembic current`, `alembic upgrade head` --- без ошибок (миграций
  пока нет).
- `alembic revision --autogenerate` --- получилась пустая миграция; она
  удалена, так как была бесполезна. Настоящая миграция появится на Шаге
  2.
- `ruff check .` --- All checks passed.
- `docker compose build identity-api` --- успешно.
- `docker compose up identity-db identity-api` --- оба контейнера
  healthy.
- `curl http://localhost:8001/health` → `{"status":"healthy"}`.
- Inventory на порту 8000 не сломан.
- GitHub Actions: оба job зелёные.

**Ещё не сделано (Шаги 2--5):**

- Модели `User`, `RefreshToken`, `LoginAttempt` и миграция.
- Безопасность: Argon2id, JWT (RS256), ключи, `get_current_user`, роли.
- Схемы, репозитории, сервисный слой, эндпоинты (`/auth/register`,
  `/auth/login`, `/auth/refresh`, `/auth/logout`, `/auth/me`, `/users`).
- Тесты (минимум 10: unit + integration).
- `/ready` с проверкой БД --- открытый вопрос, не блокирует.
- Метрики Prometheus, Redis-кэш и Nginx --- по мере появления остальных
  сервисов.

------------------------------------------------------------------------

## 3. Принятые архитектурные решения по Identity

1.  Аутентификация реализуется отдельным Identity Service со своей БД.
2.  Используются access и refresh токены:
    - access TTL --- 15 минут;
    - refresh TTL --- 30 дней.
3.  Подпись JWT --- **RS256**:
    - приватный ключ хранится только в Identity (локально в
      `.env`/секрете, не в Git);
    - публичный ключ передаётся проверяющим сервисам через `.env` на
      первом этапе;
    - JWKS планируется позже;
    - в заголовке токена присутствует `kid` --- задел под ротацию
      ключей.
4.  Access claims: `sub`, `role`, `type=access`, `exp`, `iat`, `jti`,
    `kid`.
5.  Refresh claims: `sub`, `type=refresh`, `exp`, `iat`, `jti`, `kid`.
    Claim `role` в refresh отсутствует --- роль читается из БД при
    обновлении токенов.
6.  Роли --- Enum `admin`/`user` в модели `User`, отдельная таблица
    ролей не создаётся. В access token включается claim `role` ---
    снимок роли на момент выпуска токена.
7.  Пароли хранятся только в виде хеша **Argon2id** (`argon2-cffi`).
    Пароли и хеши не попадают в логи или ответы API.
8.  Регистрация (`register`) возвращает `201` и пользователя, но не
    выдаёт токены.
9.  Login:
    - успешный --- `200` + access/refresh;
    - неверные данные --- `401` без раскрытия существования email;
    - `is_active=false` --- `403`;
    - каждая попытка записывается в `LoginAttempt`.
10. Refresh:
    - возвращает `200` и новую пару токенов;
    - старый refresh помечается `revoked_at`, `replaced_by_jti`
      указывает на новый JTI;
    - истёкший, отозванный или неизвестный refresh → `401`;
    - повторное использование уже использованного refresh → `401` и
      отзыв всей цепочки refresh-токенов пользователя.
11. Logout возвращает `204` и отзывает только переданный refresh token
    (текущую сессию).
12. `GET /me` требует access token.
13. `GET /api/v1/users` доступен только admin.
14. Межсервисные вызовы (например, Order → Inventory) используют не
    пользовательский токен, а отдельный сервисный заголовок/токен.
    Конкретный механизм фиксируется в контракте Order.
15. Порядок реализации Identity:
    - каркас;
    - модели и миграция;
    - безопасность (хеширование, ключи, выпуск/проверка токенов);
    - схемы, репозитории и сервис;
    - эндпоинты и зависимости;
    - тесты;
    - Docker/CI-финализация.

### Планируемые модели (Шаг 2)

**User**

- `id` --- UUID PK;
- `email` --- unique index, нормализация в lower-case в сервисном слое;
- `password_hash`;
- `role` --- Enum `admin`/`user`, default `user`;
- `is_active` --- bool, default `true`;
- `created_at`, `updated_at` --- timezone-aware.

**RefreshToken**

- `jti` --- UUID PK;
- `user_id` --- FK → `users.id`, `ON DELETE CASCADE`, индекс;
- `expires_at`;
- `revoked_at` --- nullable;
- `replaced_by_jti` --- nullable;
- `created_at`.

**LoginAttempt**

- `id` --- UUID PK;
- `user_id` --- nullable FK → `users.id`, `ON DELETE SET NULL`;
- `email`;
- `success` --- bool;
- `ip` --- nullable;
- `created_at`;
- индекс `(email, created_at)`.

### Планируемые эндпоинты

  ------------------------------------------------------------------------------------------
  Метод             Endpoint                  Успешный ответ               Основные ошибки /
                                                                           доступ
  ----------------- ------------------------- ---------------------------- -----------------
  POST              `/api/v1/auth/register`   `201` + user (`id`, `email`, Дубликат email →
                                              `role`, `is_active`,         `409`; валидация
                                              `created_at`)                → `422`

  POST              `/api/v1/auth/login`      `200` + `access_token`,      Неверные данные →
                                              `refresh_token`,             `401`; неактивный
                                              `token_type=bearer`          пользователь →
                                                                           `403`

  POST              `/api/v1/auth/refresh`    `200` + новая пара токенов   Ошибки токена →
                                                                           `401`; повторное
                                                                           использование ---
                                                                           отзыв цепочки

  POST              `/api/v1/auth/logout`     `204`                        Отзывает
                                                                           переданный
                                                                           refresh

  GET               `/api/v1/auth/me`         `200` + текущий пользователь Требует access
                                                                           token

  GET               `/api/v1/users`           `200` + список пользователей Только admin

  GET               `/health`                 `200 {"status":"healthy"}`   Liveness
  ------------------------------------------------------------------------------------------

------------------------------------------------------------------------

## 4. Структура репозитория (актуальная)

``` text
flow_ops/
├── compose.yaml                         # inventory-db/api, identity-db/api
├── .github/
│   └── workflows/
│       └── ci.yml                       # job'ы inventory и identity
├── docs/
│   └── FLOWOPS_HANDOFF.md               # этот документ
└── services/
    ├── inventory/                       # ЗАВЕРШЁН, не трогать
    │   └── ...                          # см. предыдущую версию handoff
    └── identity/                        # Шаг 1 закрыт
        ├── .dockerignore
        ├── .env                          # локальный, в .gitignore
        ├── .env.example
        ├── .gitignore
        ├── alembic.ini
        ├── Dockerfile                    # POETRY_VERSION=2.4.3
        ├── poetry.lock
        ├── pyproject.toml                # migrations/versions исключены из Ruff
        ├── migrations/
        │   ├── env.py                    # async, compare_type=True
        │   ├── README
        │   ├── script.py.mako
        │   └── versions/                 # пока пусто
        ├── app/
        │   ├── __init__.py
        │   ├── main.py                   # FastAPI, lifespan, /health
        │   ├── api/
        │   │   └── v1/
        │   │       └── router.py         # пустой APIRouter()
        │   ├── core/
        │   │   └── config.py             # Settings + JWT-заготовки
        │   ├── db/
        │   │   ├── base.py
        │   │   └── session.py
        │   ├── models/
        │   │   └── __init__.py           # пока пусто
        │   ├── repositories/
        │   │   └── __init__.py
        │   ├── schemas/
        │   │   └── __init__.py
        │   └── services/
        │       └── __init__.py
        └── tests/                        # появится на Шаге 5
```

------------------------------------------------------------------------

## 5. Обязательные правила проекта

1.  **Версия Poetry должна совпадать.** Версия в Dockerfile
    (`ENV POETRY_VERSION=...`) должна точно совпадать с версией Poetry,
    которой локально генерируется `poetry.lock`. Сейчас везде **2.4.3**.
    Иначе `docker compose build` может завершиться ошибкой:
    `pyproject.toml changed significantly since poetry.lock was last generated`.

2.  **Любое изменение `pyproject.toml` требует обновления lock.** Даже
    изменение секции `[tool.*]` → выполнить `poetry lock` и коммитить
    `pyproject.toml` и `poetry.lock` вместе.

3.  **Миграции в Docker --- стратегия A.** В `compose.yaml` для
    `identity-api` использовать команду:

    ``` yaml
    command: sh -c "alembic upgrade head && uvicorn app.main:app --host 0.0.0.0 --port 8000"
    ```

    При переходе к Nginx/балансировке пересмотреть в сторону стратегии
    B: отдельный one-off `identity-migrate` +
    `depends_on: service_completed_successfully`.

4.  **Не менять архитектурные решения молча.** Если обнаружена проблема,
    объяснить её и предложить варианты до изменения.

5.  **Работать небольшими шагами.** Не переходить дальше, пока текущий
    шаг не реализован и не проверен.

6.  **Предупреждать о росте контекста.** Если переписка разрастается,
    заранее предупредить и подготовить handoff.

7.  **Inventory не переписывать** без конкретной причины, бага или
    нового требования.

8.  **Секреты не коммитить.** `.env` локальный и находится в
    `.gitignore`. В Git хранится только `.env.example`. JWT-ключи ---
    локально в `.env`, в GitHub Secrets для CI или Docker secrets в
    production.

------------------------------------------------------------------------

## 6. План Identity: шаги 2--5

### Шаг 2 --- модели и миграция

- Создать `app/models/user.py`, `refresh_token.py`, `login_attempt.py`
  по согласованным схемам выше.

- Заполнить `app/models/__init__.py` импортами всех трёх моделей --- это
  нужно для Alembic autogenerate и `from app import models` в
  `migrations/env.py`.

- Для роли использовать PostgreSQL Enum:
  `sa.Enum(..., name="user_role")` или `native_enum=True` (конкретный
  способ выбрать при реализации Шага 2).

- Выполнить:

  ``` powershell
  poetry run alembic revision --autogenerate -m "create identity tables"
  ```

- Проверить, что миграция содержит:

  - `users` (unique index на email, Enum/check constraint для role);
  - `refresh_tokens` (FK и index);
  - `login_attempts` (nullable FK и composite index
    `(email, created_at)`).

- Применить и проверить миграцию:

  ``` powershell
  poetry run alembic upgrade head
  poetry run alembic downgrade -1
  poetry run alembic upgrade head
  ```

- Проверить создание таблиц, работу FK и наличие индексов.

- Внедрить стратегию A в `compose.yaml`: запуск Alembic перед Uvicorn.

### Шаг 3 --- безопасность

- Добавить зависимости: `argon2-cffi`, `pyjwt`, `cryptography`.
- После изменения `pyproject.toml` выполнить `poetry lock` и закоммитить
  оба файла.
- Создать `app/core/security.py`:
  - `hash_password` / `verify_password` (Argon2id);
  - `create_access_token` / `create_refresh_token` (RS256, `kid`);
  - `decode_token` (проверка подписи и типа);
  - `load_private_key` / `load_public_key`.
- Подготовить генерацию ключей для разработки: скрипт или инструкция в
  README, например `scripts/gen_keys.py`.
- Ключи хранить в `services/identity/keys/`, добавить эту папку в
  `.gitignore`.
- Проверить unit-тестами:
  - хеширование: round-trip и неверный пароль;
  - JWT: encode/decode, expiry, wrong `kid`, wrong type.

### Шаг 4 --- схемы, репозитории, сервис, эндпоинты и зависимости

- Схемы: `app/schemas/auth.py`, `app/schemas/user.py`.
- Репозитории: `app/repositories/user.py`, `refresh_token.py`,
  `login_attempt.py`.
- Сервисный слой:
  - `app/services/auth.py` --- register, login, refresh с ротацией и
    отзывом цепочки, logout;
  - `app/services/user.py` --- get/list.
- `app/api/deps.py`:
  - `get_current_user` --- Bearer access, проверка `type=access`,
    загрузка пользователя из БД;
  - `require_admin`.
- Эндпоинты: `app/api/v1/endpoints/auth.py`, `users.py`; подключить их в
  `router.py`.
- Обработка ошибок: `401`, `403`, `409`, `422`, без утечки внутренних
  деталей.
- Проверить через Swagger и ручной прогон:
  `register → login → refresh → logout → me`.
- Повторное использование уже использованного refresh должно приводить к
  `401` и отзыву цепочки.

### Шаг 5 --- тесты и финализация

- Создать `services/identity/tests/`.
- Написать минимум 10 тестов (unit + integration).
- Использовать моки внешних зависимостей и фикстуры для БД, клиента,
  пользователей `admin`/`user`.
- Добавить `pytest -v` в CI job `identity`.
- Обновить handoff после завершения этапа.

------------------------------------------------------------------------

## 7. Открытые вопросы на будущее (не блокируют текущий Шаг 2)

- `/ready` с проверкой БД --- определить подходящий момент внедрения.
- JWKS endpoint и ротация `kid` --- определить подходящий момент.
- Проверка JWT в каждом сервисе или общий внутренний пакет --- решить
  при появлении второго сервиса, которому понадобится проверка токенов.
- Контракт Order ↔ Inventory: endpoint, payload, события, timeout/отказ,
  идемпотентность создания заказа, компенсация/release reservation.
- Transactional outbox в Order: схема, публикация, retry/DLQ.
- Стратегия миграций B (one-off `identity-migrate`) --- при переходе к
  Nginx.

------------------------------------------------------------------------

## 8. Текущая точка старта

- **Шаг 1 Identity закрыт:** каркас, БД, Alembic, Docker и CI работают.
- **Следующий шаг --- Шаг 2 Identity:** модели `User` / `RefreshToken` /
  `LoginAttempt`, первая настоящая миграция и внедрение стратегии A в
  `compose.yaml`.
- **Inventory не трогать.**
- Перед началом Шага 2 согласовать с пользователем план из раздела 6,
  затем выдавать код частями: модели → миграция → проверка.
