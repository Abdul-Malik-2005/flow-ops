# Identity Service (FlowOps)

Сервис аутентификации и авторизации FlowOps: регистрация, логин, JWT-пара
(access + refresh), ротация refresh-токенов, аудит попыток входа и выдача
пользователей для администраторов.

Сервис владеет собственной базой (`identity_db`) и не обращается к БД других
сервисов.

## Содержание

- [Стек](#стек)
- [Структура](#структура)
- [Быстрый старт](#быстрый-старт)
- [Переменные окружения](#переменные-окружения)
- [Миграции](#миграции)
- [Тесты](#тесты)
- [API](#api)
- [Безопасность](#безопасность)
- [Эксплуатация: `/health` и `/ready`](#эксплуатация-health-и-ready)

## Стек

- Python 3.14, FastAPI, Uvicorn
- SQLAlchemy 2.x (async) + asyncpg, PostgreSQL 17
- Alembic, Pydantic Settings, python-dotenv
- Argon2id (`argon2-cffi`) для паролей, PyJWT + `cryptography` (RS256) для токенов
- Poetry 2.4.3, Ruff, pytest + pytest-asyncio

## Структура

```
app/
  api/            # роутеры и зависимости FastAPI
  core/           # конфиг и криптография (Argon2id, JWT)
  db/             # Base и фабрика асинхронных сессий
  models/         # SQLAlchemy-модели: User, RefreshToken, LoginAttempt
  repositories/   # доступ к данным, без commit/rollback
  schemas/        # Pydantic-схемы запросов/ответов
  services/       # бизнес-логика и границы транзакций
migrations/       # Alembic (env.py + versions/)
scripts/          # gen_keys.py — генерация RS256-ключей для dev
tests/            # unit/API-тесты на in-memory фейках
tests/integration/# тесты против реального PostgreSQL
```

Правило слоёв: **транзакцией владеет только сервис** (`async with
session.begin()`), репозитории делают максимум `flush()`.

## Быстрый старт

Все команды — из каталога `services/identity`, если не указано иное.

1. Установить зависимости:

   ```bash
   poetry install --with dev
   ```

2. Сгенерировать ключи RS256 (каталог `keys/` в `.gitignore`, в репозитории
   ключей нет):

   ```bash
   poetry run python scripts/gen_keys.py
   ```

   Скрипт создаёт `keys/jwt_private.pem` и `keys/jwt_public.pem` и откажется
   перезаписывать существующие файлы без `--force`.

3. Создать `.env` из шаблона:

   ```bash
   cp .env.example .env
   ```

4. Поднять БД и сервис (из корня репозитория):

   ```bash
   docker compose up -d identity-db identity-api
   ```

   `identity-db` слушает `localhost:5433`, API — `localhost:8001`.
   Миграции применяются автоматически при старте контейнера
   (`alembic upgrade head && uvicorn ...`).

5. Проверить:

   - Swagger: <http://localhost:8001/docs>
   - Liveness: <http://localhost:8001/health> → `{"status":"healthy"}`
   - Readiness: <http://localhost:8001/ready> → `{"status":"ready","database":"ok"}`

## Переменные окружения

Значения читаются `pydantic-settings` из `.env`; переменные окружения процесса
имеют приоритет над файлом (так CI и compose переопределяют URL БД).

| Переменная                  | Назначение                                                        | Значение в `.env.example`                     |
| --------------------------- | ----------------------------------------------------------------- | --------------------------------------------- |
| `DATABASE_URL`              | DSN приложения (`postgresql+asyncpg://...`)                        | `...@localhost:5433/identity_db`              |
| `TEST_DATABASE_URL`         | DSN для `tests/integration`; имя обязано оканчиваться на `_test_db` | `...@localhost:5433/identity_test_db`         |
| `DEBUG`                     | Включает SQL-эхо SQLAlchemy                                        | `false`                                       |
| `ENVIRONMENT`               | `development` / `testing` / `production`                           | `development`                                 |
| `JWT_ALGORITHM`             | Алгоритм подписи                                                   | `RS256`                                       |
| `JWT_KID`                   | `kid` в заголовке токена                                           | `identity-1`                                  |
| `JWT_PRIVATE_KEY_PATH`      | Путь к приватному ключу (PEM, PKCS#8)                              | `./keys/jwt_private.pem`                      |
| `JWT_PUBLIC_KEY_PATH`       | Путь к публичному ключу (PEM)                                      | `./keys/jwt_public.pem`                       |
| `ACCESS_TOKEN_TTL_SECONDS`  | Время жизни access-токена                                          | `900`                                         |
| `REFRESH_TOKEN_TTL_SECONDS` | Время жизни refresh-токена                                         | `2592000` (30 дней)                           |

В compose ключи монтируются как secrets в `/run/secrets/...`, а не пекутся в
образ.

## Миграции

```bash
# применить все миграции
poetry run alembic upgrade head

# откатить последнюю
poetry run alembic downgrade -1

# создать новую по разнице моделей и схемы
poetry run alembic revision --autogenerate -m "add something"
```

`migrations/env.py` берёт URL из настроек приложения (`DATABASE_URL`), поэтому
отдельный `sqlalchemy.url` в `alembic.ini` не нужен. Каждая миграция обязана
иметь рабочий `downgrade()` — это проверяется тестом round-trip (см. ниже).

## Тесты

Два независимых набора:

```bash
# быстрые unit/API-тесты на in-memory фейках, без БД
poetry run pytest tests --ignore=tests/integration

# интеграционные тесты против реального PostgreSQL
poetry run pytest tests/integration -v

# всё вместе
poetry run pytest
```

### Интеграционные тесты (`tests/integration/`)

Что проверяется на реальной БД: соответствие SQL моделям, enum-типы,
`timestamptz`, уникальный индекс по email, границы транзакций, коммит аудита
до выдачи 401/403, ротация и повторное использование refresh-токенов,
конкурентный refresh на двух независимых соединениях, round-trip миграций.

Требования и правила:

- База `identity_test_db` и переменная `TEST_DATABASE_URL`. В compose она
  создаётся автоматически при первой инициализации volume скриптом
  `docker/identity/init-test-db.sql`. Если volume уже существовал:

  ```bash
  docker compose exec identity-db createdb -U identity_user identity_test_db
  ```

- **Guard:** имя БД в `TEST_DATABASE_URL` обязано оканчиваться на `_test_db`;
  перед destructive-операциями это повторно проверяется на сервере через
  `SELECT current_database()`. Указывать на `identity_db` нельзя — тесты
  делают `TRUNCATE`.
- **Схема** создаётся ровно один раз за сессию: `DROP SCHEMA public CASCADE` →
  `CREATE SCHEMA public` → `alembic upgrade head` (в подпроцессе, потому что
  `migrations/env.py` вызывает `asyncio.run()`, а не `create_all()`, чтобы
  тестировать именно production-DDL).
- **Изоляция** — `TRUNCATE users, refresh_tokens, login_attempts RESTART IDENTITY
  CASCADE` перед каждым тестом. Транзакционный rollback не подходит: сервис
  открывает и коммитит собственные транзакции.
- **Пропуск:** без `TEST_DATABASE_URL` набор пропускается (skip). В CI
  выставлен `REQUIRE_INTEGRATION_TESTS=1`, и тогда отсутствие БД — ошибка, а не
  тихий skip.
- **Миграции:** тест создаёт одноразовую БД `<имя>_migration_test_db`, прогоняет
  `upgrade → downgrade base → upgrade` и всегда удаляет её (`DROP ... WITH
  (FORCE)`). Нужна привилегия `CREATEDB`; без неё тест падает, а не
  пропускается.
- Argon2id стоит ~100 мс на хеш, поэтому пользователи и токены в тестах
  создаются прямыми INSERT-хелперами (`tests/integration/helpers.py`), а через
  API идёт только то поведение, которое тестируется.

### Линтеры

```bash
poetry run ruff check .
poetry run ruff format --check .
```

## API

Базовый префикс — `/api/v1`.

| Метод | Путь                | Успех           | Ошибки                                            |
| ----- | ------------------- | --------------- | ------------------------------------------------- |
| POST  | `/auth/register`    | `201` + user    | `409` email уже занят, `422` невалидные данные    |
| POST  | `/auth/login`       | `200` + пара    | `401` неверные данные, `403` пользователь неактивен |
| POST  | `/auth/refresh`     | `200` + пара    | `401` токен невалиден/истёк/отозван/переиспользован, `403` неактивен |
| POST  | `/auth/logout`      | `204` всегда    | —                                                 |
| GET   | `/auth/me`          | `200` + user    | `401` нет/битый access, `403` неактивен           |
| GET   | `/users`            | `200` + список  | `401`, `403` не admin                             |

Служебные (без префикса): `GET /health`, `GET /ready`.

## Безопасность

- **Пароли** — Argon2id с параметрами OWASP: `time_cost=3`, `memory_cost=64 MiB`,
  `parallelism=4`, соль 16 байт, хеш 32 байта. Хеш никогда не покидает БД:
  ответы строятся по `UserResponse`, где поля `password_hash` нет.
- **Защита от перебора по времени** — для несуществующего email выполняется
  проверка по фиктивному хешу, поэтому неизвестный адрес отвечает за то же
  время, что и известный с неверным паролем.
- **Токены** — RS256, в заголовке `kid`. Access короткоживущий (15 мин),
  refresh — 30 дней. Refresh-токен имеет `jti`, и строка в `refresh_tokens`
  пишется **до** возврата токена клиенту.
- **Ротация** — при `/auth/refresh` старый токен получает `revoked_at` и
  `replaced_by_jti` нового. Повторное использование уже ротированного токена
  трактуется как кража: вся цепочка пользователя отзывается, и это
  коммитится **до** ответа `401`, чтобы отзыв не откатился вместе с ошибкой.
  `replaced_by_jti` — обычная UUID-колонка без FK: при ротации новая строка
  ещё не обязана существовать в момент обновления старой.
- **Logout идемпотентен** — всегда `204`; повторный вызов не меняет
  `revoked_at` и не раскрывает, был ли токен валиден.
- **Роль и активность читаются из БД** на каждом запросе (`/auth/me`,
  `/users`), а не из claim. Понижение прав или деактивация действуют в пределах
  жизни access-токена.
- **Аудит** — каждая попытка входа (`успех`, `неверный пароль`, `неизвестный
  email`, `неактивный пользователь`) пишется в `login_attempts` с email, IP и
  флагом `success`. Запись коммитится даже при ответе `401/403`.

## Эксплуатация: `/health` и `/ready`

- `GET /health` — **liveness**. Отвечает `200 {"status":"healthy"}`, не трогая
  БД: недоступность PostgreSQL не должна приводить к перезапуску живого
  контейнера.
- `GET /ready` — **readiness**. Выполняет `SELECT 1` через `get_session`:
  `200 {"status":"ready","database":"ok"}` либо
  `503 {"status":"unavailable","database":"error"}`. Именно этот эндпоинт стоит
  использовать для гейта трафика (load balancer / orchestrator readiness).

Сейчас compose-healthcheck смотрит на `/health`; при появлении оркестратора с
разделением liveness/readiness его нужно переключить на `/ready`.
