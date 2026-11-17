# FlowOps — Текущий статус (Current Handoff)

**Актуальный сервис:** `services/identity`
**Статус:** Шаг 5 **ЗАКРЫТ**. Identity завершён: 87 тестов (66 unit/API на фейках + 21 интеграционный на реальной PostgreSQL), `ruff check` и `ruff format --check` зелёные, README готов.
**Следующая задача:** новый сервис (согласовать в PLAN: Catalog или Order). Код `services/identity` и `services/inventory` не трогать.

---

## Что реализовано (Identity Service)
- **БД:** `User` (`UserRole`: `admin`/`user`), `RefreshToken`, `LoginAttempt`; миграция `1c0512ab888f`, применяется при старте контейнера.
- **Безопасность:** Argon2id (OWASP-параметры), RS256 + `kid`, ключи через `scripts/gen_keys.py` и Docker secrets, dummy-хеш против timing-атак.
- **API:** `POST /auth/register`, `/auth/login`, `/auth/refresh`, `/auth/logout`; `GET /auth/me`, `/users`; `GET /health` (liveness) и `GET /ready` (readiness, `SELECT 1`).
- **Логика:** ротация refresh с `replaced_by_jti`, отзыв всей цепочки при reuse (коммит **до** 401), идемпотентный logout, роль и `is_active` читаются из БД, аудит каждой попытки входа коммитится даже при 401/403.

## Интеграционный контур (`services/identity/tests/integration/`)
- `conftest.py` — guard: имя БД обязано оканчиваться на `_test_db`, повторная проверка через `SELECT current_database()`; схема пересоздаётся раз за сессию (`DROP/CREATE SCHEMA public` + `alembic upgrade head` в подпроцессе); autouse `TRUNCATE users, refresh_tokens, login_attempts RESTART IDENTITY CASCADE`; engine с `NullPool` на каждый тест; `api_client` с реальной `async_sessionmaker`.
- `helpers.py` — INSERT-хелперы (`create_user`, `create_refresh_row`) с memoized Argon2-хешем, `run_alembic`, `login`, `refresh_jti`.
- `test_auth_flow.py` (9) — нормализация email и `$argon2id$`, живая строка refresh + аудит, `/me` по `is_active`, `/users` по роли из БД, ротация, reuse, logout, inactive 403 с откатом, expired 401.
- `test_login_audit.py` (4) — аудит неверного пароля/неизвестного email/неактивного пользователя, проверка хелпера.
- `test_migrations.py` (1) — одноразовая БД: `upgrade → downgrade base → upgrade`, проверки `to_regclass`, head из `ScriptDirectory`, enum `user_role` = `admin, user`, `replaced_by_jti` без FK, `gen_random_uuid()`, `finally DROP ... WITH (FORCE)`; нет `CREATEDB` → fail, не skip.
- `test_refresh_concurrency.py` (1) — два независимых engine, `asyncio.gather` → `[200, 401]`, 2 строки / 0 активных, токен победителя тоже отозван.
- `test_ready.py` (3) и `test_database.py` (3) — smoke схемы и пробы.

## Изменения вне тестов
- `app/main.py` — `GET /ready`; `/health` остался liveness; compose-healthcheck пока на `/health` (переключить при появлении оркестратора).
- `pyproject.toml` — маркер `integration`, `addopts = --strict-markers`.
- `.env.example` (и локальный `.env`) — `TEST_DATABASE_URL` на `localhost:5433/identity_test_db` с комментарием про суффикс.
- `compose.yaml` + `docker/identity/init-test-db.sql` — создание `identity_test_db` при первой инициализации volume; для уже существующего volume: `docker compose exec identity-db createdb -U identity_user identity_test_db`.
- `.github/workflows/ci.yml` — `REQUIRE_INTEGRATION_TESTS: "1"` в job `identity`.
- `services/identity/README.md` — запуск (сначала `scripts/gen_keys.py`), env-таблица, миграции, тесты, API, безопасность, `/health` vs `/ready`.

## Команды
```
# из корня репозитория
docker compose up -d identity-db

cd services/identity
poetry run pytest -q
poetry run ruff check .
poetry run ruff format --check .
```

## Подводные камни
- Нужен запущенный Docker Desktop; тестовая БД — контейнер `identity-db` на `:5433`, не нативный PostgreSQL на `:5432`.
- `poetry.lock` содержал `email-validator`, но локальное venv было неполным — лечится `poetry install --with dev`.
- Guard сравнивает имя БД с `DATABASE_URL` **из файла `.env`** через `dotenv_values`: корневой `tests/conftest.py` подставляет плейсхолдер `DATABASE_URL`, указывающий на `*_test_db`, поэтому `settings.database_url` для этой проверки не годится.
- В CI `DATABASE_URL == TEST_DATABASE_URL`, `.env` отсутствует — работает суффикс-гард + проверка `current_database()`.
- Alembic после `downgrade base` оставляет пустую таблицу `alembic_version` — тест это учитывает.
- Argon2id ~100 мс на хеш: пользователи/токены в тестах создаются INSERT-хелперами.
- Схема тестовой БД пересоздаётся один раз за сессию: два параллельных pytest-прогона на одной `identity_test_db` конфликтуют.
