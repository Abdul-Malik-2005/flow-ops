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

## 5. Бэклог технических решений (на будущие сервисы)
- **JWKS-эндпоинт:** Внедрить при появлении следующих сервисов, чтобы не раздавать публичный ключ через `.env`.
- **Rate Limiting:** Добавить защиту `/auth/login` (sliding window / token bucket) после внедрения Redis.
- **Маскирование 422 ошибок:** Настроить кастомный обработчик `RequestValidationError`, чтобы вычищать поле `input` с паролями из ответов и логов.
- **Стратегия миграций B:** При переходе на несколько реплик API за Nginx вынести `alembic upgrade head` в отдельный one-off контейнер.
