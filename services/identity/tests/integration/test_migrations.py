"""Migration round-trip against a disposable database.

The session fixture leaves `identity_test_db` permanently at `head`. This test
proves the same chain also applies to an empty database and reverts cleanly —
what a rollback after a bad deploy depends on. It creates (and always drops) a
throw-away database, so nothing here can damage the shared test schema.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import make_url, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from tests.integration.helpers import SERVICE_ROOT, run_alembic

pytestmark = pytest.mark.integration

MAINTENANCE_DATABASE = "postgres"
TABLE_NAMES = ("users", "refresh_tokens", "login_attempts", "alembic_version")
_IDENTIFIER = re.compile(r"\A[a-z][a-z0-9_]{0,62}\Z")


@dataclass(frozen=True, slots=True)
class SchemaState:
    tables: tuple[str | None, ...]
    version: str | None
    role_labels: list[str]
    replaced_by_foreign_keys: int
    users_id_default: str | None


async def _admin_sql(database_url: str, *statements: str) -> None:
    """Run statements against the maintenance database in autocommit mode.

    `CREATE DATABASE` / `DROP DATABASE` cannot run inside a transaction block,
    so the connection has to be in AUTOCOMMIT isolation.
    """
    admin_url = make_url(database_url).set(database=MAINTENANCE_DATABASE)
    engine = create_async_engine(
        admin_url,
        isolation_level="AUTOCOMMIT",
        poolclass=NullPool,
    )
    try:
        async with engine.connect() as connection:
            for statement in statements:
                await connection.execute(text(statement))
    finally:
        await engine.dispose()


def _disposable_database_name(database_url: str) -> str:
    base = make_url(database_url).database or ""
    name = f"{base.removesuffix('_test_db')}_migration_test_db"
    assert _IDENTIFIER.fullmatch(name), f"unsafe database name: {name!r}"
    return name


def _disposable_url(database_url: str) -> str:
    return (
        make_url(database_url)
        .set(database=_disposable_database_name(database_url))
        .render_as_string(hide_password=False)
    )


def _head_revision() -> str:
    """Head revision from the migration scripts — never a hardcoded hash."""
    config = Config(str(SERVICE_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(SERVICE_ROOT / "migrations"))
    head = ScriptDirectory.from_config(config).get_current_head()
    assert head is not None, "migration scripts contain no head revision"
    return head


async def _schema_state(scratch_url: str) -> SchemaState:
    engine = create_async_engine(scratch_url, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            tables = (
                await connection.execute(
                    text(
                        "SELECT to_regclass('public.users'),"
                        " to_regclass('public.refresh_tokens'),"
                        " to_regclass('public.login_attempts'),"
                        " to_regclass('public.alembic_version')"
                    )
                )
            ).one()

            version = None
            if tables[3] is not None:
                version = (
                    await connection.execute(
                        text("SELECT version_num FROM alembic_version")
                    )
                ).scalar_one_or_none()

            role_labels = (
                (
                    await connection.execute(
                        text(
                            "SELECT e.enumlabel FROM pg_type t"
                            " JOIN pg_enum e ON e.enumtypid = t.oid"
                            " WHERE t.typname = 'user_role'"
                            " ORDER BY e.enumsortorder"
                        )
                    )
                )
                .scalars()
                .all()
            )

            replaced_by_foreign_keys = await connection.scalar(
                text(
                    "SELECT count(*) FROM pg_constraint c"
                    " JOIN pg_attribute a ON a.attrelid = c.conrelid"
                    " AND a.attnum = ANY (c.conkey)"
                    " WHERE c.conrelid = to_regclass('public.refresh_tokens')"
                    " AND c.contype = 'f' AND a.attname = 'replaced_by_jti'"
                )
            )

            users_id_default = await connection.scalar(
                text(
                    "SELECT column_default FROM information_schema.columns"
                    " WHERE table_schema = 'public' AND table_name = 'users'"
                    " AND column_name = 'id'"
                )
            )
    finally:
        await engine.dispose()

    return SchemaState(
        tables=tuple(str(table) if table is not None else None for table in tables),
        version=version,
        role_labels=list(role_labels),
        replaced_by_foreign_keys=replaced_by_foreign_keys,
        users_id_default=users_id_default,
    )


def _assert_schema_present(state: SchemaState) -> None:
    assert all(name is not None for name in state.tables)
    assert state.version == _head_revision()
    assert state.role_labels == ["admin", "user"]
    # `replaced_by_jti` is a plain UUID column: a foreign key would demand that
    # the replacement row already exists, which is false while rotating.
    assert state.replaced_by_foreign_keys == 0
    assert state.users_id_default == "gen_random_uuid()"


async def test_migration_chain_applies_and_reverts(database_url: str) -> None:
    name = _disposable_database_name(database_url)
    scratch_url = _disposable_url(database_url)

    try:
        await _admin_sql(
            database_url,
            f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)',
            f'CREATE DATABASE "{name}"',
        )
    except DBAPIError as exc:
        # Missing CREATEDB is an environment problem, not a reason to pretend
        # the migrations are fine — fail loudly.
        pytest.fail(f"Cannot create a disposable database (needs CREATEDB): {exc}")

    try:
        run_alembic(scratch_url, "upgrade", "head")
        _assert_schema_present(await _schema_state(scratch_url))

        run_alembic(scratch_url, "downgrade", "base")
        reverted = await _schema_state(scratch_url)
        # Alembic keeps its bookkeeping table and only clears the version row.
        assert reverted.tables == (None, None, None, "alembic_version")
        assert reverted.version is None
        assert reverted.role_labels == []
        assert reverted.users_id_default is None

        # Re-applying proves the downgrade did not leave stray objects behind.
        run_alembic(scratch_url, "upgrade", "head")
        _assert_schema_present(await _schema_state(scratch_url))
    finally:
        await _admin_sql(
            database_url,
            f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)',
        )
