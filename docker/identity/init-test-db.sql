-- Runs only when the PostgreSQL data directory is created for the first time.
-- Integration tests truncate tables, so they get a database of their own and
-- never touch the application's `identity_db`.
CREATE DATABASE identity_test_db OWNER identity_user;
