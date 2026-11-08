from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str
    test_database_url: str | None = None

    debug: bool = False
    environment: Literal["development", "testing", "production"] = "development"

    # JWT-заготовки. Используются начиная с Шага 3 (выпуск/проверка токенов).
    jwt_algorithm: Literal["RS256"] = "RS256"
    jwt_kid: str = "identity-1"
    jwt_private_key_path: str = ""
    jwt_public_key_path: str = ""
    access_token_ttl_seconds: int = 900  # 15 минут
    refresh_token_ttl_seconds: int = 2_592_000  # 30 дней

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
