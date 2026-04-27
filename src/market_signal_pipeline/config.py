"""Centralized configuration loaded from environment variables."""

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings. Values are loaded from environment or .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    env: Literal["local", "dev", "prod"] = "local"
    log_level: str = "INFO"

    alpha_vantage_api_key: str = Field(default="", alias="ALPHA_VANTAGE_API_KEY")

    azure_storage_account: str = Field(default="", alias="AZURE_STORAGE_ACCOUNT")
    azure_storage_container: str = Field(default="", alias="AZURE_STORAGE_CONTAINER")
    azure_resource_group: str = Field(default="", alias="AZURE_RESOURCE_GROUP")
    azure_subscription_id: str = Field(default="", alias="AZURE_SUBSCRIPTION_ID")
    azure_storage_account_key: str = Field(default="", alias="AZURE_STORAGE_ACCOUNT_KEY")

    database_url: str = Field(default="", alias="DATABASE_URL")


@lru_cache
def get_settings() -> Settings:
    """Return cached settings instance."""
    return Settings()
