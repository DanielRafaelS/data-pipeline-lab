import os
from typing import Literal
from functools import lru_cache
from pydantic import Field, ValidationError
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    Application configuration loaded from environment variables.
    """

    # Project
    project_name: str = Field(..., alias="PROJECT_NAME")
    environment: Literal["local", "docker", "prod"] = Field(
        "local", alias="ENVIRONMENT"
    )

    # Data Warehouse
    dw_host: str = Field(..., alias="DW_HOST")
    dw_port: int = Field(..., alias="DW_PORT")
    dw_name: str = Field(..., alias="DW_NAME")
    dw_user: str = Field(..., alias="DW_USER")
    dw_password: str = Field(..., alias="DW_PASSWORD")

    # API
    api_base_url: str = Field(..., alias="FAKESTORE_API_BASE_URL")
    api_timeout_seconds: int = Field(30, alias="API_TIMEOUT_SECONDS")

    # Pipeline
    load_mode: Literal["full", "incremental"] = Field("full", alias="LOAD_MODE")
    batch_size: int = Field(500, alias="BATCH_SIZE")
    log_level: str = Field("INFO", alias="LOG_LEVEL")

    class Config:
        env_file = f".env.{os.getenv('ENVIRONMENT', 'local')}"
        env_file_encoding = "utf-8"
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    try:
        return Settings()
    except ValidationError as e:
        raise RuntimeError(f"Configuration error: {e}") from e
