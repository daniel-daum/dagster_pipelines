import os

from dotenv import load_dotenv
from pydantic import BaseSettings

load_dotenv(
    "./.env"
)  # Only needed for local development, loads local environment variables from env file


class Settings(BaseSettings):
    DBSTR_DEV: str = str(os.getenv("DBSTR_DEV"))


settings = Settings()
