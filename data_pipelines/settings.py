import os

from dotenv import load_dotenv
from pydantic import BaseSettings

load_dotenv(
    "./.env", override=True
)  # Only needed for local development, loads local environment variables from env file


class Settings(BaseSettings):
    TARGET: str = str(os.getenv("TARGET"))
    WAREHOUSE: str = str(os.getenv("WAREHOUSE"))
    AWS_ACCESS_KEY_ID: str = str(os.getenv("AWS_ACCESS_KEY_ID"))
    AWS_SECRET_ACCESS_KEY: str = str(os.getenv("AWS_SECRET_ACCESS_KEY"))


settings = Settings()
