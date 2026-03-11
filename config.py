import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    bot_token: str = os.getenv("BOT_TOKEN", "")
    webhook_url: str = os.getenv("WEBHOOK_URL", "")
    google_sheet_id: str = os.getenv("GOOGLE_SHEET_ID", "")
    google_credentials_path: str = os.getenv(
        "GOOGLE_CREDENTIALS_PATH",
        "/etc/secrets/credentials.json",
    )
    port: int = int(os.getenv("PORT", "10000"))


settings = Settings()


def validate_settings() -> list[str]:
    missing = []

    if not settings.bot_token:
        missing.append("BOT_TOKEN")
    if not settings.webhook_url:
        missing.append("WEBHOOK_URL")
    if not settings.google_sheet_id:
        missing.append("GOOGLE_SHEET_ID")

    return missing
