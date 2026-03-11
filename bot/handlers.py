import logging

from telegram import Update
from telegram.ext import CommandHandler, ContextTypes

from constants import HELP_TEXT, START_TEXT
from services.sheets_repo import healthcheck, try_get_worksheet_title

log = logging.getLogger(__name__)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(START_TEXT)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT)


async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("pong")


async def cmd_checksheet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ok, message = healthcheck()
    prefix = "✅" if ok else "❌"
    await update.message.reply_text(f"{prefix} {message}")


async def cmd_sheetinfo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    title = try_get_worksheet_title()
    if title:
        await update.message.reply_text(f"Connected sheet: {title}")
    else:
        await update.message.reply_text("Sheet not ready.")


def register_handlers(application):
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("ping", cmd_ping))
    application.add_handler(CommandHandler("checksheet", cmd_checksheet))
    application.add_handler(CommandHandler("sheetinfo", cmd_sheetinfo))
