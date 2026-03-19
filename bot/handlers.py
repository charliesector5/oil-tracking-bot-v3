from telegram.ext import CallbackQueryHandler, CommandHandler, MessageHandler, filters

from bot.callbacks import handle_callback
from bot.conversations import (
    cmd_adjustoil,
    cmd_claimoff,
    cmd_claimphoff,
    cmd_claimspecialoff,
    cmd_clockoff,
    cmd_clockphoff,
    cmd_clockspecialoff,
    cmd_massadjustoff,
    cmd_newuser,
    cmd_startadmin,
    handle_message,
)
from constants import HELP_TEXT, START_TEXT
from services.ledger import (
    cleanup_expired_off,
    compute_overview,
    compute_user_summary,
    get_user_last_records,
    rebuild_all_balances,
)
from services.sheets_repo import get_all_rows, healthcheck, try_get_worksheet_title

SEPARATOR = "────────────────────"


async def _is_admin_in_chat(context, chat_id: int, user_id: int) -> bool:
    try:
        admins = await context.bot.get_chat_administrators(chat_id)
        return any(a.user.id == user_id for a in admins)
    except Exception:
        return False


def _off_type(row) -> str:
    kind = (row.holiday_kind or "").strip().lower()
    if kind == "special":
        return "Special"
    if kind in ("yes", "y", "true", "1"):
        return "PH"
    return "Normal"


def _build_user_detail_block(summary, recent_rows) -> str:
    lines = [
        f"{summary.user_name}",
        f"   🔹 Total: {summary.total_balance:.1f}",
        f"   🔸 Normal: {summary.normal_balance:.1f}",
        f"   🏖 Active PH: {summary.ph_active:.1f}",
        f"   ❌ Expired PH: {summary.ph_expired:.1f}",
        f"   ⭐ Active Special: {summary.special_active:.1f}",
        f"   ❌ Expired Special: {summary.special_expired:.1f}",
    ]

    if summary.normal_balance < 0:
        lines.append("   ⚠️ Negative normal balance")

    if summary.ph_active_entries:
        lines.append("")
        lines.append("   🏖✅ Active PH Details")
        for e in summary.ph_active_entries:
            lines.extend([
                f"   - {e.remarks or 'PH'}: {e.qty:.1f}",
                f"     📅 Clocked: {e.date}",
                f"     ⏳ Expiry: {e.expiry or '—'}",
            ])

    if summary.ph_expired_entries:
        lines.append("")
        lines.append("   🏖❌ Expired PH Details")
        for e in summary.ph_expired_entries:
            lines.extend([
                f"   - {e.remarks or 'PH'}: {e.qty:.1f}",
                f"     📅 Clocked: {e.date}",
                f"     ❌ Expired: {e.expiry or '—'}",
            ])

    if summary.special_active_entries:
        lines.append("")
        lines.append("   ⭐✅ Active Special Details")
        for e in summary.special_active_entries:
            lines.extend([
                f"   - {e.remarks or 'Special'}: {e.qty:.1f}",
                f"     📅 Clocked: {e.date}",
                f"     ⏳ Expiry: {e.expiry or '—'}",
            ])

    if summary.special_expired_entries:
        lines.append("")
        lines.append("   ⭐❌ Expired Special Details")
        for e in summary.special_expired_entries:
            lines.extend([
                f"   - {e.remarks or 'Special'}: {e.qty:.1f}",
                f"     📅 Clocked: {e.date}",
                f"     ❌ Expired: {e.expiry or '—'}",
            ])

    if recent_rows:
        lines.append("")
        lines.append("   📝 Recent Records")
        for r in recent_rows:
            is_plus = r.delta >= 0
            symbol = "🟢" if is_plus else "🔴"
            operator = "+" if is_plus else "-"
            amount = abs(r.delta)
            lines.extend([
                f"   - {symbol} {r.action} [{_off_type(r)}]",
                f"     {r.current_off:.1f} {operator} {amount:.1f} = {r.final_off:.1f}",
                f"     📅 {r.application_date or r.timestamp[:10]}",
                f"     📝 {r.remarks or '—'}",
            ])

    return "\n".join(lines)


async def cmd_start(update, context):
    await update.message.reply_text(START_TEXT)


async def cmd_help(update, context):
    await update.message.reply_text(HELP_TEXT)


async def cmd_ping(update, context):
    await update.message.reply_text("pong la, working don't play with me anymore")


async def cmd_checksheet(update, context):
    ok, message = healthcheck()
    prefix = "✅" if ok else "❌"
    await update.message.reply_text(f"{prefix} {message}")


async def cmd_sheetinfo(update, context):
    title = try_get_worksheet_title()
    if title:
        await update.message.reply_text(f"Connected sheet: {title}")
    else:
        await update.message.reply_text("Sheet not ready.")


async def cmd_rebuildbalances(update, context):
    chat = update.effective_chat
    if not chat or chat.type == "private":
        await update.message.reply_text("Please use /rebuildbalances inside the group.")
        return

    is_admin = await _is_admin_in_chat(context, chat.id, update.effective_user.id)
    if not is_admin:
        await update.message.reply_text("❌ Only group admins can use /rebuildbalances.")
        return

    try:
        rebuilt = rebuild_all_balances(get_all_rows)
    except Exception as exc:
        await update.message.reply_text(f"❌ Rebuild failed: {exc}")
        return

    if not rebuilt:
        await update.message.reply_text("⚠️ Rebuild completed, but no users were found in ledger.")
        return

    negative_users = [s.user_name for s in rebuilt if s.normal_balance < 0]

    lines = [
        "✅ *Balances rebuilt from ledger*",
        "",
        f"👥 Users rebuilt: {len(rebuilt)}",
    ]

    if negative_users:
        preview = ", ".join(negative_users[:10])
        if len(negative_users) > 10:
            preview += ", ..."
        lines.extend([
            "",
            f"⚠️ Users with negative normal balance: {len(negative_users)}",
            preview,
        ])

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_cleanupexpired(update, context):
    chat = update.effective_chat
    if not chat or chat.type == "private":
        await update.message.reply_text("Please use /cleanupexpired inside the group.")
        return

    is_admin = await _is_admin_in_chat(context, chat.id, update.effective_user.id)
    if not is_admin:
        await update.message.reply_text("❌ Only group admins can use /cleanupexpired.")
        return

    try:
        result = cleanup_expired_off(update.effective_user.full_name, get_all_rows)
    except Exception as exc:
        await update.message.reply_text(f"❌ Expired cleanup failed: {exc}")
        return

    if result.rows_written == 0:
        await update.message.reply_text("✅ No expired PH or Special off found to clean.")
        return

    lines = [
        "✅ *Expired off cleanup completed*",
        "",
        f"👥 Users affected: {result.users_affected}",
        f"🏖 PH cleaned: {result.ph_cleaned:.1f}",
        f"⭐ Special cleaned: {result.special_cleaned:.1f}",
        f"🧾 Ledger rows written: {result.rows_written}",
    ]

    if result.affected_users:
        preview = ", ".join(result.affected_users[:10])
        if len(result.affected_users) > 10:
            preview += ", ..."
        lines.extend([
            "",
            "*Affected users*",
            preview,
        ])

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_summary(update, context):
    uid = str(update.effective_user.id)
    s = compute_user_summary(uid, get_all_rows)

    lines = [
        "📊 *Your OIL Summary*",
        "",
        f"👤 Name: {s.user_name}",
        f"🆔 ID: {s.user_id}",
        f"🔹 Available Total OIL: {s.total_balance:.1f}",
        f"🔸 Normal OIL: {s.normal_balance:.1f}",
        f"🏖 Active PH OIL: {s.ph_active:.1f}",
        f"❌ Expired PH OIL: {s.ph_expired:.1f}",
        f"⭐ Active Special OIL: {s.special_active:.1f}",
        f"❌ Expired Special OIL: {s.special_expired:.1f}",
    ]

    if s.ph_active_entries:
        lines.append("")
        lines.append("*🏖✅ Active PH OIL Details*")
        for e in s.ph_active_entries:
            lines.append(
                f"- {e.remarks or 'PH'}: {e.qty:.1f}\n"
                f"  📅 Clocked: {e.date}\n"
                f"  ⏳ Expiry: {e.expiry or '—'}"
            )

    if s.ph_expired_entries:
        lines.append("")
        lines.append("*🏖❌ Expired PH OIL Details*")
        for e in s.ph_expired_entries:
            lines.append(
                f"- {e.remarks or 'PH'}: {e.qty:.1f}\n"
                f"  📅 Clocked: {e.date}\n"
                f"  ❌ Expired: {e.expiry or '—'}"
            )

    if s.special_active_entries:
        lines.append("")
        lines.append("*⭐✅ Active Special OIL Details*")
        for e in s.special_active_entries:
            lines.append(
                f"- {e.remarks or 'Special'}: {e.qty:.1f}\n"
                f"  📅 Clocked: {e.date}\n"
                f"  ⏳ Expiry: {e.expiry or '—'}"
            )

    if s.special_expired_entries:
        lines.append("")
        lines.append("*⭐❌ Expired Special OIL Details*")
        for e in s.special_expired_entries:
            lines.append(
                f"- {e.remarks or 'Special'}: {e.qty:.1f}\n"
                f"  📅 Clocked: {e.date}\n"
                f"  ❌ Expired: {e.expiry or '—'}"
            )

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_history(update, context):
    uid = str(update.effective_user.id)
    recent = get_user_last_records(uid, get_all_rows, limit=10)

    if not recent:
        await update.message.reply_text("📜 No records found.")
        return

    lines = ["📜 *Your Recent OIL Records*"]

    for i, r in enumerate(recent, start=1):
        is_plus = r.delta >= 0
        symbol = "🟢" if is_plus else "🔴"
        operator = "+" if is_plus else "-"
        amount = abs(r.delta)
        off_type = _off_type(r)

        lines.append("")
        lines.append(
            f"{i}) {symbol} {r.action} [{off_type}]\n"
            f"   {r.current_off:.1f} {operator} {amount:.1f} = {r.final_off:.1f}\n"
            f"   📅 {r.application_date or r.timestamp[:10]}\n"
            f"   📝 {r.remarks or '—'}"
        )

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_overview(update, context):
    chat = update.effective_chat
    if not chat or chat.type == "private":
        await update.message.reply_text("Please use /overview inside the group.")
        return

    is_admin = await _is_admin_in_chat(context, chat.id, update.effective_user.id)
    if not is_admin:
        await update.message.reply_text("❌ Only group admins can use /overview.")
        return

    items = compute_overview(get_all_rows)
    if not items:
        await update.message.reply_text("No records found.")
        return

    blocks = ["📋 *Sector OIL Overview*"]
    for s in items:
        blocks.append(
            f"\n{s.user_name}\n"
            f"   🔹 Total: {s.total_balance:.1f}\n"
            f"   🔸 Normal: {s.normal_balance:.1f}\n"
            f"   🏖 PH: {s.ph_active:.1f}\n"
            f"   ⭐ Special: {s.special_active:.1f}"
            + (f"\n   ⚠️ Negative normal balance" if s.normal_balance < 0 else "")
        )

    text = "\n".join(blocks)

    if len(text) <= 3800:
        await update.message.reply_text(text, parse_mode="Markdown")
        return

    chunk = ""
    for block in blocks:
        piece = block + "\n"
        if len(chunk) + len(piece) > 3800:
            await update.message.reply_text(chunk.strip(), parse_mode="Markdown")
            chunk = ""
        chunk += piece

    if chunk.strip():
        await update.message.reply_text(chunk.strip(), parse_mode="Markdown")


async def cmd_detailedoverview(update, context):
    chat = update.effective_chat
    if not chat or chat.type == "private":
        await update.message.reply_text("Please use /detailedoverview inside the group.")
        return

    is_admin = await _is_admin_in_chat(context, chat.id, update.effective_user.id)
    if not is_admin:
        await update.message.reply_text("❌ Only group admins can use /detailedoverview.")
        return

    items = compute_overview(get_all_rows)
    if not items:
        await update.message.reply_text("No records found.")
        return

    header = "📋 *Detailed Sector OIL Overview*"
    chunk = header

    for idx, s in enumerate(items, start=1):
        recent = get_user_last_records(s.user_id, get_all_rows, limit=3)
        block = f"\n\n{idx}) " + _build_user_detail_block(s, recent)

        if idx > 1:
            block = f"\n\n{SEPARATOR}\n" + block

        if len(chunk) + len(block) > 3800:
            await update.message.reply_text(chunk.strip(), parse_mode="Markdown")
            chunk = block.lstrip()
        else:
            chunk += block

    if chunk.strip():
        await update.message.reply_text(chunk.strip(), parse_mode="Markdown")


def register_handlers(application):
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("ping", cmd_ping))
    application.add_handler(CommandHandler("checksheet", cmd_checksheet))
    application.add_handler(CommandHandler("sheetinfo", cmd_sheetinfo))
    application.add_handler(CommandHandler("rebuildbalances", cmd_rebuildbalances))
    application.add_handler(CommandHandler("cleanupexpired", cmd_cleanupexpired))

    application.add_handler(CommandHandler("startadmin", cmd_startadmin))
    application.add_handler(CommandHandler("history", cmd_history))

    application.add_handler(CommandHandler("clockoff", cmd_clockoff))
    application.add_handler(CommandHandler("claimoff", cmd_claimoff))
    application.add_handler(CommandHandler("clockphoff", cmd_clockphoff))
    application.add_handler(CommandHandler("claimphoff", cmd_claimphoff))
    application.add_handler(CommandHandler("clockspecialoff", cmd_clockspecialoff))
    application.add_handler(CommandHandler("claimspecialoff", cmd_claimspecialoff))
    application.add_handler(CommandHandler("newuser", cmd_newuser))
    application.add_handler(CommandHandler("adjustoil", cmd_adjustoil))
    application.add_handler(CommandHandler("massadjustoff", cmd_massadjustoff))

    application.add_handler(CommandHandler("summary", cmd_summary))
    application.add_handler(CommandHandler("overview", cmd_overview))
    application.add_handler(CommandHandler("detailedoverview", cmd_detailedoverview))

    application.add_handler(CallbackQueryHandler(handle_callback))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
