import logging
from datetime import datetime, timedelta
from typing import Any, Dict
from uuid import uuid4
from zoneinfo import ZoneInfo

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

from bot.ui import (
    bold,
    build_calendar,
    cancel_keyboard,
    parse_date_yyyy_mm_dd,
    reply_quiet,
    send_group_quiet,
    validate_application_date,
    validate_half_step,
)

from services.ledger import (
    award_dos_for_date,
    compute_user_summary,
    dos_points_for_date,
    is_sg_public_holiday,
    rebuild_user_balance,
)

from services.runtime_state import pending_payloads, user_state
from services.sheets_repo import (
    append_ledger_row,
    get_all_rows,
    list_all_known_users,
)

log = logging.getLogger(__name__)

SG_TZ = ZoneInfo("Asia/Singapore")


def sg_now() -> datetime:
    return datetime.now(SG_TZ)


def sg_today():
    return sg_now().date()


def _label_from_action(action: str) -> str:
    if "claim" in action:
        return "Claim Off"
    if "clock" in action:
        return "Clock Off"
    return action


def _off_type_label(action: str, is_ph: bool = False, is_special: bool = False, is_dos: bool = False) -> str:
    if is_dos or action == "dos":
        return "DOS"
    if is_special or action in ("clockspecialoff", "claimspecialoff"):
        return "Special"
    if is_ph or action in ("clockphoff", "claimphoff"):
        return "PH"
    return "Normal"


def _adjust_type_flags(kind: str):
    is_ph = kind == "ph"
    is_special = kind == "special"
    is_dos = kind == "dos"
    return is_ph, is_special, is_dos


def _off_type_value(is_ph: bool, is_special: bool, is_dos: bool = False) -> str:
    if is_dos:
        return "DOS"
    if is_special:
        return "SPECIAL"
    if is_ph:
        return "PH"
    return "NORMAL"


def _request_action_type(action: str) -> str:
    return "CLAIM" if "claim" in action else "CLOCK"


def _extract_unique_users():
    return list_all_known_users()

def _dos_kind_and_points(app_date: str) -> tuple[str, int]:
    d = datetime.strptime(app_date, "%Y-%m-%d").date()
    if is_sg_public_holiday(d):
        return "Holiday DOS", 3
    if d.weekday() >= 5:
        return "Weekend DOS", 2
    return "Weekday DOS", 1

def build_adjust_user_keyboard(session_id: str) -> InlineKeyboardMarkup:
    users = _extract_unique_users()
    buttons = []
    row = []
    for uid, name in users:
        row.append(
            InlineKeyboardButton(
                name[:24],
                callback_data=f"adjuser|{session_id}|{uid}",
            )
        )
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    buttons.append([InlineKeyboardButton("❌ Cancel", callback_data=f"cancel|{session_id}")])
    return InlineKeyboardMarkup(buttons)


def build_redo_section_keyboard(session_id: str, section: str) -> InlineKeyboardMarkup:
    if section == "ph":
        redo_btn = InlineKeyboardButton("🔁 Redo PH Off", callback_data=f"redo_ph|{session_id}")
    else:
        redo_btn = InlineKeyboardButton("🔁 Redo Special Off", callback_data=f"redo_special|{session_id}")
    cancel_btn = InlineKeyboardButton("❌ Cancel", callback_data=f"cancel|{session_id}")
    return InlineKeyboardMarkup([[redo_btn, cancel_btn]])


async def _is_admin_in_chat(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> bool:
    try:
        admins = await context.bot.get_chat_administrators(chat_id)
        return any(a.user.id == user_id for a in admins)
    except Exception:
        return False


def _format_adjustoil_preview(payload: dict) -> str:
    amount = float(payload["amount"])
    operator = "+" if amount >= 0 else "-"
    abs_amount = abs(amount)

    lines = [
        "🛠 *Adjust OIL Confirmation*",
        "",
        f"👤 User: {payload['target_name']} ({payload['target_user_id']})",
        f"🏷 Type: {payload['oil_type'].title()}",
        f"🔢 Adjustment: {amount:+.1f}",
        f"📅 Application Date: {payload['application_date']}",
        f"📝 Remarks: {payload['remarks']}",
        "",
        "*Balances Before*",
        f"- Total: {payload['current_total']:.1f}",
        f"- Normal: {payload['current_normal']:.1f}",
        f"- PH: {payload['current_ph']:.1f}",
        f"- Special: {payload['current_special']:.1f}",
        "",
        "*Balances After*",
        f"- Total: {payload['projected_total']:.1f}",
        f"- Normal: {payload['projected_normal']:.1f}",
        f"- PH: {payload['projected_ph']:.1f}",
        f"- Special: {payload['projected_special']:.1f}",
        "",
        f"📘 Ledger Row: {payload['ledger_before']:.1f} {operator} {abs_amount:.1f} = {payload['ledger_after']:.1f}",
    ]
    if payload.get("expiry"):
        lines.append(f"⏳ Expiry: {payload['expiry']}")
    return "\n".join(lines)


def _format_massadjust_preview(payload: dict) -> str:
    amount = float(payload["amount"])
    lines = [
        "🛠 *Mass Adjust OIL Confirmation*",
        "",
        f"🏷 Type: {payload['oil_type'].title()}",
        f"🔢 Adjustment: {amount:+.1f}",
        f"👥 Users Targeted: {payload['target_count']}",
        f"📅 Application Date: {payload['application_date']}",
        f"📝 Remarks: {payload['remarks']}",
    ]
    if payload.get("expiry"):
        lines.append(f"⏳ Expiry: {payload['expiry']}")
    if payload.get("skipped"):
        lines.append("")
        lines.append(f"⚠️ Will skip {len(payload['skipped'])} user(s) due to insufficient {payload['oil_type'].title()} balance.")
    return "\n".join(lines)


def _onboarding_intro_text() -> str:
    return (
        "🆕 *Onboarding: Import Old Records*\n\n"
        "1) How many *normal OIL* days to import? (e.g. 7.5 or 0 if none)\n\n"
        "⚠️ *Important for PH / Special import*\n"
        "This onboarding uses a *FIFO approach*.\n"
        "Please key in *PH* and *Special* entries from the *oldest date to the newest date*.\n"
        "In practice, enter the entry with the *earliest expiry first*.\n"
        "If you key in a later date first and then an earlier date later, the bot will reject it."
    )


def _ph_prompt_count() -> str:
    return (
        "How many PH entries do you want to add? (0–10)\n\n"
        "⚠️ *FIFO rule*\n"
        "Please key in PH entries from *oldest date to newest date*.\n"
        "That means the *earliest expiry* should be entered first."
    )


def _special_prompt_count() -> str:
    return (
        "How many Special entries do you want to add? (0–10)\n\n"
        "⚠️ *FIFO rule*\n"
        "Please key in Special entries from *oldest date to newest date*.\n"
        "That means the *earliest expiry* should be entered first."
    )


def _validate_fifo_date(existing_entries: list[dict], new_date_str: str) -> tuple[bool, str]:
    if not existing_entries:
        return True, ""
    prev_date_str = existing_entries[-1].get("date", "")
    try:
        prev_date = datetime.strptime(prev_date_str, "%Y-%m-%d").date()
        new_date = datetime.strptime(new_date_str, "%Y-%m-%d").date()
    except Exception:
        return True, ""

    if new_date < prev_date:
        return (
            False,
            prev_date_str,
        )
    return True, ""


async def apply_adjustoil_payload(context: ContextTypes.DEFAULT_TYPE, payload: dict):
    uid = payload["target_user_id"]
    uname = payload["target_name"]
    amount = float(payload["amount"])
    approver_name = payload["admin_name"]
    app_date = payload["application_date"]
    remarks = payload["remarks"]
    is_ph = payload.get("is_ph", False)
    is_special = payload.get("is_special", False)
    is_dos = payload.get("is_dos", False)
    expiry = payload.get("expiry", "")

    append_ledger_row(
        telegram_id=uid,
        name=uname,
        action_type="ADJUST",
        off_type=_off_type_value(is_ph, is_special, is_dos),
        amount=amount,
        application_date=app_date,
        expiry_date=expiry if amount > 0 and (is_ph or is_special) else "",
        remarks=remarks,
        approved_by=approver_name,
        source="ADMIN",
    )
    rebuild_user_balance(uid, get_all_rows)


async def apply_massadjust_payload(context: ContextTypes.DEFAULT_TYPE, payload: dict):
    amount = float(payload["amount"])
    approver_name = payload["admin_name"]
    app_date = payload["application_date"]
    remarks = payload["remarks"]
    oil_type = payload["oil_type"]
    is_ph = payload["is_ph"]
    is_special = payload["is_special"]
    expiry = payload.get("expiry", "")

    users = _extract_unique_users()
    adjusted = []
    skipped = []

    for uid, uname in users:
        summary = compute_user_summary(str(uid), get_all_rows)

        if oil_type == "ph" and amount < 0 and summary.ph_active + amount < 0:
            skipped.append(uname)
            continue

        if oil_type == "special" and amount < 0 and summary.special_active + amount < 0:
            skipped.append(uname)
            continue

        append_ledger_row(
            telegram_id=uid,
            name=uname,
            action_type="MASS_ADJUST",
            off_type=_off_type_value(is_ph, is_special),
            amount=amount,
            application_date=app_date,
            expiry_date=expiry if amount > 0 and (is_ph or is_special) else "",
            remarks=remarks,
            approved_by=approver_name,
            source="ADMIN",
        )
        rebuild_user_balance(uid, get_all_rows)
        adjusted.append(uname)

    return adjusted, skipped


def build_admin_summary_text(payload: dict, approved: bool, approver_name: str, final_off: float | None) -> str:
    status = "✅ Approved" if approved else "❌ Denied"

    if payload["type"] == "single":
        is_dos = payload.get("is_dos", False)

        off_type = _off_type_label(
            payload.get("action", ""),
            payload.get("is_ph", False),
            payload.get("is_special", False),
            is_dos,
        )

        if is_dos:
            lines = [
                status,
                f"Clock DOS [{off_type}] — {payload['user_name']} ({payload['user_id']})",
                f"Points: {payload.get('dos_points', 0.0):.1f} | Type: {payload.get('dos_kind', 'DOS')} | Date: {payload['app_date']}",
                f"Reason: {payload.get('reason', '') or '—'}",
                "",
                "Balances Before",
                f"- Total: {payload.get('current_total', 0.0):.1f}",
                f"- Normal: {payload.get('current_normal', 0.0):.1f}",
                f"- PH: {payload.get('current_ph', 0.0):.1f}",
                f"- Special: {payload.get('current_special', 0.0):.1f}",
                f"- DOS Points: {payload.get('current_dos', 0.0):.1f}",
                "",
                "Balances After",
                f"- Total: {payload.get('projected_total', 0.0):.1f}",
                f"- Normal: {payload.get('projected_normal', 0.0):.1f}",
                f"- PH: {payload.get('projected_ph', 0.0):.1f}",
                f"- Special: {payload.get('projected_special', 0.0):.1f}",
                f"- DOS Points: {payload.get('projected_dos', 0.0):.1f}",
                f"Approved by: {approver_name}",
            ]
            return "\n".join(lines)

        lines = [
            status,
            f"{_label_from_action(payload['action'])} [{off_type}] — {payload['user_name']} ({payload['user_id']})",
            f"Days: {payload['days']:.1f} | Date: {payload['app_date']}",
            f"Reason: {payload.get('reason', '') or '—'}",
            "",
            "Balances Before",
            f"- Total: {payload.get('current_total', 0.0):.1f}",
            f"- Normal: {payload.get('current_normal', 0.0):.1f}",
            f"- PH: {payload.get('current_ph', 0.0):.1f}",
            f"- Special: {payload.get('current_special', 0.0):.1f}",
            f"- DOS Points: {payload.get('current_dos', 0.0):.1f}",
            "",
            "Balances After",
            f"- Total: {payload.get('projected_total', 0.0):.1f}",
            f"- Normal: {payload.get('projected_normal', 0.0):.1f}",
            f"- PH: {payload.get('projected_ph', 0.0):.1f}",
            f"- Special: {payload.get('projected_special', 0.0):.1f}",
            f"- DOS Points: {payload.get('projected_dos', 0.0):.1f}",
        ]

        if (payload.get("is_ph") or payload.get("is_special")) and payload.get("expiry"):
            lines.append(f"Expiry: {payload['expiry']}")

        if payload.get("warn_negative_normal"):
            lines.extend([
                "",
                "⚠️ Warning",
                f"Normal OIL will go negative: {payload.get('projected_normal', 0.0):.1f}",
            ])

        lines.append(f"Approved by: {approver_name}")
        return "\n".join(lines)

    if payload["type"] == "newuser":
        return "\n".join([
            status,
            f"Onboarding — {payload['user_name']} ({payload['user_id']})",
            f"Normal OIL: {payload.get('normal_days', 0)}",
            f"PH entries: {len(payload.get('ph_entries', []))}",
            f"Special entries: {len(payload.get('special_entries', []))}",
            f"Approved by: {approver_name}",
        ])

    return f"{status} by {approver_name}"


async def update_all_admin_pm(context: ContextTypes.DEFAULT_TYPE, payload: dict, summary_text: str):
    for admin_id, msg_id in payload.get("admin_msgs", []):
        try:
            await context.bot.edit_message_text(
                chat_id=admin_id,
                message_id=msg_id,
                text=summary_text,
            )
        except Exception:
            try:
                await context.bot.send_message(chat_id=admin_id, text=summary_text)
            except Exception:
                pass


async def cmd_startadmin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        await reply_quiet(update, "Please PM me and use /startadmin there.")
        return

    user_state[update.effective_user.id] = {
        "flow": "admin_session",
        "stage": "ready",
        "owner_id": update.effective_user.id,
    }
    await update.message.reply_text("✅ Admin session started here. You’ll receive approval prompts in this PM.")


async def start_flow_days(update: Update, context: ContextTypes.DEFAULT_TYPE, flow: str, action: str, is_ph: bool):
    uid = update.effective_user.id
    sid = str(uuid4())[:10]

    user_state[uid] = {
        "sid": sid,
        "flow": flow,
        "action": action,
        "stage": "awaiting_days",
        "group_id": update.effective_chat.id if update.effective_chat else None,
        "is_ph": is_ph,
        "owner_id": uid,
    }

    summary = compute_user_summary(str(uid), get_all_rows)

    icon = "🏖" if is_ph else ("⭐" if flow == "special" else ("🗂" if action.startswith("claim") else "🕒"))

    if action == "claimoff":
        current_bucket = summary.normal_balance
        bucket_label = "Normal OIL"
    elif action == "claimphoff":
        current_bucket = summary.ph_active
        bucket_label = "Active PH OIL"
    elif action == "claimspecialoff":
        current_bucket = summary.special_active
        bucket_label = "Active Special OIL"
    elif action == "clockphoff":
        current_bucket = summary.ph_active
        bucket_label = "Current Active PH OIL"
    elif action == "clockspecialoff":
        current_bucket = summary.special_active
        bucket_label = "Current Active Special OIL"
    else:
        current_bucket = summary.normal_balance
        bucket_label = "Current Normal OIL"

    verb = "claim" if "claim" in action else "clock"

    await reply_quiet(
        update,
        f"{icon} Your current {bucket_label}: {current_bucket:.1f}\n\n"
        f"How many days do you want to {verb}? (0.5 to 3, in 0.5 steps)\n"
        f"Date limits will be shown next.",
        reply_markup=cancel_keyboard(sid),
    )


async def cmd_clockoff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start_flow_days(update, context, "normal", "clockoff", False)


async def cmd_claimoff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start_flow_days(update, context, "normal", "claimoff", False)


async def cmd_clockphoff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start_flow_days(update, context, "ph", "clockphoff", True)


async def cmd_claimphoff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start_flow_days(update, context, "ph", "claimphoff", True)


async def cmd_clockspecialoff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start_flow_days(update, context, "special", "clockspecialoff", False)


async def cmd_claimspecialoff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start_flow_days(update, context, "special", "claimspecialoff", False)


async def cmd_clockdos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    sid = str(uuid4())[:10]

    user_state[uid] = {
        "sid": sid,
        "flow": "dos",
        "action": "clockdos",
        "stage": "awaiting_app_date",
        "group_id": update.effective_chat.id if update.effective_chat else None,
        "owner_id": uid,
        "is_ph": False,
        "is_dos": True,
        "min_date": sg_today() - timedelta(days=365),
        "max_date": sg_today(),
    }

    summary = compute_user_summary(str(uid), get_all_rows)

    await reply_quiet(
        update,
        f"🪖 Your current DOS Points: {summary.dos_points:.1f}\n\n"
        f"Select the DOS date:",
        reply_markup=build_calendar(
            sid,
            sg_today(),
            sg_today() - timedelta(days=365),
            sg_today(),
        ),
    )


async def cmd_adjustoil(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat or chat.type == "private":
        await update.message.reply_text("Please use /adjustoil inside the group.")
        return

    is_admin = await _is_admin_in_chat(context, chat.id, update.effective_user.id)
    if not is_admin:
        await reply_quiet(update, "❌ Only group admins can use /adjustoil.")
        return

    sid = str(uuid4())[:10]
    user_state[update.effective_user.id] = {
        "sid": sid,
        "flow": "adjustoil",
        "stage": "awaiting_type",
        "group_id": chat.id,
        "owner_id": update.effective_user.id,
        "admin_name": update.effective_user.full_name,
    }

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Normal", callback_data=f"adjtype|{sid}|normal"),
            InlineKeyboardButton("PH", callback_data=f"adjtype|{sid}|ph"),
        ],
        [
            InlineKeyboardButton("Special", callback_data=f"adjtype|{sid}|special"),
            InlineKeyboardButton("DOS", callback_data=f"adjtype|{sid}|dos"),
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"cancel|{sid}")],
    ])

    await reply_quiet(
        update,
        "🛠 Select which OIL type to adjust:",
        reply_markup=kb,
    )


async def cmd_massadjustoff(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if not chat or chat.type == "private":
        await update.message.reply_text("Please use /massadjustoff inside the group.")
        return

    is_admin = await _is_admin_in_chat(context, chat.id, update.effective_user.id)
    if not is_admin:
        await reply_quiet(update, "❌ Only group admins can use /massadjustoff.")
        return

    sid = str(uuid4())[:10]
    user_state[update.effective_user.id] = {
        "sid": sid,
        "flow": "massadjustoff",
        "stage": "awaiting_type",
        "group_id": chat.id,
        "owner_id": update.effective_user.id,
        "admin_name": update.effective_user.full_name,
    }

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Normal", callback_data=f"massadjtype|{sid}|normal"),
            InlineKeyboardButton("PH", callback_data=f"massadjtype|{sid}|ph"),
            InlineKeyboardButton("Special", callback_data=f"massadjtype|{sid}|special"),
        ],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"cancel|{sid}")],
    ])

    await reply_quiet(
        update,
        "🛠 Select which OIL type to mass adjust:",
        reply_markup=kb,
    )


async def cmd_newuser(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type == "private":
        await update.message.reply_text("Please send /newuser in the group where records live.")
        return

    uid = update.effective_user.id
    sid = str(uuid4())[:10]
    rows = get_all_rows()
    exists = any(len(r) > 1 and r[1] == str(uid) for r in rows[1:])
    if exists:
        await reply_quiet(update, "You already have records here. Import is only for brand-new users.")
        return

    user_state[uid] = {
        "sid": sid,
        "flow": "newuser",
        "stage": "awaiting_normal_days",
        "group_id": chat.id,
        "newuser": {
            "normal_days": None,
            "ph_entries": [],
            "special_entries": [],
        },
        "owner_id": uid,
    }

    await reply_quiet(
        update,
        _onboarding_intro_text(),
        parse_mode="Markdown",
        reply_markup=cancel_keyboard(sid),
    )


async def finalize_single_request(update: Update, context: ContextTypes.DEFAULT_TYPE, st: Dict[str, Any], app_date: str):
    uid = update.effective_user.id
    user = update.effective_user
    group_id = st.get("group_id") or (update.effective_chat.id if update.effective_chat else None)

    action = st["action"]
    is_ph = st["is_ph"]
    is_special = action in ("clockspecialoff", "claimspecialoff")
    is_dos = action == "clockdos"

    summary = compute_user_summary(str(uid), get_all_rows)
    current_normal = summary.normal_balance
    current_ph = summary.ph_active
    current_special = summary.special_active
    current_total = summary.total_balance
    current_dos = summary.dos_points

    projected_normal = current_normal
    projected_ph = current_ph
    projected_special = current_special
    projected_dos = current_dos

    days = float(st.get("days", 0.0))
    dos_points = 0
    dos_kind = ""

    if action == "claimoff":
        projected_normal = current_normal - days
    elif action == "claimphoff":
        projected_ph = current_ph - days
    elif action == "claimspecialoff":
        projected_special = current_special - days
    elif action == "clockoff":
        projected_normal = current_normal + days
    elif action == "clockphoff":
        projected_ph = current_ph + days
    elif action == "clockspecialoff":
        projected_special = current_special + days
    elif action == "clockdos":
        dos_kind, dos_points = _dos_kind_and_points(app_date)
        projected_dos = current_dos + float(dos_points)

    if action == "claimphoff" and days > current_ph:
        await reply_quiet(
            update,
            f"❌ You only have {current_ph:.1f} active PH OIL available.\n"
            f"Requested claim: {days:.1f}",
        )
        return

    if action == "claimspecialoff" and days > current_special:
        await reply_quiet(
            update,
            f"❌ You only have {current_special:.1f} active Special OIL available.\n"
            f"Requested claim: {days:.1f}",
        )
        return

    ok, msg = validate_application_date(action, app_date)
    if not ok:
        await reply_quiet(update, msg)
        return

    expiry = ""
    if action in ("clockphoff", "clockspecialoff"):
        try:
            d = datetime.strptime(app_date, "%Y-%m-%d").date()
            expiry = (d + timedelta(days=365)).strftime("%Y-%m-%d")
        except Exception:
            expiry = ""

    key = str(uuid4())[:12]
    payload = {
        "type": "single",
        "user_id": str(uid),
        "user_name": user.full_name,
        "group_id": group_id,
        "action": action,
        "days": days,
        "dos_points": dos_points,
        "dos_kind": dos_kind,
        "reason": st.get("reason", ""),
        "app_date": app_date,
        "is_ph": is_ph,
        "is_special": is_special,
        "is_dos": is_dos,
        "expiry": expiry,
        "admin_msgs": [],
        "current_total": current_total,
        "current_normal": current_normal,
        "current_ph": current_ph,
        "current_special": current_special,
        "current_dos": current_dos,
        "projected_total": projected_normal + projected_ph + projected_special,
        "projected_normal": projected_normal,
        "projected_ph": projected_ph,
        "projected_special": projected_special,
        "projected_dos": projected_dos,
        "warn_negative_normal": projected_normal < 0,
    }

    try:
        admins = await context.bot.get_chat_administrators(group_id)
    except Exception:
        admins = []

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Approve", callback_data=f"approve|{key}"),
        InlineKeyboardButton("❌ Deny", callback_data=f"deny|{key}"),
    ]])

    off_type = _off_type_label(action, is_ph, is_special, is_dos)
    display_action = "Clock DOS" if is_dos else _label_from_action(action)

    text_lines = [
        f"🆕 *{display_action} Request [{off_type}]*",
        "",
        f"👤 User: {user.full_name} ({uid})",
    ]

    if is_dos:
        text_lines.extend([
            f"🪖 DOS Type: {dos_kind}",
            f"📌 Points: {dos_points:.1f}",
        ])
    else:
        text_lines.append(f"📅 Days: {days:.1f}")

    text_lines.extend([
        f"🗓 Application Date: {app_date}",
        f"📝 Reason: {st.get('reason', '') or '—'}",
        "",
        "*Balances Before*",
        f"- Total: {current_total:.1f}",
        f"- Normal: {current_normal:.1f}",
        f"- PH: {current_ph:.1f}",
        f"- Special: {current_special:.1f}",
        f"- DOS Points: {current_dos:.1f}",
        "",
        "*Balances After*",
        f"- Total: {(projected_normal + projected_ph + projected_special):.1f}",
        f"- Normal: {projected_normal:.1f}",
        f"- PH: {projected_ph:.1f}",
        f"- Special: {projected_special:.1f}",
        f"- DOS Points: {projected_dos:.1f}",
    ])

    if expiry:
        if is_ph:
            text_lines.append(f"🏖 PH Expiry: {expiry}")
        if is_special:
            text_lines.append(f"⭐ Special Expiry: {expiry}")

    if projected_normal < 0 and action == "claimoff":
        text_lines.extend([
            "",
            "⚠️ *Warning*",
            f"Normal OIL will go negative after approval: {projected_normal:.1f}",
        ])

    text = "\n".join(text_lines)

    sent_any = False
    admin_msgs = []
    for a in admins:
        if a.user.is_bot:
            continue
        try:
            msg = await context.bot.send_message(
                chat_id=a.user.id,
                text=text,
                parse_mode="Markdown",
                reply_markup=kb,
            )
            admin_msgs.append((a.user.id, msg.message_id))
            sent_any = True
        except Exception:
            pass

    payload["admin_msgs"] = admin_msgs
    pending_payloads[key] = payload

    if sent_any:
        await send_group_quiet(context, group_id, "📩 Request submitted to admins for approval.")
    else:
        await send_group_quiet(context, group_id, "⚠️ Could not reach any admin. Please ensure the bot can PM admins.")

    user_state.pop(uid, None)


async def newuser_review(update: Update, context: ContextTypes.DEFAULT_TYPE, st: Dict[str, Any], via_edit=None):
    nu = st["newuser"]
    uid = update.effective_user.id
    uname = update.effective_user.full_name
    gid = st["group_id"]

    lines = [f"👤 {uname} ({uid})"]
    lines.append(f"Normal OIL days to import: {nu['normal_days']}")
    lines.append(f"PH entries: {len(nu['ph_entries'])}")
    for e in nu["ph_entries"]:
        lines.append(f"  • {e['date']} — {e['reason']}")
    lines.append(f"Special entries: {len(nu['special_entries'])}")
    for e in nu["special_entries"]:
        lines.append(f"  • {e['date']} — {e['reason']}")

    key = str(uuid4())[:12]
    payload = {
        "type": "newuser",
        "group_id": gid,
        "user_id": str(uid),
        "user_name": uname,
        "normal_days": float(nu["normal_days"] or 0.0),
        "ph_entries": nu["ph_entries"],
        "special_entries": nu["special_entries"],
        "admin_msgs": [],
    }

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Approve", callback_data=f"approve|{key}"),
        InlineKeyboardButton("❌ Deny", callback_data=f"deny|{key}"),
    ]])

    text = "🔎 *Import Review*\n" + "\n".join(lines)

    try:
        admins = await context.bot.get_chat_administrators(gid)
    except Exception:
        admins = []

    sent = False
    admin_msgs = []
    for a in admins:
        if a.user.is_bot:
            continue
        try:
            msg = await context.bot.send_message(
                chat_id=a.user.id,
                text=text,
                parse_mode="Markdown",
                reply_markup=kb,
            )
            admin_msgs.append((a.user.id, msg.message_id))
            sent = True
        except Exception:
            pass

    payload["admin_msgs"] = admin_msgs
    pending_payloads[key] = payload

    if sent:
        if via_edit:
            await via_edit.edit_message_text("Submitted to admins for approval.")
        else:
            await send_group_quiet(context, gid, "Submitted to admins for approval.")
    else:
        if via_edit:
            await via_edit.edit_message_text("⚠️ Couldn’t reach any admin.")
        else:
            await send_group_quiet(context, gid, "⚠️ Couldn’t reach any admin.")

    user_state.pop(uid, None)


async def handle_single_apply(update: Update, context: ContextTypes.DEFAULT_TYPE, payload: Dict[str, Any], approved: bool, approver_name: str, approver_id: int):
    gid = payload["group_id"]

    if not approved:
        await send_group_quiet(context, gid, f"❌ Request for {payload['user_name']} denied by {approver_name}.")
        summary = build_admin_summary_text(payload, approved=False, approver_name=approver_name, final_off=None)
        await update_all_admin_pm(context, payload, summary)
        return

    uid = payload["user_id"]
    uname = payload["user_name"]
    action = payload["action"]
    reason = payload.get("reason", "")
    is_dos = bool(payload.get("is_dos"))

    if is_dos:
        award_dos_for_date(
            telegram_id=uid,
            name=uname,
            work_date=payload["app_date"],
            approved_by=approver_name,
            source="USER",
            remarks=reason,
            get_all_rows_fn=get_all_rows,
        )
    else:
        days = float(payload["days"])
        app_date = payload["app_date"]
        is_ph = bool(payload.get("is_ph"))
        is_special = bool(payload.get("is_special"))
        expiry = payload.get("expiry", "")

        append_ledger_row(
            telegram_id=uid,
            name=uname,
            action_type=_request_action_type(action),
            off_type=_off_type_value(is_ph, is_special),
            amount=(-days if "claim" in action else days),
            application_date=app_date,
            expiry_date=(expiry if "clock" in action and (is_ph or is_special) else ""),
            remarks=reason,
            approved_by=approver_name,
            source="USER",
        )
        rebuild_user_balance(uid, get_all_rows)

    await send_group_quiet(context, gid, f"✅ Request for {uname} approved by {approver_name}.")
    summary = build_admin_summary_text(payload, approved=True, approver_name=approver_name, final_off=None)
    await update_all_admin_pm(context, payload, summary)


async def handle_newuser_apply(update: Update, context: ContextTypes.DEFAULT_TYPE, payload: Dict[str, Any], approved: bool, approver_name: str, approver_id: int):
    gid = payload["group_id"]
    uid = payload["user_id"]
    uname = payload["user_name"]
    normal_days = float(payload.get("normal_days", 0.0))
    ph_entries = payload.get("ph_entries", [])
    special_entries = payload.get("special_entries", [])

    if not approved:
        await send_group_quiet(context, gid, f"❌ Onboarding import for {uname} denied by {approver_name}.")
        summary = build_admin_summary_text(payload, approved=False, approver_name=approver_name, final_off=None)
        await update_all_admin_pm(context, payload, summary)
        return

    if normal_days > 0:
        append_ledger_row(
            telegram_id=uid,
            name=uname,
            action_type="IMPORT",
            off_type="NORMAL",
            amount=normal_days,
            application_date=sg_today().strftime("%Y-%m-%d"),
            expiry_date="",
            remarks="Transfer from old record",
            approved_by=approver_name,
            source="USER",
        )

    for entry in ph_entries:
        dstr = entry.get("date")
        reason = entry.get("reason", "")
        if not dstr:
            continue

        expiry = ""
        try:
            d = datetime.strptime(dstr, "%Y-%m-%d").date()
            expiry = (d + timedelta(days=365)).strftime("%Y-%m-%d")
        except Exception:
            pass

        append_ledger_row(
            telegram_id=uid,
            name=uname,
            action_type="IMPORT",
            off_type="PH",
            amount=1.0,
            application_date=dstr,
            expiry_date=expiry,
            remarks=reason,
            approved_by=approver_name,
            source="USER",
        )

    for entry in special_entries:
        dstr = entry.get("date")
        reason = entry.get("reason", "")
        if not dstr:
            continue

        expiry = ""
        try:
            d = datetime.strptime(dstr, "%Y-%m-%d").date()
            expiry = (d + timedelta(days=365)).strftime("%Y-%m-%d")
        except Exception:
            pass

        append_ledger_row(
            telegram_id=uid,
            name=uname,
            action_type="IMPORT",
            off_type="SPECIAL",
            amount=1.0,
            application_date=dstr,
            expiry_date=expiry,
            remarks=reason,
            approved_by=approver_name,
            source="USER",
        )

    rebuild_user_balance(uid, get_all_rows)

    await send_group_quiet(context, gid, f"✅ Onboarding import for {uname} approved by {approver_name}.")
    summary = build_admin_summary_text(payload, approved=True, approver_name=approver_name, final_off=None)
    await update_all_admin_pm(context, payload, summary)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    uid = update.effective_user.id
    text = update.message.text.strip()

    if text.lower() == "-quit":
        user_state.pop(uid, None)
        await reply_quiet(update, "🧹 Cancelled.")
        return

    st = user_state.get(uid)
    if not st:
        return

    if st.get("owner_id") != uid:
        return

    if st["flow"] in ("normal", "ph", "special") and st["stage"] == "awaiting_days":
        try:
            days = float(text)
            if days <= 0 or days > 3 or not validate_half_step(days):
                raise ValueError()
        except ValueError:
            await reply_quiet(update, "❌ Invalid input. Enter 0.5 to 3.0 in 0.5 steps.", reply_markup=cancel_keyboard(st["sid"]))
            return

        st["days"] = days
        st["stage"] = "awaiting_app_date"
        st["min_date"] = sg_today() - timedelta(days=365)
        st["max_date"] = sg_today() + (
            timedelta(days=365) if st["action"] in ("claimoff", "claimphoff", "claimspecialoff") else timedelta(days=0)
        )

        await reply_quiet(
            update,
            f"{bold('📅 Select Application Date:')}\n"
            f"Allowed date range: {st['min_date']} to {st['max_date']}",
            parse_mode="Markdown",
            reply_markup=build_calendar(st["sid"], sg_today(), st["min_date"], st["max_date"]),
        )
        return

    if st["flow"] == "adjustoil" and st["stage"] == "awaiting_amount":
        try:
            amount = float(text)
            if amount == 0 or not validate_half_step(amount):
                raise ValueError()
        except ValueError:
            await reply_quiet(
                update,
                "❌ Invalid input. Use positive to add or negative to subtract in 0.5 steps.\nExamples: 1.0, -0.5",
                reply_markup=cancel_keyboard(st["sid"]),
            )
            return

        target_uid = st["target_user_id"]
        target_name = st["target_name"]
        oil_type = st["oil_type"]
        is_ph, is_special, is_dos = _adjust_type_flags(oil_type)

        summary = compute_user_summary(str(target_uid), get_all_rows)
        current_total = summary.total_balance
        current_normal = summary.normal_balance
        current_ph = summary.ph_active
        current_special = summary.special_active
        current_dos = summary.dos_points

        projected_normal = current_normal
        projected_ph = current_ph
        projected_special = current_special
        projected_dos = current_dos

        if oil_type == "normal":
            projected_normal += amount
        elif oil_type == "ph":
            projected_ph += amount
        elif oil_type == "special":
            projected_special += amount
        elif oil_type == "dos":
            projected_dos += amount

        if oil_type == "ph" and projected_ph < 0:
            await reply_quiet(
                update,
                f"❌ PH OIL cannot go below 0.\nCurrent active PH: {current_ph:.1f}\nRequested adjustment: {amount:+.1f}",
                reply_markup=cancel_keyboard(st["sid"]),
            )
            return

        if oil_type == "special" and projected_special < 0:
            await reply_quiet(
                update,
                f"❌ Special OIL cannot go below 0.\nCurrent active Special: {current_special:.1f}\nRequested adjustment: {amount:+.1f}",
                reply_markup=cancel_keyboard(st["sid"]),
            )
            return
        
        if oil_type == "dos":
            if amount != int(amount):
                await reply_quiet(
                    update,
                    "❌ DOS must use whole-number points only. Examples: 1, 2, 3, -1",
                    reply_markup=cancel_keyboard(st["sid"]),
                )
                return
            if projected_dos < 0:
                await reply_quiet(
                    update,
                    f"❌ DOS points cannot go below 0.\nCurrent DOS: {current_dos:.1f}\nRequested adjustment: {amount:+.1f}",
                    reply_markup=cancel_keyboard(st["sid"]),
                )
                return

        app_date = sg_today().strftime("%Y-%m-%d")
        expiry = ""
        if amount > 0 and oil_type in ("ph", "special"):
            expiry = (sg_today() + timedelta(days=365)).strftime("%Y-%m-%d")

        st["amount"] = amount
        st["application_date"] = app_date
        st["is_ph"] = is_ph
        st["is_special"] = is_special
        st["current_total"] = current_total
        st["current_normal"] = current_normal
        st["current_ph"] = current_ph
        st["current_special"] = current_special
        st["projected_total"] = projected_normal + projected_ph + projected_special
        st["projected_normal"] = projected_normal
        st["projected_ph"] = projected_ph
        st["projected_special"] = projected_special
        st["expiry"] = expiry
        st["stage"] = "awaiting_reason"
        st["is_dos"] = is_dos
        st["current_dos"] = current_dos
        st["projected_dos"] = projected_dos
        st["projected_total"] = projected_normal + projected_ph + projected_special
        st["ledger_before"] = current_dos if oil_type == "dos" else current_total
        st["ledger_after"] = projected_dos if oil_type == "dos" else (projected_normal + projected_ph + projected_special)

        await reply_quiet(
            update,
            "📝 Enter reason for this adjustment.",
            reply_markup=cancel_keyboard(st["sid"]),
        )
        return

    if st["flow"] == "adjustoil" and st["stage"] == "awaiting_reason":
        reason = text.strip()
        if not reason or reason.lower() == "nil":
            await reply_quiet(
                update,
                "❌ Reason is required for admin adjustment.",
                reply_markup=cancel_keyboard(st["sid"]),
            )
            return

        payload = {
            "target_user_id": st["target_user_id"],
            "target_name": st["target_name"],
            "oil_type": st["oil_type"],
            "amount": st["amount"],
            "application_date": st["application_date"],
            "remarks": reason[:120],
            "admin_name": st["admin_name"],
            "is_ph": st["is_ph"],
            "is_special": st["is_special"],
            "current_total": st["current_total"],
            "current_normal": st["current_normal"],
            "current_ph": st["current_ph"],
            "current_special": st["current_special"],
            "projected_total": st["projected_total"],
            "projected_normal": st["projected_normal"],
            "projected_ph": st["projected_ph"],
            "projected_special": st["projected_special"],
            "expiry": st.get("expiry", ""),
            "is_dos": st["is_dos"],
            "current_dos": st["current_dos"],
            "projected_dos": st["projected_dos"],
            "ledger_before": st["ledger_before"],
            "ledger_after": st["ledger_after"],
        }

        st["payload"] = payload
        st["stage"] = "awaiting_confirm"

        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Confirm", callback_data=f"adjconfirm|{st['sid']}"),
                InlineKeyboardButton("❌ Cancel", callback_data=f"cancel|{st['sid']}"),
            ]
        ])

        await reply_quiet(
            update,
            _format_adjustoil_preview(payload),
            parse_mode="Markdown",
            reply_markup=kb,
        )
        return

    if st["flow"] == "massadjustoff" and st["stage"] == "awaiting_amount":
        try:
            amount = float(text)
            if amount == 0 or not validate_half_step(amount):
                raise ValueError()
        except ValueError:
            await reply_quiet(
                update,
                "❌ Invalid input. Use positive to add or negative to subtract in 0.5 steps.\nExamples: 1.0, -0.5",
                reply_markup=cancel_keyboard(st["sid"]),
            )
            return

        oil_type = st["oil_type"]
        users = _extract_unique_users()
        skipped = []

        if amount < 0 and oil_type in ("ph", "special"):
            for target_uid, target_name in users:
                summary = compute_user_summary(str(target_uid), get_all_rows)
                if oil_type == "ph" and summary.ph_active + amount < 0:
                    skipped.append(target_name)
                elif oil_type == "special" and summary.special_active + amount < 0:
                    skipped.append(target_name)

        app_date = sg_today().strftime("%Y-%m-%d")
        expiry = ""
        if amount > 0 and oil_type in ("ph", "special"):
            expiry = (sg_today() + timedelta(days=365)).strftime("%Y-%m-%d")

        st["amount"] = amount
        st["application_date"] = app_date
        st["is_ph"] = oil_type == "ph"
        st["is_special"] = oil_type == "special"
        st["expiry"] = expiry
        st["skipped"] = skipped
        st["stage"] = "awaiting_reason"

        await reply_quiet(
            update,
            "📝 Enter reason for this mass adjustment.",
            reply_markup=cancel_keyboard(st["sid"]),
        )
        return

    if st["flow"] == "massadjustoff" and st["stage"] == "awaiting_reason":
        reason = text.strip()
        if not reason or reason.lower() == "nil":
            await reply_quiet(
                update,
                "❌ Reason is required for mass adjustment.",
                reply_markup=cancel_keyboard(st["sid"]),
            )
            return

        payload = {
            "oil_type": st["oil_type"],
            "amount": st["amount"],
            "application_date": st["application_date"],
            "remarks": reason[:120],
            "admin_name": st["admin_name"],
            "is_ph": st["is_ph"],
            "is_special": st["is_special"],
            "expiry": st.get("expiry", ""),
            "target_count": len(_extract_unique_users()),
            "skipped": st.get("skipped", []),
        }

        st["payload"] = payload
        st["stage"] = "awaiting_confirm"

        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Confirm", callback_data=f"massadjconfirm|{st['sid']}"),
                InlineKeyboardButton("❌ Cancel", callback_data=f"cancel|{st['sid']}"),
            ]
        ])

        await reply_quiet(
            update,
            _format_massadjust_preview(payload),
            parse_mode="Markdown",
            reply_markup=kb,
        )
        return

    if st["flow"] in ("normal", "ph", "special", "dos") and st["stage"] == "awaiting_reason":
        txt = text.strip()
        action = st.get("action", "")
        optional = action in ("claimoff", "claimphoff", "claimspecialoff", "clockdos")

        if optional:
            st["reason"] = "—" if txt.lower() == "nil" or txt == "" else txt[:80]
        else:
            if not txt or txt.lower() == "nil":
                await reply_quiet(update, "❌ Remarks required.", reply_markup=cancel_keyboard(st["sid"]))
                return
            st["reason"] = txt[:80]

        await finalize_single_request(update, context, st, st.get("app_date", ""))
        return

    if st["flow"] == "newuser":
        nu = st["newuser"]

        if st["stage"] == "awaiting_normal_days":
            try:
                nd = float(text)
                if nd < 0:
                    raise ValueError()
            except ValueError:
                await reply_quiet(update, "Please enter a non-negative number.", reply_markup=cancel_keyboard(st["sid"]))
                return

            nu["normal_days"] = nd
            st["stage"] = "ph_ask_count"
            await reply_quiet(
                update,
                _ph_prompt_count(),
                parse_mode="Markdown",
                reply_markup=cancel_keyboard(st["sid"]),
            )
            return

        if st["stage"] == "ph_ask_count":
            try:
                cnt = int(text)
                if cnt < 0 or cnt > 10:
                    raise ValueError()
            except ValueError:
                await reply_quiet(update, "Enter an integer between 0 and 10.", reply_markup=cancel_keyboard(st["sid"]))
                return

            nu["ph_count"] = cnt
            if cnt == 0:
                st["stage"] = "special_ask_count"
                await reply_quiet(
                    update,
                    _special_prompt_count(),
                    parse_mode="Markdown",
                    reply_markup=cancel_keyboard(st["sid"]),
                )
                return

            st["ph_idx"] = 0
            st["stage"] = "ph_date"
            st["min_date"] = sg_today() - timedelta(days=365)
            st["max_date"] = sg_today()

            await reply_quiet(
                update,
                f"PH Entry 1/{nu['ph_count']} — {bold('Select Application Date')}\n\n"
                f"⚠️ FIFO approach: enter PH from *oldest date to newest date*.",
                parse_mode="Markdown",
                reply_markup=build_calendar(st["sid"], sg_today(), st["min_date"], st["max_date"]),
            )
            return

        if st["stage"] == "ph_reason":
            idx = st["ph_idx"]
            txt = text.strip()
            if not txt or txt.lower() == "nil":
                await reply_quiet(update, "❌ PH name is required.", reply_markup=cancel_keyboard(st["sid"]))
                return

            nu["ph_entries"][idx]["reason"] = txt[:80]
            idx += 1

            if idx < nu["ph_count"]:
                st["ph_idx"] = idx
                st["stage"] = "ph_date"
                st["min_date"] = sg_today() - timedelta(days=365)
                st["max_date"] = sg_today()

                await reply_quiet(
                    update,
                    f"PH Entry {idx+1}/{nu['ph_count']} — {bold('Select Application Date')}\n\n"
                    f"⚠️ Continue using FIFO order: next date must be the same or later than the previous PH date.",
                    parse_mode="Markdown",
                    reply_markup=build_calendar(st["sid"], sg_today(), st["min_date"], st["max_date"]),
                )
            else:
                st["stage"] = "special_ask_count"
                await reply_quiet(
                    update,
                    _special_prompt_count(),
                    parse_mode="Markdown",
                    reply_markup=cancel_keyboard(st["sid"]),
                )
            return

        if st["stage"] == "special_ask_count":
            try:
                cnt = int(text)
                if cnt < 0 or cnt > 10:
                    raise ValueError()
            except ValueError:
                await reply_quiet(update, "Enter an integer between 0 and 10.", reply_markup=cancel_keyboard(st["sid"]))
                return

            nu["special_count"] = cnt
            if cnt == 0:
                await newuser_review(update, context, st)
                return

            st["special_idx"] = 0
            st["stage"] = "special_date"
            st["min_date"] = sg_today() - timedelta(days=365)
            st["max_date"] = sg_today()

            await reply_quiet(
                update,
                f"Special Entry 1/{nu['special_count']} — {bold('Select Application Date')}\n\n"
                f"⚠️ FIFO approach: enter Special from *oldest date to newest date*.",
                parse_mode="Markdown",
                reply_markup=build_calendar(st["sid"], sg_today(), st["min_date"], st["max_date"]),
            )
            return

        if st["stage"] == "special_reason":
            idx = st["special_idx"]
            txt = text.strip()
            if not txt or txt.lower() == "nil":
                await reply_quiet(update, "❌ Special name is required.", reply_markup=cancel_keyboard(st["sid"]))
                return

            nu["special_entries"][idx]["reason"] = txt[:80]
            idx += 1

            if idx < nu["special_count"]:
                st["special_idx"] = idx
                st["stage"] = "special_date"
                st["min_date"] = sg_today() - timedelta(days=365)
                st["max_date"] = sg_today()

                await reply_quiet(
                    update,
                    f"Special Entry {idx+1}/{nu['special_count']} — {bold('Select Application Date')}\n\n"
                    f"⚠️ Continue using FIFO order: next date must be the same or later than the previous Special date.",
                    parse_mode="Markdown",
                    reply_markup=build_calendar(st["sid"], sg_today(), st["min_date"], st["max_date"]),
                )
            else:
                await newuser_review(update, context, st)
            return

    if st.get("stage") == "awaiting_app_date_manual":
        d = parse_date_yyyy_mm_dd(text)
        if not d:
            await reply_quiet(update, "Invalid date. Please type YYYY-MM-DD.", reply_markup=cancel_keyboard(st["sid"]))
            return

        ok, msg = validate_application_date(st.get("action", ""), d)
        if not ok:
            await reply_quiet(update, msg, reply_markup=cancel_keyboard(st["sid"]))
            return

        st["app_date"] = d
        st["stage"] = "awaiting_reason"

        if st.get("action") == "clockoff":
            prompt = "📝 Enter clocking reason."
        elif st.get("action") == "clockphoff":
            prompt = "📝 Enter PH name."
        elif st.get("action") == "clockspecialoff":
            prompt = "📝 Enter Special Off name."
        else:
            prompt = "📝 Enter remarks (optional). Type 'nil' to skip."

        await reply_quiet(update, prompt, reply_markup=cancel_keyboard(st["sid"]))
        return

    if st.get("stage") == "ph_date_manual":
        d = parse_date_yyyy_mm_dd(text)
        if not d:
            await reply_quiet(update, "Invalid date. Please type YYYY-MM-DD.", reply_markup=cancel_keyboard(st["sid"]))
            return

        ok, msg = validate_application_date("newuser_ph", d)
        if not ok:
            await reply_quiet(update, msg, reply_markup=cancel_keyboard(st["sid"]))
            return

        nu = st["newuser"]
        is_ok, prev_date = _validate_fifo_date(nu["ph_entries"], d)
        if not is_ok:
            await reply_quiet(
                update,
                f"❌ This PH date is earlier than the previous PH entry.\n\n"
                f"Previous PH date: {prev_date}\n"
                f"New PH date must be {prev_date} or later.\n\n"
                f"⚠️ FIFO approach requires oldest to newest order.",
                reply_markup=build_redo_section_keyboard(st["sid"], "ph"),
            )
            return

        idx = st.get("ph_idx", 0)
        nu["ph_entries"].append({"date": d, "reason": None})
        st["stage"] = "ph_reason"

        await reply_quiet(
            update,
            f"PH Entry {idx+1}/{nu['ph_count']} — Enter {bold('PH name')}:",
            parse_mode="Markdown",
            reply_markup=cancel_keyboard(st["sid"]),
        )
        return

    if st.get("stage") == "special_date_manual":
        d = parse_date_yyyy_mm_dd(text)
        if not d:
            await reply_quiet(update, "Invalid date. Please type YYYY-MM-DD.", reply_markup=cancel_keyboard(st["sid"]))
            return

        ok, msg = validate_application_date("newuser_ph", d)
        if not ok:
            await reply_quiet(update, msg, reply_markup=cancel_keyboard(st["sid"]))
            return

        nu = st["newuser"]
        is_ok, prev_date = _validate_fifo_date(nu["special_entries"], d)
        if not is_ok:
            await reply_quiet(
                update,
                f"❌ This Special date is earlier than the previous Special entry.\n\n"
                f"Previous Special date: {prev_date}\n"
                f"New Special date must be {prev_date} or later.\n\n"
                f"⚠️ FIFO approach requires oldest to newest order.",
                reply_markup=build_redo_section_keyboard(st["sid"], "special"),
            )
            return

        idx = st.get("special_idx", 0)
        nu["special_entries"].append({"date": d, "reason": None})
        st["stage"] = "special_reason"

        await reply_quiet(
            update,
            f"Special Entry {idx+1}/{nu['special_count']} — Enter {bold('Special name')}:",
            parse_mode="Markdown",
            reply_markup=cancel_keyboard(st["sid"]),
        )
        return
