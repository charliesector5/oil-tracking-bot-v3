import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional
from uuid import uuid4

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
from services.ledger import compute_user_summary
from services.runtime_state import pending_payloads, user_state
from services.sheets_repo import (
    append_row,
    compute_ph_entries_active,
    compute_special_entries_breakdown,
    get_all_rows,
    last_off_for_user,
)

log = logging.getLogger(__name__)


def _label_from_action(action: str) -> str:
    if "claim" in action:
        return "Claim Off"
    if "clock" in action:
        return "Clock Off"
    return action


def _off_type_label(action: str, is_ph: bool = False, is_special: bool = False) -> str:
    if is_special or action in ("clockspecialoff", "claimspecialoff"):
        return "Special"
    if is_ph or action in ("clockphoff", "claimphoff"):
        return "PH"
    return "Normal"


def _sheet_action_label(action: str) -> str:
    return "Claim Off" if "claim" in action else "Clock Off"


def _sheet_action_from_amount(amount: float) -> str:
    return "Clock Off" if amount >= 0 else "Claim Off"


def _extract_unique_users():
    rows = get_all_rows()
    seen = set()
    users = []
    for r in rows[1:]:
        if len(r) < 3:
            continue
        uid = str(r[1]).strip()
        name = str(r[2]).strip() or uid
        if not uid or uid in seen:
            continue
        seen.add(uid)
        users.append((uid, name))
    users.sort(key=lambda x: x[1].lower())
    return users


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


async def _is_admin_in_chat(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> bool:
    try:
        admins = await context.bot.get_chat_administrators(chat_id)
        return any(a.user.id == user_id for a in admins)
    except Exception:
        return False


def _adjust_type_flags(kind: str):
    is_ph = kind == "ph"
    is_special = kind == "special"
    return is_ph, is_special


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
        f"📘 Ledger Row: {payload['current_total']:.1f} {operator} {abs_amount:.1f} = {payload['projected_total']:.1f}",
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


async def apply_adjustoil_payload(context: ContextTypes.DEFAULT_TYPE, payload: dict):
    uid = payload["target_user_id"]
    uname = payload["target_name"]
    amount = float(payload["amount"])
    approver_name = payload["admin_name"]
    app_date = payload["application_date"]
    remarks = payload["remarks"]
    is_ph = payload["is_ph"]
    is_special = payload["is_special"]
    expiry = payload.get("expiry", "")

    current_off = last_off_for_user(uid)
    final_off = current_off + amount

    ph_total = 0.0
    special_total = 0.0

    if is_ph:
        before, _ = compute_ph_entries_active(uid)
        ph_total = before + amount

    if is_special:
        before, _active_special, _expired_special = compute_special_entries_breakdown(uid)
        special_total = before + amount

    append_row(
        user_id=uid,
        user_name=uname,
        action=_sheet_action_from_amount(amount),
        current_off=current_off,
        add_subtract=amount,
        final_off=final_off,
        approved_by=approver_name,
        application_date=app_date,
        remarks=remarks,
        is_ph=is_ph,
        ph_total=ph_total,
        expiry=expiry,
        is_special=is_special,
        special_total=special_total,
    )


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

        current_off = last_off_for_user(uid)
        final_off = current_off + amount

        ph_total = 0.0
        special_total = 0.0

        if is_ph:
            before, _ = compute_ph_entries_active(uid)
            ph_total = before + amount

        if is_special:
            before, _active_special, _expired_special = compute_special_entries_breakdown(uid)
            special_total = before + amount

        append_row(
            user_id=uid,
            user_name=uname,
            action=_sheet_action_from_amount(amount),
            current_off=current_off,
            add_subtract=amount,
            final_off=final_off,
            approved_by=approver_name,
            application_date=app_date,
            remarks=remarks,
            is_ph=is_ph,
            ph_total=ph_total,
            expiry=expiry,
            is_special=is_special,
            special_total=special_total,
        )
        adjusted.append(uname)

    return adjusted, skipped


def build_admin_summary_text(payload: dict, approved: bool, approver_name: str, final_off: float | None) -> str:
    status = "✅ Approved" if approved else "❌ Denied"

    if payload["type"] == "single":
        off_type = _off_type_label(
            payload.get("action", ""),
            payload.get("is_ph", False),
            payload.get("is_special", False),
        )

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
            "",
            "Balances After",
            f"- Total: {payload.get('projected_total', 0.0):.1f}",
            f"- Normal: {payload.get('projected_normal', 0.0):.1f}",
            f"- PH: {payload.get('projected_ph', 0.0):.1f}",
            f"- Special: {payload.get('projected_special', 0.0):.1f}",
        ]

        if (payload.get("is_ph") or payload.get("is_special")) and payload.get("expiry"):
            lines.append(f"Expiry: {payload['expiry']}")

        if payload.get("warn_negative_normal"):
            lines.extend([
                "",
                "⚠️ Warning",
                f"Normal OIL will go negative: {payload.get('projected_normal', 0.0):.1f}",
            ])

        if final_off is not None and approved:
            lines.append("")
            lines.append(f"Final Off Row Value: {final_off:.1f}")

        lines.append(f"Approved by: {approver_name}")
        return "\n".join(lines)

    if payload["type"] == "newuser":
        return "\n".join([
            status,
            f"Onboarding — {payload['user_name']} ({payload['user_id']})",
            f"Normal OIL: {payload.get('normal_days', 0)}",
            f"PH entries: {len(payload.get('ph_entries', []))}",
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


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    rows = get_all_rows()
    urows = [r for r in rows if len(r) > 1 and r[1] == uid]
    if not urows:
        await reply_quiet(update, "📜 No logs found.")
        return

    last5 = urows[-5:]
    out = []
    for r in last5:
        ts = r[0] if len(r) > 0 else ""
        action = r[3] if len(r) > 3 else ""
        delta = r[5] if len(r) > 5 else ""
        final = r[6] if len(r) > 6 else ""
        remarks = r[9] if len(r) > 9 else ""
        out.append(f"{ts} | {action} | {delta} → {final} | {remarks}")

    await reply_quiet(update, "📜 Your last 5 OIL logs:\n\n" + "\n".join(out))


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
            InlineKeyboardButton("Special", callback_data=f"adjtype|{sid}|special"),
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
    exists = any(len(r) > 1 and r[1] == str(uid) for r in rows)
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
        },
        "owner_id": uid,
    }

    await reply_quiet(
        update,
        "🆕 *Onboarding: Import Old Records*\n\n"
        "1) How many *normal OIL* days to import? (e.g. 7.5 or 0 if none)",
        parse_mode="Markdown",
        reply_markup=cancel_keyboard(sid),
    )


async def finalize_single_request(update: Update, context: ContextTypes.DEFAULT_TYPE, st: Dict[str, Any], app_date: str):
    uid = update.effective_user.id
    user = update.effective_user
    group_id = st.get("group_id") or (update.effective_chat.id if update.effective_chat else None)

    days = float(st["days"])
    current_off = last_off_for_user(str(uid))
    add = days if st["action"] in ("clockoff", "clockphoff", "clockspecialoff") else -days
    final = current_off + add
    is_ph = st["is_ph"]
    is_special = st["action"] in ("clockspecialoff", "claimspecialoff")

    summary = compute_user_summary(str(uid), get_all_rows)
    current_normal = summary.normal_balance
    current_ph = summary.ph_active
    current_special = summary.special_active
    current_total = summary.total_balance

    projected_normal = current_normal
    projected_ph = current_ph
    projected_special = current_special

    if st["action"] == "claimoff":
        projected_normal = current_normal - days
    elif st["action"] == "claimphoff":
        projected_ph = current_ph - days
    elif st["action"] == "claimspecialoff":
        projected_special = current_special - days
    elif st["action"] == "clockoff":
        projected_normal = current_normal + days
    elif st["action"] == "clockphoff":
        projected_ph = current_ph + days
    elif st["action"] == "clockspecialoff":
        projected_special = current_special + days

    # Hard stop for PH / Special negative claims
    if st["action"] == "claimphoff" and days > current_ph:
        await reply_quiet(
            update,
            f"❌ You only have {current_ph:.1f} active PH OIL available.\n"
            f"Requested claim: {days:.1f}",
        )
        return

    if st["action"] == "claimspecialoff" and days > current_special:
        await reply_quiet(
            update,
            f"❌ You only have {current_special:.1f} active Special OIL available.\n"
            f"Requested claim: {days:.1f}",
        )
        return

    ok, msg = validate_application_date(st["action"], app_date)
    if not ok:
        await reply_quiet(update, msg)
        return

    expiry = ""
    ph_total_after = None
    special_total_after = None

    if is_ph:
        if st["action"] == "clockphoff":
            try:
                d = datetime.strptime(app_date, "%Y-%m-%d").date()
                expiry = (d + timedelta(days=365)).strftime("%Y-%m-%d")
            except Exception:
                expiry = ""
        before, _ = compute_ph_entries_active(str(uid))
        ph_total_after = before + (days if st["action"] == "clockphoff" else -days)

    if is_special:
        if st["action"] == "clockspecialoff":
            try:
                d = datetime.strptime(app_date, "%Y-%m-%d").date()
                expiry = (d + timedelta(days=365)).strftime("%Y-%m-%d")
            except Exception:
                expiry = ""
        before, _active_special, _expired_special = compute_special_entries_breakdown(str(uid))
        special_total_after = before + (days if st["action"] == "clockspecialoff" else -days)

    key = str(uuid4())[:12]
    payload = {
        "type": "single",
        "user_id": str(uid),
        "user_name": user.full_name,
        "group_id": group_id,
        "action": st["action"],
        "days": days,
        "reason": st.get("reason", ""),
        "app_date": app_date,
        "current_off": current_off,
        "final_off": final,
        "is_ph": is_ph,
        "is_special": is_special,
        "expiry": expiry,
        "ph_total_after": ph_total_after,
        "special_total_after": special_total_after,
        "admin_msgs": [],

        # split balances for admin preview / approval summary
        "current_total": current_total,
        "current_normal": current_normal,
        "current_ph": current_ph,
        "current_special": current_special,
        "projected_total": projected_normal + projected_ph + projected_special,
        "projected_normal": projected_normal,
        "projected_ph": projected_ph,
        "projected_special": projected_special,
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

    off_type = _off_type_label(st["action"], is_ph, is_special)
    display_action = _label_from_action(st["action"])

    text_lines = [
        f"🆕 *{display_action} Request [{off_type}]*",
        "",
        f"👤 User: {user.full_name} ({uid})",
        f"📅 Days: {days:.1f}",
        f"🗓 Application Date: {app_date}",
        f"📝 Reason: {st.get('reason', '') or '—'}",
        "",
        "*Balances Before*",
        f"- Total: {current_total:.1f}",
        f"- Normal: {current_normal:.1f}",
        f"- PH: {current_ph:.1f}",
        f"- Special: {current_special:.1f}",
        "",
        "*Balances After*",
        f"- Total: {(projected_normal + projected_ph + projected_special):.1f}",
        f"- Normal: {projected_normal:.1f}",
        f"- PH: {projected_ph:.1f}",
        f"- Special: {projected_special:.1f}",
    ]

    if is_ph and expiry:
        text_lines.append(f"🏖 PH Expiry: {expiry}")

    if is_special and expiry:
        text_lines.append(f"⭐ Special Expiry: {expiry}")

    if projected_normal < 0 and st["action"] == "claimoff":
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

    key = str(uuid4())[:12]
    payload = {
        "type": "newuser",
        "group_id": gid,
        "user_id": str(uid),
        "user_name": uname,
        "normal_days": float(nu["normal_days"] or 0.0),
        "ph_entries": nu["ph_entries"],
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
    days = float(payload["days"])
    app_date = payload["app_date"]
    reason = payload.get("reason", "")
    is_ph = bool(payload.get("is_ph"))
    is_special = bool(payload.get("is_special"))

    current = last_off_for_user(uid)
    add = days if "clock" in action else -days
    final = current + add

    expiry = payload.get("expiry", "")
    ph_total = 0.0
    special_total = 0.0

    if is_ph:
        before, _ = compute_ph_entries_active(uid)
        ph_total = before + (days if action == "clockphoff" else -days)

    if is_special:
        before, _active_special, _expired_special = compute_special_entries_breakdown(uid)
        special_total = before + (days if action == "clockspecialoff" else -days)

    append_row(
        user_id=uid,
        user_name=uname,
        action=_sheet_action_label(action),
        current_off=current,
        add_subtract=add,
        final_off=final,
        approved_by=approver_name,
        application_date=app_date,
        remarks=reason,
        is_ph=is_ph,
        ph_total=ph_total,
        expiry=expiry,
        is_special=is_special,
        special_total=special_total,
    )

    await send_group_quiet(context, gid, f"✅ Request for {uname} approved by {approver_name}.")
    summary = build_admin_summary_text(payload, approved=True, approver_name=approver_name, final_off=final)
    await update_all_admin_pm(context, payload, summary)


async def handle_newuser_apply(update: Update, context: ContextTypes.DEFAULT_TYPE, payload: Dict[str, Any], approved: bool, approver_name: str, approver_id: int):
    gid = payload["group_id"]
    uid = payload["user_id"]
    uname = payload["user_name"]
    normal_days = float(payload.get("normal_days", 0.0))
    ph_entries = payload.get("ph_entries", [])

    if not approved:
        await send_group_quiet(context, gid, f"❌ Onboarding import for {uname} denied by {approver_name}.")
        summary = build_admin_summary_text(payload, approved=False, approver_name=approver_name, final_off=None)
        await update_all_admin_pm(context, payload, summary)
        return

    if normal_days > 0:
        current = last_off_for_user(uid)
        add = normal_days
        final = current + add
        append_row(
            user_id=uid,
            user_name=uname,
            action="Clock Off",
            current_off=current,
            add_subtract=add,
            final_off=final,
            approved_by=approver_name,
            application_date=date.today().strftime("%Y-%m-%d"),
            remarks="Transfer from old record",
            is_ph=False,
            ph_total=0.0,
            expiry="",
        )

    for entry in ph_entries:
        dstr = entry.get("date")
        reason = entry.get("reason", "")
        if not dstr:
            continue

        current = last_off_for_user(uid)
        add = 1.0
        final = current + add

        expiry = ""
        try:
            d = datetime.strptime(dstr, "%Y-%m-%d").date()
            expiry = (d + timedelta(days=365)).strftime("%Y-%m-%d")
        except Exception:
            pass

        before, _ = compute_ph_entries_active(uid)
        ph_after = before + 1.0

        append_row(
            user_id=uid,
            user_name=uname,
            action="Clock Off",
            current_off=current,
            add_subtract=add,
            final_off=final,
            approved_by=approver_name,
            application_date=dstr,
            remarks=reason,
            is_ph=True,
            ph_total=ph_after,
            expiry=expiry,
        )

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
        st["min_date"] = date.today() - timedelta(days=365)
        st["max_date"] = date.today() + (
            timedelta(days=365) if st["action"] in ("claimoff", "claimphoff", "claimspecialoff") else timedelta(days=0)
        )

        await reply_quiet(
            update,
            f"{bold('📅 Select Application Date:')}\n"
            f"Allowed date range: {st['min_date']} to {st['max_date']}",
            parse_mode="Markdown",
            reply_markup=build_calendar(st["sid"], date.today(), st["min_date"], st["max_date"]),
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
        is_ph, is_special = _adjust_type_flags(oil_type)

        summary = compute_user_summary(str(target_uid), get_all_rows)
        current_total = summary.total_balance
        current_normal = summary.normal_balance
        current_ph = summary.ph_active
        current_special = summary.special_active

        projected_normal = current_normal
        projected_ph = current_ph
        projected_special = current_special

        if oil_type == "normal":
            projected_normal += amount
        elif oil_type == "ph":
            projected_ph += amount
        elif oil_type == "special":
            projected_special += amount

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

        app_date = date.today().strftime("%Y-%m-%d")
        expiry = ""
        if amount > 0 and oil_type in ("ph", "special"):
            expiry = (date.today() + timedelta(days=365)).strftime("%Y-%m-%d")

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
            "remarks": f"Admin adjustment by {st['admin_name']}: {reason[:120]}",
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

        app_date = date.today().strftime("%Y-%m-%d")
        expiry = ""
        if amount > 0 and oil_type in ("ph", "special"):
            expiry = (date.today() + timedelta(days=365)).strftime("%Y-%m-%d")

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
            "remarks": f"Mass adjustment by {st['admin_name']}: {reason[:120]}",
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

    if st["flow"] in ("normal", "ph", "special") and st["stage"] == "awaiting_reason":
        txt = text.strip()
        action = st.get("action", "")
        optional = action in ("claimoff", "claimphoff", "claimspecialoff")
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
                "How many PH entries do you want to add? (0–10)",
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
                await newuser_review(update, context, st)
                return

            st["ph_idx"] = 0
            st["stage"] = "ph_date"
            st["min_date"] = date.today() - timedelta(days=365)
            st["max_date"] = date.today()

            await reply_quiet(
                update,
                f"PH Entry 1/{nu['ph_count']} — {bold('Select Application Date')}",
                parse_mode="Markdown",
                reply_markup=build_calendar(st["sid"], date.today(), st["min_date"], st["max_date"]),
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
                st["min_date"] = date.today() - timedelta(days=365)
                st["max_date"] = date.today()

                await reply_quiet(
                    update,
                    f"PH Entry {idx+1}/{nu['ph_count']} — {bold('Select Application Date')}",
                    parse_mode="Markdown",
                    reply_markup=build_calendar(st["sid"], date.today(), st["min_date"], st["max_date"]),
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
        idx = st.get("ph_idx", 0)
        nu["ph_entries"].append({"date": d, "reason": None})
        st["stage"] = "ph_reason"

        await reply_quiet(
            update,
            f"PH Entry {idx+1}/{nu['ph_count']} — Enter {bold('PH name')}:",
            parse_mode="Markdown",
            reply_markup=cancel_keyboard(st["sid"]),
        )
