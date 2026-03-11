import logging
from services.ledger import compute_user_summary
from services.sheets_repo import get_all_rows
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
    if action == "clockoff":
        return "Clock Off"
    if action == "claimoff":
        return "Claim Off"
    if action == "clockphoff":
        return "Clock PH Off"
    if action == "claimphoff":
        return "Claim PH Off"
    if action == "clockspecialoff":
        return "Clock Special Off"
    if action == "claimspecialoff":
        return "Claim Special Off"
    return action


def build_admin_summary_text(payload: dict, approved: bool, approver_name: str, final_off: float | None) -> str:
    status = "✅ Approved" if approved else "❌ Denied"

    if payload["type"] == "single":
        lines = [
            status,
            f"{_label_from_action(payload['action'])} — {payload['user_name']} ({payload['user_id']})",
            f"Days: {payload['days']} | Date: {payload['app_date']}",
            f"Reason: {payload.get('reason', '') or '—'}",
        ]
        if (payload.get("is_ph") or payload.get("is_special")) and payload.get("expiry"):
            lines.append(f"Expiry: {payload['expiry']}")
        if final_off is not None and approved:
            lines.append(f"Final Off: {final_off:.1f}")
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
    }

    try:
        admins = await context.bot.get_chat_administrators(group_id)
    except Exception:
        admins = []

    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Approve", callback_data=f"approve|{key}"),
        InlineKeyboardButton("❌ Deny", callback_data=f"deny|{key}"),
    ]])

    label = _label_from_action(st["action"])
    text = (
        f"🆕 *{label} Request*\n\n"
        f"👤 User: {user.full_name} ({uid})\n"
        f"📅 Days: {days}\n"
        f"🗓 Application Date: {app_date}\n"
        f"📝 Reason: {st.get('reason', '') or '—'}\n\n"
        f"📊 Current Off: {current_off:.1f}\n"
        f"📈 New Balance: {final:.1f}"
    )

    if is_ph and expiry:
        text += f"\n🏖 PH Expiry: {expiry}"
        if ph_total_after is not None:
            text += f"\n🏖 PH Total After: {ph_total_after:.1f}"

    if is_special and expiry:
        text += f"\n⭐ Special Expiry: {expiry}"
        if special_total_after is not None:
            text += f"\n⭐ Special Total After: {special_total_after:.1f}"

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
        action=_label_from_action(action),
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
        st["max_date"] = date.today() + (timedelta(days=365) if st["action"] in ("claimoff", "claimphoff", "claimspecialoff") else timedelta(days=0))

        await reply_quiet(
            update,
            f"{bold('📅 Select Application Date:')}\n"
            f"Allowed date range: {st['min_date']} to {st['max_date']}",
            parse_mode="Markdown",
            reply_markup=build_calendar(st["sid"], date.today(), st["min_date"], st["max_date"]),
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
