from datetime import date, datetime

from bot.conversations import (
    apply_adjustoil_payload,
    apply_massadjust_payload,
    build_adjust_user_keyboard,
    build_admin_summary_text,
    handle_newuser_apply,
    handle_single_apply,
)
from bot.ui import (
    _is_group,
    build_calendar,
    build_calendar_with_recovery,
    cancel_keyboard,
    send_group_quiet,
    validate_application_date,
)
from services.runtime_state import pending_payloads, user_state


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
        return False, prev_date_str
    return True, ""


async def handle_callback(update, context):
    if not update.callback_query:
        return

    q = update.callback_query
    await q.answer()

    data = q.data or ""
    parts = data.split("|")
    if not parts:
        return

    kind = parts[0]
    sid = parts[1] if len(parts) > 1 else ""

    uid = q.from_user.id
    st = user_state.get(uid)

    def not_owner_block():
        return (not st) or (st.get("sid") != sid) or (st.get("owner_id") != uid)

    if kind == "cancel":
        if not_owner_block():
            await q.answer("This isn’t your session.", show_alert=True)
            return
        user_state.pop(uid, None)
        try:
            await q.edit_message_text("🧹 Cancelled.")
        except Exception:
            pass
        return

    if kind == "noop":
        return

    if kind in (
        "calnav",
        "manual",
        "cal",
        "adjtype",
        "adjuser",
        "adjconfirm",
        "massadjtype",
        "massadjconfirm",
        "redo_ph",
        "redo_special",
    ):
        if not_owner_block():
            await q.answer("This isn’t your session.", show_alert=True)
            return

    if kind == "redo_ph":
        nu = st.get("newuser", {})
        nu["ph_entries"] = []
        nu["ph_count"] = None
        st["ph_idx"] = 0
        st["stage"] = "ph_ask_count"
        try:
            await q.edit_message_text(
                "🔁 PH onboarding has been reset.\n\n"
                "Please key in PH entries again from *oldest date to newest date*.\n"
                "That means the *earliest expiry* should be entered first.\n\n"
                "How many PH entries do you want to add? (0–10)",
                parse_mode="Markdown",
                reply_markup=cancel_keyboard(sid),
            )
        except Exception:
            pass
        return

    if kind == "redo_special":
        nu = st.get("newuser", {})
        nu["special_entries"] = []
        nu["special_count"] = None
        st["special_idx"] = 0
        st["stage"] = "special_ask_count"
        try:
            await q.edit_message_text(
                "🔁 Special onboarding has been reset.\n\n"
                "Please key in Special entries again from *oldest date to newest date*.\n"
                "That means the *earliest expiry* should be entered first.\n\n"
                "How many Special entries do you want to add? (0–10)",
                parse_mode="Markdown",
                reply_markup=cancel_keyboard(sid),
            )
        except Exception:
            pass
        return

    if kind == "adjtype":
        oil_type = parts[2]
        st["oil_type"] = oil_type
        st["stage"] = "awaiting_target_user"

        try:
            await q.edit_message_text(
                f"🛠 Selected OIL type: {oil_type.title()}\n\nChoose the personnel to adjust:",
                reply_markup=build_adjust_user_keyboard(sid),
            )
        except Exception:
            pass
        return

    if kind == "adjuser":
        target_uid = parts[2]
        from bot.conversations import _extract_unique_users

        users = {u: name for u, name in _extract_unique_users()}
        st["target_user_id"] = str(target_uid)
        st["target_name"] = users.get(str(target_uid), str(target_uid))
        st["stage"] = "awaiting_amount"

        try:
            await q.edit_message_text(
                f"👤 Selected: {st['target_name']} ({st['target_user_id']})\n"
                f"🏷 OIL Type: {st['oil_type'].title()}\n\n"
                f"Enter adjustment amount.\n"
                f"Use positive to add, negative to subtract.\n"
                f"Examples: 1.0, -0.5",
                reply_markup=cancel_keyboard(sid),
            )
        except Exception:
            pass
        return

    if kind == "adjconfirm":
        payload = st.get("payload")
        if not payload:
            await q.answer("Nothing to confirm.", show_alert=True)
            return

        await apply_adjustoil_payload(context, payload)

        try:
            await q.edit_message_text(
                "✅ Adjustment applied successfully.\n\n"
                f"User: {payload['target_name']} ({payload['target_user_id']})\n"
                f"Type: {payload['oil_type'].title()}\n"
                f"Adjustment: {payload['amount']:+.1f}"
            )
        except Exception:
            pass

        user_state.pop(uid, None)
        return

    if kind == "massadjtype":
        oil_type = parts[2]
        st["oil_type"] = oil_type
        st["stage"] = "awaiting_amount"

        try:
            await q.edit_message_text(
                f"🛠 Selected OIL type: {oil_type.title()}\n\n"
                f"Enter adjustment amount.\n"
                f"Use positive to add, negative to subtract.\n"
                f"Examples: 1.0, -0.5",
                reply_markup=cancel_keyboard(sid),
            )
        except Exception:
            pass
        return

    if kind == "massadjconfirm":
        payload = st.get("payload")
        if not payload:
            await q.answer("Nothing to confirm.", show_alert=True)
            return

        adjusted, skipped = await apply_massadjust_payload(context, payload)

        lines = [
            "✅ Mass adjustment applied successfully.",
            "",
            f"Type: {payload['oil_type'].title()}",
            f"Adjustment: {payload['amount']:+.1f}",
            f"Adjusted users: {len(adjusted)}",
            f"Skipped users: {len(skipped)}",
        ]

        if skipped:
            preview = ", ".join(skipped[:10])
            if len(skipped) > 10:
                preview += ", ..."
            lines.append(f"Skipped: {preview}")

        try:
            await q.edit_message_text("\n".join(lines))
        except Exception:
            pass

        user_state.pop(uid, None)
        return

    if kind == "calnav":
        try:
            target = datetime.strptime(parts[2], "%Y-%m-%d").date()
        except Exception:
            target = date.today()

        min_d = st.get("min_date")
        max_d = st.get("max_date")
        await q.edit_message_reply_markup(reply_markup=build_calendar(sid, target, min_d, max_d))
        return

    if kind == "manual":
        if st["flow"] in ("normal", "ph", "special", "dos") and st["stage"] == "awaiting_app_date":
            st["stage"] = "awaiting_app_date_manual"
            await q.edit_message_text("⌨️ Type the application date as YYYY-MM-DD.", reply_markup=cancel_keyboard(sid))
            return

        if st["flow"] == "newuser" and st["stage"] == "ph_date":
            st["stage"] = "ph_date_manual"
            await q.edit_message_text(
                "⌨️ Type the PH application date as YYYY-MM-DD.\n\n"
                "⚠️ FIFO approach: date must not be earlier than the previous PH entry.",
                reply_markup=cancel_keyboard(sid),
            )
            return

        if st["flow"] == "newuser" and st["stage"] == "special_date":
            st["stage"] = "special_date_manual"
            await q.edit_message_text(
                "⌨️ Type the Special application date as YYYY-MM-DD.\n\n"
                "⚠️ FIFO approach: date must not be earlier than the previous Special entry.",
                reply_markup=cancel_keyboard(sid),
            )
            return

        return

    if kind == "cal":
        chosen = parts[2]

        if st["flow"] in ("normal", "ph", "special", "dos") and st["stage"] == "awaiting_app_date":
            ok, msg = validate_application_date(st.get("action", ""), chosen)
            if not ok:
                await q.answer(msg, show_alert=True)
                return

            st["app_date"] = chosen
            try:
                await q.edit_message_text(f"📅 Application Date: {chosen}")
            except Exception:
                pass

            st["stage"] = "awaiting_reason"

            if st.get("action") == "clockoff":
                prompt = "📝 Enter clocking reason."
            elif st.get("action") == "clockphoff":
                prompt = "📝 Enter PH name."
            elif st.get("action") == "clockspecialoff":
                prompt = "📝 Enter Special Off name."
            elif st.get("action") == "clockdos":
                from bot.conversations import _dos_kind_and_points
                dos_kind, dos_points = _dos_kind_and_points(chosen)
                st["dos_kind"] = dos_kind
                st["dos_points"] = dos_points
                prompt = (
                    f"🪖 DOS detected: {dos_kind} = {dos_points} point(s)\n\n"
                    f"📝 Enter remarks (optional). Type 'nil' to skip."
                )
            else:
                prompt = "📝 Enter remarks (optional). Type 'nil' to skip."

            if update.effective_chat and _is_group(update.effective_chat.type):
                await send_group_quiet(
                    context,
                    q.message.chat.id,
                    prompt,
                    reply_markup=cancel_keyboard(st["sid"]),
                )
            else:
                await context.bot.send_message(
                    chat_id=q.message.chat.id,
                    text=prompt,
                    reply_markup=cancel_keyboard(st["sid"]),
                )
            return

        if st["flow"] == "newuser" and st["stage"] in ("ph_date", "special_date"):
            ok, msg = validate_application_date("newuser_ph", chosen)
            if not ok:
                await q.answer(msg, show_alert=True)
                return

            nu = st["newuser"]

            if st["stage"] == "ph_date":
                is_ok, prev_date = _validate_fifo_date(nu["ph_entries"], chosen)
                if not is_ok:
                    recovery_markup = build_calendar_with_recovery(
                        sid,
                        datetime.strptime(chosen, "%Y-%m-%d").date(),
                        st.get("min_date"),
                        st.get("max_date"),
                        "ph",
                    )

                    await send_group_quiet(
                        context,
                        q.message.chat.id,
                        f"❌ This PH date is earlier than the previous PH entry.\n\n"
                        f"Previous PH date: {prev_date}\n"
                        f"New PH date must be {prev_date} or later.\n\n"
                        f"Please select a valid later date, or choose an option below.",
                        reply_markup=recovery_markup,
                    )
                    return

                idx = st["ph_idx"]
                nu["ph_entries"].append({"date": chosen, "reason": None})

                try:
                    await q.edit_message_text(f"📅 PH Entry {idx+1}/{nu['ph_count']} — Date: {chosen}")
                except Exception:
                    pass

                st["stage"] = "ph_reason"
                await send_group_quiet(
                    context,
                    q.message.chat.id,
                    f"PH Entry {idx+1}/{nu['ph_count']} — Enter *PH name*:",
                    parse_mode="Markdown",
                    reply_markup=cancel_keyboard(sid),
                )
                return

            if st["stage"] == "special_date":
                is_ok, prev_date = _validate_fifo_date(nu["special_entries"], chosen)
                if not is_ok:
                    recovery_markup = build_calendar_with_recovery(
                        sid,
                        datetime.strptime(chosen, "%Y-%m-%d").date(),
                        st.get("min_date"),
                        st.get("max_date"),
                        "special",
                    )

                    await send_group_quiet(
                        context,
                        q.message.chat.id,
                        f"❌ This Special date is earlier than the previous Special entry.\n\n"
                        f"Previous Special date: {prev_date}\n"
                        f"New Special date must be {prev_date} or later.\n\n"
                        f"Please select a valid later date, or choose an option below.",
                        reply_markup=recovery_markup,
                    )
                    return

                idx = st["special_idx"]
                nu["special_entries"].append({"date": chosen, "reason": None})

                try:
                    await q.edit_message_text(f"📅 Special Entry {idx+1}/{nu['special_count']} — Date: {chosen}")
                except Exception:
                    pass

                st["stage"] = "special_reason"
                await send_group_quiet(
                    context,
                    q.message.chat.id,
                    f"Special Entry {idx+1}/{nu['special_count']} — Enter *Special name*:",
                    parse_mode="Markdown",
                    reply_markup=cancel_keyboard(sid),
                )
                return

    if kind in ("approve", "deny"):
        key = parts[1] if len(parts) > 1 else ""
        payload = pending_payloads.pop(key, None)
        approver = q.from_user.full_name
        approver_id = q.from_user.id
        approved = kind == "approve"

        if not payload:
            try:
                await q.edit_message_text("⚠️ This request has already been handled.")
            except Exception:
                pass
            return

        if payload.get("type") == "newuser":
            await handle_newuser_apply(update, context, payload, approved, approver, approver_id)
            summary = build_admin_summary_text(
                payload,
                approved=approved,
                approver_name=approver,
                final_off=None,
            )
            try:
                await q.edit_message_text(summary)
            except Exception:
                pass
            return

        if payload.get("type") == "single":
            await handle_single_apply(update, context, payload, approved, approver, approver_id)
            summary = build_admin_summary_text(
                payload,
                approved=approved,
                approver_name=approver,
                final_off=None,
            )
            try:
                await q.edit_message_text(summary)
            except Exception:
                pass
            return
