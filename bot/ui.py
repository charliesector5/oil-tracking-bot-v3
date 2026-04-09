from datetime import date, datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

SG_TZ = ZoneInfo("Asia/Singapore")


def sg_now() -> datetime:
    return datetime.now(SG_TZ)


def sg_today() -> date:
    return sg_now().date()


def cancel_keyboard(session_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("❌ Cancel", callback_data=f"cancel|{session_id}")]]
    )


def bold(text: str) -> str:
    return f"*{text}*"


def _is_group(chat_type: str) -> bool:
    return chat_type in ("group", "supergroup")


async def reply_quiet(update: Update, text: str, **kwargs):
    if update.effective_chat and _is_group(update.effective_chat.type):
        kwargs.setdefault("disable_notification", True)
    return await update.message.reply_text(text, **kwargs)


async def send_group_quiet(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str, **kwargs):
    kwargs.setdefault("disable_notification", True)
    return await context.bot.send_message(chat_id=chat_id, text=text, **kwargs)


def month_start(d: date) -> date:
    return date(d.year, d.month, 1)


def month_add(d: date, delta_months: int) -> date:
    y = d.year + (d.month - 1 + delta_months) // 12
    m = (d.month - 1 + delta_months) % 12 + 1
    return date(y, m, 1)


def build_calendar(
    session_id: str,
    cur: date,
    min_date: Optional[date] = None,
    max_date: Optional[date] = None,
) -> InlineKeyboardMarkup:
    header = [
        InlineKeyboardButton(
            f"📅 {cur.strftime('%B %Y')}",
            callback_data=f"noop|{session_id}",
        )
    ]
    weekdays = ["Su", "Mo", "Tu", "We", "Th", "Fr", "Sa"]
    week_hdr = [InlineKeyboardButton(d, callback_data=f"noop|{session_id}") for d in weekdays]

    first = month_start(cur)
    start_wd = first.weekday()
    start_offset = (start_wd + 1) % 7
    next_m = month_add(first, 1)
    days_in_month = (next_m - first).days

    rows = []
    row = []
    for _ in range(start_offset):
        row.append(InlineKeyboardButton(" ", callback_data=f"noop|{session_id}"))

    day_num = 1
    while day_num <= days_in_month:
        while len(row) < 7 and day_num <= days_in_month:
            d = date(cur.year, cur.month, day_num)
            in_range = True
            if min_date and d < min_date:
                in_range = False
            if max_date and d > max_date:
                in_range = False

            if in_range:
                row.append(
                    InlineKeyboardButton(
                        str(day_num),
                        callback_data=f"cal|{session_id}|{d.strftime('%Y-%m-%d')}",
                    )
                )
            else:
                row.append(InlineKeyboardButton("·", callback_data=f"noop|{session_id}"))

            day_num += 1

        while len(row) < 7:
            row.append(InlineKeyboardButton(" ", callback_data=f"noop|{session_id}"))
        rows.append(row)
        row = []

    prev_month = month_add(first, -1)
    next_month = month_add(first, 1)
    allow_prev = (min_date is None) or (prev_month >= date(min_date.year, min_date.month, 1))
    allow_next = (max_date is None) or (next_month <= date(max_date.year, max_date.month, 1))

    nav = [
        InlineKeyboardButton(
            "« Prev",
            callback_data=(
                f"calnav|{session_id}|{prev_month.strftime('%Y-%m-01')}"
                if allow_prev
                else f"noop|{session_id}"
            ),
        ),
        InlineKeyboardButton("Manual entry", callback_data=f"manual|{session_id}"),
        InlineKeyboardButton(
            "Next »",
            callback_data=(
                f"calnav|{session_id}|{next_month.strftime('%Y-%m-01')}"
                if allow_next
                else f"noop|{session_id}"
            ),
        ),
    ]
    cancel = [InlineKeyboardButton("❌ Cancel", callback_data=f"cancel|{session_id}")]

    return InlineKeyboardMarkup([header, week_hdr] + rows + [nav, cancel])


def build_calendar_with_recovery(
    sid: str,
    current_date: date,
    min_date: Optional[date],
    max_date: Optional[date],
    section: str,
) -> InlineKeyboardMarkup:
    kb = build_calendar(sid, current_date, min_date, max_date)

    if section == "ph":
        redo_btn = InlineKeyboardButton("🔁 Redo PH Off", callback_data=f"redo_ph|{sid}")
    else:
        redo_btn = InlineKeyboardButton("🔁 Redo Special Off", callback_data=f"redo_special|{sid}")

    cancel_btn = InlineKeyboardButton("❌ Cancel", callback_data=f"cancel|{sid}")

    rows = [list(row) for row in kb.inline_keyboard]

    if rows:
        rows[-1] = [redo_btn, cancel_btn]
    else:
        rows.append([redo_btn, cancel_btn])

    return InlineKeyboardMarkup(rows)


def validate_half_step(value: float) -> bool:
    return abs((value * 10) % 5) < 1e-9


def parse_date_yyyy_mm_dd(value: str) -> Optional[str]:
    try:
        d = datetime.strptime(value.strip(), "%Y-%m-%d").date()
        return d.strftime("%Y-%m-%d")
    except Exception:
        return None


def validate_application_date(action: str, dstr: str) -> tuple[bool, str]:
    try:
        d = datetime.strptime(dstr, "%Y-%m-%d").date()
    except Exception:
        return False, "Invalid date. Use YYYY-MM-DD."

    today = sg_today()
    past_365 = today - timedelta(days=365)

    if action in ("clockoff", "clockphoff", "clockspecialoff", "clockdos", "mass", "newuser_ph"):
        if d < past_365 or d > today:
            return False, f"Date must be between {past_365} and {today}."
        return True, ""

    if action in ("claimoff", "claimphoff", "claimspecialoff"):
        future_365 = today + timedelta(days=365)
        if d < past_365 or d > future_365:
            return False, f"Date must be between {past_365} and {future_365}."
        return True, ""

    return True, ""
