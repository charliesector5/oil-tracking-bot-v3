import logging
from datetime import datetime
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from config import settings

log = logging.getLogger(__name__)

_SPREADSHEET = None
_LEDGER_WS = None
_BALANCES_WS = None

LEDGER_TAB_NAME = "ledger"
BALANCES_TAB_NAME = "balances"

SG_TZ = ZoneInfo("Asia/Singapore")


def sg_now() -> datetime:
    return datetime.now(SG_TZ)


LEDGER_HEADERS = [
    "Timestamp",
    "Telegram ID",
    "Name",
    "Action Type",
    "Off Type",
    "Amount",
    "Application Date",
    "Expiry Date",
    "Remarks",
    "Approved By",
    "Source",
]

BALANCE_HEADERS = [
    "Telegram ID",
    "Name",
    "Normal Off",
    "Active PH Off",
    "Expired PH Off",
    "Active Special Off",
    "Expired Special Off",
    "DOS Points",
    "Available Total",
    "Last Updated",
]


def init_gsheet():
    global _SPREADSHEET, _LEDGER_WS, _BALANCES_WS

    log.info("🔐 Connecting to Google Sheets...")
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        settings.google_credentials_path,
        scope,
    )
    client = gspread.authorize(creds)

    _SPREADSHEET = client.open_by_key(settings.google_sheet_id)
    _LEDGER_WS = _SPREADSHEET.worksheet(LEDGER_TAB_NAME)
    _BALANCES_WS = _SPREADSHEET.worksheet(BALANCES_TAB_NAME)

    _ensure_headers()
    log.info("✅ Google Sheets ready. Tabs: ledger, balances")


def _ensure_headers():
    if _LEDGER_WS is None or _BALANCES_WS is None:
        raise RuntimeError("Google Sheets not initialised yet.")

    try:
        ledger_row_1 = _LEDGER_WS.row_values(1)
        if ledger_row_1 != LEDGER_HEADERS:
            _LEDGER_WS.update("A1:K1", [LEDGER_HEADERS])
    except Exception:
        log.exception("Failed to ensure ledger headers")
        raise

    try:
        balances_row_1 = _BALANCES_WS.row_values(1)
        if balances_row_1 != BALANCE_HEADERS:
            _BALANCES_WS.update("A1:J1", [BALANCE_HEADERS])
    except Exception:
        log.exception("Failed to ensure balances headers")
        raise


def get_spreadsheet():
    if _SPREADSHEET is None:
        raise RuntimeError("Google Sheet is not initialised yet.")
    return _SPREADSHEET


def get_ledger_worksheet():
    if _LEDGER_WS is None:
        raise RuntimeError("Ledger worksheet is not initialised yet.")
    return _LEDGER_WS


def get_balances_worksheet():
    if _BALANCES_WS is None:
        raise RuntimeError("Balances worksheet is not initialised yet.")
    return _BALANCES_WS


def try_get_worksheet_title() -> Optional[str]:
    try:
        return get_ledger_worksheet().title
    except Exception:
        return None


def get_all_rows() -> List[List[str]]:
    """
    Compatibility helper.
    For V3, this returns all ledger rows.
    """
    try:
        return get_ledger_worksheet().get_all_values()
    except Exception:
        log.exception("Failed to read ledger rows")
        return []


def get_all_ledger_rows() -> List[List[str]]:
    try:
        return get_ledger_worksheet().get_all_values()
    except Exception:
        log.exception("Failed to read ledger rows")
        return []


def get_all_balance_rows() -> List[List[str]]:
    try:
        return get_balances_worksheet().get_all_values()
    except Exception:
        log.exception("Failed to read balance rows")
        return []


def get_row_count() -> int:
    return len(get_all_ledger_rows())


def get_header_row() -> List[str]:
    rows = get_all_ledger_rows()
    return rows[0] if rows else []


def healthcheck() -> tuple[bool, str]:
    try:
        ledger_ws = get_ledger_worksheet()
        balances_ws = get_balances_worksheet()
        ledger_rows = len(get_all_ledger_rows())
        balance_rows = len(get_all_balance_rows())
        return True, (
            f"Sheet OK | ledger={ledger_ws.title} ({ledger_rows} rows) | "
            f"balances={balances_ws.title} ({balance_rows} rows)"
        )
    except Exception as exc:
        log.exception("Google Sheet healthcheck failed")
        return False, f"Sheet error: {exc}"


def append_ledger_row(
    telegram_id: str,
    name: str,
    action_type: str,
    off_type: str,
    amount: float,
    application_date: str,
    expiry_date: Optional[str],
    remarks: str,
    approved_by: str,
    source: str,
) -> None:
    now = sg_now().strftime("%Y-%m-%d %H:%M:%S")
    row = [
        now,
        str(telegram_id),
        name or "",
        action_type,
        off_type,
        f"{float(amount):+.2f}",
        application_date,
        expiry_date or "",
        remarks or "",
        approved_by or "",
        source,
    ]
    get_ledger_worksheet().append_row(row)


def get_balance_row_by_user(telegram_id: str) -> Optional[Dict[str, str]]:
    rows = get_all_balance_rows()
    if not rows or len(rows) < 2:
        return None

    headers = rows[0]
    for row in rows[1:]:
        if len(row) < 1:
            continue
        if str(row[0]).strip() == str(telegram_id):
            padded = row + [""] * (len(headers) - len(row))
            return dict(zip(headers, padded))
    return None


def upsert_balance_row(
    telegram_id: str,
    name: str,
    normal_off: float,
    active_ph_off: float,
    expired_ph_off: float,
    active_special_off: float,
    expired_special_off: float,
    dos_points: float,
    available_total: float,
) -> None:
    ws = get_balances_worksheet()
    rows = get_all_balance_rows()
    now = sg_now().strftime("%Y-%m-%d %H:%M:%S")

    row_values = [
        str(telegram_id),
        name or "",
        f"{float(normal_off):.1f}",
        f"{float(active_ph_off):.1f}",
        f"{float(expired_ph_off):.1f}",
        f"{float(active_special_off):.1f}",
        f"{float(expired_special_off):.1f}",
        f"{float(dos_points):.1f}",
        f"{float(available_total):.1f}",
        now,
    ]

    for idx, row in enumerate(rows[1:], start=2):
        if len(row) > 0 and str(row[0]).strip() == str(telegram_id):
            ws.update(f"A{idx}:J{idx}", [row_values])
            return

    ws.append_row(row_values)

def clear_balances_data():
    """
    Clears all balance rows except the header.
    Useful later for rebuild commands.
    """
    ws = get_balances_worksheet()
    rows = get_all_balance_rows()
    if len(rows) <= 1:
        return

    last_row = len(rows)
    ws.batch_clear([f"A2:I{last_row}"])


def list_all_balance_records() -> List[Dict[str, str]]:
    rows = get_all_balance_rows()
    if not rows or len(rows) < 2:
        return []

    headers = rows[0]
    out: List[Dict[str, str]] = []

    for row in rows[1:]:
        padded = row + [""] * (len(headers) - len(row))
        out.append(dict(zip(headers, padded)))

    return out


def list_all_known_users() -> List[tuple[str, str]]:
    """
    Returns users from balances first.
    Falls back to unique users from ledger if balances is empty.
    """
    balance_rows = get_all_balance_rows()
    users: List[tuple[str, str]] = []

    if len(balance_rows) > 1:
        for r in balance_rows[1:]:
            if len(r) < 2:
                continue
            uid = str(r[0]).strip()
            name = str(r[1]).strip() or uid
            if uid:
                users.append((uid, name))

        users.sort(key=lambda x: x[1].lower())
        return users

    seen = set()
    for r in get_all_ledger_rows()[1:]:
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
