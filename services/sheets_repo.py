import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from config import settings

log = logging.getLogger(__name__)

_WORKSHEET = None


def init_gsheet():
    global _WORKSHEET
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
    _WORKSHEET = client.open_by_key(settings.google_sheet_id).sheet1
    log.info("✅ Google Sheets ready.")


def get_worksheet():
    if _WORKSHEET is None:
        raise RuntimeError("Google Sheet is not initialised yet.")
    return _WORKSHEET


def get_all_rows() -> List[List[str]]:
    try:
        return get_worksheet().get_all_values()
    except Exception:
        log.exception("Failed to read sheet")
        return []


def get_row_count() -> int:
    return len(get_all_rows())


def get_header_row() -> List[str]:
    rows = get_all_rows()
    return rows[0] if rows else []


def try_get_worksheet_title() -> Optional[str]:
    try:
        return get_worksheet().title
    except Exception:
        return None


def healthcheck() -> tuple[bool, str]:
    try:
        ws = get_worksheet()
        return True, f"Sheet OK: {ws.title} | rows={get_row_count()}"
    except Exception as exc:
        log.exception("Google Sheet healthcheck failed")
        return False, f"Sheet error: {exc}"


def append_row(
    user_id: str,
    user_name: str,
    action: str,
    current_off: float,
    add_subtract: float,
    final_off: float,
    approved_by: str,
    application_date: str,
    remarks: str,
    is_ph: bool,
    ph_total: float,
    expiry: Optional[str],
    is_special: bool = False,
    special_total: float = 0.0,
) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row = [
        now,
        str(user_id),
        user_name or "",
        action,
        f"{current_off:.1f}",
        f"{'+' if add_subtract >= 0 else ''}{add_subtract:.1f}",
        f"{final_off:.1f}",
        approved_by,
        application_date,
        remarks,
        "Special" if is_special else ("Yes" if is_ph else "No"),
        f"{ph_total:.1f}" if is_ph else "",
        expiry or "",
        f"{special_total:.1f}" if is_special else "",
    ]
    get_worksheet().append_row(row, value_input_option="USER_ENTERED")


def last_off_for_user(user_id: str) -> float:
    rows = get_all_rows()
    urows = [r for r in rows if len(r) > 1 and r[1] == str(user_id)]
    if not urows:
        return 0.0
    try:
        return float(urows[-1][6])
    except Exception:
        return 0.0


def compute_ph_entries_active(user_id: str) -> Tuple[float, List[Dict[str, Any]]]:
    active_total, active_entries, _expired_total = _compute_ph_entries_breakdown(user_id)
    return active_total, active_entries


def _compute_ph_entries_breakdown(user_id: str) -> Tuple[float, List[Dict[str, Any]], float]:
    rows = get_all_rows()
    ph_events = []

    for r in rows[1:]:
        if len(r) < 13:
            continue

        rid = r[1]
        action = r[3]
        is_ph = len(r) >= 11 and r[10].strip().lower() in ("yes", "y", "true", "1")
        if rid != str(user_id) or not is_ph:
            continue

        qty_raw = r[5].strip() if len(r) > 5 else ""
        qty = 0.0
        if qty_raw:
            try:
                qty = float(qty_raw.replace("+", ""))
            except Exception:
                qty = 0.0
            if qty_raw.startswith("-"):
                qty = -abs(qty)

        app_date = r[8].strip() if len(r) > 8 else ""
        expiry = r[12].strip() if len(r) > 12 else ""
        reason = r[9].strip() if len(r) > 9 else ""

        ph_events.append({
            "action": action,
            "qty": qty,
            "date": app_date,
            "expiry": expiry,
            "reason": reason,
        })

    grants = []
    claims = []
    for e in ph_events:
        if e["qty"] > 0:
            grants.append({**e, "remaining": float(e["qty"])})
        elif e["qty"] < 0:
            claims.append(abs(float(e["qty"])))

    for claim_qty in claims:
        left = claim_qty
        for g in grants:
            if left <= 0:
                break
            take = min(g["remaining"], left)
            g["remaining"] -= take
            left -= take

    today = date.today()
    active_entries = []
    active_total = 0.0
    expired_total = 0.0

    for g in grants:
        rem = float(g.get("remaining", 0.0))
        if rem <= 0:
            continue

        expired = False
        if g.get("expiry"):
            try:
                expired = datetime.strptime(g["expiry"], "%Y-%m-%d").date() < today
            except Exception:
                expired = False

        item = {
            "date": g.get("date", ""),
            "qty": rem,
            "expiry": g.get("expiry", ""),
            "reason": g.get("reason", ""),
        }

        if expired:
            expired_total += rem
        else:
            active_total += rem
            active_entries.append(item)

    return active_total, active_entries, expired_total


def compute_special_entries_breakdown(user_id: str) -> Tuple[float, List[Dict[str, Any]], float]:
    rows = get_all_rows()
    events = []

    for r in rows[1:]:
        if len(r) < 14:
            continue

        rid = r[1].strip()
        if rid != str(user_id):
            continue

        kind_col = r[10].strip().lower() if len(r) > 10 else ""
        if kind_col != "special":
            continue

        qty_raw = r[13].strip() if len(r) > 13 and r[13].strip() else (r[5].strip() if len(r) > 5 else "")
        qty = 0.0
        if qty_raw:
            try:
                qty = float(qty_raw.replace("+", ""))
            except Exception:
                qty = 0.0
            if qty_raw.startswith("-"):
                qty = -abs(qty)

        events.append({
            "qty": qty,
            "date": r[8].strip() if len(r) > 8 else "",
            "reason": r[9].strip() if len(r) > 9 else "",
            "expiry": r[12].strip() if len(r) > 12 else "",
        })

    grants = []
    claims = []
    for e in events:
        if e["qty"] > 0:
            grants.append({**e, "remaining": float(e["qty"])})
        elif e["qty"] < 0:
            claims.append(abs(float(e["qty"])))

    for claim_qty in claims:
        left = claim_qty
        for g in grants:
            if left <= 0:
                break
            take = min(g["remaining"], left)
            g["remaining"] -= take
            left -= take

    today = date.today()
    active_entries = []
    active_total = 0.0
    expired_total = 0.0

    for g in grants:
        rem = float(g.get("remaining", 0.0))
        if rem <= 0:
            continue

        expired = False
        if g.get("expiry"):
            try:
                expired = datetime.strptime(g["expiry"], "%Y-%m-%d").date() < today
            except Exception:
                expired = False

        item = {
            "date": g.get("date", ""),
            "qty": rem,
            "expiry": g.get("expiry", ""),
            "reason": g.get("reason", ""),
        }

        if expired:
            expired_total += rem
        else:
            active_total += rem
            active_entries.append(item)

    return active_total, active_entries, expired_total
