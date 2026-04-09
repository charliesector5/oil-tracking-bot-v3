from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import holidays
from typing import Callable, Dict, List, Optional
from zoneinfo import ZoneInfo

from services.sheets_repo import (
    append_ledger_row,
    clear_balances_data,
    get_all_rows,
    list_all_known_users,
    upsert_balance_row,
)

SG_TZ = ZoneInfo("Asia/Singapore")


def sg_now() -> datetime:
    return datetime.now(SG_TZ)


def sg_today() -> date:
    return sg_now().date()

DOS_CONVERSION_THRESHOLD = 3
DOS_CONVERSION_DAYS = 1.0


def is_sg_public_holiday(day: date) -> bool:
    sg_holidays = holidays.country_holidays("SG", years=day.year)
    return day in sg_holidays


def dos_points_for_date(day: date) -> int:
    if is_sg_public_holiday(day):
        return 3
    if day.weekday() >= 5: 
        return 2
    return 1


def _safe_float(value) -> float:
    try:
        if value is None:
            return 0.0
        s = str(value).strip().replace("+", "")
        if not s:
            return 0.0
        return float(s)
    except Exception:
        return 0.0


def _safe_date(value: str) -> Optional[date]:
    try:
        return datetime.strptime(str(value).strip(), "%Y-%m-%d").date()
    except Exception:
        return None


def _safe_timestamp_date(value: str) -> Optional[date]:
    try:
        return datetime.strptime(str(value).strip(), "%Y-%m-%d %H:%M:%S").date()
    except Exception:
        return None


def _event_ref_date(application_date: str, timestamp: str) -> date:
    return _safe_date(application_date) or _safe_timestamp_date(timestamp) or sg_today()


def _holiday_kind_from_off_type(off_type: str) -> str:
    ot = (off_type or "").strip().upper()
    if ot == "PH":
        return "Yes"
    if ot == "SPECIAL":
        return "Special"
    return "No"


def _display_action(action_type: str) -> str:
    at = (action_type or "").strip().upper()
    if at == "CLOCK":
        return "Clock Off"
    if at == "CLAIM":
        return "Claim Off"
    if at == "IMPORT":
        return "Import"
    if at == "ADJUST":
        return "Adjust Off"
    if at == "MASS_ADJUST":
        return "Mass Adjust"
    if at == "EXPIRE_CLEANUP":
        return "Expiry Cleanup"
    return action_type or ""


@dataclass
class EntryDetail:
    date: str
    qty: float
    expiry: str
    remarks: str


@dataclass
class LedgerEvent:
    timestamp: str
    user_id: str
    user_name: str
    action_type: str
    off_type: str
    amount: float
    application_date: str
    expiry: str
    remarks: str
    approved_by: str
    source: str


@dataclass
class LedgerRow:
    timestamp: str
    user_id: str
    user_name: str
    action: str
    action_type: str
    off_type: str
    current_off: float
    delta: float
    final_off: float
    approved_by: str
    application_date: str
    remarks: str
    holiday_kind: str
    expiry: str
    source: str


@dataclass
class UserSummary:
    user_id: str
    user_name: str
    total_balance: float
    normal_balance: float
    ph_active: float
    ph_expired: float
    special_active: float
    special_expired: float
    dos_points: float
    ph_active_entries: List[EntryDetail]
    ph_expired_entries: List[EntryDetail]
    special_active_entries: List[EntryDetail]
    special_expired_entries: List[EntryDetail]
    last_action: str
    last_application_date: str


@dataclass
class CleanupResult:
    users_affected: int
    ph_cleaned: float
    special_cleaned: float
    rows_written: int
    affected_users: List[str]


def _parse_ledger_events(get_all_rows_fn: Callable[[], List[List[str]]]) -> List[LedgerEvent]:
    rows = get_all_rows_fn()
    if not rows:
        return []

    headers = [str(h).strip() for h in rows[0]]
    header_map = {h: i for i, h in enumerate(headers)}

    def _cell(row: List[str], key: str) -> str:
        idx = header_map.get(key)
        if idx is None or idx >= len(row):
            return ""
        return str(row[idx]).strip()

    events: List[LedgerEvent] = []
    for row in rows[1:]:
        if not row:
            continue

        user_id = _cell(row, "Telegram ID")
        user_name = _cell(row, "Name")
        if not user_id:
            continue

        events.append(
            LedgerEvent(
                timestamp=_cell(row, "Timestamp"),
                user_id=user_id,
                user_name=user_name,
                action_type=_cell(row, "Action Type").upper(),
                off_type=_cell(row, "Off Type").upper(),
                amount=_safe_float(_cell(row, "Amount")),
                application_date=_cell(row, "Application Date"),
                expiry=_cell(row, "Expiry Date"),
                remarks=_cell(row, "Remarks"),
                approved_by=_cell(row, "Approved By"),
                source=_cell(row, "Source").upper(),
            )
        )
    return events


def _allocate_from_grants(
    grants: List[Dict],
    qty: float,
    ref_date: date,
    mode: str = "active",
) -> None:
    left = float(abs(qty))
    if left <= 0:
        return

    def _is_expired(g: Dict) -> bool:
        exp = _safe_date(g.get("expiry", ""))
        return bool(exp and exp < ref_date)

    if mode == "expired":
        eligible = [g for g in grants if g["remaining"] > 0 and _is_expired(g)]
    else:
        eligible = [g for g in grants if g["remaining"] > 0 and not _is_expired(g)]

    for g in eligible:
        if left <= 0:
            break
        take = min(g["remaining"], left)
        g["remaining"] -= take
        left -= take

    if left > 0:
        for g in grants:
            if left <= 0:
                break
            if g["remaining"] <= 0:
                continue
            take = min(g["remaining"], left)
            g["remaining"] -= take
            left -= take


def _active_total(grants: List[Dict], ref_date: date) -> float:
    total = 0.0
    for g in grants:
        exp = _safe_date(g.get("expiry", ""))
        if exp and exp < ref_date:
            continue
        total += float(g["remaining"])
    return total


def _expired_total(grants: List[Dict], ref_date: date) -> float:
    total = 0.0
    for g in grants:
        exp = _safe_date(g.get("expiry", ""))
        if exp and exp < ref_date:
            total += float(g["remaining"])
    return total


def _grant_details(grants: List[Dict], ref_date: date, expired: bool) -> List[EntryDetail]:
    out: List[EntryDetail] = []
    for g in grants:
        rem = float(g["remaining"])
        if rem <= 0:
            continue
        exp = _safe_date(g.get("expiry", ""))
        is_expired = bool(exp and exp < ref_date)
        if is_expired != expired:
            continue
        out.append(
            EntryDetail(
                date=g.get("date", ""),
                qty=rem,
                expiry=g.get("expiry", ""),
                remarks=g.get("remarks", ""),
            )
        )
    return out


def _build_user_state(events: List[LedgerEvent]):
    normal_balance = 0.0
    ph_grants: List[Dict] = []
    special_grants: List[Dict] = []
    dos_points_balance = 0.0
    history_rows: List[LedgerRow] = []

    for e in events:
        ref_date = _event_ref_date(e.application_date, e.timestamp)

        before_total = (
            normal_balance
            + _active_total(ph_grants, ref_date)
            + _active_total(special_grants, ref_date)
        )
        before_dos = dos_points_balance

        if e.off_type == "NORMAL":
            normal_balance += e.amount

        elif e.off_type == "PH":
            if e.amount > 0:
                ph_grants.append(
                    {
                        "remaining": float(e.amount),
                        "date": e.application_date,
                        "expiry": e.expiry,
                        "remarks": e.remarks,
                    }
                )
            elif e.amount < 0:
                mode = "expired" if e.action_type == "EXPIRE_CLEANUP" else "active"
                _allocate_from_grants(ph_grants, abs(e.amount), ref_date, mode=mode)

        elif e.off_type == "SPECIAL":
            if e.amount > 0:
                special_grants.append(
                    {
                        "remaining": float(e.amount),
                        "date": e.application_date,
                        "expiry": e.expiry,
                        "remarks": e.remarks,
                    }
                )
            elif e.amount < 0:
                mode = "expired" if e.action_type == "EXPIRE_CLEANUP" else "active"
                _allocate_from_grants(special_grants, abs(e.amount), ref_date, mode=mode)

        elif e.off_type == "DOS":
            dos_points_balance += e.amount

        after_total = (
            normal_balance
            + _active_total(ph_grants, ref_date)
            + _active_total(special_grants, ref_date)
        )
        after_dos = dos_points_balance

        row_current = before_dos if e.off_type == "DOS" else before_total
        row_final = after_dos if e.off_type == "DOS" else after_total

        history_rows.append(
            LedgerRow(
                timestamp=e.timestamp,
                user_id=e.user_id,
                user_name=e.user_name,
                action=_display_action(e.action_type),
                action_type=e.action_type,
                off_type=e.off_type,
                current_off=row_current,
                delta=e.amount,
                final_off=row_final,
                approved_by=e.approved_by,
                application_date=e.application_date,
                remarks=e.remarks,
                holiday_kind=_holiday_kind_from_off_type(e.off_type),
                expiry=e.expiry,
                source=e.source,
            )
        )

    return normal_balance, ph_grants, special_grants, dos_points_balance, history_rows


def compute_user_summary(
    user_id: str,
    get_all_rows_fn: Callable[[], List[List[str]]] = get_all_rows,
) -> UserSummary:
    events = [e for e in _parse_ledger_events(get_all_rows_fn) if e.user_id == str(user_id)]

    if not events:
        return UserSummary(
            user_id=str(user_id),
            user_name="Unknown",
            total_balance=0.0,
            normal_balance=0.0,
            ph_active=0.0,
            ph_expired=0.0,
            special_active=0.0,
            special_expired=0.0,
            dos_points=0.0,
            ph_active_entries=[],
            ph_expired_entries=[],
            special_active_entries=[],
            special_expired_entries=[],
            last_action="",
            last_application_date="",
        )

    normal_balance, ph_grants, special_grants, dos_points_balance, history_rows = _build_user_state(events)
    today = sg_today()

    ph_active = _active_total(ph_grants, today)
    ph_expired = _expired_total(ph_grants, today)
    special_active = _active_total(special_grants, today)
    special_expired = _expired_total(special_grants, today)

    total_balance = normal_balance + ph_active + special_active
    last = history_rows[-1]

    return UserSummary(
        user_id=last.user_id,
        user_name=last.user_name or "Unknown",
        total_balance=total_balance,
        normal_balance=normal_balance,
        ph_active=ph_active,
        ph_expired=ph_expired,
        special_active=special_active,
        special_expired=special_expired,
        dos_points=dos_points_balance,
        ph_active_entries=_grant_details(ph_grants, today, expired=False),
        ph_expired_entries=_grant_details(ph_grants, today, expired=True),
        special_active_entries=_grant_details(special_grants, today, expired=False),
        special_expired_entries=_grant_details(special_grants, today, expired=True),
        last_action=last.action,
        last_application_date=last.application_date,
    )


def compute_overview(
    get_all_rows_fn: Callable[[], List[List[str]]] = get_all_rows,
) -> List[UserSummary]:
    events = _parse_ledger_events(get_all_rows_fn)
    seen = set()
    user_ids: List[str] = []

    for e in events:
        if not e.user_id or e.user_id in seen:
            continue
        seen.add(e.user_id)
        user_ids.append(e.user_id)

    summaries = [compute_user_summary(uid, get_all_rows_fn) for uid in user_ids]
    summaries.sort(key=lambda x: x.user_name.lower())
    return summaries


def get_user_last_records(
    user_id: str,
    get_all_rows_fn: Callable[[], List[List[str]]] = get_all_rows,
    limit: int = 5,
) -> List[LedgerRow]:
    events = [e for e in _parse_ledger_events(get_all_rows_fn) if e.user_id == str(user_id)]
    if not events:
        return []
    _, _, _, history_rows = _build_user_state(events)
    return history_rows[-limit:]

def award_dos_for_date(
    telegram_id: str,
    name: str,
    work_date: str,
    approved_by: str,
    source: str = "ADMIN",
    remarks: str = "",
    get_all_rows_fn: Callable[[], List[List[str]]] = get_all_rows,
) -> UserSummary:
    work_day = _safe_date(work_date)
    if not work_day:
        raise ValueError("work_date must be YYYY-MM-DD")

    points = float(dos_points_for_date(work_day))
    reason = remarks or f"DOS earned on {work_date}"

    append_ledger_row(
        telegram_id=telegram_id,
        name=name,
        action_type="ADJUST",
        off_type="DOS",
        amount=points,
        application_date=work_date,
        expiry_date="",
        remarks=reason,
        approved_by=approved_by,
        source=source,
    )

    summary = compute_user_summary(str(telegram_id), get_all_rows_fn)
    conversions = int(summary.dos_points // DOS_CONVERSION_THRESHOLD)

    for _ in range(conversions):
        append_ledger_row(
            telegram_id=telegram_id,
            name=name,
            action_type="ADJUST",
            off_type="DOS",
            amount=-float(DOS_CONVERSION_THRESHOLD),
            application_date=work_date,
            expiry_date="",
            remarks="DOS auto-conversion: -3 points",
            approved_by=approved_by,
            source=source,
        )

        append_ledger_row(
            telegram_id=telegram_id,
            name=name,
            action_type="ADJUST",
            off_type="NORMAL",
            amount=+float(DOS_CONVERSION_DAYS),
            application_date=work_date,
            expiry_date="",
            remarks="DOS auto-conversion: +1.0 normal OIL",
            approved_by=approved_by,
            source=source,
        )

    return rebuild_user_balance(str(telegram_id), get_all_rows_fn)


def rebuild_user_balance(
    user_id: str,
    get_all_rows_fn: Callable[[], List[List[str]]] = get_all_rows,
) -> UserSummary:
    summary = compute_user_summary(user_id, get_all_rows_fn)
    if summary.user_name == "Unknown" and not summary.user_id:
        return summary

    upsert_balance_row(
        telegram_id=summary.user_id,
        name=summary.user_name,
        normal_off=summary.normal_balance,
        active_ph_off=summary.ph_active,
        expired_ph_off=summary.ph_expired,
        active_special_off=summary.special_active,
        expired_special_off=summary.special_expired,
        dos_points=summary.dos_points,
        available_total=summary.total_balance,
    )
    return summary


def rebuild_all_balances(
    get_all_rows_fn: Callable[[], List[List[str]]] = get_all_rows,
) -> List[UserSummary]:
    clear_balances_data()

    users = list_all_known_users()
    rebuilt: List[UserSummary] = []

    if not users:
        events = _parse_ledger_events(get_all_rows_fn)
        seen = set()
        for e in events:
            if e.user_id and e.user_id not in seen:
                seen.add(e.user_id)
                users.append((e.user_id, e.user_name or e.user_id))

    for user_id, _name in users:
        summary = rebuild_user_balance(user_id, get_all_rows_fn)
        rebuilt.append(summary)

    rebuilt.sort(key=lambda x: x.user_name.lower())
    return rebuilt


def cleanup_expired_off(
    admin_name: str,
    get_all_rows_fn: Callable[[], List[List[str]]] = get_all_rows,
    remarks: str = "Expired off cleanup",
) -> CleanupResult:
    summaries = compute_overview(get_all_rows_fn)

    users_affected = 0
    ph_cleaned = 0.0
    special_cleaned = 0.0
    rows_written = 0
    affected_users: List[str] = []
    today_str = sg_today().strftime("%Y-%m-%d")

    for s in summaries:
        wrote_for_user = False

        if s.ph_expired > 0:
            append_ledger_row(
                telegram_id=s.user_id,
                name=s.user_name,
                action_type="EXPIRE_CLEANUP",
                off_type="PH",
                amount=-float(s.ph_expired),
                application_date=today_str,
                expiry_date="",
                remarks=remarks,
                approved_by=admin_name,
                source="ADMIN",
            )
            ph_cleaned += float(s.ph_expired)
            rows_written += 1
            wrote_for_user = True

        if s.special_expired > 0:
            append_ledger_row(
                telegram_id=s.user_id,
                name=s.user_name,
                action_type="EXPIRE_CLEANUP",
                off_type="SPECIAL",
                amount=-float(s.special_expired),
                application_date=today_str,
                expiry_date="",
                remarks=remarks,
                approved_by=admin_name,
                source="ADMIN",
            )
            special_cleaned += float(s.special_expired)
            rows_written += 1
            wrote_for_user = True

        if wrote_for_user:
            users_affected += 1
            affected_users.append(s.user_name)

    if rows_written > 0:
        rebuild_all_balances(get_all_rows_fn)

    return CleanupResult(
        users_affected=users_affected,
        ph_cleaned=ph_cleaned,
        special_cleaned=special_cleaned,
        rows_written=rows_written,
        affected_users=affected_users,
    )
