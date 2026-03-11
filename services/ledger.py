from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, List

from services.sheets_repo import get_all_rows


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


def _safe_date(value: str) -> date | None:
    try:
        return datetime.strptime(str(value).strip(), "%Y-%m-%d").date()
    except Exception:
        return None


@dataclass
class LedgerRow:
    timestamp: str
    user_id: str
    user_name: str
    action: str
    current_off: float
    delta: float
    final_off: float
    approved_by: str
    application_date: str
    remarks: str
    holiday_kind: str
    ph_total: float
    expiry: str
    special_total: float


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
    last_action: str
    last_application_date: str


def _parse_rows() -> List[LedgerRow]:
    rows = get_all_rows()
    if not rows:
        return []

    parsed: List[LedgerRow] = []
    for r in rows[1:]:
        if len(r) < 14:
            r = r + [""] * (14 - len(r))

        parsed.append(
            LedgerRow(
                timestamp=r[0].strip(),
                user_id=r[1].strip(),
                user_name=r[2].strip(),
                action=r[3].strip(),
                current_off=_safe_float(r[4]),
                delta=_safe_float(r[5]),
                final_off=_safe_float(r[6]),
                approved_by=r[7].strip(),
                application_date=r[8].strip(),
                remarks=r[9].strip(),
                holiday_kind=r[10].strip(),
                ph_total=_safe_float(r[11]),
                expiry=r[12].strip(),
                special_total=_safe_float(r[13]),
            )
        )
    return parsed


def _rows_for_user(user_id: str) -> List[LedgerRow]:
    return [r for r in _parse_rows() if r.user_id == str(user_id)]


def _allocate_remaining(grants: List[dict], claims: List[float]) -> None:
    for claim_qty in claims:
        left = float(claim_qty)
        for grant in grants:
            if left <= 0:
                break
            take = min(grant["remaining"], left)
            grant["remaining"] -= take
            left -= take


def _ph_breakdown(rows: List[LedgerRow]) -> tuple[float, float]:
    today = date.today()
    grants = []
    claims = []

    for r in rows:
        kind = r.holiday_kind.lower()
        if kind not in ("yes", "y", "true", "1"):
            continue

        qty = r.delta
        if qty > 0:
            grants.append(
                {
                    "remaining": float(qty),
                    "expiry": r.expiry,
                }
            )
        elif qty < 0:
            claims.append(abs(float(qty)))

    _allocate_remaining(grants, claims)

    active = 0.0
    expired = 0.0

    for g in grants:
        rem = float(g["remaining"])
        if rem <= 0:
            continue

        exp = _safe_date(g["expiry"])
        if exp and exp < today:
            expired += rem
        else:
            active += rem

    return active, expired


def _special_breakdown(rows: List[LedgerRow]) -> tuple[float, float]:
    today = date.today()
    grants = []
    claims = []

    for r in rows:
        kind = r.holiday_kind.lower()
        if kind != "special":
            continue

        qty = r.special_total if r.special_total and r.delta == 0 else r.delta
        if qty > 0:
            grants.append(
                {
                    "remaining": float(qty if r.delta > 0 else 0.0) if r.delta <= 0 else float(r.delta),
                    "expiry": r.expiry,
                }
            )
        elif qty < 0:
            claims.append(abs(float(qty)))

    if not grants:
        # fallback to delta-based parsing only
        for r in rows:
            if r.holiday_kind.lower() != "special":
                continue
            if r.delta > 0:
                grants.append({"remaining": float(r.delta), "expiry": r.expiry})
            elif r.delta < 0:
                claims.append(abs(float(r.delta)))

    _allocate_remaining(grants, claims)

    active = 0.0
    expired = 0.0

    for g in grants:
        rem = float(g["remaining"])
        if rem <= 0:
            continue

        exp = _safe_date(g["expiry"])
        if exp and exp < today:
            expired += rem
        else:
            active += rem

    return active, expired


def compute_user_summary(user_id: str) -> UserSummary:
    rows = _rows_for_user(user_id)

    if not rows:
        return UserSummary(
            user_id=str(user_id),
            user_name="Unknown",
            total_balance=0.0,
            normal_balance=0.0,
            ph_active=0.0,
            ph_expired=0.0,
            special_active=0.0,
            special_expired=0.0,
            last_action="",
            last_application_date="",
        )

    last = rows[-1]
    total_balance = last.final_off

    ph_active, ph_expired = _ph_breakdown(rows)
    special_active, special_expired = _special_breakdown(rows)

    normal_balance = total_balance - ph_active - special_active

    return UserSummary(
        user_id=last.user_id,
        user_name=last.user_name or "Unknown",
        total_balance=total_balance,
        normal_balance=normal_balance,
        ph_active=ph_active,
        ph_expired=ph_expired,
        special_active=special_active,
        special_expired=special_expired,
        last_action=last.action,
        last_application_date=last.application_date,
    )


def compute_overview() -> List[UserSummary]:
    rows = _parse_rows()
    user_ids = []
    seen = set()

    for r in rows:
        if not r.user_id:
            continue
        if r.user_id in seen:
            continue
        seen.add(r.user_id)
        user_ids.append(r.user_id)

    summaries = [compute_user_summary(uid) for uid in user_ids]
    summaries.sort(key=lambda x: x.user_name.lower())
    return summaries
