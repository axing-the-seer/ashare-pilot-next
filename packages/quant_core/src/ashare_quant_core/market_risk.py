"""Point-in-time security-name risk policy for model eligibility."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

RISK_NAME_POLICY_ID = "exclude-st-and-delisting-names/v1"


def risk_name_reason(name: str) -> str | None:
    normalized = name.strip().upper()
    if "ST" in normalized:
        return "ST_RISK_WARNING"
    if "退" in name:
        return "DELISTING_NAME"
    return None


@dataclass(frozen=True)
class SecurityRiskInterval:
    symbol: str
    name: str
    start_date: date
    end_date: date | None
    reason: str

    def __post_init__(self) -> None:
        expected = risk_name_reason(self.name)
        if expected is None or self.reason != expected:
            raise ValueError("risk interval name and reason are inconsistent")
        if self.end_date is not None and self.end_date < self.start_date:
            raise ValueError("risk interval ends before it starts")

    def active_on(self, day: date) -> bool:
        return self.start_date <= day and (self.end_date is None or day <= self.end_date)
