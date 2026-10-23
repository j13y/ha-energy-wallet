"""Recalculating past days from meter history.

Kept free of Home Assistant imports so it can be unit tested on its own.
Days still in the recorder's state history are rebuilt reading by reading,
exactly as live tracking counts them. Older days only have hourly long-term
statistics, so each hour's energy is spread evenly over its quarter-hours.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, timedelta, timezone, tzinfo

from .wallet import SLOT_SECONDS, PriceFn, Wallet

QUARTERS_PER_HOUR = 3600 // SLOT_SECONDS

DayRange = tuple[date, date]  # [first, end)


def plan_days(start: date, end: date, states_from: date, live_since: date | None) -> tuple[DayRange, DayRange]:
    """Split [start, end) into days rebuilt from hourly statistics and from states.

    Days after ``live_since`` that are no longer in the state history keep their
    live values: those were counted per reading, which hourly data cannot match.
    """
    hourly_end = min(end, states_from)
    if live_since is not None:
        hourly_end = min(hourly_end, live_since + timedelta(days=1))
    return (start, max(start, hourly_end)), (max(start, states_from), max(start, states_from, end))


def iter_days(days: DayRange) -> Iterable[date]:
    day, end = days
    while day < end:
        yield day
        day += timedelta(days=1)


class Rebuild:
    """Accumulates past energy in a scratch Wallet."""

    def __init__(self, tz: tzinfo, max_power_kw: float) -> None:
        self.wallet = Wallet()
        self.tz = tz
        self.max_power_kw = max_power_kw

    def _day(self, ts: datetime) -> str:
        return ts.astimezone(self.tz).date().isoformat()

    def add_readings(self, kind: str, readings: Iterable[tuple[datetime, float, bool]]) -> None:
        """Cumulative readings as (time, kWh, resets_allowed), oldest first."""
        for ts, value, resets_allowed in readings:
            delta, _ = self.wallet.ingest_reading(kind, value, ts, resets_allowed, self.max_power_kw)
            if delta > 0:
                self.wallet.add_energy(kind, delta, ts, self._day(ts))

    def add_hourly(self, kind: str, changes: Iterable[tuple[datetime, float]]) -> None:
        """Energy per hour as (hour start, kWh)."""
        for start, kwh in changes:
            if kwh <= 0:
                continue
            for i in range(QUARTERS_PER_HOUR):
                ts = start + timedelta(seconds=SLOT_SECONDS * i)
                self.wallet.add_energy(kind, kwh / QUARTERS_PER_HOUR, ts, self._day(ts))

    def finish(self, cost_fn: PriceFn, rev_fn: PriceFn) -> dict[str, dict[str, float]]:
        """Price all energy; slots without a price end up as unpriced kWh."""
        self.wallet.settle(cost_fn, rev_fn, datetime.max.replace(tzinfo=timezone.utc))
        return self.wallet.days
