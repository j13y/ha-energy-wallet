from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from custom_components.energy_wallet.backfill import Rebuild, iter_days, plan_days
from custom_components.energy_wallet.wallet import EXPORT, IMPORT, Wallet, slot_start

TZ = ZoneInfo("Europe/Warsaw")
D = date.fromisoformat


def test_plan_month_with_live_tracking_since_today():
    hourly, states = plan_days(D("2026-10-01"), D("2026-10-23"), D("2026-10-14"), D("2026-10-23"))
    assert hourly == (D("2026-10-01"), D("2026-10-14"))
    assert states == (D("2026-10-14"), D("2026-10-23"))


def test_plan_keeps_live_days_older_than_state_history():
    # Live since 2 October, states only from the 14th: days 3..13 keep their live values.
    hourly, states = plan_days(D("2026-09-01"), D("2026-10-23"), D("2026-10-14"), D("2026-10-02"))
    assert hourly == (D("2026-09-01"), D("2026-10-03"))
    assert states == (D("2026-10-14"), D("2026-10-23"))


def test_plan_start_inside_state_history():
    hourly, states = plan_days(D("2026-10-20"), D("2026-10-23"), D("2026-10-14"), D("2026-10-21"))
    assert list(iter_days(hourly)) == []
    assert states == (D("2026-10-20"), D("2026-10-23"))


def test_plan_nothing_to_do_on_first_day_of_month():
    hourly, states = plan_days(D("2026-10-01"), D("2026-10-01"), D("2026-09-22"), D("2026-09-30"))
    assert list(iter_days(hourly)) == list(iter_days(states)) == []


def test_hourly_energy_is_spread_over_quarters_and_priced():
    hour = datetime(2026, 10, 5, 10, tzinfo=timezone.utc)
    rebuild = Rebuild(TZ, 50)
    rebuild.add_hourly(EXPORT, [(hour, 4.0), (hour + timedelta(hours=1), -1.0)])
    prices = {hour + timedelta(minutes=15 * i): 0.1 * (i + 1) for i in range(4)}
    days = rebuild.finish(lambda s: 1.0, prices.get)
    assert days["2026-10-05"]["exp"] == pytest.approx(4.0)
    assert days["2026-10-05"]["rev"] == pytest.approx(0.1 + 0.2 + 0.3 + 0.4)


def test_energy_without_price_is_unpriced():
    rebuild = Rebuild(TZ, 50)
    rebuild.add_hourly(EXPORT, [(datetime(2024, 6, 1, 10, tzinfo=timezone.utc), 2.0)])
    days = rebuild.finish(lambda s: 1.0, lambda s: None)
    assert days["2024-06-01"]["exp_unpriced"] == pytest.approx(2.0)
    assert days["2024-06-01"]["rev"] == 0


def test_readings_are_counted_like_live_tracking():
    t0 = datetime(2026, 10, 14, 21, 50, tzinfo=timezone.utc)  # 23:50 local
    rebuild = Rebuild(TZ, 50)
    rebuild.add_readings(IMPORT, [(t0, 100.0, False), (t0 + timedelta(minutes=5), 100.5, False),
                                  (t0 + timedelta(minutes=15), 101.0, False)])
    days = rebuild.finish(lambda s: 2.0, lambda s: None)
    # The first reading is the baseline; the last one falls after local midnight.
    assert days["2026-10-14"]["imp"] == pytest.approx(0.5)
    assert days["2026-10-15"]["imp"] == pytest.approx(0.5)
    assert days["2026-10-15"]["cost"] == pytest.approx(1.0)


def test_replace_days_keeps_totals_consistent_and_drops_queued_energy():
    t = datetime(2026, 10, 20, 10, tzinfo=timezone.utc)
    w = Wallet()
    w.add_energy(IMPORT, 3.0, t, "2026-10-20")
    w.add_energy(IMPORT, 1.0, t + timedelta(days=1), "2026-10-21")
    w.settle(lambda s: 1.0 if s < t + timedelta(days=1) else None, lambda s: None, t + timedelta(hours=1))
    assert w.totals["cost"] == pytest.approx(3.0)

    new = {"2026-10-19": {"imp": 2.0, "exp": 0.0, "cost": 4.0, "rev": 0.0, "imp_unpriced": 0.0, "exp_unpriced": 0.0},
           "2026-10-21": {"imp": 5.0, "exp": 0.0, "cost": 10.0, "rev": 0.0, "imp_unpriced": 0.0, "exp_unpriced": 0.0}}
    w.replace_days(new, {"2026-10-19", "2026-10-21"})
    assert w.days["2026-10-20"]["cost"] == pytest.approx(3.0)
    assert w.totals["cost"] == pytest.approx(3.0 + 4.0 + 10.0)
    assert w.totals["imp"] == pytest.approx(3.0 + 2.0 + 5.0)
    assert str(slot_start(t + timedelta(days=1))) not in w.pending


def test_live_since_defaults_to_first_stored_day():
    assert Wallet({"days": {"2026-10-19": {}, "2026-10-20": {}}}).live_since == "2026-10-19"
    assert Wallet({"days": {"2026-10-19": {}}, "live_since": "2026-10-01"}).live_since == "2026-10-01"
    assert Wallet().live_since is None
