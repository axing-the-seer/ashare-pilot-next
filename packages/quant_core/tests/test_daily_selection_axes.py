"""Panel axes must be valid before positional risk exclusions are applied."""

import numpy as np
import pytest
from ashare_quant_core.daily_selection import apply_risk_history, validate_panel_axes


def test_risk_rejects_unordered_dates_before_changing_any_values():
    x = np.zeros((3, 2, 36), dtype=np.float32)
    y = np.zeros((3, 2, 3), dtype=np.float32)
    days = np.array(["2026-11-25", "2026-11-23", "2026-11-24"])
    symbols = np.array(["SYNTH_A", "SYNTH_B"])
    risk = {
        "format": "market-risk-history/v1",
        "policy_id": "exclude-st-and-delisting-names/v1",
        "records": [{"symbol": "SYNTH_B", "start_date": "2026-11-25", "end_date": "2026-11-25"}],
    }
    with pytest.raises(ValueError, match="strictly increasing"):
        apply_risk_history(x, y, days, symbols, risk, array_ops=np)
    assert not np.isnan(x).any() and not np.isnan(y).any()


def test_sorted_dates_preserve_existing_risk_behavior_and_missing_labels():
    x = np.zeros((3, 2, 36), dtype=np.float32)
    y = np.zeros((3, 2, 3), dtype=np.float32)
    y[0, 0, 2] = np.nan
    days = np.array(["2026-11-23", "2026-11-24", "2026-11-25"])
    symbols = np.array(["SYNTH_A", "SYNTH_B"])
    risk = {
        "format": "market-risk-history/v1",
        "policy_id": "exclude-st-and-delisting-names/v1",
        "records": [{"symbol": "SYNTH_B", "start_date": "2026-11-25", "end_date": "2026-11-25"}],
    }
    validate_panel_axes(x, y, days, symbols, feature_count=36, horizon_count=3)
    assert apply_risk_history(x, y, days, symbols, risk, array_ops=np) == {
        "excluded_rows": 1,
        "excluded_symbols": 1,
    }
    assert np.isnan(x[2, 1]).all() and np.isnan(y[2, 1]).all()
    assert np.isnan(y[0, 0, 2]) and not np.isnan(x[:2]).any()


@pytest.mark.parametrize("variation", ["duplicate_dates", "invalid_date", "duplicate_symbols"])
def test_panel_axes_reject_ambiguous_dates_or_symbols(variation):
    x = np.zeros((2, 2, 36), dtype=np.float32)
    y = np.zeros((2, 2, 3), dtype=np.float32)
    days = np.array(["2026-11-23", "2026-11-24"])
    symbols = np.array(["SYNTH_A", "SYNTH_B"])
    if variation == "duplicate_dates":
        days[1] = days[0]
    elif variation == "invalid_date":
        days[1] = "2026-13-01"
    else:
        symbols[1] = symbols[0]
    with pytest.raises(ValueError, match="panel"):
        validate_panel_axes(x, y, days, symbols, feature_count=36, horizon_count=3)
