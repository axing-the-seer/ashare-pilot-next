"""Frozen v2 selection/risk semantics shared by research and personal inference.

Numerical array/rank operations are supplied by the adapter so quant_core's
standard-library-only base does not acquire optional ML dependencies. These
bodies preserve the existing operations, dtypes, stable ties and thresholds.
"""

from datetime import date

from .market_risk import RISK_NAME_POLICY_ID

TOP_K = 8
HOLD_BUFFER = 24
INDUSTRY_CAP = 2
SIZE_FLOOR = -0.30
LIQUIDITY_FLOOR = 0.30
VOLATILITY_PENALTY = 0.25


def screen_and_rank(
    model,
    base_row,
    financial_row,
    industry_row,
    amount_history,
    *,
    feature_names,
    array_ops,
    rankdata,
):
    np = array_ops
    DAILY_COMPOSITE_FEATURE_NAMES = feature_names
    eligible = (
        np.isfinite(base_row).all(axis=1)
        & np.isfinite(financial_row).all(axis=1)
        & (industry_row >= 0)
        & (
            base_row[:, DAILY_COMPOSITE_FEATURE_NAMES.index("circulating_market_value_rank")]
            >= SIZE_FLOOR
        )
    )
    amount_available = np.isfinite(amount_history).any(axis=0)
    median_amount = np.full(len(base_row), np.nan, dtype=np.float64)
    median_amount[amount_available] = np.nanmedian(amount_history[:, amount_available], axis=0)
    liquid_base = eligible & np.isfinite(median_amount) & (median_amount > 0)
    liquidity = np.full(len(base_row), np.nan, dtype=np.float32)
    count = int(liquid_base.sum())
    if count:
        liquidity[liquid_base] = (rankdata(median_amount[liquid_base], method="average") - 1) / max(
            count - 1, 1
        )
    eligible &= liquidity >= LIQUIDITY_FLOOR
    indices = np.where(eligible)[0]
    if len(indices) < TOP_K:
        return [], eligible, liquidity
    features = np.concatenate((base_row[indices], financial_row[indices]), axis=1)
    raw = model.score(features)
    deviation = float(np.std(raw))
    standardized = (raw - float(np.mean(raw))) / deviation if deviation > 0 else raw * 0
    volatility = base_row[indices, DAILY_COMPOSITE_FEATURE_NAMES.index("volatility_20d")] + 0.5
    adjusted = standardized - VOLATILITY_PENALTY * volatility
    order = np.argsort(-adjusted, kind="stable")
    ranked = [
        {
            "index": int(indices[local]),
            "raw_score": float(raw[local]),
            "standardized_score": float(standardized[local]),
            "adjusted_score": float(adjusted[local]),
            "liquidity_percentile": float(liquidity[indices[local]]),
        }
        for local in order
    ]
    return ranked, eligible, liquidity


def choose_targets(ranked, symbols, industries, holdings):
    ranks = {str(symbols[item["index"]]): rank for rank, item in enumerate(ranked, 1)}
    by_symbol = {str(symbols[item["index"]]): item for item in ranked}
    selected = []
    counts: dict[int, int] = {}
    for symbol in sorted(holdings, key=lambda value: ranks.get(value, 10**9)):
        item = by_symbol.get(symbol)
        if item is None or ranks[symbol] > HOLD_BUFFER:
            continue
        industry = int(industries[item["index"]])
        if counts.get(industry, 0) >= INDUSTRY_CAP:
            continue
        selected.append(symbol)
        counts[industry] = counts.get(industry, 0) + 1
    for item in ranked:
        symbol = str(symbols[item["index"]])
        industry = int(industries[item["index"]])
        if symbol in selected or counts.get(industry, 0) >= INDUSTRY_CAP:
            continue
        selected.append(symbol)
        counts[industry] = counts.get(industry, 0) + 1
        if len(selected) == TOP_K:
            break
    return selected


def apply_risk_history(x, y, days, symbols, document, *, array_ops) -> dict[str, int]:
    np = array_ops
    if document.get("format") != "market-risk-history/v1":
        raise ValueError("invalid market risk history")
    if document.get("policy_id") != RISK_NAME_POLICY_ID:
        raise ValueError("unexpected market risk policy")
    ordered_days = [str(value) for value in days]
    if len(set(ordered_days)) != len(ordered_days):
        raise ValueError("duplicate panel dates")
    # A dated exchange-state snapshot is not a history. Refuse to extend it
    # beyond its explicit coverage, especially in model training or backtests.
    coverage = document.get("coverage")
    if coverage is not None:
        if not isinstance(coverage, dict) or set(coverage) != {"start", "end"}:
            raise ValueError("invalid market risk coverage")
        try:
            first_day = date.fromisoformat(coverage["start"])
            last_day = date.fromisoformat(coverage["end"])
            parsed_days = [date.fromisoformat(day) for day in ordered_days]
        except (TypeError, ValueError):
            raise ValueError("invalid market risk coverage") from None
        if first_day > last_day or any(day < first_day or day > last_day for day in parsed_days):
            raise ValueError("market risk snapshot does not cover requested dates")
    symbol_index = {str(value): index for index, value in enumerate(symbols)}
    excluded_rows = 0
    excluded_symbols = set()
    for row in document["records"]:
        j = symbol_index.get(row["symbol"])
        if j is None:
            continue
        start = row["start_date"]
        end = row["end_date"] or ordered_days[-1]
        first = int(np.searchsorted(ordered_days, start, side="left"))
        last = int(np.searchsorted(ordered_days, end, side="right"))
        if first >= last:
            continue
        present = np.isfinite(x[first:last, j]).all(axis=1)
        excluded_rows += int(present.sum())
        excluded_symbols.add(row["symbol"])
        x[first:last, j, :] = np.nan
        y[first:last, j, :] = np.nan
    return {"excluded_rows": excluded_rows, "excluded_symbols": len(excluded_symbols)}
