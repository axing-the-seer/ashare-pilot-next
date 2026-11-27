from datetime import UTC, date, datetime
from unittest.mock import Mock

import pytest
from ashare_data_gateway.dataset_publication import load_published_dataset
from ashare_data_gateway.market_daily import acquire_day
from ashare_data_gateway.tushare_models import DailyBarRecord


def options(tmp_path):
    return dict(
        trade_date=date(2026, 9, 25),
        generated_at=datetime(2026, 9, 28, tzinfo=UTC),
        publication_root=tmp_path,
        source_base_url="https://api.tushare.pro",
    )


def test_all_exchanges_units_and_resume_without_provider_access(tmp_path):
    client = Mock()
    client.fetch_daily_by_date.return_value = tuple(
        DailyBarRecord(
            ts_code=s,
            trade_date=date(2026, 9, 25),
            open=10,
            high=11,
            low=9,
            close=10,
            vol=100,
            amount=100,
        )
        for s in ("000001.SZ", "600001.SH", "920001.BJ")
    )
    first = acquire_day(client, **options(tmp_path))
    assert first["symbols"] == 3
    second = acquire_day(client, **options(tmp_path))
    assert second == {**first, "cached": True}
    assert client.fetch_daily_by_date.call_count == 1
    loaded = load_published_dataset(publication_root=tmp_path, dataset_id=first["dataset_id"])
    assert loaded.records[0].volume == 10000
    assert loaded.records[0].amount == 100000


def test_empty_response_cannot_publish_success_receipt(tmp_path):
    client = Mock()
    client.fetch_daily_by_date.return_value = ()
    with pytest.raises(ValueError, match="no market bars"):
        acquire_day(client, **options(tmp_path))
    assert not (tmp_path / "market-days/2026-09-25.json").exists()
