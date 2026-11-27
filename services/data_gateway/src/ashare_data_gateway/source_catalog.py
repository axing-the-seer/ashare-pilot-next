"""Load explicitly scoped upstream field definitions for source capture."""

import json
import re
from pathlib import Path

DEFINITIONS = Path(__file__).with_name("source_catalog_daily_definitions.json")


def definitions(path: Path = DEFINITIONS) -> list[dict]:
    value = json.loads(path.read_text())
    if value["format"] == "source-field-definitions/v1":
        return value["datasets"]
    if (
        value["format"] != "source-field-definitions-slice/v1"
        or value.get("sourceFormat") != "source-field-definitions/v1"
        or not re.fullmatch(r"[a-f0-9]{64}", value.get("sourceSha256", ""))
        or value.get("scope") != ["tushare.daily", "tushare.trade_cal"]
        or {row["id"] for row in value["datasets"]} != set(value["scope"])
        or len(value["datasets"]) != len(value["scope"])
    ):
        raise ValueError("unsupported source definitions")
    return value["datasets"]
