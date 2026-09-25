"""Unit tests for ingestion logic that does not need a database."""
from datetime import datetime

import pytest

from include.ingestion import extract
from include.ingestion.cli import month_starts
from include.ingestion.sources import SOURCES_BY_NAME


def _write(path, header, rows=()):
    path.write_text("\n".join([",".join(header), *[",".join(r) for r in rows]]) + "\n", encoding="utf-8")


def test_contract_accepts_expected_header(tmp_path):
    src = SOURCES_BY_NAME["sellers"]
    f = tmp_path / src.file
    _write(f, src.columns, [("s1", "01001", "sao paulo", "SP")])
    extract.validate_contract(src, f)


def test_contract_accepts_byte_order_mark(tmp_path):
    src = SOURCES_BY_NAME["category_translation"]
    f = tmp_path / src.file
    f.write_text("﻿" + ",".join(src.columns) + "\n", encoding="utf-8")
    extract.validate_contract(src, f)


def test_contract_rejects_renamed_column(tmp_path):
    src = SOURCES_BY_NAME["sellers"]
    f = tmp_path / src.file
    _write(f, ("seller_id", "seller_zip", "seller_city", "seller_state"))
    with pytest.raises(extract.SchemaContractError, match="missing=\\['seller_zip_code_prefix'\\]"):
        extract.validate_contract(src, f)


def test_contract_rejects_added_column(tmp_path):
    src = SOURCES_BY_NAME["sellers"]
    f = tmp_path / src.file
    _write(f, (*src.columns, "seller_tier"))
    with pytest.raises(extract.SchemaContractError, match="unexpected=\\['seller_tier'\\]"):
        extract.validate_contract(src, f)


def test_month_starts_splits_on_calendar_months():
    windows = list(month_starts(datetime(2017, 11, 15), datetime(2018, 2, 1)))
    assert windows == [
        (datetime(2017, 11, 1), datetime(2017, 12, 1)),
        (datetime(2017, 12, 1), datetime(2018, 1, 1)),
        (datetime(2018, 1, 1), datetime(2018, 2, 1)),
    ]
