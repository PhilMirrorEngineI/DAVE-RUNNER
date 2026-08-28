from datetime import datetime, timedelta, timezone

import pytest

from standalone.historical_continuity import scan_continuity_archive


def make_records(count):
    base = datetime(2026, 8, 28, 12, 0, tzinfo=timezone.utc)

    records = []

    for index in range(count):
        record_id = count - index

        records.append({
            "id": record_id,
            "timestamp": (
                base - timedelta(seconds=index)
            ).isoformat(),
            "save_id": f"record-{record_id}",
        })

    return records


def cursor_tuple(record):
    return (
        record["timestamp"],
        record["id"],
    )


def test_historical_scan_traverses_more_than_200_records():
    source = make_records(450)
    calls = []

    def fetch_page(payload):
        calls.append(dict(payload))

        before_timestamp = payload.get("before_timestamp")
        before_id = payload.get("before_id")

        eligible = source

        if before_timestamp is not None:
            eligible = [
                record
                for record in source
                if cursor_tuple(record)
                < (before_timestamp, before_id)
            ]

        return eligible[:payload["limit"]]

    records, meta = scan_continuity_archive(
        fetch_page,
        page_size=200,
    )

    assert len(records) == 450
    assert len({record["id"] for record in records}) == 450

    assert meta["exhaustive"] is True
    assert meta["scanned_count"] == 450
    assert meta["available_count"] == 450
    assert meta["pages"] == 3

    assert calls[0] == {
        "limit": 200,
    }

    assert calls[1]["before_timestamp"] == source[199]["timestamp"]
    assert calls[1]["before_id"] == source[199]["id"]


def test_historical_scan_deduplicates_record_ids():
    source = make_records(250)
    call_number = 0

    def fetch_page(payload):
        nonlocal call_number
        call_number += 1

        if call_number == 1:
            return source[:200]

        if call_number == 2:
            # Deliberately repeat the previous page boundary.
            return [source[199]] + source[200:]

        return []

    records, meta = scan_continuity_archive(
        fetch_page,
        page_size=200,
    )

    assert len(records) == 250
    assert len({record["id"] for record in records}) == 250
    assert meta["exhaustive"] is True
    assert meta["scanned_count"] == 250
    assert meta["available_count"] == 250


def test_newer_insert_between_pages_does_not_change_older_scope():
    source = make_records(450)
    original_ids = {record["id"] for record in source}

    calls = 0

    def fetch_page(payload):
        nonlocal calls
        calls += 1

        if calls == 2:
            source.insert(0, {
                "id": 9999,
                "timestamp": (
                    datetime(
                        2026,
                        8,
                        29,
                        12,
                        0,
                        tzinfo=timezone.utc,
                    ).isoformat()
                ),
                "save_id": "newer-insert",
            })

        before_timestamp = payload.get("before_timestamp")
        before_id = payload.get("before_id")

        eligible = source

        if before_timestamp is not None:
            eligible = [
                record
                for record in source
                if cursor_tuple(record)
                < (before_timestamp, before_id)
            ]

        return eligible[:payload["limit"]]

    records, meta = scan_continuity_archive(
        fetch_page,
        page_size=200,
    )

    returned_ids = {record["id"] for record in records}

    assert returned_ids == original_ids
    assert 9999 not in returned_ids
    assert meta["exhaustive"] is True
    assert meta["available_count"] == 450


def test_mid_scan_transport_failure_fails_closed():
    source = make_records(450)
    calls = 0

    def fetch_page(payload):
        nonlocal calls
        calls += 1

        if calls == 1:
            return source[:200]

        raise RuntimeError("simulated transport failure")

    records, meta = scan_continuity_archive(
        fetch_page,
        page_size=200,
    )

    assert len(records) == 200

    assert meta["exhaustive"] is False
    assert meta["scanned_count"] == 200
    assert meta["available_count"] is None
    assert meta["pages"] == 1
    assert meta["errors"]


def test_page_size_cannot_exceed_api_maximum():
    seen = []

    def fetch_page(payload):
        seen.append(dict(payload))
        return []

    records, meta = scan_continuity_archive(
        fetch_page,
        page_size=500,
    )

    assert records == []
    assert seen == [{"limit": 200}]
    assert meta["exhaustive"] is True
    assert meta["available_count"] == 0
