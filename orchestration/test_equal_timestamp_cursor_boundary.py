from standalone.historical_continuity import scan_continuity_archive


def test_equal_timestamp_ids_cross_page_boundary_without_skip_or_duplicate():
    shared_timestamp = "2026-08-28 20:00:00+00:00"

    source = []

    # 198 records newer than the tied timestamp group.
    for record_id in range(400, 202, -1):
        source.append({
            "id": record_id,
            "timestamp": f"2026-08-28 21:{record_id % 60:02d}:00+00:00",
            "save_id": f"newer-{record_id}",
        })

    # Four records with EXACTLY the same timestamp.
    # With page_size=200:
    # page 1 ends on id 201,
    # page 2 must continue with ids 200 and 199.
    source.extend([
        {"id": 202, "timestamp": shared_timestamp, "save_id": "tie-202"},
        {"id": 201, "timestamp": shared_timestamp, "save_id": "tie-201"},
        {"id": 200, "timestamp": shared_timestamp, "save_id": "tie-200"},
        {"id": 199, "timestamp": shared_timestamp, "save_id": "tie-199"},
    ])

    # Older records after the tied group.
    source.extend([
        {
            "id": record_id,
            "timestamp": "2026-08-27 20:00:00+00:00",
            "save_id": f"older-{record_id}",
        }
        for record_id in range(198, 188, -1)
    ])

    # Production ordering contract.
    source = sorted(
        source,
        key=lambda record: (
            record["timestamp"],
            record["id"],
        ),
        reverse=True,
    )

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
                if (
                    record["timestamp"],
                    record["id"],
                ) < (
                    before_timestamp,
                    before_id,
                )
            ]

        return eligible[:payload["limit"]]

    records, meta = scan_continuity_archive(
        fetch_page,
        page_size=200,
    )

    returned_ids = [record["id"] for record in records]
    expected_ids = [record["id"] for record in source]

    assert len(calls) == 2

    # Page-1 boundary falls inside the equal-timestamp group.
    assert calls[1]["before_timestamp"] == shared_timestamp
    assert calls[1]["before_id"] == 201

    # Equal timestamp continues by descending lower ID.
    assert returned_ids[198:202] == [202, 201, 200, 199]

    # Cursor record appears exactly once.
    assert returned_ids.count(201) == 1

    # No tied records skipped.
    for tied_id in (202, 201, 200, 199):
        assert returned_ids.count(tied_id) == 1

    # Every fixture record returned exactly once.
    assert returned_ids == expected_ids
    assert len(returned_ids) == len(set(returned_ids))

    assert meta["exhaustive"] is True
    assert meta["scanned_count"] == len(source)
    assert meta["available_count"] == len(source)
    assert meta["errors"] == []
