import standalone.notepad as notepad


class FakeResponse:
    def __init__(self, records):
        self.records = records

    def raise_for_status(self):
        return None

    def json(self):
        return {
            "data": {
                "count": len(self.records),
                "items": self.records,
            }
        }


class FakeSession:
    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []

    def post(
        self,
        url,
        headers=None,
        json=None,
        timeout=None,
    ):
        self.calls.append({
            "url": url,
            "headers": headers,
            "json": dict(json or {}),
            "timeout": timeout,
        })

        if self.pages:
            records = self.pages.pop(0)
        else:
            records = []

        return FakeResponse(records)


def make_record(record_id, timestamp):
    return {
        "id": record_id,
        "timestamp": timestamp,
        "save_id": f"record-{record_id}",
    }


def test_normal_pmei_retrieval_remains_one_call_at_100(monkeypatch):
    fake = FakeSession([
        [
            make_record(
                10,
                "2026-08-28T12:00:00+00:00",
            )
        ]
    ])

    monkeypatch.setattr(
        notepad,
        "API_KEY",
        "test-key",
    )
    monkeypatch.setattr(
        notepad,
        "session",
        fake,
    )

    records, meta = notepad.get_pmei_records()

    assert len(records) == 1
    assert meta["success"] is True
    assert len(fake.calls) == 1
    assert fake.calls[0]["json"] == {
        "limit": 100,
    }


def test_historical_transport_uses_cursor_route(monkeypatch):
    first_page = [
        make_record(
            record_id,
            (
                "2026-08-28T12:"
                f"{(200 - record_id) // 60:02d}:"
                f"{(200 - record_id) % 60:02d}+00:00"
            ),
        )
        for record_id in range(200, 0, -1)
    ]

    second_page = [
        make_record(
            0,
            "2026-08-28T11:00:00+00:00",
        )
    ]

    fake = FakeSession([
        first_page,
        second_page,
    ])

    monkeypatch.setattr(
        notepad,
        "API_KEY",
        "test-key",
    )
    monkeypatch.setattr(
        notepad,
        "session",
        fake,
    )

    records, meta = (
        notepad.get_pmei_historical_records()
    )

    assert len(records) == 201
    assert meta["exhaustive"] is True
    assert meta["scanned_count"] == 201
    assert meta["available_count"] == 201

    assert len(fake.calls) == 2

    assert fake.calls[0]["url"].endswith(
        "/memory/continuity/get"
    )
    assert fake.calls[0]["json"] == {
        "limit": 200,
    }

    assert fake.calls[1]["url"].endswith(
        "/memory/continuity/get"
    )

    assert fake.calls[1]["json"]["limit"] == 200
    assert (
        fake.calls[1]["json"]["before_timestamp"]
        ==
        first_page[-1]["timestamp"]
    )
    assert (
        fake.calls[1]["json"]["before_id"]
        ==
        first_page[-1]["id"]
    )


def test_historical_transport_failure_is_not_success(monkeypatch):
    class BrokenSession:
        def post(self, *args, **kwargs):
            raise RuntimeError(
                "simulated historical transport failure"
            )

    monkeypatch.setattr(
        notepad,
        "API_KEY",
        "test-key",
    )
    monkeypatch.setattr(
        notepad,
        "session",
        BrokenSession(),
    )

    records, meta = (
        notepad.get_pmei_historical_records()
    )

    assert records == []
    assert meta["success"] is False
    assert meta["exhaustive"] is False
    assert meta["available_count"] is None
    assert meta["errors"]
