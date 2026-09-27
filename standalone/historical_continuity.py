API_MAX_PAGE_SIZE = 200


def scan_continuity_archive(
    fetch_page,
    page_size=API_MAX_PAGE_SIZE,
):
    """
    Traverse continuity records newest-to-oldest using a compound
    (timestamp, id) cursor.

    fetch_page(payload) must return a list of continuity records.

    This function:
      - clamps every page to the API maximum
      - advances strictly from the last returned record
      - deduplicates by record id
      - terminates deterministically
      - fails closed if traversal cannot be proven exhaustive
    """

    try:
        requested_page_size = int(page_size)
    except (TypeError, ValueError):
        requested_page_size = API_MAX_PAGE_SIZE

    page_limit = min(
        max(requested_page_size, 1),
        API_MAX_PAGE_SIZE,
    )

    records = []
    seen_ids = set()

    meta = {
        "exhaustive": False,
        "scanned_count": 0,
        "available_count": None,
        "pages": 0,
        "page_size": page_limit,
        "errors": [],
    }

    before_timestamp = None
    before_id = None
    previous_cursor = None

    while True:
        payload = {
            "limit": page_limit,
        }

        if before_timestamp is not None:
            payload["before_timestamp"] = before_timestamp
            payload["before_id"] = before_id

        try:
            page = fetch_page(payload)
        except Exception as error:
            meta["errors"].append(
                f"{type(error).__name__}: {error}"
            )
            meta["scanned_count"] = len(records)
            return records, meta

        if not isinstance(page, list):
            meta["errors"].append(
                "Historical continuity page was not a list"
            )
            meta["scanned_count"] = len(records)
            return records, meta

        meta["pages"] += 1

        if not page:
            meta["exhaustive"] = True
            meta["scanned_count"] = len(records)
            meta["available_count"] = len(records)
            return records, meta

        for record in page:
            if not isinstance(record, dict):
                meta["errors"].append(
                    "Historical continuity record was not an object"
                )
                meta["scanned_count"] = len(records)
                return records, meta

            record_id = record.get("id")

            if record_id is None:
                meta["errors"].append(
                    "Historical continuity record missing id"
                )
                meta["scanned_count"] = len(records)
                return records, meta

            if record_id not in seen_ids:
                seen_ids.add(record_id)
                records.append(record)

        boundary = page[-1]

        boundary_timestamp = boundary.get("timestamp")
        boundary_id = boundary.get("id")

        if boundary_timestamp is None or boundary_id is None:
            meta["errors"].append(
                "Historical continuity page boundary missing timestamp or id"
            )
            meta["scanned_count"] = len(records)
            return records, meta

        current_cursor = (
            boundary_timestamp,
            boundary_id,
        )

        if current_cursor == previous_cursor:
            meta["errors"].append(
                "Historical continuity cursor did not advance"
            )
            meta["scanned_count"] = len(records)
            return records, meta

        previous_cursor = current_cursor

        if len(page) < page_limit:
            meta["exhaustive"] = True
            meta["scanned_count"] = len(records)
            meta["available_count"] = len(records)
            return records, meta

        before_timestamp = boundary_timestamp
        before_id = boundary_id
