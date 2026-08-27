"""
PMEi PERSISTENCE TRANSPORT

Network-capable transport boundary for governed PMEi persistence.

IMPORTANT:
- Disabled by default.
- A request cannot be sent unless explicitly enabled.
- This layer does not decide what is save-worthy.
- This layer does not create authority.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from urllib import request as urllib_request
from urllib.error import HTTPError, URLError

from .pmei_persistence_adapter import PMEiSaveRequest


TRANSPORT_DISABLED = "TRANSPORT_DISABLED"
TRANSPORT_SENT = "TRANSPORT_SENT"
TRANSPORT_FAILED = "TRANSPORT_FAILED"


@dataclass(frozen=True)
class PMEiTransportResult:
    ok: bool
    status: str
    request_sent: bool
    status_code: int | None = None
    response_data: dict | None = None
    error: str = ""


class PMEiPersistenceTransport:

    def __init__(
        self,
        *,
        base_url: str = "",
        api_key: str = "",
        enabled: bool = False,
        timeout: float = 15.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.enabled = enabled
        self.timeout = timeout

    def send(
        self,
        save_request: PMEiSaveRequest,
    ) -> PMEiTransportResult:

        if not isinstance(
            save_request,
            PMEiSaveRequest,
        ):
            return PMEiTransportResult(
                ok=False,
                status=TRANSPORT_FAILED,
                request_sent=False,
                error="PMEiSaveRequest required",
            )

        if not self.enabled:
            return PMEiTransportResult(
                ok=False,
                status=TRANSPORT_DISABLED,
                request_sent=False,
                error=(
                    "live PMEi persistence transport "
                    "is disabled"
                ),
            )

        if not self.base_url:
            return PMEiTransportResult(
                ok=False,
                status=TRANSPORT_FAILED,
                request_sent=False,
                error="PMEi base URL is required",
            )

        if not self.api_key:
            return PMEiTransportResult(
                ok=False,
                status=TRANSPORT_FAILED,
                request_sent=False,
                error="PMEi API key is required",
            )

        url = (
            self.base_url
            + save_request.path
        )

        body = json.dumps(
            save_request.payload
        ).encode("utf-8")

        req = urllib_request.Request(
            url=url,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Authorization": (
                    f"Bearer {self.api_key}"
                ),
            },
        )

        try:
            with urllib_request.urlopen(
                req,
                timeout=self.timeout,
            ) as response:

                status_code = response.getcode()

                raw = response.read().decode(
                    "utf-8"
                )

                response_data = (
                    json.loads(raw)
                    if raw
                    else {}
                )

                return PMEiTransportResult(
                    ok=True,
                    status=TRANSPORT_SENT,
                    request_sent=True,
                    status_code=status_code,
                    response_data=response_data,
                )

        except HTTPError as exc:

            return PMEiTransportResult(
                ok=False,
                status=TRANSPORT_FAILED,
                request_sent=True,
                status_code=exc.code,
                error=str(exc),
            )

        except (URLError, OSError) as exc:

            return PMEiTransportResult(
                ok=False,
                status=TRANSPORT_FAILED,
                request_sent=True,
                error=str(exc),
            )


def build_pmei_persistence_transport(
    *,
    base_url: str = "",
    api_key: str = "",
    enabled: bool = False,
    timeout: float = 15.0,
):
    return PMEiPersistenceTransport(
        base_url=base_url,
        api_key=api_key,
        enabled=enabled,
        timeout=timeout,
    )
