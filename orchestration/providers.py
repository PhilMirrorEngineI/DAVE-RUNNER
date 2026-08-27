"""
PMEi WORKER PROVIDER ADAPTERS

Purpose
-------
Provide a narrow execution boundary between PMEi governed workers and
external/local inference providers.

The orchestration engine remains responsible for:
- which worker is active;
- whether a worker is eligible to act;
- causal state transitions;
- Human Gate placement.

The provider is responsible only for:
- receiving a bounded worker request;
- invoking the configured model/provider;
- returning model output.

A provider cannot:
- choose the next worker;
- approve a transition;
- manufacture Human Gate approval;
- write PMEi continuity;
- mutate orchestration state directly;
- deploy code;
- self-authorise.

Current provider support
------------------------
- Ollama local HTTP API.
- Disabled provider for deterministic tests / safe failure.

No external web access is performed here.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import requests


# =============================================================================
# CONFIG
# =============================================================================

DEFAULT_OLLAMA_URL = os.getenv(
    "OLLAMA_URL",
    "http://127.0.0.1:11434",
).rstrip("/")

DEFAULT_OLLAMA_MODEL = os.getenv(
    "OLLAMA_MODEL",
    "",
).strip()

DEFAULT_OLLAMA_TIMEOUT = float(
    os.getenv(
        "OLLAMA_TIMEOUT",
        "180",
    )
)

DEFAULT_OLLAMA_NUM_CTX = int(
    os.getenv(
        "OLLAMA_NUM_CTX",
        "8192",
    )
)

DEFAULT_OLLAMA_NUM_PREDICT = int(
    os.getenv(
        "OLLAMA_NUM_PREDICT",
        "512",
    )
)


# =============================================================================
# CONTRACTS
# =============================================================================

@dataclass
class ProviderRequest:
    """
    Bounded inference request supplied to a provider.

    This object contains no routing authority.
    """

    worker_role: str
    task: str

    system_prompt: str = ""

    context: Dict[str, Any] = field(
        default_factory=dict
    )

    model: str = ""

    temperature: float = 0.0

    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


@dataclass
class ProviderResponse:
    """
    Provider result returned to the worker execution layer.

    A ProviderResponse is not itself an Orchestration WorkerResult.
    Translation into a governed WorkerResult happens elsewhere.
    """

    provider: str
    model: str

    ok: bool

    output_text: str = ""

    raw: Dict[str, Any] = field(
        default_factory=dict
    )

    error: str = ""

    metadata: Dict[str, Any] = field(
        default_factory=dict
    )


# =============================================================================
# ERRORS
# =============================================================================

class ProviderError(RuntimeError):
    """Base provider execution error."""


class ProviderUnavailableError(ProviderError):
    """Configured provider is not reachable or available."""


class ProviderResponseError(ProviderError):
    """Provider returned an unusable response."""


# =============================================================================
# BASE PROVIDER
# =============================================================================

class BaseProvider:
    """
    Abstract provider boundary.

    Providers perform inference only.
    They do not receive orchestration transition authority.
    """

    provider_name = "base"

    def execute(
        self,
        request: ProviderRequest,
    ) -> ProviderResponse:

        raise NotImplementedError


# =============================================================================
# DISABLED PROVIDER
# =============================================================================

class DisabledProvider(BaseProvider):
    """
    Safe provider used when inference must remain disabled.

    Useful for tests and governance checks.
    """

    provider_name = "disabled"

    def execute(
        self,
        request: ProviderRequest,
    ) -> ProviderResponse:

        return ProviderResponse(
            provider=self.provider_name,
            model=request.model,
            ok=False,
            error="Provider execution is disabled.",
            metadata={
                "worker_role":
                    request.worker_role,
            },
        )


# =============================================================================
# OLLAMA PROVIDER
# =============================================================================

class OllamaProvider(BaseProvider):
    """
    Local Ollama inference adapter.

    Uses Ollama's /api/chat endpoint.

    The provider receives the active worker identity from the governed
    execution layer. It does not choose or alter worker routing.

    Worker inference is deliberately bounded by num_ctx and num_predict.
    This prevents a small governed worker task from inheriting an
    unnecessarily huge model context window.
    """

    provider_name = "ollama"

    def __init__(
        self,
        base_url: str = DEFAULT_OLLAMA_URL,
        model: str = DEFAULT_OLLAMA_MODEL,
        timeout: float = DEFAULT_OLLAMA_TIMEOUT,
        num_ctx: int = DEFAULT_OLLAMA_NUM_CTX,
        num_predict: int = DEFAULT_OLLAMA_NUM_PREDICT,
    ) -> None:

        self.base_url = (
            str(base_url)
            .strip()
            .rstrip("/")
        )

        self.model = (
            str(model)
            .strip()
        )

        self.timeout = float(
            timeout
        )

        self.num_ctx = int(
            num_ctx
        )

        self.num_predict = int(
            num_predict
        )

        if not self.base_url:

            raise ValueError(
                "Ollama base URL is required."
            )

        if self.num_ctx < 512:

            raise ValueError(
                "Ollama num_ctx must be at least 512."
            )

        if self.num_predict < 1:

            raise ValueError(
                "Ollama num_predict must be at least 1."
            )

    # -------------------------------------------------------------------------
    # HEALTH
    # -------------------------------------------------------------------------

    def health(
        self,
    ) -> Dict[str, Any]:

        url = (
            self.base_url
            +
            "/api/tags"
        )

        try:

            response = requests.get(
                url,
                timeout=min(
                    self.timeout,
                    15.0,
                ),
            )

        except requests.RequestException as exc:

            return {
                "ok":
                    False,

                "provider":
                    self.provider_name,

                "url":
                    self.base_url,

                "error":
                    str(exc),
            }

        if not response.ok:

            return {
                "ok":
                    False,

                "provider":
                    self.provider_name,

                "url":
                    self.base_url,

                "status_code":
                    response.status_code,

                "error":
                    response.text[:1000],
            }

        try:

            payload = response.json()

        except Exception as exc:

            return {
                "ok":
                    False,

                "provider":
                    self.provider_name,

                "url":
                    self.base_url,

                "error":
                    (
                        "Invalid Ollama health "
                        f"response: {exc}"
                    ),
            }

        models = []

        raw_models = payload.get(
            "models"
        )

        if isinstance(
            raw_models,
            list,
        ):

            for item in raw_models:

                if not isinstance(
                    item,
                    dict,
                ):
                    continue

                name = (
                    item.get(
                        "name"
                    )
                    or
                    item.get(
                        "model"
                    )
                )

                if name:

                    models.append(
                        str(name)
                    )

        return {
            "ok":
                True,

            "provider":
                self.provider_name,

            "url":
                self.base_url,

            "models":
                models,

            "configured_model":
                self.model,

            "num_ctx":
                self.num_ctx,

            "num_predict":
                self.num_predict,
        }

    # -------------------------------------------------------------------------
    # MODEL RESOLUTION
    # -------------------------------------------------------------------------

    def resolve_model(
        self,
        request: ProviderRequest,
    ) -> str:

        model = (
            request.model
            or
            self.model
        )

        model = str(
            model
        ).strip()

        if not model:

            raise ProviderError(
                "No Ollama model configured. "
                "Set OLLAMA_MODEL or supply "
                "ProviderRequest.model."
            )

        return model

    # -------------------------------------------------------------------------
    # MESSAGE BUILD
    # -------------------------------------------------------------------------

    def build_messages(
        self,
        request: ProviderRequest,
    ) -> list:

        messages = []

        system_prompt = (
            request.system_prompt
            or
            ""
        ).strip()

        if system_prompt:

            messages.append(
                {
                    "role":
                        "system",

                    "content":
                        system_prompt,
                }
            )

        context_text = ""

        if request.context:

            context_text = json.dumps(
                request.context,
                indent=2,
                ensure_ascii=False,
                default=str,
            )

        user_parts = [
            (
                "ACTIVE GOVERNED WORKER\n"
                f"{request.worker_role}"
            ),
            (
                "CURRENT TASK\n"
                f"{request.task}"
            ),
        ]

        if context_text:

            user_parts.append(
                (
                    "BOUNDED CONTEXT\n"
                    f"{context_text}"
                )
            )

        user_parts.append(
            """
AUTHORITY BOUNDARY

You are performing inference for the active worker only.

Do not choose the next worker.
Do not claim a transition has been approved.
Do not manufacture human approval.
Do not claim PMEi was written.
Do not deploy or self-modify.
Return only the work product for the active worker.
""".strip()
        )

        messages.append(
            {
                "role":
                    "user",

                "content":
                    "\n\n".join(
                        user_parts
                    ),
            }
        )

        return messages

    # -------------------------------------------------------------------------
    # EXECUTION
    # -------------------------------------------------------------------------

    def execute(
        self,
        request: ProviderRequest,
    ) -> ProviderResponse:

        model = self.resolve_model(
            request
        )

        url = (
            self.base_url
            +
            "/api/chat"
        )

        payload = {
            "model":
                model,

            "stream":
                False,

            "messages":
                self.build_messages(
                    request
                ),

            "options": {
                "temperature":
                    float(
                        request.temperature
                    ),

                "num_ctx":
                    self.num_ctx,

                "num_predict":
                    self.num_predict,
            },
        }

        try:

            response = requests.post(
                url,
                json=payload,
                timeout=self.timeout,
            )

        except requests.RequestException as exc:

            raise ProviderUnavailableError(
                "Ollama request failed: "
                f"{exc}"
            ) from exc

        if not response.ok:

            raise ProviderUnavailableError(
                "Ollama returned "
                f"HTTP {response.status_code}: "
                f"{response.text[:1500]}"
            )

        try:

            data = response.json()

        except Exception as exc:

            raise ProviderResponseError(
                "Ollama returned invalid JSON: "
                f"{exc}"
            ) from exc

        message = data.get(
            "message"
        )

        if not isinstance(
            message,
            dict,
        ):

            raise ProviderResponseError(
                "Ollama response contains "
                "no message object."
            )

        output_text = str(
            message.get(
                "content"
            )
            or
            ""
        ).strip()

        if not output_text:

            raise ProviderResponseError(
                "Ollama response contains "
                "no usable output text."
            )

        return ProviderResponse(
            provider=self.provider_name,

            model=model,

            ok=True,

            output_text=output_text,

            raw=data,

            metadata={
                "worker_role":
                    request.worker_role,

                "base_url":
                    self.base_url,

                "done":
                    data.get(
                        "done"
                    ),

                "done_reason":
                    data.get(
                        "done_reason"
                    ),

                "num_ctx":
                    self.num_ctx,

                "num_predict":
                    self.num_predict,

                "prompt_eval_count":
                    data.get(
                        "prompt_eval_count"
                    ),

                "eval_count":
                    data.get(
                        "eval_count"
                    ),

                "total_duration":
                    data.get(
                        "total_duration"
                    ),

                "load_duration":
                    data.get(
                        "load_duration"
                    ),
            },
        )


# =============================================================================
# FACTORY
# =============================================================================

def build_provider(
    provider_name: Optional[str] = None,
) -> BaseProvider:
    """
    Construct the configured inference provider.

    Current values:
        ollama
        disabled

    Default:
        ollama
    """

    name = (
        provider_name
        or
        os.getenv(
            "PMEI_WORKER_PROVIDER",
            "ollama",
        )
    )

    name = (
        str(name)
        .strip()
        .lower()
    )

    if name == "ollama":

        return OllamaProvider()

    if name in {
        "disabled",
        "none",
        "off",
    }:

        return DisabledProvider()

    raise ValueError(
        "Unknown PMEi worker provider: "
        f"{name}"
    )