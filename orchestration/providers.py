from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import requests

from .ollama_worker_transport import receive_worker_response, WorkerTransportError


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
        "1536",
    )
)


@dataclass
class ProviderRequest:
    worker_role: str
    task: str
    system_prompt: str = ""
    context: Dict[str, Any] = field(default_factory=dict)
    model: str = ""
    temperature: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    output_schema: Optional[Dict[str, Any]] = None


@dataclass
class ProviderResponse:
    provider: str
    model: str
    ok: bool
    output_text: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)
    error: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


class ProviderError(RuntimeError):
    def __init__(self, message, *, provider_diagnostics=None):
        super().__init__(message)
        self.provider_diagnostics = provider_diagnostics


class ProviderUnavailableError(ProviderError):
    pass


class ProviderResponseError(ProviderError):
    pass


class BaseProvider:
    provider_name = "base"

    def execute(
        self,
        request: ProviderRequest,
    ) -> ProviderResponse:
        raise NotImplementedError


class DisabledProvider(BaseProvider):
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
                "worker_role": request.worker_role,
            },
        )


class OllamaProvider(BaseProvider):
    """
    Bounded local Ollama provider.

    The provider performs inference only. It does not choose workers,
    advance orchestration state, approve transitions, write PMEi,
    or manufacture human approval.

    Liquid currently may emit <think>...</think> inside message.content
    even when think=False is supplied. Those wrappers are removed before
    the worker work product is returned.
    """

    provider_name = "ollama"

    def __init__(
        self,
        base_url: str = DEFAULT_OLLAMA_URL,
        model: str = DEFAULT_OLLAMA_MODEL,
        timeout: float = DEFAULT_OLLAMA_TIMEOUT,
        num_ctx: int = DEFAULT_OLLAMA_NUM_CTX,
        num_predict: int = DEFAULT_OLLAMA_NUM_PREDICT,
        num_gpu: int | None = None,
        worker_max_seconds: float | None = None,
    ) -> None:
        self.base_url = str(base_url).strip().rstrip("/")
        self.model = str(model).strip()
        self.timeout = float(timeout)
        self.worker_max_seconds = float(
            os.getenv("OLLAMA_WORKER_MAX_SECONDS", "600")
            if worker_max_seconds is None else worker_max_seconds
        )
        self.num_ctx = int(num_ctx)
        self.num_predict = int(num_predict)
        self.num_gpu = (
            None
            if num_gpu is None
            else int(num_gpu)
        )

        if not self.base_url:
            raise ValueError("Ollama base URL is required.")

        if self.num_ctx < 512:
            raise ValueError("Ollama num_ctx must be at least 512.")

        if self.num_predict < 1:
            raise ValueError("Ollama num_predict must be at least 1.")

    def health(
        self,
    ) -> Dict[str, Any]:
        url = self.base_url + "/api/tags"

        try:
            response = requests.get(
                url,
                timeout=min(self.timeout, 15.0),
            )
        except requests.RequestException as exc:
            return {
                "ok": False,
                "provider": self.provider_name,
                "url": self.base_url,
                "error": str(exc),
            }

        if not response.ok:
            return {
                "ok": False,
                "provider": self.provider_name,
                "url": self.base_url,
                "status_code": response.status_code,
                "error": response.text[:1000],
            }

        try:
            payload = response.json()
        except Exception as exc:
            return {
                "ok": False,
                "provider": self.provider_name,
                "url": self.base_url,
                "error": f"Invalid Ollama health response: {exc}",
            }

        models = []

        for item in payload.get("models") or []:
            if not isinstance(item, dict):
                continue

            name = item.get("name") or item.get("model")

            if name:
                models.append(str(name))

        return {
            "ok": True,
            "provider": self.provider_name,
            "url": self.base_url,
            "models": models,
            "configured_model": self.model,
            "num_ctx": self.num_ctx,
            "num_predict": self.num_predict,
        }

    def resolve_model(
        self,
        request: ProviderRequest,
    ) -> str:
        model = str(
            request.model
            or
            self.model
            or
            ""
        ).strip()

        if not model:
            raise ProviderError(
                "No Ollama model configured. "
                "Set OLLAMA_MODEL or supply ProviderRequest.model."
            )

        return model

    def build_messages(
        self,
        request: ProviderRequest,
    ) -> list:
        """
        Build the smallest provider-facing message set.

        Preferred path:
            governed worker role
            + deterministic PMEi worker packet

        Raw PMEi continuity passages are not reconstructed here.

        If no deterministic worker packet is present, fall back to the
        supplied bounded context for compatibility with non-PMEi callers.
        """

        messages = []

        system_prompt = str(
            request.system_prompt
            or
            ""
        ).strip()

        if system_prompt:
            messages.append(
                {
                    "role": "system",
                    "content": system_prompt,
                }
            )

        worker_packet = ""

        if isinstance(
            request.context,
            dict,
        ):
            pmei_evidence = request.context.get(
                "pmei_evidence"
            )

            if isinstance(
                pmei_evidence,
                dict,
            ):
                worker_packet = str(
                    pmei_evidence.get(
                        "worker_packet"
                    )
                    or
                    ""
                ).strip()

        parts = [
            (
                "ACTIVE GOVERNED WORKER\n"
                f"{request.worker_role}"
            ),
        ]

        if worker_packet:

            parts.append(
                (
                    "PMEI GOVERNED INPUT\n"
                    f"{worker_packet}"
                )
            )

        else:

            parts.append(
                (
                    "CURRENT TASK\n"
                    f"{request.task}"
                )
            )

            if request.context:

                context_text = json.dumps(
                    request.context,
                    indent=2,
                    ensure_ascii=False,
                    default=str,
                )

                parts.append(
                    (
                        "BOUNDED CONTEXT\n"
                        f"{context_text}"
                    )
                )

        # Separate restrictions from factual evidence. Only the executor-bound
        # snapshot is used; arbitrary job.context and provider output are ignored.
        if isinstance(request.context, dict) and "task_requirements" in request.context:
            from .task_requirements import render_task_requirements, TaskRequirementsError
            try:
                requirements = render_task_requirements(
                    request.context["task_requirements"],
                    expected_job_id=(request.metadata.get("job_id")
                                     if isinstance(request.metadata, dict) else None),
                    expected_worker=request.worker_role,
                )
            except TaskRequirementsError as exc:
                raise ProviderError("Task requirements blocked: " + str(exc)) from exc
            if requirements:
                parts.append(requirements)

        # The executor binds this packet to engine history and the active role.
        # Carry it even when PMEi evidence exists; do not serialize raw history
        # or job.context, which may contain unrelated/untrusted material.
        if isinstance(request.context, dict) and request.context.get("worker_handoff") is not None:
            from .worker_handoff import render_worker_handoff
            parts.append(render_worker_handoff(
                request.context["worker_handoff"], expected_worker=request.worker_role))

        parts.append(
            """EXECUTION BOUNDARY

Return only the work product for the active worker.

Do not choose the next worker.
Do not advance orchestration state.
Do not manufacture human approval."""
        )

        messages.append(
            {
                "role": "user",
                "content": "\n\n".join(
                    parts
                ),
            }
        )

        return messages

    def clean_output_text(
        self,
        value: str,
    ) -> str:
        text = str(value or "").strip()

        if not text:
            return ""

        text = re.sub(
            r"<think>.*?</think>",
            "",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        ).strip()

        return text

    def execute(
        self,
        request: ProviderRequest,
    ) -> ProviderResponse:
        model = self.resolve_model(request)

        payload = {
            "model": model,
            "stream": False,
            "think": False,
            "messages": self.build_messages(request),
            "options": {
                "temperature": float(request.temperature),
                "num_ctx": self.num_ctx,
                "num_predict": self.num_predict,
                **(
                    {"num_gpu": self.num_gpu}
                    if self.num_gpu is not None
                    else {}
                ),
            },
        }

        if request.output_schema is not None:
            if not isinstance(request.output_schema, dict) or not request.output_schema:
                raise ProviderError("output_schema must be a non-empty JSON schema object.")
            payload["format"] = request.output_schema

        diagnostics = None
        if isinstance(request.metadata, dict) and request.metadata.get("worker_work_product") is True:
            try:
                data, diagnostics = receive_worker_response(
                    self.base_url + "/api/chat", payload, timeout=self.timeout,
                    max_seconds=self.worker_max_seconds,
                )
            except WorkerTransportError as exc:
                raise ProviderUnavailableError(
                    str(exc), provider_diagnostics=exc.provider_diagnostics,
                ) from exc
        else:
            try:
                response = requests.post(
                    self.base_url + "/api/chat",
                    json=payload,
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                raise ProviderUnavailableError(
                    f"Ollama request failed: {exc}"
                ) from exc

            if not response.ok:
                raise ProviderUnavailableError(
                    f"Ollama returned HTTP {response.status_code}: "
                    f"{response.text[:1500]}"
                )

            try:
                data = response.json()
            except Exception as exc:
                raise ProviderResponseError(
                    f"Ollama returned invalid JSON: {exc}"
                ) from exc

        message = data.get("message")

        if not isinstance(message, dict):
            raise ProviderResponseError(
                "Ollama response contains no message object.", provider_diagnostics=diagnostics
            )

        raw_content = str(
            message.get("content")
            or
            ""
        ).strip()

        done_reason = data.get("done_reason")

        # Fail closed if a reasoning wrapper starts but never closes.
        # This means the bounded generation budget was consumed before a
        # governed worker answer was produced.
        lower_content = raw_content.lower()

        if (
            "<think>" in lower_content
            and
            "</think>" not in lower_content
        ):
            raise ProviderResponseError(
                "Ollama generation ended inside an unfinished <think> block. "
                "No governed worker answer was produced. "
                f"done_reason={done_reason}; "
                f"num_predict={self.num_predict}.", provider_diagnostics=diagnostics
            )

        output_text = self.clean_output_text(
            raw_content
        )

        if not output_text:
            if done_reason in {
                "length",
                "max_tokens",
            }:
                raise ProviderResponseError(
                    "Ollama exhausted the bounded prediction budget "
                    "before producing a usable worker answer. "
                    f"num_predict={self.num_predict}.", provider_diagnostics=diagnostics
                )

            raise ProviderResponseError(
                "Ollama response contains no usable output text.", provider_diagnostics=diagnostics
            )

        return ProviderResponse(
            provider=self.provider_name,
            model=model,
            ok=True,
            output_text=output_text,
            raw=data,
            metadata={
                "worker_role": request.worker_role,
                **({"provider_diagnostics": diagnostics} if diagnostics else {}),
                "base_url": self.base_url,
                "done": data.get("done"),
                "done_reason": done_reason,
                "num_ctx": self.num_ctx,
                "num_predict": self.num_predict,
                "prompt_eval_count": data.get("prompt_eval_count"),
                "eval_count": data.get("eval_count"),
                "total_duration": data.get("total_duration"),
                "load_duration": data.get("load_duration"),
                "prompt_eval_duration": data.get("prompt_eval_duration"),
                "eval_duration": data.get("eval_duration"),
                "reasoning_wrapper_removed": raw_content != output_text,
            },
        )


def build_provider(
    provider_name: Optional[str] = None,
) -> BaseProvider:
    name = (
        provider_name
        or
        os.getenv(
            "PMEI_WORKER_PROVIDER",
            "ollama",
        )
    )

    name = str(name).strip().lower()

    if name == "ollama":
        return OllamaProvider()

    if name in {
        "disabled",
        "none",
        "off",
    }:
        return DisabledProvider()

    raise ValueError(
        f"Unknown PMEi worker provider: {name}"
    )
