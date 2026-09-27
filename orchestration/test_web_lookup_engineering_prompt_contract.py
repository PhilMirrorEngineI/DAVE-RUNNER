from orchestration.executor import WorkerExecutor
from orchestration.source_router import WEB_LOOKUP


def test_web_lookup_engineering_prompt_preserves_governed_disposition_contract():
    executor = object.__new__(WorkerExecutor)

    prompt = executor.system_prompt_for_worker(
        "engineering",
        source_route=WEB_LOOKUP,
    )

    # WEB_LOOKUP-specific evidence contract survives.
    assert "external sourced evidence" in prompt.lower()
    assert "Preserve source attribution." in prompt

    # Mandatory Engineering causal/output contract must also survive.
    assert "GOVERNED DISPOSITION" in prompt
    assert "status: READY_FOR_BUILD" in prompt
    assert "build_required: true" in prompt
    assert "status: NO_BUILD_REQUIRED" in prompt
    assert "build_required: false" in prompt
    assert "Do not emit next_worker." in prompt

def test_web_lookup_engineering_prompt_requires_complete_engineering_work_product():
    executor = object.__new__(WorkerExecutor)

    prompt = executor.system_prompt_for_worker(
        "engineering",
        source_route=WEB_LOOKUP,
    )

    marker = "Return Engineering work product using exactly these sections:"
    assert marker in prompt

    work_product = prompt[prompt.index(marker):]

    assert "SUPPORTED EVIDENCE" in work_product
    assert "ENGINEERING ANALYSIS" in work_product
    assert "UNVERIFIED" in work_product
    assert "BUILDER REQUIREMENT" in work_product
    assert "GOVERNED DISPOSITION" in work_product

    assert work_product.index("SUPPORTED EVIDENCE") < work_product.index("ENGINEERING ANALYSIS")
    assert work_product.index("ENGINEERING ANALYSIS") < work_product.index("UNVERIFIED")
    assert work_product.index("UNVERIFIED") < work_product.index("BUILDER REQUIREMENT")
    assert work_product.index("BUILDER REQUIREMENT") < work_product.index("GOVERNED DISPOSITION")
