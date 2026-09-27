from orchestration.engineering_work_product import EngineeringWorkProductContract
from orchestration.executor import WorkerExecutor
from orchestration.worker_packet import build_worker_packet_builder


BAKERY_TASK = (
    "I run a small independent bakery. Wholesale flour costs have risen 14%, "
    "but raising all my prices risks losing regular customers. How should I "
    "decide which products to increase, which to leave unchanged, and what "
    "evidence should I gather before making the decision?"
)


def empty_packet():
    return build_worker_packet_builder().build(
        worker_role="engineering",
        task=BAKERY_TASK,
        evidence_packet={
            "retrieval_ok": False,
            "records_received": 0,
            "evidence": [],
            "transport": {},
            "error": "No independent evidence.",
        },
        job_id="bakery-regression",
    )


def test_bakery_task_is_preserved_as_user_supplied_unverified_not_absent():
    packet = empty_packet()
    text = packet.rendered_text
    assert "Wholesale flour costs have risen 14%" in text
    assert "USER-SUPPLIED CONTEXT BOUNDARY:" in text
    assert "user-supplied context, not independently verified evidence" in text
    assert "Do not describe information explicitly present in TASK as absent" in text
    assert "NO DIRECT EVIDENCE" in text


def test_shared_worker_prompt_preserves_user_report_vs_absence_boundary():
    prompt = WorkerExecutor.system_prompt_for_worker.__get__(
        object.__new__(WorkerExecutor), WorkerExecutor
    )("governance")
    assert "USER-SUPPLIED CONTEXT" in prompt
    assert "explicitly supplied premise absent" in prompt
    assert "not independently verified evidence" in prompt


def test_structured_engineering_prompt_repeats_same_boundary():
    contract = EngineeringWorkProductContract.for_task(
        worker_role="engineering",
        task=BAKERY_TASK,
        deliverable="PLAN",
    )
    prompt = contract.prompt()
    assert "Preserve explicit premises from the original task" in prompt
    assert "user-reported / UNVERIFIED" in prompt
    assert "calling them absent solely because independent evidence is missing" in prompt
