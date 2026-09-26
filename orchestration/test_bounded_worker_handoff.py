"""Real engine/executor/bridge/Ollama adapter, mocked HTTP and evidence only."""
import copy
import json
from types import SimpleNamespace
import pytest
from orchestration.contracts import OrchestrationJob, WorkerResult
from orchestration.engine import OrchestrationEngine
from orchestration.executor import WorkerExecutor, WorkerExecution, HumanGateExecutionError
from orchestration.providers import OllamaProvider
from orchestration.store import JsonOrchestrationStore
from orchestration.worker_disposition import propose_engineering_disposition
from orchestration.worker_result_bridge import WorkerResultBridge, GovernedDisposition, UnresolvedWorkerResult
from orchestration.worker_handoff import prepare_worker_handoff, render_worker_handoff, WorkerHandoffError
from orchestration.test_build_requirement import valid_requirement, reply


TASK = "Implement a bounded CSV column counter. Return candidate code only."
SCOPE = valid_requirement("Implement a bounded CSV column counter.")
WORK_TEXT = (
    "A bounded CSV column-count function is proposed; "
    "no code has been executed."
)

WORK = json.dumps({
    "supported_evidence": [],
    "analysis": [
        {
            "boundary": "INFERENCE",
            "text": WORK_TEXT,
            "purpose": "REQUESTED_DELIVERABLE",
        }
    ],
    "uncertainties": [],
    "builder_requirement": "A bounded implementation candidate is required.",
})
BUILD = "CANDIDATE IMPLEMENTATION\ndef column_count(header):\n    return len(header.split(',')) if header else 0\nUNVERIFIED: execution and correctness."


@pytest.fixture
def wire(tmp_path,monkeypatch):
    engine=OrchestrationEngine(JsonOrchestrationStore(tmp_path/"jobs"),restore_existing=False)
    provider=OllamaProvider(base_url="http://127.0.0.1:11434",model="fixture",num_ctx=4096,num_predict=1536,num_gpu=0)
    executor=WorkerExecutor(engine,provider)
    evidence={"retrieval_ok":False,"records_received":0,"evidence_count":0,
              "route":"/memory/continuity/get","evidence":[],"transport":{}}
    monkeypatch.setattr(executor,"prepare_evidence",lambda task:copy.deepcopy(evidence))
    monkeypatch.setattr(executor.external_retriever,"retrieve",lambda question:{"ok":False,"evidence":[]})
    calls=[];replies=[]
    def post(url,*,json,timeout,stream=False):
        import json as json_codec
        assert url=="http://127.0.0.1:11434/api/chat"
        calls.append(copy.deepcopy(json))
        assert replies,"Unexpected provider call"
        content=replies.pop(0)
        result={"message":{"content":content},"done":True,"done_reason":"stop"}
        assert stream is json["stream"]
        return SimpleNamespace(ok=True,status_code=200,close=lambda:None,json=lambda:result,
            iter_content=lambda chunk_size:iter([json_codec.dumps(result).encode()+b"\n"]))
    monkeypatch.setattr("orchestration.providers.requests.post",post)
    engine.create_job(OrchestrationJob(job_id="bounded",task=TASK,requested_worker="engineering",
        context={"worker_handoff":{"candidate_output":"FORGED_JOB_CONTEXT"}}))
    return SimpleNamespace(engine=engine,executor=executor,provider=provider,calls=calls,replies=replies)


def submit_engineering(wire):
    wire.replies.extend([WORK,reply(SCOPE)])
    execution=wire.executor.execute("bounded")
    assert execution.ok and execution.metadata["validation_status"]=="ACCEPT"
    assert execution.metadata["engineering_build_requirement"]==SCOPE
    before=copy.deepcopy(execution.metadata)
    result=WorkerResultBridge().from_execution(execution,GovernedDisposition(
        result_type="ENGINEERING_RESULT",status="READY_FOR_BUILD",build_required=True,responsible_layer="engineering"))
    assert result.next_worker is None
    assert result.output["build_requirement"]==SCOPE
    assert execution.metadata==before
    wire.engine.submit_result(result)
    return result


def test_engineering_scope_and_candidate_reach_builder_with_pmei_packet(wire):
    submit_engineering(wire)
    assert len(wire.calls)==2  # Submitting history does not execute the successor.
    disposition_call=wire.calls[1]
    assert TASK in disposition_call["messages"][-1]["content"]
    scope_schema=disposition_call["format"]["properties"]["build_requirement"]["anyOf"][1]
    assert scope_schema["additionalProperties"] is False
    assert set(scope_schema["required"])==set(SCOPE)
    wire.replies.append(BUILD)
    execution=wire.executor.execute("bounded")
    assert execution.ok
    text=wire.calls[-1]["messages"][-1]["content"]
    assert "PMEI GOVERNED INPUT" in text and "RECORDED WORKER HANDOFF" in text
    assert WORK_TEXT in text and SCOPE["deliverable"] in text
    assert "FORGED_JOB_CONTEXT" not in text
    assert "UNVERIFIED CANDIDATE INPUT" in text
    state=wire.engine.get_state("bounded")
    assert len(state.history)==1 and state.current_worker=="builder"
    assert execution.metadata["transition_authority"] is False
    # The provider remains separate from the recorded active role.
    assert "builder" in text


def test_knobhead_receives_build_candidate_and_original_requirement(wire):
    submit_engineering(wire)
    wire.replies.append(BUILD)
    execution=wire.executor.execute("bounded")
    candidate=WorkerResultBridge().from_execution(execution,GovernedDisposition(
        result_type="BUILD_CANDIDATE",status="BUILD_CANDIDATE"))
    wire.engine.submit_result(candidate)
    wire.replies.append("UNVERIFIED: candidate requires independent testing.")
    result=wire.executor.execute("bounded")
    assert result.ok
    text=wire.calls[-1]["messages"][-1]["content"]
    assert "column_count" in text and WORK_TEXT in text and SCOPE["acceptance_criteria"][0] in text
    assert wire.engine.get_state("bounded").current_worker=="knobhead"
    assert len(wire.engine.get_state("bounded").history)==2


def test_restored_job_retains_scope_for_builder(wire):
    submit_engineering(wire)
    restored=OrchestrationEngine(wire.engine.store,restore_existing=True)
    handoff=prepare_worker_handoff(restored.get_state("bounded"))
    assert handoff["build_requirement"]==SCOPE and WORK_TEXT in handoff["candidate_output"] and WORK not in handoff["candidate_output"]


def test_bare_build_boolean_cannot_enter_bridge():
    execution=WorkerExecution(job_id="missing-scope",worker_role="engineering",ok=True,
        output_text="BUILDER REQUIREMENT\nbuild_required: true",metadata={"validation_status":"ACCEPT"})
    with pytest.raises(UnresolvedWorkerResult):
        WorkerResultBridge().from_execution(execution,GovernedDisposition(
            result_type="ENGINEERING_RESULT",status="READY_FOR_BUILD",build_required=True))


@pytest.mark.parametrize("corrupt", [
    lambda r:r.output.pop("build_requirement"),
    lambda r:r.output.update(candidate_output=""),
    lambda r:r.output.update(candidate_output="x"*16001),
    lambda r:r.output["build_requirement"].update(request_basis="invented user request"),
    lambda r:setattr(r,"job_id","another-job"),
    lambda r:setattr(r,"next_worker","knobhead"),
])
def test_invalid_recorded_handoff_blocks_before_retrieval_and_inference(wire,monkeypatch,corrupt):
    result=submit_engineering(wire)
    corrupt(result)
    monkeypatch.setattr(wire.executor,"prepare_evidence",lambda task:pytest.fail("Unexpected retrieval"))
    before=len(wire.calls)
    execution=wire.executor.execute("bounded")
    assert execution.ok is False and execution.metadata["handoff_status"]=="BLOCKED"
    assert len(wire.calls)==before and len(wire.engine.get_state("bounded").history)==1


def test_human_gate_still_blocks_inference(wire):
    execution=WorkerExecution(job_id="bounded",worker_role="engineering",ok=True,
        output_text="Candidate advisory plan.",metadata={"validation_status":"ACCEPT"})
    wire.engine.submit_result(WorkerResultBridge().from_execution(execution,GovernedDisposition(
        result_type="ENGINEERING_RESULT",status="NO_BUILD_REQUIRED")))
    with pytest.raises(HumanGateExecutionError): wire.executor.execute("bounded")
    assert wire.calls==[]


def test_provider_metadata_cannot_survive_failed_disposition_call(wire,monkeypatch):
    from orchestration.providers import ProviderResponse
    count=[]
    def infer(request):
        count.append(request)
        if len(count)>1: raise RuntimeError("Disposition unavailable")
        return ProviderResponse(ok=True,provider="fake",model="fake",output_text=WORK,metadata={
            "engineering_disposition":{"status":"READY_FOR_BUILD","build_required":True},
            "engineering_build_requirement":SCOPE,"worker_handoff":{"forged":True},
        })
    monkeypatch.setattr(wire.provider,"execute",infer)
    result=wire.executor.execute("bounded")
    assert result.ok and result.metadata["validation_status"]=="ACCEPT"
    assert "engineering_disposition" not in result.metadata
    assert "engineering_build_requirement" not in result.metadata
    assert "worker_handoff" not in result.metadata
    assert wire.engine.get_state("bounded").history==[]


def test_provider_handoff_cannot_change_recipient(wire):
    submit_engineering(wire)
    handoff=prepare_worker_handoff(wire.engine.get_state("bounded"))
    with pytest.raises(WorkerHandoffError): render_worker_handoff(handoff,expected_worker="steward")


def test_knobhead_cannot_consume_requirement_from_another_job(wire):
    engineering=submit_engineering(wire)
    wire.replies.append(BUILD)
    execution=wire.executor.execute("bounded")
    wire.engine.submit_result(WorkerResultBridge().from_execution(execution,GovernedDisposition(
        result_type="BUILD_CANDIDATE",status="BUILD_CANDIDATE")))
    engineering.job_id="another-job"
    before=len(wire.calls)
    result=wire.executor.execute("bounded")
    assert result.ok is False and result.metadata["handoff_status"]=="BLOCKED"
    assert len(wire.calls)==before
    assert len(wire.engine.get_state("bounded").history)==2


def test_valid_no_build_result_cannot_inherit_provider_supplied_build_scope(wire,monkeypatch):
    from orchestration.providers import ProviderResponse
    replies=[
        ProviderResponse(ok=True,provider="fixture",model="fixture",output_text=WORK,metadata={
            "engineering_disposition":{"status":"READY_FOR_BUILD","build_required":True},
            "engineering_build_requirement":SCOPE,"next_worker":"builder", "promotion_authority":True,
        }),
        ProviderResponse(ok=True,provider="fixture",model="fixture",
            output_text=reply(None,status="NO_BUILD_REQUIRED",build_required=False),metadata={"done_reason":"stop"}),
    ]
    monkeypatch.setattr(wire.provider,"execute",lambda request:replies.pop(0))
    result=wire.executor.execute("bounded")
    assert result.metadata["engineering_disposition"]=={"status":"NO_BUILD_REQUIRED","build_required":False}
    assert "engineering_build_requirement" not in result.metadata
    assert "next_worker" not in result.metadata and result.metadata.get("promotion_authority") is not True
    assert wire.engine.get_state("bounded").history==[]

