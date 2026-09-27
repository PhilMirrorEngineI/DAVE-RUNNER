"""Automatic execution with real engine/executor/bridge, mocked inference only."""
import copy
import json
from dataclasses import replace
from types import SimpleNamespace
import pytest
from orchestration import automatic_continuation as auto, webapp
from orchestration.automatic_continuation import AutomaticContinuation, read_report, JOURNAL
from orchestration.continuation_disposition import parse_disposition, ContinuationDispositionError, OUTCOMES
from orchestration.contracts import OrchestrationJob
from orchestration.engine import OrchestrationEngine
from orchestration.executor import WorkerExecution
from orchestration.providers import ProviderResponse
from orchestration.test_bounded_worker_handoff import wire, TASK, SCOPE, WORK, WORK_TEXT, BUILD
from orchestration.test_build_requirement import reply

REVIEW = 'UNVERIFIED: Candidate review found no identified implementation defect. Human review and independent tests remain necessary.'


def outcome(status, basis, layer=None):
    return json.dumps({'status': status, 'responsible_layer': layer, 'basis': basis})


def chain(wire, review='ACCEPT_CANDIDATE'):
    wire.replies.extend([
        WORK,
        reply(SCOPE),
        REVIEW,
        outcome(review, 'Candidate review', 'engineering' if review == 'REVISE_CANDIDATE' else None),
    ])


def drive(wire, **limits):
    runner = AutomaticContinuation(wire.engine, wire.executor, **limits)
    receipt = runner.start('bounded', launch=lambda callback: callback())
    assert receipt['result_status'] == 'ORCHESTRATION_QUEUED'
    return read_report(wire.engine, 'bounded')


def test_full_chain_uses_recorded_candidates_and_stops_before_human(wire):
    chain(wire)
    report = drive(wire)
    assert report['result_status'] == 'AWAITING_HUMAN'
    state = wire.engine.get_state('bounded')
    assert [r.worker_role for r in state.history] == ['engineering', 'knobhead']
    assert [r.next_worker for r in state.history] == ['knobhead', 'human_gate']
    assert len(wire.calls) == 4 and not wire.replies
    assert WORK_TEXT in wire.calls[2]['messages'][-1]['content']
    assert WORK_TEXT in wire.calls[2]['messages'][-1]['content']
    for i in (3,):
        assert wire.calls[i]['format']['additionalProperties'] is False
        assert set(wire.calls[i]['format']['required']) == {'status', 'responsible_layer', 'basis'}
    assert report['history_count'] == 2 and report['causal_continuation']['successor_executed'] is True
    assert all(report[key] is False for key in ('transition_authority', 'promotion_authority', 'verification_authority'))
    assert WORK_TEXT in report['text'] and REVIEW in report['text']
    assert 'no human approval' in report['text']


def test_chat_queues_once_and_status_is_observation_only(wire, monkeypatch):
    monkeypatch.setattr(webapp, 'engine', wire.engine)
    monkeypatch.setattr(webapp, 'executor', wire.executor)
    monkeypatch.setattr(webapp, 'provider', wire.provider)
    monkeypatch.setattr(webapp, '_foh_pmei_prepare_for_question', lambda task: {})
    pending = []
    monkeypatch.setattr(auto, 'launch_background', pending.append)
    wire.replies.append(json.dumps({'action':'REQUEST_WORKER', 'requested_worker':'engineering','question':None}))
    chain(wire)
    client = webapp.app.test_client()
    receipt = client.post('/chat', data={'message': TASK, 'history':'[]'})
    assert receipt.status_code == 202
    body = receipt.get_json(); job_id = body['job_id']
    assert body['initial_request']['requested_worker'] == 'engineering'
    assert body['execution'] is None and len(wire.calls) == 1 and len(pending) == 1
    path = wire.engine.store.path_for(job_id)
    before = path.read_bytes(); mtime = path.stat().st_mtime_ns
    for _ in range(3):
        assert client.get(body['status_url']).get_json()['result_status'] == 'ORCHESTRATION_QUEUED'
    assert path.read_bytes() == before and path.stat().st_mtime_ns == mtime and len(wire.calls) == 1
    callback = pending.pop(); callback(); callback()  # Replayed scheduler callback cannot execute twice.
    report = client.get(body['status_url']).get_json()
    assert report['result_status'] == 'AWAITING_HUMAN' and report['history_count'] == 2
    assert len(wire.calls) == 5
    assert client.get('/orchestration/jobs/unknown-job').status_code == 404
    assert client.get('/orchestration/jobs/a.bad').status_code == 400


@pytest.mark.parametrize('role,status,target', [
    ('architecture','ENGINEERING_REQUIRED','engineering'),
    ('governance','ARCHITECTURE_REQUIRED','architecture'),
    ('findings','GOVERNANCE_REVIEW_REQUIRED','governance'),
    ('findings','STEWARDSHIP_REVIEW_REQUIRED','steward'),
    ('steward','ENGINEERING_REQUIRED','engineering'),
])
def test_other_roles_follow_existing_engine_law(wire, role, status, target):
    state=wire.engine.get_state('bounded'); state.job.requested_worker=role; state.current_worker=role
    wire.engine.persist_state(state)
    first='UNVERIFIED: Further specialist assessment is needed.'
    last=(WORK if target=='engineering' else 'UNVERIFIED: Candidate assessment is available for human review.')
    wire.replies.extend([first, outcome(status,'Further specialist assessment is needed.'), last,
        reply(None,status='NO_BUILD_REQUIRED',build_required=False) if target=='engineering'
        else outcome('NO_ACTION_REQUIRED','Candidate assessment is available for human review.')])
    report=drive(wire)
    assert report['result_status']=='AWAITING_HUMAN'
    assert [r.worker_role for r in state.history]==[role,target]
    assert len(wire.calls)==4
    assert first in wire.calls[2]['messages'][-1]['content']


def test_advisory_no_build_reaches_gate_without_builder(wire):
    wire.replies.extend([WORK,reply(None,status='NO_BUILD_REQUIRED',build_required=False)])
    report=drive(wire)
    assert report['result_status']=='AWAITING_HUMAN'
    assert report['history_count']==1 and len(wire.calls)==2
    assert not report['causal_continuation']['successor_attempted']


def test_invalid_outcome_never_falls_back_to_build_prose(wire):
    wire.replies.extend([WORK,reply(SCOPE),REVIEW,'ACCEPT_CANDIDATE'])
    report=drive(wire)
    assert report['result_status']=='CANDIDATE_ONLY'
    assert report['current_worker']=='knobhead' and report['history_count']==1
    assert len(wire.calls)==4


def test_revision_bound_preserves_second_review_but_does_not_run_again(wire):
    chain(wire,'REVISE_CANDIDATE');chain(wire,'REVISE_CANDIDATE')
    report=drive(wire)
    assert report['result_status']=='REVISION_LIMIT'
    assert report['history_count']==4 and len(wire.calls)==8
    assert report['current_worker']=='engineering'
    assert WORK_TEXT in wire.calls[6]['messages'][-1]['content']


def test_one_revision_can_reach_human_gate(wire):
    chain(wire,'REVISE_CANDIDATE');chain(wire)
    assert drive(wire)['result_status']=='AWAITING_HUMAN'
    assert len(wire.calls)==8


def test_step_limit_stops_at_recorded_successor(wire):
    chain(wire)
    report=drive(wire,max_steps=1)
    assert report['result_status']=='STEP_LIMIT' and report['current_worker']=='knobhead'
    assert report['history_count']==1 and len(wire.calls)==2


def test_time_limit_after_disposition_does_not_commit_or_run_successor(wire,monkeypatch):
    state=wire.engine.get_state('bounded');state.current_worker='architecture';wire.engine.persist_state(state)
    wire.replies.extend([WORK,outcome('ENGINEERING_REQUIRED','A bounded CSV column-count function is proposed')])
    ticks=iter([0,0,0,2])
    report=drive(wire,max_seconds=1,clock=lambda:next(ticks))
    assert report['result_status']=='TIME_LIMIT' and report['history_count']==0 and len(wire.calls)==2


def fake_execution(job_id='bounded', **overrides):
    values=dict(job_id=job_id,worker_role='engineering',ok=True,provider='fixture',model='fixture',output_text=WORK,
        metadata={'validation_status':'ACCEPT','transition_authority':False,
        'engineering_disposition':{'status':'NO_BUILD_REQUIRED','build_required':False}})
    values.update(overrides)
    return WorkerExecution(**values)


@pytest.mark.parametrize('change,stop', [
    ({'job_id':'foreign'},'EXECUTION_IDENTITY_MISMATCH'),
    ({'worker_role':'steward'},'EXECUTION_IDENTITY_MISMATCH'),
    ({'ok':False},'WORKER_RESULT_REJECTED'),
    ({'output_text':'x'*16001},'OUTPUT_BOUND_EXCEEDED'),
    ({'output_text':''},'INCOMPLETE_WORK_PRODUCT'),
    ({'metadata':{'validation_status':'REJECT'}},'WORKER_RESULT_REJECTED'),
    ({'metadata':{'validation_status':'ACCEPT','done_reason':'length'}},'INCOMPLETE_WORK_PRODUCT'),
    ({'metadata':{'validation_status':'ACCEPT','transition_authority':True}},'AUTHORITY_CLAIM_REJECTED'),
])
def test_failed_work_cannot_advance(wire,monkeypatch,change,stop):
    monkeypatch.setattr(wire.executor,'execute',lambda job_id:fake_execution(**change))
    report=drive(wire)
    assert report['result_status']==stop and report['history_count']==0 and wire.calls==[]


def test_provider_outcome_failure_does_not_advance(wire,monkeypatch):
    state=wire.engine.get_state('bounded');state.current_worker='findings';wire.engine.persist_state(state)
    monkeypatch.setattr(wire.executor,'execute',lambda job_id:fake_execution(worker_role='findings'))
    monkeypatch.setattr(wire.provider,'execute',lambda request:ProviderResponse(ok=False,provider='fixture',model='fixture',error='quota'))
    report=drive(wire)
    assert report['result_status']=='CANDIDATE_ONLY' and report['history_count']==0


def test_job_changed_on_disk_is_preserved_and_never_overwritten(wire,monkeypatch):
    saved=[]
    def changed(job_id):
        other=OrchestrationEngine(wire.engine.store,restore_existing=True)
        state=other.get_state(job_id);state.job.context['human_edit']='keep this'
        other.persist_state(state);saved.append(wire.engine.store.path_for(job_id).read_bytes())
        return fake_execution()
    monkeypatch.setattr(wire.executor,'execute',changed)
    report=drive(wire)
    assert report['result_status']=='STATE_CHANGED' and report['history_count']==0
    assert wire.engine.store.path_for('bounded').read_bytes()==saved[0]
    assert report['automatic_continuation']['requires_review_before_retry']


def test_queued_state_change_stops_before_inference(wire):
    callbacks=[]
    runner=AutomaticContinuation(wire.engine,wire.executor)
    runner.start('bounded',launch=callbacks.append)
    state=wire.engine.get_state('bounded');state.job.constraints.append('human edit')
    wire.engine.persist_state(state);before=wire.engine.store.path_for('bounded').read_bytes()
    callbacks[0]()
    assert read_report(wire.engine,'bounded')['result_status']=='STATE_CHANGED'
    assert not wire.calls and wire.engine.store.path_for('bounded').read_bytes()==before


def test_uncertain_submit_is_not_retried_or_hidden(wire,monkeypatch):
    monkeypatch.setattr(wire.executor,'execute',lambda job_id:fake_execution())
    real=wire.engine.persist_state
    def fail_after_mutation(state):
        if state.history:raise OSError('simulated write failure')
        return real(state)
    monkeypatch.setattr(wire.engine,'persist_state',fail_after_mutation)
    report=drive(wire)
    assert report['result_status']=='STATE_ERROR'
    assert report['automatic_continuation']['submission_uncertain'] is True
    assert report['history_count']==0  # Persisted view, not the mutated cache.
    assert len(wire.engine.get_state('bounded').history)==1
    AutomaticContinuation(wire.engine,wire.executor).start('bounded',launch=lambda cb:pytest.fail('replay'))


def test_restart_and_repeated_start_do_not_resume_or_reexecute(wire):
    callbacks=[];runner=AutomaticContinuation(wire.engine,wire.executor)
    receipt=runner.start('bounded',launch=callbacks.append)
    runner.start('bounded',launch=lambda cb:pytest.fail('duplicate launch'))
    record=wire.engine.get_state('bounded').job.context[JOURNAL]
    auto._ACTIVE.discard(record['run_id'])  # Simulated stopped process.
    restored=OrchestrationEngine(wire.engine.store,restore_existing=True)
    report=AutomaticContinuation(restored,wire.executor).start('bounded',launch=lambda cb:pytest.fail('resume'))
    assert report['result_status']=='CONTINUATION_UNCONFIRMED' and not wire.calls
    # Even loss of the journal cannot erase the durable per-job claim.
    state=restored.get_state('bounded');state.job.context.pop(JOURNAL);restored.persist_state(state)
    assert AutomaticContinuation(restored,wire.executor).start('bounded')['result_status']=='CONTINUATION_ALREADY_CLAIMED'


def test_real_background_scheduler_completes_without_status_driving_it(wire,monkeypatch):
    import threading
    done=threading.Event();real_stop=AutomaticContinuation._stop
    def stop(self,*args,**kwargs):
        value=real_stop(self,*args,**kwargs);done.set();return value
    monkeypatch.setattr(AutomaticContinuation,'_stop',stop)
    monkeypatch.setattr(wire.executor,'execute',lambda job_id:fake_execution())
    AutomaticContinuation(wire.engine,wire.executor).start('bounded')
    assert done.wait(5)
    assert read_report(wire.engine,'bounded')['result_status']=='AWAITING_HUMAN'


@pytest.mark.parametrize('value', [
    {},[],None,{'status':'BUILD_CANDIDATE','responsible_layer':None,'basis':'invented'},
    {'status':'BUILD_CANDIDATE','responsible_layer':'engineering','basis':'candidate'},
    {'status':'BUILD_CANDIDATE','responsible_layer':None,'basis':'candidate','next_worker':'knobhead'},
    {'status':'ACCEPT','responsible_layer':None,'basis':'candidate'},
    {'status':'BUILD_CANDIDATE','responsible_layer':None,'basis':True},
])
def test_outcome_validation_is_strict(value):
    with pytest.raises(ContinuationDispositionError):parse_disposition('builder',json.dumps(value),'candidate')


def test_duplicate_json_and_revision_without_layer_are_rejected():
    with pytest.raises(ContinuationDispositionError):
        parse_disposition('builder','{"status":"HOLD","status":"BUILD_CANDIDATE","responsible_layer":null,"basis":"candidate"}','candidate')
    with pytest.raises(ContinuationDispositionError):
        parse_disposition('knobhead',outcome('REVISE_CANDIDATE','candidate'),'candidate')


@pytest.mark.parametrize('role',list(OUTCOMES))
def test_hold_is_never_submitted_as_a_transition(role):
    status='INSUFFICIENT_EVIDENCE' if role=='knobhead' else 'HOLD'
    assert parse_disposition(role,outcome(status,'candidate'),'candidate').governed is None


def test_scheduler_failure_leaves_visible_stop_and_durable_claim(wire):
    def fail(callback):raise RuntimeError('scheduler unavailable')
    report=AutomaticContinuation(wire.engine,wire.executor).start('bounded',launch=fail)
    assert report['result_status']=='START_FAILED' and report['ok'] is False
    assert report['automatic_continuation']['requires_review_before_retry']
    assert not wire.calls
    assert AutomaticContinuation(wire.engine,wire.executor).start('bounded')['result_status']=='START_FAILED'


def test_truncated_outcome_json_cannot_advance(wire,monkeypatch):
    state=wire.engine.get_state('bounded');state.current_worker='architecture';wire.engine.persist_state(state)
    monkeypatch.setattr(wire.executor,'execute',lambda job_id:fake_execution(worker_role='architecture'))
    monkeypatch.setattr(wire.provider,'execute',lambda request:ProviderResponse(ok=True,provider='fixture',model='fixture',
        output_text=outcome('ENGINEERING_REQUIRED',WORK),metadata={'done_reason':'length'}))
    report=drive(wire)
    assert report['result_status']=='CANDIDATE_ONLY' and not report['history_count']


def test_no_automatic_execution_at_preexisting_human_gate(wire,monkeypatch):
    state=wire.engine.get_state('bounded');state.current_worker='human_gate';state.status='AWAITING_HUMAN'
    wire.engine.persist_state(state)
    monkeypatch.setattr(wire.executor,'execute',lambda job_id:pytest.fail('human gate inference'))
    assert drive(wire)['result_status']=='AWAITING_HUMAN'
    assert not wire.calls


def test_status_does_not_disclose_a_mismatched_stored_job(wire,monkeypatch):
    monkeypatch.setattr(webapp,'engine',wire.engine)
    state=wire.engine.get_state('bounded');payload=wire.engine._state_to_payload(state)
    payload['job']['job_id']='a-different-job'
    wire.engine.store.save('bounded',payload)
    assert webapp.app.test_client().get('/orchestration/jobs/bounded').status_code==503
