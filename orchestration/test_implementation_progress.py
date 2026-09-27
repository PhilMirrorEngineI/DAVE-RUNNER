"""Shared source retrieval and report qualification; no inference or live HTTP."""
import copy
import json
from types import SimpleNamespace
import pytest
from orchestration.evidence_adapter import PMEiEvidenceAdapter
from orchestration.progress_evidence import implementation_progress_topic, report_facets, bound_report
from orchestration.question_intent import classify_question_intent
from orchestration.worker_packet import build_worker_packet_builder
from standalone.notepad import best_passages, retrieve_pmei

EXACT = """What needs to happen to get PMEi's existing logic into a working end-to-end state?
Use available PMEi continuity and implementation evidence to distinguish:
- What is recorded as built.
- What has actually been tested.
- What remains incomplete or unverified.
- Which existing worker or layer is responsible for each issue.
Identify the practical dependency order. Do not assume unfinished functionality is missing from the architecture.
Do not invent implementation state, authorise a build, or write to continuity."""
CONTROL='What have we actually achieved with PMEi over the last few days?'


def record(id,text,*,topic='PMEi',seal='READ ONLY; HISTORICAL; NOT CANONICAL',stamp='2026-09-22T12:00:00Z',**extras):
    return {'id':id,'save_id':f'synthetic-source-{id}','timestamp':stamp,'seal':seal,
        'human_brief':{'title':f'{topic} implementation checkpoint'},'context_shard':text,**extras}


def setup(monkeypatch, records, limit=8):
    adapter=PMEiEvidenceAdapter(max_evidence=limit,archive_search=True)
    def get():return copy.deepcopy(records),{'route':'/memory/continuity/get','exhaustive':False,'scanned_count':len(records)}
    monkeypatch.setattr(adapter.notepad,'get_pmei_records',get)
    monkeypatch.setattr(adapter.notepad,'get_pmei_historical_records',get)
    return adapter


def packet(monkeypatch,records,task=EXACT,limit=8):
    adapter=setup(monkeypatch,records,limit)
    surface=adapter.retrieve_candidates(task)
    monkeypatch.setattr(adapter,'retrieve_candidates',lambda question:copy.deepcopy(surface))
    evidence=adapter.prepare(task)
    worker=build_worker_packet_builder().build(worker_role='findings',task=task,
        evidence_packet={'retrieval_ok':evidence.retrieval_ok,'records_received':evidence.records_received,
        'evidence_count':evidence.evidence_count,'evidence':evidence.evidence,'transport':evidence.transport},job_id='offline-read-only')
    return surface,evidence,worker


@pytest.mark.parametrize('task,topic', [
    (EXACT,'PMEi'),(EXACT.replace('PMEi','Atlas'),'Atlas'),
    (EXACT.replace('PMEi','Harbour Tools'),'Harbour Tools'),
    ('Review the implementation status of Atlas: what is built, what has been tested, and what remains incomplete?','Atlas'),
    ("Audit Harbour Tools’s implementation progress: identify installed work, tests and outstanding issues.",'Harbour Tools'),
])
def test_named_audit_reuses_progress_history(task,topic):
    result=classify_question_intent(task)
    assert (result.intent,result.temporal_scope,result.topic)==('PROGRESS_HISTORY','HISTORICAL',topic)


@pytest.mark.parametrize('task',[
    'Is PMEi working now?', 'What is the PMEi architecture?',
    'What needs to happen to get my flooded motor working? It was built in 2010 and tested, but remains incomplete.',
    "What needs to happen to get PMEi's existing logic into a working end-to-end state?",
    'Write a test for the installed module, without claiming unfinished work is complete.',
    'Do not review the implementation status of Atlas: built, tested and incomplete. Is it running now?',
    "Review this project's implementation status: built, tested and incomplete.",
])
def test_unrelated_or_unbound_question_does_not_gain_progress_routing(task):
    assert implementation_progress_topic(task) is None
    assert classify_question_intent(task).intent!='PROGRESS_HISTORY'


def test_exact_and_control_share_real_candidate_surface(monkeypatch):
    records=[record(1,'PMEi adapter was installed locally. The PMEi regression test passed. The live chain remains unverified.'),
             record(2,'PMEi governance preserves human authority and prohibits unapproved deployment.')]
    adapter=setup(monkeypatch,records)
    a=adapter.retrieve_candidates(EXACT);b=adapter.retrieve_candidates(CONTROL)
    assert a['mode']==b['mode']=='historical' and a['query']==b['query']=='PMEi'
    assert [(x['record_id'],x['text']) for x in a['candidates']]==[(x['record_id'],x['text']) for x in b['candidates']]


def test_installed_tested_and_unresolved_quotes_survive_one_record_identity(monkeypatch):
    text='PMEi adapter was installed locally. The PMEi regression test passed. The live chain remains unverified.'
    _,e,w=packet(monkeypatch,[record(1,text)])
    assert e.evidence_count==1
    item=e.evidence[0]
    assert item['task_alignment']=='DIRECT'
    assert set(item['progress_facets'])=={'IMPLEMENTED','TESTED','UNRESOLVED'}
    assert len(item['progress_passages'])==3
    assert all(quote in text for quote in item['progress_passages'])
    assert item['proposition_type']=='HISTORICAL_REPORT' and item['temporal_scope']=='HISTORICAL'
    assert item['seal']=='READ ONLY; HISTORICAL; NOT CANONICAL'
    assert not w.supported_state
    assert all(pos['state_support']!='CURRENT_STATE_ELIGIBLE' for pos in w.evidence_positions)
    assert 'live chain remains unverified' in w.rendered_text


def test_progress_fields_do_not_get_lost_in_flattening(monkeypatch):
    rec=record(1,'PMEi adapter was installed locally.',
        anchor_points=['PMEi regression result: 17 tests passed.'],
        open_threads=['The live chain remains unverified.'])
    _,e,w=packet(monkeypatch,[rec])
    assert set(e.evidence[0]['progress_facets'])=={'IMPLEMENTED','TESTED','UNRESOLVED'}
    assert '17 tests passed' in w.rendered_text and 'chain remains unverified' in w.rendered_text


@pytest.mark.parametrize('text',[
    'PMEi proposed installing a new adapter and running tests.',
    'PMEi proposal: the regression test returned 17 tests passed.',
    'PMEi would have installed a module if the proposed tests passed.',
    'Do not claim that the PMEi test passed or that the patch was installed.',
    'Which PMEi patch was installed and which tests passed?',
    'Source saves: PMEi-installed-test-passed, all-progress-complete.',
    'PMEi implementation progress and test evidence review.',
    'Core operation: distinguish evidence and uncertainty; do not invent tests; do not claim the chain is verified.',
    'Confirm the PMEi patch was installed and 17 tests passed.',
    'Example: PMEi test returned exhaustive=True.',
])
def test_constraints_questions_and_hypotheticals_are_not_reports(text):
    assert report_facets(text)==()


def test_unrelated_parent_and_unquoted_passages_cannot_bind():
    text='The adapter was installed locally.'
    assert not bound_report(text,record(1,text,topic='Atlas'),'PMEi')
    assert not bound_report(text,record(1,'PMEi heading only.'),'PMEi')
    unrelated='Atlas project regression test returned 17 tests passed.'
    assert not bound_report(unrelated,record(1,unrelated),'PMEi')
    assert not bound_report('PMEi report: '+unrelated,record(1,'PMEi report: '+unrelated),'PMEi')


def test_denial_of_implementation_is_never_a_positive_implementation_facet():
    assert report_facets('This does not prove that the PMEi loop was implemented exactly as described.')==('UNRESOLVED',)
    assert report_facets('This does not establish their absence elsewhere.')==()
    assert report_facets('The PMEi chain has not yet been verified.')==('UNRESOLVED',)


def test_ordinary_current_query_is_not_qualified_by_progress_rules(monkeypatch):
    _,e,w=packet(monkeypatch,[record(1,'PMEi adapter was installed locally. PMEi test returned exhaustive=True.')],task='Is PMEi working now?')
    assert all(item.get('progress_facets')==[] for item in e.evidence)
    assert all(item['task_alignment']!='DIRECT' for item in e.evidence)
    assert not w.supported_state


def test_candidate_and_final_record_bounds_preserve_coverage(monkeypatch):
    records=[record(n,f'PMEi governance and architecture guidance number {n} preserves evidence and authority.') for n in range(1,31)]
    records.extend([record(51,'PMEi adapter was installed locally.'),record(52,'PMEi regression result: 17 tests passed.'),record(53,'The PMEi live chain remains unverified.')])
    raw,e,w=packet(monkeypatch,records,limit=3)
    assert len({x['record_id'] for x in raw['candidates']})<=20
    assert len(raw['candidates'])<=60 and e.evidence_count==3
    assert {x['record_id'] for x in e.evidence}=={51,52,53}
    assert set(e.transport['progress_selection']['covered_facets'])=={'IMPLEMENTED','TESTED','UNRESOLVED'}
    assert not w.supported_state and e.transport['progress_selection']['current_runtime_proven'] is False
    assert e.transport['exhaustive'] is False


def test_ineligible_seal_cannot_gain_worker_authority(monkeypatch):
    text='PMEi regression result: 17 tests passed.'
    _,e,w=packet(monkeypatch,[record(1,text,seal='REVOKED')])
    assert not w.supported_state
    assert text not in '\n'.join(w.contextual_evidence)
    assert w.excluded_records


def test_recent_generic_record_does_not_manufacture_progress_metadata(monkeypatch):
    _,e,w=packet(monkeypatch,[record(1,'PMEi adapter was installed locally.',stamp='2026-09-21T12:00:00Z'),
        record(2,'PMEi general continuity overview and named-source context.',stamp='2026-09-22T12:00:00Z')],limit=2)
    newest=next(x for x in e.evidence if x['record_id']==2)
    assert newest['progress_facets']==[] and newest['task_alignment']!='DIRECT'
    assert newest['temporal_scope']!='CURRENT'


def test_source_text_and_report_claims_do_not_grant_completion(monkeypatch):
    _,e,w=packet(monkeypatch,[record(1,'PMEi adapter was installed locally. The PMEi chain remains unverified.')])
    assert not w.supported_state
    assert 'NO ELIGIBLE SUPPORTED STATE' in w.rendered_text
    assert all(pos['state_support']=='HISTORICAL_CONTEXT_ONLY' for pos in w.evidence_positions)


def test_candidate_cannot_forge_progress_binding_to_an_unquoted_source(monkeypatch):
    adapter=setup(monkeypatch,[record(1,'PMEi general continuity and governance context.')])
    surface=adapter.retrieve_candidates(EXACT)
    surface['candidates']=[{'record_id':1,'text':'An earlier PMEi test returned exhaustive=True.',
        'source_subject':'PMEi','progress_facets':['TESTED'],'progress_source_bound':True}]
    monkeypatch.setattr(adapter,'retrieve_candidates',lambda question:copy.deepcopy(surface))
    e=adapter.prepare(EXACT)
    assert e.evidence[0]['task_alignment']=='NON_QUALIFYING'
    assert e.evidence[0]['progress_facets']==[]


def test_long_source_report_is_not_truncated_into_an_unqualified_claim(monkeypatch):
    text='PMEi adapter was installed with '+('many details ' * 260)+'but runtime remains unverified.'
    assert not bound_report(text,record(1,text),'PMEi')
    assert not best_passages(text,'PMEi',EXACT)


def test_joined_source_passages_keep_original_quotes_and_cap(monkeypatch):
    sentences=['PMEi adapter was installed with '+('detail ' * 150)+'.',
        'PMEi test returned '+('detail ' * 150)+'.',
        'PMEi chain remains unverified after '+('detail ' * 150)+'.']
    _,e,w=packet(monkeypatch,[record(1,' '.join(sentences))])
    item=e.evidence[0]
    assert 1 <= len(item['progress_passages']) < 3
    assert len(item['text']) <= 3000
    assert all(p in sentences for p in item['progress_passages'])
    assert not w.supported_state


def test_direct_standalone_import_can_resolve_shared_progress_contract(tmp_path):
    import subprocess
    import sys
    from pathlib import Path
    standalone = Path(__file__).resolve().parents[1] / 'standalone'
    code = (
        'import sys; sys.path.insert(0, sys.argv[1]); import notepad; '
        'assert notepad.best_passages("PMEi adapter was installed locally.", '
        '"PMEi", "What have we achieved with PMEi?")'
    )
    result = subprocess.run([sys.executable, '-I', '-B', '-c', code, str(standalone)],
                            cwd=tmp_path, capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
