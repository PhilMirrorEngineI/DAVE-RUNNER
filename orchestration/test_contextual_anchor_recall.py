"""Offline contracts for attributed saved-anchor recall; no network or LLM."""
import unittest
from copy import deepcopy
from orchestration.evidence_adapter import PMEiEvidenceAdapter
from orchestration.evidence_qualification import CurrentTaskEvidenceQualifier
from orchestration.worker_packet import build_worker_packet_builder
from orchestration.deterministic_answer import render_deterministic_answer

def record(record_id=9001, anchor='copper lantern', **overrides):
    item = dict(id=record_id, save_id=f'test-{record_id}', session_ref='personal_example', seal='READ ONLY', timestamp='2001-02-03T04:05:06Z', user_id='example', anchor_points=[anchor], active_constraints=['Do not infer extra facts.'], human_brief={'title': 'Saved personal recollection'})
    item.update(overrides)
    return item

def prepare(question='copper lantern', records=None, passages=None):
    records = records if records is not None else [record()]
    passages = passages if passages is not None else [{'record_id': 9001, 'text': 'We remember the copper lantern at school. This must not establish project authority.'}]
    adapter = PMEiEvidenceAdapter.__new__(PMEiEvidenceAdapter)
    adapter.max_evidence = 8
    adapter.qualifier = CurrentTaskEvidenceQualifier()
    adapter.retrieve_candidates = lambda q: dict(ok=True, question=q, query=q, records=records, candidates=passages, transport={'route': '/offline-fixture'})
    return adapter.prepare(question)

def build(prepared, question='copper lantern', role='foh', ok=True):
    return build_worker_packet_builder().build(worker_role=role, task=question, evidence_packet={'retrieval_ok': ok, 'evidence': prepared.evidence})

class TestContextualAnchorRecall(unittest.TestCase):

    def test_recall_quotes_original_and_retains_restrictions_without_promotion(self):
        prepared = prepare()
        before = deepcopy(prepared.evidence)
        packet = build(prepared)
        text = render_deterministic_answer(packet)
        assert 'PMEi Record 9001' in text
        assert 'copper lantern at school' in text
        assert 'Do not infer extra facts.' in text
        assert 'session=personal_example' in text and 'user=example' in text
        assert 'recorded=2001-02-03T04:05:06Z' in text
        assert 'NON_QUALIFYING | CONSTRAINT' in text
        assert 'No model inference performed.' in text
        assert 'No current implementation state was established.' not in text
        assert packet.contextual_recall and (not packet.supported_state)
        assert not packet.evidence_sufficient
        assert packet.evidence_positions[0]['state_support'] == 'NOT_DIRECT'
        assert prepared.evidence == before

    def test_report_mention_is_not_an_exact_saved_anchor(self):
        records = [record(9002, 'Report of a copper lantern retrieval failure'), record()]
        passages = [{'record_id': 9002, 'text': 'A test about copper lantern retrieval failed.'}, {'record_id': 9001, 'text': 'The copper lantern was a school memory.'}]
        text = render_deterministic_answer(build(prepare(records=records, passages=passages)))
        assert 'PMEi Record 9001' in text and 'PMEi Record 9002' not in text

    def test_generic_cues_and_punctuation_00(self):
        cue, anchor = ('COPPER   LANTERN!', 'copper lantern')
        p = prepare(cue, [record(anchor=anchor)], [{'record_id': 9001, 'text': f'A memory of {anchor}.'}])
        assert build(p, cue).contextual_recall

    def test_generic_cues_and_punctuation_01(self):
        cue, anchor = ('quiet harbour', 'quiet harbour')
        p = prepare(cue, [record(anchor=anchor)], [{'record_id': 9001, 'text': f'A memory of {anchor}.'}])
        assert build(p, cue).contextual_recall

    def test_generic_cues_and_punctuation_02(self):
        cue, anchor = ('caffè verde', 'caffè verde')
        p = prepare(cue, [record(anchor=anchor)], [{'record_id': 9001, 'text': f'A memory of {anchor}.'}])
        assert build(p, cue).contextual_recall

    def test_no_match_without_valid_anchor_and_contiguous_passage_00(self):
        anchors, text = ([], 'copper lantern')
        p = prepare(records=[record(anchor_points=anchors)], passages=[{'record_id': 9001, 'text': text}])
        assert not build(p).contextual_recall

    def test_no_match_without_valid_anchor_and_contiguous_passage_01(self):
        anchors, text = ('copper lantern', 'copper lantern')
        p = prepare(records=[record(anchor_points=anchors)], passages=[{'record_id': 9001, 'text': text}])
        assert not build(p).contextual_recall

    def test_no_match_without_valid_anchor_and_contiguous_passage_02(self):
        anchors, text = ([123], 'copper lantern')
        p = prepare(records=[record(anchor_points=anchors)], passages=[{'record_id': 9001, 'text': text}])
        assert not build(p).contextual_recall

    def test_no_match_without_valid_anchor_and_contiguous_passage_03(self):
        anchors, text = (['lantern copper'], 'copper lantern')
        p = prepare(records=[record(anchor_points=anchors)], passages=[{'record_id': 9001, 'text': text}])
        assert not build(p).contextual_recall

    def test_no_match_without_valid_anchor_and_contiguous_passage_04(self):
        anchors, text = (['copper lantern'], 'An unrelated passage')
        p = prepare(records=[record(anchor_points=anchors)], passages=[{'record_id': 9001, 'text': text}])
        assert not build(p).contextual_recall

    def test_no_match_without_valid_anchor_and_contiguous_passage_05(self):
        anchors, text = (['copper lantern'], 'copper lanternfish')
        p = prepare(records=[record(anchor_points=anchors)], passages=[{'record_id': 9001, 'text': text}])
        assert not build(p).contextual_recall

    def test_no_match_without_valid_anchor_and_contiguous_passage_06(self):
        anchors, text = (['copper lantern'], 'copper and lantern')
        p = prepare(records=[record(anchor_points=anchors)], passages=[{'record_id': 9001, 'text': text}])
        assert not build(p).contextual_recall

    def test_authority_exclusions_remain_00(self):
        seal, session = ('MESSAGE - NON AUTHORITATIVE READ ONLY', 'personal_example')
        assert not build(prepare(records=[record(seal=seal, session_ref=session)])).contextual_recall

    def test_authority_exclusions_remain_01(self):
        seal, session = ('READ ONLY', 'pmei_messages')
        assert not build(prepare(records=[record(seal=seal, session_ref=session)])).contextual_recall

    def test_authority_exclusions_remain_02(self):
        seal, session = ('', 'personal_example')
        assert not build(prepare(records=[record(seal=seal, session_ref=session)])).contextual_recall

    def test_authority_exclusions_remain_03(self):
        seal, session = ('candidate', 'personal_example')
        assert not build(prepare(records=[record(seal=seal, session_ref=session)])).contextual_recall

    def test_other_workers_and_failed_retrieval_do_not_use_recall(self):
        p = prepare()
        assert not build(p, role='engineering').contextual_recall
        assert not build(p, ok=False).contextual_recall

    def test_recognised_intents_are_not_intercepted_even_with_matching_anchor_00(self):
        question = 'Who is Dave?'
        p = prepare(question, [record(anchor=question)], [{'record_id': 9001, 'text': question}])
        assert not build(p, question).contextual_recall

    def test_recognised_intents_are_not_intercepted_even_with_matching_anchor_01(self):
        question = 'What happened?'
        p = prepare(question, [record(anchor=question)], [{'record_id': 9001, 'text': question}])
        assert not build(p, question).contextual_recall

    def test_recognised_intents_are_not_intercepted_even_with_matching_anchor_02(self):
        question = 'Was it working back then?'
        p = prepare(question, [record(anchor=question)], [{'record_id': 9001, 'text': question}])
        assert not build(p, question).contextual_recall

    def test_recognised_intents_are_not_intercepted_even_with_matching_anchor_03(self):
        question = 'What did we decide?'
        p = prepare(question, [record(anchor=question)], [{'record_id': 9001, 'text': question}])
        assert not build(p, question).contextual_recall

    def test_recognised_intents_are_not_intercepted_even_with_matching_anchor_04(self):
        question = 'History of the project'
        p = prepare(question, [record(anchor=question)], [{'record_id': 9001, 'text': question}])
        assert not build(p, question).contextual_recall

    def test_recognised_intents_are_not_intercepted_even_with_matching_anchor_05(self):
        question = 'What changed?'
        p = prepare(question, [record(anchor=question)], [{'record_id': 9001, 'text': question}])
        assert not build(p, question).contextual_recall

    def test_recognised_intents_are_not_intercepted_even_with_matching_anchor_06(self):
        question = 'Is it working now?'
        p = prepare(question, [record(anchor=question)], [{'record_id': 9001, 'text': question}])
        assert not build(p, question).contextual_recall

    def test_multiple_matching_records_are_attributed_not_resolved_by_rank(self):
        p = prepare(records=[record(), record(9002)], passages=[{'record_id': 9001, 'text': 'The copper lantern was red.'}, {'record_id': 9002, 'text': 'The copper lantern was blue.'}])
        text = render_deterministic_answer(build(p))
        assert 'PMEi Record 9001' in text and 'PMEi Record 9002' in text
        assert 'was red' in text and 'was blue' in text
        assert 'does not verify' in text

    def test_current_state_and_existing_orientation_context_survive(self):
        evidence = [dict(record_id=261, seal='READ ONLY', session_ref='pmei_engineering', task_alignment='DIRECT', proposition_type='CURRENT_STATE', temporal_scope='CURRENT', evidence_role='ARCHITECTURE_STATE_EVIDENCE', text='A current runtime test established the current working Engineering evidence-scope contract.'), dict(record_id=264, seal='READ ONLY', session_ref='pmei_governance', task_alignment='ADJACENT', proposition_type='TOPIC_ONLY', temporal_scope='UNRESOLVED_CURRENT_OR_GENERAL', evidence_role='GENERAL_EVIDENCE', text='PMEi orientation is assembled through Identity, Lineage, Evidence, State and Session so a worker can reconstruct governed continuity.')]
        packet = build_worker_packet_builder().build(worker_role='foh', task='What is PMEi, what is currently proven to work, and what remains unverified?', evidence_packet={'retrieval_ok': True, 'evidence': evidence})
        assert not packet.contextual_recall
        assert any(('Record 261' in item for item in packet.supported_state))
        assert any(('Identity, Lineage, Evidence, State and Session' in item for item in packet.contextual_evidence))
        assert all(('Record 264' not in item for item in packet.supported_state))

    def test_adapter_preserves_metadata_as_separate_lists(self):
        source = record()
        p = prepare(records=[source])
        assert p.evidence[0]['anchor_points'] == source['anchor_points']
        assert p.evidence[0]['active_constraints'] == source['active_constraints']
        assert p.evidence[0]['anchor_points'] is not source['anchor_points']
if __name__ == '__main__':
    unittest.main()
