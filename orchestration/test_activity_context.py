from datetime import date
from types import SimpleNamespace
import unittest
from standalone import notepad
from orchestration.activity_evidence import activity_passages
from orchestration.request_interpretation import interpret_request
from orchestration.evidence_adapter import PMEiEvidenceAdapter
from orchestration.evidence_qualification import CurrentTaskEvidenceQualifier
from orchestration.worker_packet import build_worker_packet_builder
from orchestration.deterministic_answer import render_deterministic_answer

def request(subject='Alex'):
    return interpret_request(f'What has {subject} been doing for the last 12 months?',reference_date=date(2026,9,15)).as_dict()

def record(number,text,**extra):
    r=dict(id=number,user_id='example',seal='READ ONLY',session_ref='example',timestamp='2026-09-14T00:00:00Z',
           human_brief={'title':'Example source','summary':text},active_constraints=['No authority promotion.'])
    r.update(extra)
    return r

def adapter(records):
    a=PMEiEvidenceAdapter.__new__(PMEiEvidenceAdapter)
    a.max_evidence=8;a.archive_search=True;a.qualifier=CurrentTaskEvidenceQualifier()
    a.notepad=SimpleNamespace(record_text=notepad.record_text,retrieve_pmei=notepad.retrieve_pmei,
        build_subject_terms=notepad.build_subject_terms,
        get_pmei_historical_records=lambda:(records,dict(exhaustive=True,route='/offline',success=True)),
        get_pmei_records=lambda:(records,dict(exhaustive=True,route='/offline',success=True)))
    return a

def packet(prepared):
    return build_worker_packet_builder().build(worker_role='foh',task=prepared.question,
        evidence_packet=dict(retrieval_ok=prepared.retrieval_ok,evidence=prepared.evidence,
                             records_received=prepared.records_received,transport=prepared.transport))

class TestActivityContext(unittest.TestCase):
    def test_generic_names_and_regular_and_irregular_actions(self):
        for subject in ['Alex','James','Morgan Lee','Zoë','Atlas']:
            for verb in ['repaired','ran','built','supplied','requested','successfully repaired']:
                with self.subTest(subject=subject,verb=verb):
                    result=activity_passages(record(9001,f'{subject} {verb} a documented task.'),request(subject))
                    self.assertEqual(len(result),1)
                    self.assertEqual(result[0]['position'],'DATE_UNRESOLVED')

    def test_incidental_role_labels_are_not_actions(self):
        for text in ['Human authority: Alex.','Alex remains final authority.',
                     'The worker ran a test. Human authority: Alex.',
                     'Alex must run the test.','Alex should repair the boat.',
                     'If Alex repaired a boat, this would be an example.',
                     'Example: Alex repaired a boat.',
                     'Alex never ran the test.','Alex did not repair the boat.']:
            with self.subTest(text=text):
                self.assertEqual(activity_passages(record(9001,text),request()),[])

    def test_source_fields_are_not_merged_and_duplicate_quote_is_deduped(self):
        r=record(9001,'Alex repaired a boat.',context_shard='Alex repaired a boat. Alex built a table.')
        result=activity_passages(r,request())
        self.assertEqual(len(result),2)
        self.assertEqual(result[0]['source_field'],'human_brief.summary')
        self.assertEqual(result[1]['source_field'],'context_shard')

    def test_only_explicit_leading_date_binds_event_time(self):
        cases=[('On 5 July 2026, Alex repaired a boat.','IN_REQUESTED_WINDOW'),
               ('2026-07-05: Alex repaired a boat.','IN_REQUESTED_WINDOW'),
               ('On 5 July 1992, Alex repaired a boat.','OUTSIDE_REQUESTED_WINDOW'),
               ('Alex recalled the school play on 5 July 1992.','DATE_UNRESOLVED'),
               ('Alex repaired a boat yesterday.','DATE_UNRESOLVED')]
        for text,expected in cases:
            with self.subTest(text=text):
                self.assertEqual(activity_passages(record(9001,text),request())[0]['position'],expected)

    def test_invalid_date_does_not_become_valid_event(self):
        p=activity_passages(record(9001,'On 31 February 2026, Alex repaired a boat.'),request())[0]
        self.assertEqual(p['position'],'DATE_UNRESOLVED')

    def test_passage_after_twenty_incidental_matches_survives(self):
        records=[record(i,'The worker executed tests. Human authority: Alex.') for i in range(1,26)]
        records.append(record(9001,'Alex repaired a boat and supplied a detailed account of the work.'))
        p=adapter(records).prepare('What has Alex been doing for the last 12 months?')
        self.assertEqual([x['record_id'] for x in p.evidence],[9001])
        self.assertEqual(p.transport['subject_matching_records'],26)
        self.assertEqual(p.transport['activity_selection']['activity_matching_records'],1)

    def test_activity_output_preserves_restrictions_and_no_state_promotion(self):
        p=adapter([record(9001,'Alex repaired a boat and supplied a detailed account of the work.')]).prepare('What has Alex been doing for the last year?')
        out=packet(p);rendered=render_deterministic_answer(out)
        self.assertIn('READ-ONLY ACTIVITY CONTEXT',rendered)
        self.assertIn('PMEi Record 9001',rendered)
        self.assertIn('Activity date unresolved',rendered)
        self.assertIn('No authority promotion.',rendered)
        self.assertNotIn('No current implementation state was established.',rendered)
        self.assertEqual(out.supported_state,[])
        self.assertEqual(p.evidence[0]['task_alignment'],'NON_QUALIFYING')

    def test_outside_period_omitted_unknown_date_retained(self):
        p=adapter([record(9001,'On 5 July 1992, Alex repaired a boat and described the work.'),
                   record(9002,'Alex repaired a boat and described the work.',timestamp='2000-01-01T00:00:00Z')]).prepare('What has Alex been doing for the last year?')
        self.assertEqual([x['record_id'] for x in p.evidence],[9002])
        self.assertEqual(p.transport['activity_selection']['outside_window_passages'],1)

    def test_authority_exclusion_happens_before_candidate_limits(self):
        p=adapter([record(i,'Alex repaired a boat and described the work.',seal='MESSAGE - NON AUTHORITATIVE') for i in range(25)]+
                  [record(9001,'Alex built a table and described the materials used.')]).prepare('What has Alex been doing?')
        self.assertEqual([x['record_id'] for x in p.evidence],[9001])
        self.assertEqual(p.transport['activity_selection']['authority_excluded_records'],25)

    def test_no_match_is_successful_scan_not_proof_of_inactivity(self):
        p=adapter([record(9001,'Human authority: Alex.')]).prepare('What has Alex been doing?')
        self.assertTrue(p.retrieval_ok)
        self.assertEqual(p.evidence,[])
        self.assertIn('does not establish that the subject had no activities',render_deterministic_answer(packet(p)))

    def test_partial_scan_and_else_limits_are_visible(self):
        p=adapter([record(9001,'Alex repaired a boat and described the work.')]).prepare('What else has Alex been doing?')
        p.transport['exhaustive']=False
        text=render_deterministic_answer(packet(p))
        self.assertIn('flag: False',text)
        self.assertIn('Previously displayed activities have not been excluded',text)

    def test_wrong_name_not_bound(self):
        self.assertEqual(activity_passages(record(9001,'Jameson repaired a boat.'),request('James')),[])

    def test_following_denial_is_not_stripped_from_quote(self):
        p=activity_passages(record(9001,'Alex repaired a boat. This claim was later rejected as false.'),request())
        self.assertEqual(len(p),1)
        self.assertIn('rejected as false',p[0]['text'])

    def test_original_record_is_not_mutated(self):
        import copy
        r=record(9001,'Alex repaired a boat and described the work.')
        before=copy.deepcopy(r)
        adapter([r]).prepare('What has Alex been doing?')
        self.assertEqual(r,before)

    def test_non_activity_retrieval_has_no_new_activity_fields(self):
        rows=notepad.retrieve_pmei([record(9001,'A copper lantern was remembered as a useful object from a school event.')],
                                  'copper lantern','copper lantern',{'route':'/offline'})
        self.assertTrue(rows)
        self.assertNotIn('activity',rows[0])

if __name__=='__main__':
    unittest.main()
