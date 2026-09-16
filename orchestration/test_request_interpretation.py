"""Offline request and shared-retrieval contracts. No network or model calls."""
from datetime import date, datetime
from types import SimpleNamespace
import unittest
from orchestration.request_interpretation import interpret_request, text_mentions_subject
from orchestration.evidence_adapter import PMEiEvidenceAdapter
from orchestration.evidence_qualification import CurrentTaskEvidenceQualifier

DAY = date(2026, 9, 15)


class TestRequestInterpretation(unittest.TestCase):
    def test_same_grammar_across_users_and_projects(self):
        for name in ['Alex', 'James', 'Morgan Lee', 'Zoë', 'O’Neill', 'Atlas', 'Service Nine']:
            with self.subTest(name=name):
                r=interpret_request(f'What else has {name} been doing for the last 12 months?',reference_date=DAY)
                self.assertTrue(r.ready)
                self.assertEqual(r.subject,name)
                self.assertEqual((r.start_date,r.end_date),('2025-09-15','2026-09-15'))
                self.assertTrue(r.additional_requested)
                self.assertEqual(r.time_basis,'EVENT_TIME')

    def test_equivalent_request_forms(self):
        for question in ['What has Alex done over the past year?',
                         'What did Alex do during the previous 12 months?',
                         'What has Alex been working on in the last twelve months?',
                         "Summarise Alex's activities over the past 12 months",
                         'List Alex’s activities during the last year']:
            with self.subTest(question=question):
                r=interpret_request(question,reference_date=DAY)
                self.assertTrue(r.ready)
                self.assertEqual(r.subject,'Alex')
                self.assertEqual(r.start_date,'2025-09-15')

    def test_no_period_is_unbounded_not_invented(self):
        r=interpret_request('What has Alex been doing?',reference_date=DAY)
        self.assertTrue(r.ready)
        self.assertIsNone(r.start_date)
        self.assertIsNone(r.end_date)

    def test_calendar_month_clamps(self):
        r=interpret_request('What has Alex done in the last month?',reference_date=date(2024,3,31))
        self.assertEqual(r.start_date,'2024-02-29')

    def test_calendar_year_leap_day(self):
        r=interpret_request('What has Alex done in the last year?',reference_date=date(2024,2,29))
        self.assertEqual(r.start_date,'2023-02-28')

    def test_day_and_week_windows(self):
        for expr, expected in [('last 2 weeks','2026-09-01'),('past three days','2026-09-12')]:
            with self.subTest(expr=expr):
                self.assertEqual(interpret_request('What has Alex done '+expr,reference_date=DAY).start_date,expected)

    def test_unsupported_and_invalid_time_is_unresolved(self):
        for tail in ['in 2025','last 0 months','last months','last -2 months','since yesterday',
                     'last 999999999999999999999 years','last month and last year']:
            with self.subTest(tail=tail):
                r=interpret_request('What has Alex done '+tail,reference_date=DAY)
                self.assertFalse(r.ready)
                self.assertTrue(r.clarification)

    def test_unbound_references_do_not_invent_identity(self):
        for subject in ['I','he','she','they','this project','my project','the user']:
            with self.subTest(subject=subject):
                r=interpret_request(f'What has {subject} been doing for the last year?',reference_date=DAY)
                self.assertFalse(r.ready)
                self.assertIsNone(r.subject)
                self.assertEqual(r.subject_terms,())

    def test_explicit_caller_binding_only(self):
        r=interpret_request('What has he been doing?',reference_date=DAY,bound_subject='Morgan Lee')
        self.assertTrue(r.ready)
        self.assertEqual(r.subject,'Morgan Lee')
        self.assertIsNone(interpret_request('What has he been doing?',reference_date=DAY).subject)

    def test_multiple_subjects_remain_unresolved(self):
        self.assertFalse(interpret_request('What have Alex and Morgan been doing?',reference_date=DAY).ready)

    def test_other_question_families_and_sparse_cues_untouched(self):
        for question in ['Who is Alex?', 'What happened?', 'What changed?', 'History of Atlas',
                         'What did we decide?', 'Was Atlas working then?', 'Is Atlas working now?',
                         'copper lantern', 'What is Atlas?', 'Tell me about Record 9001']:
            with self.subTest(question=question):
                self.assertEqual(interpret_request(question,reference_date=DAY).operation,'UNRESOLVED')

    def test_identity_matching_preserves_names(self):
        self.assertTrue(text_mentions_subject('James repaired a boat.','James'))
        self.assertTrue(text_mentions_subject("James's boat was repaired.",'James'))
        self.assertFalse(text_mentions_subject('Jame repaired a boat.','James'))
        self.assertFalse(text_mentions_subject('Jameson repaired a boat.','James'))
        self.assertTrue(text_mentions_subject('Zoë painted a room.','Zoë'))
        self.assertFalse(text_mentions_subject('Zoe painted a room.','Zoë'))

    def test_multiword_subject_not_partial(self):
        self.assertTrue(text_mentions_subject('Morgan Lee repaired a room.','Morgan Lee'))
        self.assertFalse(text_mentions_subject('Morgan called Lee.','Morgan Lee'))

    def test_reference_date_type_is_explicit(self):
        with self.assertRaises(TypeError):
            interpret_request('What has Alex done?',reference_date=datetime(2026,9,15))


def fake_adapter(records):
    calls=[]
    def retrieve(**kwargs):
        calls.append(('retrieve',kwargs))
        return [dict(record_id=r['id'],text=r['human_brief']['summary'],source='fixture') for r in kwargs['records']]
    def historical():
        calls.append(('historical',None))
        return records,dict(exhaustive=True,route='/offline-fixture')
    def ordinary():
        calls.append(('ordinary',None))
        return records,dict(exhaustive=False,route='/offline-fixture')
    a=PMEiEvidenceAdapter.__new__(PMEiEvidenceAdapter)
    a.max_evidence=8
    a.archive_search=False
    a.qualifier=CurrentTaskEvidenceQualifier()
    a.notepad=SimpleNamespace(get_pmei_records=ordinary,get_pmei_historical_records=historical,
        retrieve_pmei=retrieve,build_subject_terms=lambda q:q.lower().split(),
        record_text=lambda r:r['human_brief']['summary'])
    return a,calls


def rec(number, text, owner='sample', timestamp='2026-09-14T00:00:00Z'):
    return dict(id=number,user_id=owner,seal='READ ONLY',session_ref='sample',timestamp=timestamp,
                human_brief=dict(title='Source',summary=text))


class TestRequestRetrievalIntegration(unittest.TestCase):
    def test_prepares_subject_query_through_existing_archive_and_ranking(self):
        a,calls=fake_adapter([rec(9001,'James repaired a boat.'),rec(9002,'Jame painted a room.'),
                              rec(9003,'The last 12 months were busy.'),rec(9004,'Jameson made a table.')])
        q='What else has James been doing for the last 12 months?'
        p=a.prepare(q)
        self.assertEqual(p.question,q)
        self.assertEqual(p.query,'James')
        self.assertEqual([r['record_id'] for r in p.evidence],[9001])
        self.assertEqual(calls[0][0],'historical')
        args=calls[1][1]
        self.assertEqual(args['question'],'James')
        self.assertEqual(args['query'],'James')
        self.assertEqual(p.request_context['subject'],'James')
        self.assertEqual(p.request_context['time_basis'],'EVENT_TIME')
        self.assertEqual(p.request_context,p.transport['request_interpretation'])
        self.assertTrue(p.transport['activity_time_filter_applied'])
        self.assertEqual(p.transport['activity_time_filter_scope'], 'EXPLICIT_LEADING_ACTION_DATE_ONLY')
        self.assertEqual(p.records_received,4)

    def test_ownership_is_not_activity_subject(self):
        a,_=fake_adapter([rec(9001,'Morgan repaired a boat.',owner='alex')])
        p=a.prepare('What has Alex been doing?')
        self.assertTrue(p.retrieval_ok)
        self.assertEqual(p.evidence,[])
        self.assertEqual(p.transport['activity_selection']['activity_matching_records'], 0)

    def test_save_date_does_not_filter_or_date_activity(self):
        a,_=fake_adapter([rec(9001,'Alex recalled a school play in 1992.'),
                         rec(9002,'Alex repaired a boat.',timestamp='2010-01-01T00:00:00Z')])
        p=a.prepare('What has Alex been doing for the last year?')
        self.assertEqual([r['record_id'] for r in p.evidence],[9001,9002])
        self.assertTrue(p.transport['activity_time_filter_applied'])
        self.assertEqual(p.transport['activity_time_filter_scope'], 'EXPLICIT_LEADING_ACTION_DATE_ONLY')
        # Retrieval candidates only: neither becomes a dated recent activity.
        self.assertFalse(any('event_date' in r for r in p.evidence))

    def test_unresolved_reference_stops_before_retrieval(self):
        a,calls=fake_adapter([rec(9001,'Alex repaired a boat.')])
        p=a.prepare('What has he been doing for the last year?')
        self.assertEqual(calls,[])
        self.assertFalse(p.retrieval_ok)
        self.assertTrue(p.error.startswith('Clarification needed:'))
        self.assertFalse(p.request_context['ready'])

    def test_non_activity_path_keeps_original_question_and_mode(self):
        a,calls=fake_adapter([rec(9001,'A copper lantern was recalled.')])
        p=a.prepare('copper lantern')
        self.assertEqual(calls[0][0],'ordinary')
        self.assertEqual(calls[1][1]['question'],'copper lantern')
        self.assertEqual(p.request_context,{})

    def test_snapshot_cannot_infer_current_state_from_new_request_fields(self):
        a,_=fake_adapter([rec(9001,'Alex repaired a boat. Do not infer verification.')])
        p=a.prepare('What has Alex been doing for the last year?')
        self.assertEqual(p.evidence[0]['task_alignment'],'NON_QUALIFYING')
        self.assertNotEqual(p.evidence[0]['temporal_scope'],'CURRENT')


if __name__=='__main__':
    unittest.main()
