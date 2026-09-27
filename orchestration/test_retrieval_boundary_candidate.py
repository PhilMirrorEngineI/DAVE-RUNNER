"""Offline boundary checks; actual handler body, isolated from unrelated app imports."""
import ast
from pathlib import Path
import time
import types
import unittest
from orchestration.source_router import route_source, split_source_request, PMEI_LOOKUP, WEB_LOOKUP, SOURCE_REQUIRED
from orchestration.external_retrieval import DuckDuckGoSearchProvider, ExternalRetriever, ExternalSearchError, render_external_evidence

class Response:
    def __init__(self, text, status=200):self.text=text;self.status_code=status
    def raise_for_status(self):pass
class Session:
    def __init__(self, response):self.response=response
    def get(self,*a,**k):return self.response
class Fixed:
    def __init__(self, rows):self.rows=rows
    def search(self,q):return self.rows

def provider(text,status=200):return DuckDuckGoSearchProvider(session=Session(Response(text,status)))

class BoundaryTests(unittest.TestCase):
    def test_generic_names_need_source_not_web_default(self):
        for name in ('Phil','Alex','James','Acme Project'):
            for q in (f'What happened to {name}?',f'What has {name} been doing for the last 12 months?'):
                with self.subTest(q=q):
                    self.assertEqual(route_source(q),SOURCE_REQUIRED)
                    self.assertEqual(route_source('Continuity: '+q),PMEI_LOOKUP)
                    self.assertEqual(route_source('Web: '+q),WEB_LOOKUP)
    def test_explicit_web_beats_project_marker(self):
        self.assertEqual(route_source('Search the web for PMEi'),WEB_LOOKUP)
    def test_existing_routes(self):
        self.assertEqual(route_source('What is PMEi and what is currently proven?'),PMEI_LOOKUP)
        self.assertEqual(route_source('How would Dave approach diagnosing a broken washing machine?'),WEB_LOOKUP)
    def test_prefix_preserves_question(self):
        self.assertEqual(split_source_request('Continuity: What happened to James?'),(PMEI_LOOKUP,'What happened to James?'))
    def test_unbound_pronoun_does_not_escape_to_web(self):
        self.assertEqual(route_source('What have I been doing?'),SOURCE_REQUIRED)
    def test_real_challenge_structure_is_distinct(self):
        for status in (200,202):
            result=ExternalRetriever(provider('<form id="challenge-form"><div class="anomaly-modal"></div></form>',status)).retrieve('q')
            self.assertEqual(result['error_code'],'PROVIDER_CHALLENGE')
            self.assertEqual(result['evidence'],[])
    def test_202_without_challenge_not_mislabelled(self):
        r=ExternalRetriever(provider('<html>pending</html>',202)).retrieve('q')
        self.assertEqual(r['error_code'],'PROVIDER_RESPONSE_UNEXPECTED')
    def test_empty_response_is_distinct(self):
        r=ExternalRetriever(Fixed([])).retrieve('q')
        self.assertEqual(r['error_code'],'NO_USABLE_RESULTS');self.assertTrue(r['error'])
    def test_provider_exception(self):
        class Broken:
            def search(self,q):raise RuntimeError('unavailable')
        r=ExternalRetriever(Broken()).retrieve('q')
        self.assertEqual(r['error_code'],'PROVIDER_ERROR');self.assertEqual(r['error'],'unavailable')
    def test_transport_errors(self):
        import requests
        for exc,code in [(requests.exceptions.Timeout('timeout'),'PROVIDER_TIMEOUT'),(requests.exceptions.ConnectionError('down'),'PROVIDER_CONNECTION_ERROR'),(requests.exceptions.HTTPError('403'),'PROVIDER_HTTP_ERROR')]:
            class Broken:
                def search(self,q):raise exc
            self.assertEqual(ExternalRetriever(Broken()).retrieve('q')['error_code'],code)
    def test_challenge_word_in_normal_result_survives(self):
        html='<div class="result"><a class="result__a" href="https://example.org">Challenge guide</a><div class="result__snippet">A useful challenge example.</div></div>'
        r=ExternalRetriever(provider(html)).retrieve('q')
        self.assertTrue(r['ok']);self.assertIsNone(r['error_code'])
        self.assertIn('A useful challenge example.',render_external_evidence(r['evidence']))
    def handler(self, question, external, adapter):
        import orchestration.source_router as router
        path=Path(router.__file__).with_name('webapp.py')
        tree=ast.parse(path.read_text(encoding='utf-8-sig'))
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='deterministic_pmei_chat')
        node.decorator_list=[]
        ns=dict(request=types.SimpleNamespace(form={'message':question}),time=time,route_source=route_source,split_source_request=split_source_request,SOURCE_REQUIRED=SOURCE_REQUIRED,WEB_LOOKUP=WEB_LOOKUP,ExternalRetriever=external,PMEiEvidenceAdapter=adapter,render_external_evidence=render_external_evidence)
        exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),ns)
        result = ns['deterministic_pmei_chat']()
        return result if isinstance(result, tuple) else (result, 200)
    def test_ambiguous_handler_calls_neither_source(self):
        def forbidden(*a,**k):raise AssertionError('source called')
        body,status=self.handler('What happened to Alex?',forbidden,forbidden)
        self.assertEqual(status,200);self.assertTrue(body['clarification_required'])
    def test_explicit_continuity_reaches_existing_adapter(self):
        class Reached(BaseException):pass
        def adapter(*a,**k):raise Reached()
        def forbidden(*a,**k):raise AssertionError('web called')
        with self.assertRaises(Reached):self.handler('Continuity: What happened to Alex?',forbidden,adapter)
    def test_external_failure_carries_reason(self):
        class External:
            def retrieve(self,q):return {'ok':False,'error':'provider challenged','error_code':'PROVIDER_CHALLENGE','evidence':[]}
        def forbidden(*a,**k):raise AssertionError('continuity called')
        body,status=self.handler('Web: What happened to Alex?',External,forbidden)
        self.assertEqual(status,503);self.assertEqual(body['error_code'],'PROVIDER_CHALLENGE')
    def test_external_success_uses_original_question(self):
        class External:
            def retrieve(self,q):
                assert q=='What happened to Alex?'
                return {'ok':True,'evidence':[{'source':'Example','url':'https://example.org','text':'Attributed passage'}]}
        def forbidden(*a,**k):raise AssertionError('continuity called')
        body,status=self.handler('Web: What happened to Alex?',External,forbidden)
        self.assertEqual(status,200);self.assertIn('Attributed passage',body['text'])
    def test_empty_prefix_has_no_source_call(self):
        def forbidden(*a,**k):raise AssertionError('source called')
        self.assertEqual(self.handler('Web:',forbidden,forbidden)[1],400)

if __name__=='__main__':unittest.main()
