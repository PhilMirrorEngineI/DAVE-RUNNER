import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from orchestration.external_retrieval import BraveSearchProvider, ExternalRetriever, _brave_key

ROW={'title':'Guide <b>one</b>','url':'https://example.org/guide','description':'Water &amp; drainage.'}
class Response:
    def __init__(self,body=None,status=200):self.body=body;self.status_code=status
    def json(self):
        if isinstance(self.body,Exception):raise self.body
        return self.body
class Session:
    def __init__(self,response):self.response=response;self.calls=[]
    def get(self,*args,**kwargs):
        self.calls.append((args,kwargs))
        if isinstance(self.response,Exception):raise self.response
        return self.response
class BraveTests(unittest.TestCase):
    def result(self,body=None,status=200,key='fixture-key'):
        session=Session(Response(body,status))
        result=ExternalRetriever(BraveSearchProvider(api_key=key,session=session)).retrieve('washing machine')
        return result,session
    def test_headers_endpoint_and_normalisation(self):
        r,s=self.result({'web':{'results':[ROW]}})
        self.assertTrue(r['ok']);e=r['evidence'][0]
        self.assertEqual(e['text'],'Water & drainage.');self.assertEqual(e['source'],'Guide one')
        self.assertEqual(e['provider'],'brave');self.assertTrue(e['retrieved_at_utc'])
        args,kw=s.calls[0]
        self.assertEqual(args[0],'https://api.search.brave.com/res/v1/web/search')
        self.assertEqual(kw['params']['q'],'washing machine');self.assertEqual(kw['params']['count'],8)
        self.assertEqual(kw['headers']['X-Subscription-Token'],'fixture-key')
        self.assertFalse(kw['allow_redirects']);self.assertEqual(len(s.calls),1)
    def test_missing_key_no_request(self):
        r,s=self.result({},key='');self.assertEqual(r['error_code'],'PROVIDER_NOT_CONFIGURED');self.assertEqual(s.calls,[])
    def test_status_codes_no_body_or_key_leak(self):
        for status,code in [(401,'PROVIDER_AUTH_ERROR'),(403,'PROVIDER_AUTH_ERROR'),(429,'PROVIDER_RATE_LIMIT'),(500,'PROVIDER_HTTP_ERROR'),(302,'PROVIDER_HTTP_ERROR')]:
            with self.subTest(status=status):
                r,s=self.result({'error':'fixture-key'},status)
                self.assertEqual(r['error_code'],code);self.assertNotIn('fixture-key',str(r));self.assertEqual(len(s.calls),1)
    def test_bad_json(self):
        self.assertEqual(self.result(ValueError('fixture-key'))[0]['error_code'],'PROVIDER_RESPONSE_INVALID')
    def test_bad_schema(self):
        for body in ([],{'error':'bad'},{'web':None},{'web':{'results':{}}}):
            self.assertEqual(self.result(body)[0]['error_code'],'PROVIDER_RESPONSE_INVALID')
    def test_no_web_results(self):
        for body in ({'query':{'original':'x'}},{'web':{'results':[]}}):
            self.assertEqual(self.result(body)[0]['error_code'],'NO_USABLE_RESULTS')
    def test_url_validation_and_dedup(self):
        rows=[None,{'title':'x','description':'x','url':'javascript:bad'},dict(ROW,url='https://user:pass@example.org'),ROW,ROW]
        self.assertEqual(len(self.result({'web':{'results':rows}})[0]['evidence']),1)
    def test_limit_preserves_order(self):
        rows=[dict(ROW,url=f'https://example.org/{i}',title=str(i)) for i in range(25)]
        r=BraveSearchProvider(api_key='x',session=Session(Response({'web':{'results':rows}})),max_results=3).search('q')
        self.assertEqual([x['source'] for x in r],['0','1','2'])
    def test_sanitised_transport_errors(self):
        import requests
        for error,code in [(requests.exceptions.Timeout('fixture-key'),'PROVIDER_TIMEOUT'),(requests.exceptions.ConnectionError('fixture-key'),'PROVIDER_CONNECTION_ERROR'),(RuntimeError('fixture-key'),'PROVIDER_ERROR')]:
            r=ExternalRetriever(BraveSearchProvider(api_key='fixture-key',session=Session(error))).retrieve('q')
            self.assertEqual(r['error_code'],code);self.assertNotIn('fixture-key',str(r))
    def test_env_precedence(self):
        with patch.dict(os.environ,{'BRAVE_API_KEY':'environment-fixture'}):
            self.assertEqual(_brave_key('/does/not/exist'),'environment-fixture')
    def test_dotenv_quotes_and_comments(self):
        with patch.dict(os.environ,{},clear=True), tempfile.TemporaryDirectory() as d:
            p=Path(d)/'.env'
            for line in ['BRAVE_API_KEY=file-fixture # comment','export BRAVE_API_KEY="file-fixture"','BRAVE_API_KEY=\'file-fixture\'']:
                p.write_text('UNRELATED=not-exported\n'+line)
                self.assertEqual(_brave_key(p),'file-fixture');self.assertNotIn('UNRELATED',os.environ)
            self.assertEqual(_brave_key(Path(d)/'absent'),'')
    def test_default_and_injection(self):
        with patch('orchestration.external_retrieval.MultiPassSearchProvider',return_value='default'):
            self.assertEqual(ExternalRetriever().provider,'default')
            marker=object();self.assertIs(ExternalRetriever(marker).provider,marker)

if __name__=='__main__':unittest.main()
