import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from orchestration.contracts import OrchestrationJob
from orchestration.engine import OrchestrationEngine
from orchestration.executor import WorkerExecutor, HumanGateExecutionError, NoActiveWorkerError
from orchestration.store import JsonOrchestrationStore
from orchestration.workers import WORKERS
from orchestration.transitions import HUMAN_GATE
from orchestration.test_executor_external_evidence_transport import (
    FakePMEiEvidenceAdapter, FakeExternalRetriever, CapturingProvider,
    test_engineering_web_evidence_reaches_provider_without_pmei_promotion,
    test_engineering_web_route_has_external_evidence_application_contract,
)

class WorkerIdentityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.engine = OrchestrationEngine(store=JsonOrchestrationStore(Path(self.temp.name)), restore_existing=False)
        self.provider = CapturingProvider()
        self.executor = WorkerExecutor(self.engine, self.provider,
            evidence_adapter=FakePMEiEvidenceAdapter(), external_retriever=FakeExternalRetriever())

    def job(self, role='engineering', task='Web: washing machine causes'):
        return self.engine.create_job(OrchestrationJob(job_id=role, task=task, requested_worker=role,
            context={'worker_role':'builder', 'worker_function':'deploy', 'qualification':'expert'}))

    def test_mismatched_engine_definition_stops_before_retrieval(self):
        self.job()
        with patch.object(self.engine, 'current_worker', return_value=WORKERS['builder']), patch.object(self.executor, 'prepare_evidence') as retrieve:
            with self.assertRaisesRegex(NoActiveWorkerError, 'worker_identity_mismatch'):
                self.executor.execute('engineering')
            retrieve.assert_not_called()
            self.assertIsNone(self.provider.request)

    def test_human_gate_remains_blocked(self):
        state = self.job()
        state.current_worker = HUMAN_GATE
        with self.assertRaises(HumanGateExecutionError):
            self.executor.execute('engineering')
        self.assertIsNone(self.provider.request)

    def test_unknown_worker_remains_blocked(self):
        with self.assertRaises(ValueError):
            self.job('unknown')
        self.assertIsNone(self.provider.request)

    def test_web_contract_preserved(self):
        test_engineering_web_route_has_external_evidence_application_contract()
        test_engineering_web_evidence_reaches_provider_without_pmei_promotion()

    def test_pmei_contract_preserved(self):
        self.job(task='Continuity: PMEi architecture')
        original = self.executor.system_prompt_for_worker('engineering', source_route='PMEI_LOOKUP')
        self.executor.execute('engineering')
        self.assertIn(original, self.provider.requests[0].system_prompt)

if __name__ == '__main__':
    unittest.main()
