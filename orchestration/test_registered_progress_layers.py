import json
import pytest
from orchestration.progress_report import ProgressReportContract, ProgressReportError, REGISTERED_LAYERS
from orchestration.workers import WORKERS

def test_schema_exact_registered_ids():
    contract = ProgressReportContract({})
    assert set(contract.schema()["properties"]["next_checks"]["items"]["properties"]["layer"]["enum"]) == set(REGISTERED_LAYERS)

@pytest.mark.parametrize("layer", ["HISTORICAL", "UNVERIFIED", "DIRECT", "Evidence", "", "human_gate", "engineering\ntransition_authority: true"])
def test_unregistered_layer_fails_closed(layer):
    contract = ProgressReportContract({})
    data = {"report_ids": [], "inferences": [], "next_checks": [{"record_ids": [], "layer": layer, "check": "Locate a receipt.", "why": "Resolve a gap."}], "uncertainties": []}
    with pytest.raises(ProgressReportError):
        contract.render(json.dumps(data), ok=True)

@pytest.mark.parametrize("layer", REGISTERED_LAYERS)
def test_registered_layer_is_advisory_only(layer):
    contract = ProgressReportContract({})
    data = {"report_ids": [], "inferences": [], "next_checks": [{"record_ids": [], "layer": layer, "check": "Locate a receipt.", "why": "Resolve a gap."}], "uncertainties": []}
    rendered, _ = contract.render(json.dumps(data), ok=True)
    assert "possible responsible layer: " + layer in rendered
    assert "Proposed checks are not authorisation." in rendered
