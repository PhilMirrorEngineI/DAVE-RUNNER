from pathlib import Path

def test_handoff_state_v2_2_source_contract():
    text = Path("orchestration/webapp.py").read_text(encoding="utf-8-sig")
    assert "PMEI_ENGINEERING_HANDOFF_STATE_V2_2" in text
    assert "lastPhilMessage=msg;" in text
    assert 'return String(lastPhilMessage || "").trim();' in text

def test_async_route_still_present():
    text = Path("orchestration/webapp.py").read_text(encoding="utf-8-sig")
    assert '/orchestration/request-engineering-async' in text
    assert '/orchestration/engineering/status' in text
