from pathlib import Path

def test_engineering_card_projection_source_contract():
    text = Path("orchestration/webapp.py").read_text(encoding="utf-8-sig")
    assert "PMEI_ENGINEERING_CARD_PROJECTION_V2_3" in text
    assert 'document.querySelector(\'.worker-card[data-worker="engineering"]\')' in text
    assert 'state.innerHTML = \'<span class="dot"></span>WORKING\'' in text
    assert 'state.innerHTML = \'<span class="dot"></span>RESULT\'' in text
    assert 'open.textContent = "OPEN RESULT"' in text

def test_engineering_activity_prefers_async_job():
    text = Path("orchestration/webapp.py").read_text(encoding="utf-8-sig")
    assert "window.__PMEI_ENGINEERING_ASYNC_JOB__" in text
    assert "const latest=asyncEngineering||" in text
    assert "showWorkerTab('activity')" in text

def test_existing_async_routes_preserved():
    text = Path("orchestration/webapp.py").read_text(encoding="utf-8-sig")
    assert '/orchestration/request-engineering-async' in text
    assert '/orchestration/engineering/status' in text
