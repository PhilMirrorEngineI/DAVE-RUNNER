from __future__ import annotations

import time
import uuid
from pathlib import Path

from flask import Flask, redirect, render_template_string, request, url_for

from orchestration.contracts import OrchestrationJob
from orchestration.engine import OrchestrationEngine
from orchestration.executor import WorkerExecutor
from orchestration.providers import OllamaProvider
from orchestration.store import JsonOrchestrationStore


APP_ROOT = Path(__file__).resolve().parent.parent
STATE_ROOT = APP_ROOT / ".local_orchestration_state"
STATE_ROOT.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)

engine = OrchestrationEngine(
    store=JsonOrchestrationStore(STATE_ROOT),
    restore_existing=False,
)

provider = OllamaProvider(
    model="nemotron-3-nano:4b",
    num_predict=512,
)

executor = WorkerExecutor(
    engine,
    provider,
)

LAST_RUN = {}


PAGE = """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>PMEi Local Orchestration</title>

<style>
* { box-sizing: border-box; }

body {
    margin: 0;
    font-family: Segoe UI, Arial, sans-serif;
    background: #0d1117;
    color: #e6edf3;
}

.shell {
    max-width: 1150px;
    margin: auto;
    padding: 32px 20px 60px;
}

h1 { margin-bottom: 4px; }

.subtitle {
    color: #8b949e;
    margin-bottom: 24px;
}

.badges {
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
}

.badge {
    padding: 8px 12px;
    border: 1px solid #30363d;
    border-radius: 999px;
    background: #161b22;
}

.green { color: #3fb950; }
.red { color: #f85149; }
.yellow { color: #d29922; }

.card {
    margin-top: 20px;
    padding: 20px;
    background: #161b22;
    border: 1px solid #30363d;
    border-radius: 14px;
}

.flow {
    display: grid;
    grid-template-columns: repeat(5, 1fr);
    gap: 10px;
}

.flowbox {
    text-align: center;
    padding: 14px 8px;
    border: 1px solid #30363d;
    border-radius: 10px;
    background: #0d1117;
}

.active {
    border-color: #58a6ff;
    box-shadow: 0 0 0 1px #58a6ff inset;
}

textarea {
    width: 100%;
    min-height: 120px;
    background: #0d1117;
    color: #e6edf3;
    border: 1px solid #30363d;
    border-radius: 10px;
    padding: 14px;
    font: inherit;
}

button {
    margin-top: 12px;
    padding: 11px 18px;
    border: 0;
    border-radius: 9px;
    background: #238636;
    color: white;
    font-weight: 600;
    cursor: pointer;
}

.grid {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 12px;
}

.metric {
    padding: 14px;
    background: #0d1117;
    border: 1px solid #30363d;
    border-radius: 10px;
}

.label {
    color: #8b949e;
    font-size: 12px;
    text-transform: uppercase;
}

.value {
    margin-top: 6px;
    font-size: 17px;
    overflow-wrap: anywhere;
}

pre {
    white-space: pre-wrap;
    overflow-wrap: anywhere;
    padding: 16px;
    background: #0d1117;
    border: 1px solid #30363d;
    border-radius: 10px;
    line-height: 1.5;
}

@media (max-width: 750px) {
    .flow, .grid {
        grid-template-columns: 1fr;
    }
}
</style>
</head>

<body>
<div class="shell">

<h1>PMEi Local Orchestration</h1>
<div class="subtitle">Governed local inference runtime</div>

<div class="badges">
    <div class="badge">
        PMEi control:
        <span class="green">DETERMINISTIC</span>
    </div>

    <div class="badge">
        Model:
        <span class="green">Nemotron 4B</span>
    </div>

    <div class="badge">
        Model transition authority:
        <span class="red">NO</span>
    </div>

    <div class="badge">
        PMEi writes:
        <span class="red">NO</span>
    </div>
</div>


<div class="card">
<h2>Worker Chain</h2>

<div class="flow">

<div class="flowbox {% if current_worker == 'engineering' %}active{% endif %}">
Engineering
</div>

<div class="flowbox {% if current_worker == 'builder' %}active{% endif %}">
Builder
</div>

<div class="flowbox {% if current_worker == 'knobhead' %}active{% endif %}">
Knobhead
</div>

<div class="flowbox {% if current_worker == 'HUMAN_GATE' %}active{% endif %}">
Human Gate
</div>

<div class="flowbox">
Human Authority
</div>

</div>
</div>


<div class="card">
<h2>Run Governed Job</h2>

<form method="post" action="/run">

<textarea name="task" required
placeholder="Enter a bounded Engineering task...">{{ task }}</textarea>

<button type="submit">
Run PMEi Worker
</button>

</form>
</div>


{% if result %}

<div class="card">
<h2>Runtime</h2>

<div class="grid">

<div class="metric">
<div class="label">Job</div>
<div class="value">{{ result.job_id }}</div>
</div>

<div class="metric">
<div class="label">Worker</div>
<div class="value">{{ result.worker }}</div>
</div>

<div class="metric">
<div class="label">Provider</div>
<div class="value">{{ result.provider }}</div>
</div>

<div class="metric">
<div class="label">Model</div>
<div class="value">{{ result.model }}</div>
</div>

<div class="metric">
<div class="label">Validator</div>
<div class="value">{{ result.validation }}</div>
</div>

<div class="metric">
<div class="label">Wall Time</div>
<div class="value">{{ result.wall_seconds }} seconds</div>
</div>

<div class="metric">
<div class="label">Before</div>
<div class="value">{{ result.before }}</div>
</div>

<div class="metric">
<div class="label">After</div>
<div class="value">{{ result.after }}</div>
</div>

<div class="metric">
<div class="label">Model Mutated State</div>
<div class="value">{{ result.mutated }}</div>
</div>

<div class="metric">
<div class="label">Transition Authority</div>
<div class="value">{{ result.authority }}</div>
</div>

</div>
</div>


<div class="card">
<h2>Worker Output</h2>
<pre>{{ result.output }}</pre>
</div>


{% if result.validation_issues %}

<div class="card">
<h2>Validator Issues</h2>
<pre>{{ result.validation_issues }}</pre>
</div>

{% endif %}

{% endif %}

</div>
</body>
</html>
"""


@app.get("/")
def index():
    result = LAST_RUN or None

    current_worker = (
        result.get("worker", "engineering")
        if result
        else "engineering"
    )

    return render_template_string(
        PAGE,
        result=result,
        current_worker=current_worker,
        task=LAST_RUN.get("task", "") if LAST_RUN else "",
    )


@app.post("/run")
def run_job():
    global LAST_RUN

    task = request.form.get("task", "").strip()

    if not task:
        return redirect(url_for("index"))

    job_id = "web-" + uuid.uuid4().hex[:12]

    engine.create_job(
        OrchestrationJob(
            job_id=job_id,
            task=task,
            requested_worker="engineering",
        )
    )

    before = engine.get_state(job_id)

    started = time.perf_counter()

    execution = executor.execute(job_id)

    wall_seconds = time.perf_counter() - started

    after = engine.get_state(job_id)

    validation = execution.metadata.get("validation_status")

    if validation is None:
        validation = "ACCEPT" if execution.ok else "UNKNOWN"

    LAST_RUN = {
        "job_id": job_id,
        "task": task,
        "worker": after.current_worker,
        "provider": execution.provider,
        "model": execution.model,
        "validation": validation,
        "validation_issues": execution.metadata.get(
            "validation_issues"
        ) or [],
        "wall_seconds": round(wall_seconds, 2),
        "before": (
            f"{before.status} / {before.current_worker} / "
            f"history {len(before.history)}"
        ),
        "after": (
            f"{after.status} / {after.current_worker} / "
            f"history {len(after.history)}"
        ),
        "mutated": execution.metadata.get(
            "orchestration_state_changed",
            False,
        ),
        "authority": execution.metadata.get(
            "transition_authority",
            False,
        ),
        "output": (
            execution.output_text
            or execution.error
            or "(no output)"
        ),
    }

    return redirect(url_for("index"))


if __name__ == "__main__":
    app.run(
        host="127.0.0.1",
        port=5000,
        debug=False,
    )
