from __future__ import annotations

from collections import deque
from datetime import datetime
import threading
from threading import Lock

import json

import os
import time
import uuid
from pathlib import Path
from openai import OpenAI
import requests

from flask import Flask, redirect, render_template_string, request, url_for

from orchestration.contracts import OrchestrationJob
from orchestration.task_requirement_recorder import record_task_requirements
from orchestration.engine import OrchestrationEngine
from orchestration.workers import get_worker
from orchestration.executor import WorkerExecutor
from orchestration.providers import OllamaProvider
from orchestration.foh_initial_request import (
    InitialRequestError,
    InitialRequestProposal,
    propose_initial_request,
)
from orchestration.foh_chat_guard import obvious_foh_chat
from orchestration.user_interaction_profile import (
    UserInteractionProfileStore,
    render_profile_instruction,
)
from orchestration.output_validator import WorkerOutputValidator
from orchestration.deterministic_answer import render_deterministic_answer
from orchestration.question_intent import classify_question_intent
from orchestration.context_inspection import render_context_inspection
from orchestration.relationship_bridge import translate_governed_evidence
from orchestration.deterministic_relationship_engine import run_relationship_engine
from orchestration.evidence_adapter import PMEiEvidenceAdapter
from orchestration.source_router import WEB_LOOKUP, SOURCE_REQUIRED, route_source, split_source_request
from orchestration.external_retrieval import ExternalRetriever, render_external_evidence
from orchestration.evidence_relationship import EvidenceItem
from orchestration.worker_packet import build_worker_packet_builder
from orchestration.store import JsonOrchestrationStore
from orchestration.worker_disposition import (
    EngineeringDispositionError,
    parse_engineering_disposition,
)
from orchestration.worker_result_bridge import (
    GovernedDisposition,
    UnresolvedWorkerResult,
    WorkerResultBridge,
)


APP_ROOT = Path(__file__).resolve().parent.parent
STATE_ROOT = APP_ROOT / ".local_orchestration_state"
STATE_ROOT.mkdir(parents=True, exist_ok=True)
USER_PROFILE_ROOT = APP_ROOT / ".local_user_profiles"
USER_PROFILE_ROOT.mkdir(parents=True, exist_ok=True)
USER_PROFILE_REF = os.getenv("FOH_USER_REF", "local-owner").strip() or "local-owner"
user_profile_store = UserInteractionProfileStore(USER_PROFILE_ROOT, USER_PROFILE_REF)

app = Flask(__name__)


# PMEI_RUNTIME_LIVE_FEED_V1
LIVE_EVENTS = deque(maxlen=250)

# PMEI_ENGINEERING_ASYNC_V2
ENGINEERING_RUN = {
    "status": "IDLE",
    "job_id": None,
    "task": None,
    "started_at": None,
    "finished_at": None,
    "validation": None,
    "provider": None,
    "model": None,
    "wall_seconds": None,
    "mutated": False,
    "transition_authority": False,
    "error": None,
}

def _engineering_run_snapshot():
    return dict(ENGINEERING_RUN)

def _set_engineering_run(**updates):
    ENGINEERING_RUN.update(updates)
    return dict(ENGINEERING_RUN)

LIVE_EVENTS_LOCK = Lock()


def add_live_event(kind: str, message: str, **metadata):
    event = {
        "id": uuid.uuid4().hex[:12],
        "time": datetime.now().strftime("%H:%M:%S"),
        "kind": str(kind).upper(),
        "message": str(message),
        "metadata": metadata,
    }

    with LIVE_EVENTS_LOCK:
        LIVE_EVENTS.append(event)

    return event


@app.get("/live-feed")
def runtime_live_feed():
    with LIVE_EVENTS_LOCK:
        events = list(LIVE_EVENTS)

    return {
        "ok": True,
        "events": events,
        "retention": "ram_only",
        "max_events": LIVE_EVENTS.maxlen,
    }


add_live_event(
    "SYSTEM",
    "PMEi UI runtime started",
    persistence=False,
)


engine = OrchestrationEngine(
    store=JsonOrchestrationStore(STATE_ROOT),
    restore_existing=False,
)

provider = OllamaProvider(
    model="nemotron-3-nano:4b",
    num_ctx=4096,
    num_gpu=0,
)


FOH_OLLAMA_MODEL = os.getenv(
    "FOH_OLLAMA_MODEL",
    "nemotron-3-nano:4b",
).strip() or "nemotron-3-nano:4b"

FOH_NUM_PREDICT = int(
    os.getenv(
        "FOH_NUM_PREDICT",
        "512",
    )
)

executor = WorkerExecutor(
    engine,
    provider,
)

LAST_RUN = {}


PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>PMEi Cockpit</title>
<style>
:root{--bg:#06111a;--panel:#0a1824;--line:#173348;--text:#eaf2fb;--muted:#91a8bc;--blue:#39a7ff;--green:#32d583;--purple:#b867ff;--orange:#ff8a34;--yellow:#f5c451;--cyan:#26d7c7;--danger:#ff5468}
*{box-sizing:border-box}html,body{margin:0;min-height:100%;background:var(--bg);color:var(--text);font-family:Segoe UI,Arial,sans-serif}body{overflow-x:hidden}button,input,textarea{font:inherit}button{cursor:pointer}
.topbar{height:64px;border-bottom:1px solid var(--line);display:flex;align-items:center;gap:14px;padding:0 20px;background:#061019;position:sticky;top:0;z-index:20}.brand{font-size:25px;font-weight:700;letter-spacing:.03em;margin-right:12px}.badge{border:1px solid #27455c;border-radius:7px;padding:8px 12px;font-size:12px;letter-spacing:.08em;color:#bdd1e3}.badge.green{border-color:#1d6b48;color:#65e4a3}.badge.red{border-color:#7c2433;color:#ff7180}.spacer{flex:1}.clock{color:#b8c7d4;font-variant-numeric:tabular-nums}
.shell{display:grid;grid-template-columns:170px minmax(0,1fr);min-height:calc(100vh - 64px)}.nav{border-right:1px solid var(--line);padding:16px 10px;background:#07131d;display:flex;flex-direction:column;gap:6px}.nav a{color:#c7d5e0;text-decoration:none;padding:13px 12px;border-radius:9px;font-size:15px}.nav a.active{border:1px solid #2f73a5;background:#0a2030;color:#4db6ff}.nav-spacer{flex:1}.statusbox{border-top:1px solid var(--line);padding:14px 4px;color:var(--muted);font-size:12px;line-height:1.7}
.workspace{display:grid;grid-template-columns:minmax(560px,1fr) 340px;gap:14px;padding:14px;min-width:0;align-items:stretch}.foh-main{border:1px solid var(--line);border-radius:12px;background:var(--panel);min-width:0;display:flex;flex-direction:column;min-height:calc(100vh - 92px)}.foh-head{padding:16px;border-bottom:1px solid var(--line);display:flex;gap:14px;align-items:center}.avatar{width:58px;height:58px;border-radius:50%;overflow:hidden;border:1px solid #28475e;background:#102638;display:grid;place-items:center;flex:0 0 auto}.avatar.small{width:44px;height:44px}.avatar-img{width:100%;height:100%;object-fit:cover}.avatar-fallback{font-weight:700;color:#b8d4e9}.foh-title{font-size:22px;font-weight:700}.sub{color:var(--muted);font-size:13px;margin-top:4px}.connected{margin-left:auto;color:#56e39a;border:1px solid #1f6f4a;border-radius:999px;padding:5px 9px;font-size:11px}
.chat-scroll{flex:1;overflow:auto;padding:18px;min-height:360px}.intro{border:1px solid #18384d;background:#091722;border-radius:10px;padding:16px;color:#cfe0ed;margin-bottom:14px}.msg{border:1px solid #183248;background:#0b1a27;border-radius:10px;padding:12px 14px;margin-bottom:10px;line-height:1.5}.msg .who{font-size:12px;font-weight:700;color:#51b7ff;margin-bottom:5px}.msg.foh .who{color:#47d5c6}.msg .meta{font-size:11px;color:var(--muted)}.route-card{border:1px solid #168a76;background:#09221f;border-radius:10px;padding:12px 14px;margin:12px 0;display:flex;align-items:center;gap:12px}.route-card .arrow{font-size:25px;color:#35d8b5}.route-card .grow{flex:1}.route-card b{color:#55e6c3}.chat-compose{padding:12px;border-top:1px solid var(--line);display:grid;grid-template-columns:1fr auto;gap:8px}.chat-compose textarea{resize:none;min-height:58px;max-height:140px;border:1px solid #25445b;background:#091722;color:var(--text);border-radius:9px;padding:12px}.primary{border:1px solid #2c71a6;background:#11629a;color:white;border-radius:9px;padding:0 18px;font-weight:700}.capabilities{padding:8px 14px 12px;color:var(--muted);font-size:11px;border-top:1px solid #112b3c;display:flex;gap:12px;flex-wrap:wrap}.capabilities .ok{color:#53dca0}.capabilities .no{color:#ff7c88}
.worker-column{display:flex;flex-direction:column;gap:10px;min-width:0}.worker-heading{display:flex;align-items:end;justify-content:space-between;padding:2px 2px 4px}.worker-heading h2{font-size:15px;margin:0}.worker-heading span{font-size:11px;color:var(--muted)}.worker-card{border:1px solid var(--line);border-radius:10px;background:var(--panel);padding:10px;display:grid;grid-template-columns:44px minmax(0,1fr) auto;gap:10px;align-items:center;min-height:78px;transition:.15s}.worker-card:hover{transform:translateY(-1px);background:#0c1d2b}.worker-card.engineering{border-color:#227fc1}.worker-card.findings{border-color:#7d3fb1}.worker-card.governance{border-color:#2b8a57}.worker-card.builder{border-color:#b75e25}.worker-card.knobhead{border-color:#9d7b1e}.worker-card.steward{border-color:#1c8b82}.worker-name{font-size:13px;font-weight:800}.worker-desc{color:var(--muted);font-size:11px;margin-top:3px;line-height:1.35}.state{font-size:10px;text-transform:uppercase;color:var(--muted)}.state.active{color:#60e29d}.state.waiting{color:#f7cf67}.dot{display:inline-block;width:7px;height:7px;border-radius:50%;background:currentColor;margin-right:4px}.worker-open{grid-column:2/4;justify-self:start;border:0;background:transparent;color:#55b9ff;padding:0;font-size:11px}
.activity{grid-column:1/-1;border:1px solid var(--line);border-radius:11px;background:#081620;padding:12px 14px;display:flex;gap:10px;align-items:center;overflow-x:auto}.activity-title{font-size:11px;color:var(--muted);font-weight:700;margin-right:4px;white-space:nowrap}.node{border:1px solid #255270;border-radius:9px;padding:9px 12px;min-width:175px;background:#0a1a27}.node .n1{font-size:11px;color:#4cb9ff;font-weight:700}.node .n2{font-size:10px;color:var(--muted);margin-top:3px}.patharrow{font-size:22px;color:#8ba3b5}.node.pending{border-style:dashed;color:var(--muted)}
.modal{position:fixed;inset:0;background:rgba(0,0,0,.64);display:none;align-items:center;justify-content:center;padding:22px;z-index:100}.modal.open{display:flex}.modal-card{width:min(820px,94vw);max-height:88vh;overflow:auto;background:#081823;border:1px solid #28516c;border-radius:13px}.modal-head{padding:14px 16px;border-bottom:1px solid var(--line);display:flex;gap:12px;align-items:center;position:sticky;top:0;background:#081823;z-index:2}.modal-title{font-size:18px;font-weight:800}.close{margin-left:auto;border:0;background:transparent;color:#bdd0df;font-size:24px}.tabs{display:flex;gap:5px;padding:10px 14px 0}.tab{border:1px solid #26485f;background:#0a1b28;color:#b9cadd;border-radius:7px;padding:7px 10px;font-size:11px}.tab.active{border-color:#3b9bd8;color:white;background:#0b2b40}.tabbody{padding:14px 16px 18px}.kv{display:grid;grid-template-columns:150px 1fr;gap:7px 10px;font-size:13px;line-height:1.5}.kv b{color:#8fb0c8}.worker-conversation{border:1px solid #17374b;border-radius:9px;padding:12px;min-height:120px;background:#07141e}.worker-input{display:grid;grid-template-columns:1fr auto;gap:8px;margin-top:10px}.worker-input input{background:#07141e;border:1px solid #264a62;color:var(--text);border-radius:8px;padding:10px}.worker-input button{border:1px solid #38566d;background:#102536;color:#8199ab;border-radius:8px;padding:0 14px}.note{font-size:11px;color:var(--muted);margin-top:8px}
@media(max-width:1150px){.shell{grid-template-columns:70px minmax(0,1fr)}.nav a{font-size:0;text-align:center}.statusbox{display:none}}@media(max-width:900px){.workspace{grid-template-columns:1fr}.worker-column{display:grid;grid-template-columns:repeat(2,minmax(0,1fr))}.worker-heading{grid-column:1/-1}.activity{grid-column:1}}@media(max-width:650px){.shell{display:block}.nav{display:none}.workspace{padding:8px}.worker-column{grid-template-columns:1fr}.topbar{padding:0 10px}.brand{font-size:19px}.badge{display:none}.foh-head{align-items:flex-start;flex-wrap:wrap}.connected{margin-left:0}}

/* PMEI_COCKPIT_V5_FIT_PASS */
.topbar{
  height:56px !important;
  padding:0 14px !important;
  gap:10px !important;
}
.brand{
  font-size:23px !important;
  margin-right:8px !important;
}
.badge{
  padding:7px 10px !important;
}
.shell{
  min-height:calc(100vh - 56px) !important;
  grid-template-columns:160px minmax(0,1fr) !important;
}
.nav{
  padding:10px 8px !important;
}
.nav a{
  padding:11px 10px !important;
}
.workspace{
  gap:8px !important;
  padding:6px !important;
}
.foh-main{
  min-height:calc(100vh - 68px) !important;
}
.foh-head{
  padding:11px 14px !important;
  gap:10px !important;
}
.foh-head .avatar{
  width:52px !important;
  height:52px !important;
}
.foh-title{
  font-size:21px !important;
}
.chat-scroll{
  padding:12px 14px !important;
  min-height:300px !important;
}
.intro{
  padding:12px 14px !important;
  margin-bottom:10px !important;
}
.msg{
  padding:10px 12px !important;
  margin-bottom:8px !important;
}
.chat-compose{
  padding:8px 12px !important;
  gap:7px !important;
}
.chat-compose textarea{
  min-height:52px !important;
  padding:10px 12px !important;
}
.primary{
  padding:0 16px !important;
}
.capabilities{
  padding:6px 12px 8px !important;
  gap:10px !important;
}
.worker-column{
  gap:7px !important;
}
.worker-heading{
  padding:0 2px 2px !important;
}
.worker-card{
  min-height:68px !important;
  padding:8px !important;
  gap:8px !important;
  grid-template-columns:40px minmax(0,1fr) auto !important;
}
.worker-card .avatar.small{
  width:40px !important;
  height:40px !important;
}
.worker-name{
  font-size:12.5px !important;
}
.worker-desc{
  font-size:10.5px !important;
  margin-top:2px !important;
}
.worker-open{
  font-size:10.5px !important;
}
.activity{
  padding:8px 10px !important;
  gap:8px !important;
}
.node{
  min-width:160px !important;
  padding:7px 10px !important;
}
@media(max-width:1150px){
  .shell{
    grid-template-columns:64px minmax(0,1fr) !important;
  }
}


.eng-send-btn{border:1px solid rgba(255,255,255,.18);background:rgba(255,255,255,.06);color:inherit;border-radius:10px;padding:8px 11px;font:inherit;font-size:12px;font-weight:700;letter-spacing:.03em;cursor:pointer;white-space:nowrap}
.eng-send-btn:hover{background:rgba(255,255,255,.10)}
.eng-send-btn[disabled]{opacity:.55;cursor:wait}
.eng-send-btn.working::after{content:"  ?"}
.eng-send-btn.result::after{content:"  ?"}
.eng-send-btn.blocked::after{content:"  !"}


/* PMEI_PORT_WORKFLOW_STUDIO_UI_V1 — presentation only */
:root{--bg:#060f20;--panel:#0b1930;--line:#234365;--blue:#58b8ff;--text:#eaf5ff}
body{background:radial-gradient(ellipse at 50% 0%,#102c50 0%,#071429 55%,#050e1d 100%)}
.topbar{background:#071427!important;border-bottom-color:#23466c!important}
.shell{grid-template-columns:185px minmax(0,1fr)!important}
.nav{background:#08162a!important}
.workspace{display:grid!important;grid-template-columns:minmax(0,1fr) 355px!important;grid-template-rows:minmax(450px,1fr) auto!important;gap:12px!important;padding:12px!important}
.foh-main{grid-column:2;grid-row:1;min-height:0!important;max-height:calc(100vh - 160px);border-color:#29547b;background:#0a1a31}
.foh-head{flex-wrap:wrap}.foh-title{font-size:17px!important}.foh-head .avatar{width:38px!important;height:38px!important}
.chat-scroll{min-height:220px!important}.chat-compose{grid-template-columns:1fr auto auto!important}
.worker-column{grid-column:1;grid-row:1;display:grid!important;grid-template-columns:repeat(2,minmax(0,1fr));grid-auto-rows:min-content;align-content:start;gap:9px!important;border:1px solid #254766;border-radius:14px;padding:15px;background:radial-gradient(circle at 50% 40%,#12355a 0%,#09192f 60%,#081529 100%);position:relative}
.worker-heading{grid-column:1/-1;padding:4px 0 12px!important;border-bottom:1px solid #24435d;margin-bottom:5px}.worker-heading h2{font-size:18px!important;letter-spacing:.09em}.worker-heading span{font-size:12px}
.worker-card{min-height:88px!important;background:#0b203a!important;border:1px solid #295276!important;cursor:pointer;border-radius:12px!important;padding:10px!important}
.worker-card:hover,.worker-card.port-selected{border-color:#62c6ff!important;box-shadow:0 0 0 1px #337ba5,0 0 22px #1167a244;transform:none!important}
.worker-card .avatar{display:none!important}.worker-card{grid-template-columns:minmax(0,1fr) auto!important}.worker-card>div:nth-child(2){grid-column:1}.worker-card .state{grid-column:2;grid-row:1}.worker-card .worker-open{grid-column:1/-1;justify-self:start}
.worker-name{font-size:13px!important;color:#dff4ff}.worker-desc{font-size:11px!important}
.activity{grid-column:1/-1!important;grid-row:2;min-height:78px;border-color:#254c71;background:#091a30}
.port-heading{grid-column:1/-1;text-align:center;margin:10px 0 2px;color:#cbeaff;font-size:12px;letter-spacing:.2em}
.port-hub{grid-column:1/-1;justify-self:center;border:2px solid #49bfff;background:radial-gradient(circle,#1b5e92,#0c2847 72%);color:white;border-radius:100%;width:145px;height:145px;box-shadow:0 0 32px #2fafff55;font-weight:800;letter-spacing:.08em;font-size:17px;cursor:pointer;margin:8px 0;position:relative}
.port-hub small{display:block;color:#9adcf9;font-size:10px;margin-top:7px;font-weight:500}
.port-inspector{grid-column:1/-1;border:1px solid #2c5c7f;background:#0b1d35;border-radius:11px;padding:12px;min-height:120px}
.port-inspector h3{font-size:14px;letter-spacing:.08em;margin:0 0 6px;color:#78cfff}.port-inspector p{font-size:12px;line-height:1.45;color:#b9cfe2;margin:5px 0}
.port-actions{display:flex;gap:7px;flex-wrap:wrap;margin-top:10px}.port-actions button{background:#103757;border:1px solid #367eac;border-radius:7px;color:#e3f5ff;padding:8px 12px;font-size:12px}.port-actions button:disabled{opacity:.5;cursor:not-allowed}
.port-output{white-space:pre-wrap;overflow-wrap:anywhere;max-height:240px;overflow:auto;font-size:12px;color:#d9ebf9;margin-top:9px}
.port-activity{grid-column:1/-1;font-size:11px;color:#aac6d8;padding:8px;border-top:1px solid #21415d}
@media(max-width:1100px){.workspace{grid-template-columns:1fr!important}.worker-column{grid-column:1;grid-row:1}.foh-main{grid-column:1;grid-row:2;max-height:none}.activity{grid-row:3}.shell{grid-template-columns:65px minmax(0,1fr)!important}}
@media(max-width:650px){.shell{display:block!important}.worker-column{grid-template-columns:1fr 1fr!important}.port-hub{width:110px;height:110px}.worker-card{grid-template-columns:1fr!important}.worker-card .state{grid-column:1;grid-row:auto}.chat-compose{grid-template-columns:1fr auto!important}.chat-compose textarea{grid-column:1/-1}}

</style>
</head>
<body>
<!-- PMEI_COCKPIT_V5_FOH_ROUTED -->
<header class="topbar"><div class="brand">PMEi COCKPIT</div><div class="badge green">LOCAL</div><div class="badge">READ ONLY</div><div class="badge red">SYSTEM FROZEN</div><div class="badge green">TEST SURFACE</div><div class="spacer"></div><div class="clock" id="clock">--:--:--</div></header>
<div class="shell">
<nav class="nav"><a href="#" class="active">? &nbsp; Cockpit</a><a href="#">? &nbsp; Continuity</a><a href="#">? &nbsp; Evidence</a><a href="#">? &nbsp; Claims</a><a href="#">? &nbsp; Workers</a><a href="#">? &nbsp; Trace</a><div class="nav-spacer"></div><div class="statusbox"><b>SYSTEM STATUS</b><br><span style="color:#54e1a0">?</span> LOCAL<br>Runtime<br><br><b>DATA</b><br>Live job trace only</div></nav>
<main class="workspace">
<section class="foh-main">
<div class="foh-head"><div class="avatar"><img class="avatar-img" src="/static/cockpit/front-of-house.png" alt=""></div><div><div class="foh-title">FRONT-OF-HOUSE DAVE</div><div class="sub">Your persistent PMEi conversation and orchestration entry point.</div></div><button class="tab" type="button" onclick="openWorker('foh','profile')" title="Open Front-of-House Dave qualifications">QUALIFICATIONS</button><button type="button" class="eng-send-btn" id="sendEngineeringBtn" title="Send the current bounded request to Engineering Dave">SEND TO ENGINEERING</button><div class="connected" id="chatStatus">CHECKING</div></div><!-- PMEI_FOH_QUALIFICATIONS_BUTTON_V1 -->
<div class="chat-scroll" id="chatMessages"><div class="intro"><b>Front-of-House</b><br>Discuss the job here. FOH may use bounded PMEi continuity context and coordinate where work should begin. Worker results remain owned by the worker that produced them.</div>{% if last_run %}<div class="route-card"><div class="arrow">?</div><div class="grow"><div class="meta">LATEST GOVERNED JOB</div><b>{{ last_run.worker|upper }} DAVE</b><br><span style="color:#b7cad9">{{ last_run.task }}</span></div><button class="tab" onclick="openWorker('{{ last_run.worker }}','activity')">OPEN ACTIVITY</button></div>{% endif %}</div>
<div class="chat-compose"><textarea id="chatInput" placeholder="Message Front-of-House Dave..."></textarea><button class="tab" id="sendDeterministic" onclick="sendDeterministicChat()" title="Use deterministic PMEi evidence path with no LLM">ZERO-LLM</button><button class="primary" id="sendChat" onclick="sendChat()">SEND</button></div>
<div class="capabilities"><span class="ok">? Conversation</span><span class="ok">? PMEi Read</span><span class="ok">? PMEi READ ONLY Write</span><span class="no">? Promotion</span><span class="no">? State changes</span><span class="no">? Seal / approval</span></div>
</section>
<aside class="worker-column"><div class="worker-heading"><h2>WORKERS</h2><span>Click a worker</span></div>
<div class="worker-card engineering" data-worker="engineering"><div class="avatar small"><img class="avatar-img" src="/static/cockpit/engineering.png" alt=""></div><div><div class="worker-name">ENGINEERING DAVE</div><div class="worker-desc">System behaviour, implementation and technical facts.</div></div><div class="state {% if current_worker == 'engineering' and last_run %}active{% endif %}"><span class="dot"></span>{% if current_worker == 'engineering' and last_run %}LATEST{% else %}IDLE{% endif %}</div><button class="worker-open" onclick="openWorker('engineering','profile')">OPEN</button></div>
<div class="worker-card findings" data-worker="findings"><div class="avatar small"><img class="avatar-img" src="/static/cockpit/findings.png" alt=""></div><div><div class="worker-name">FINDINGS DAVE</div><div class="worker-desc">Finds gaps, anomalies, patterns and candidate observations.</div></div><div class="state {% if current_worker == 'findings' and last_run %}active{% endif %}"><span class="dot"></span>{% if current_worker == 'findings' and last_run %}LATEST{% else %}IDLE{% endif %}</div><button class="worker-open" onclick="openWorker('findings','profile')">OPEN</button></div>
<div class="worker-card governance" data-worker="governance"><div class="avatar small"><img class="avatar-img" src="/static/cockpit/governance.png" alt=""></div><div><div class="worker-name">GOVERNANCE DAVE</div><div class="worker-desc">Contracts, rules, authority and operating boundaries.</div></div><div class="state {% if current_worker == 'governance' and last_run %}active{% endif %}"><span class="dot"></span>{% if current_worker == 'governance' and last_run %}LATEST{% else %}IDLE{% endif %}</div><button class="worker-open" onclick="openWorker('governance','profile')">OPEN</button></div>
<div class="worker-card builder" data-worker="builder"><div class="avatar small"><img class="avatar-img" src="/static/cockpit/builder.png" alt=""></div><div><div class="worker-name">BUILDER DAVE</div><div class="worker-desc">Implements bounded, approved code changes and builds.</div></div><div class="state {% if current_worker == 'builder' and last_run %}active{% endif %}"><span class="dot"></span>{% if current_worker == 'builder' and last_run %}LATEST{% else %}IDLE{% endif %}</div><button class="worker-open" onclick="openWorker('builder','profile')">OPEN</button></div>
<div class="worker-card knobhead" data-worker="knobhead"><div class="avatar small"><img class="avatar-img" src="/static/cockpit/knobhead.png" alt=""></div><div><div class="worker-name">KNOBHEAD DAVE</div><div class="worker-desc">Adversarial verification. Challenges evidence and claims.</div></div><div class="state {% if current_worker == 'knobhead' and last_run %}active{% endif %}"><span class="dot"></span>{% if current_worker == 'knobhead' and last_run %}LATEST{% else %}IDLE{% endif %}</div><button class="worker-open" onclick="openWorker('knobhead','profile')">OPEN</button></div>
<div class="worker-card steward" data-worker="steward"><div class="avatar small"><img class="avatar-img" src="/static/cockpit/steward.png" alt=""></div><div><div class="worker-name">STEWARD DAVE</div><div class="worker-desc">Continuity, provenance, lineage and supersession.</div></div><div class="state {% if current_worker == 'steward' and last_run %}active{% endif %}"><span class="dot"></span>{% if current_worker == 'steward' and last_run %}LATEST{% else %}IDLE{% endif %}</div><button class="worker-open" onclick="openWorker('steward','profile')">OPEN</button></div>
</aside>
<section class="activity"><div class="activity-title">CURRENT ACTIVITY</div><div class="node"><div class="n1">FOH DAVE</div><div class="n2">Persistent conversation</div></div><div class="patharrow">?</div>{% if last_run %}<div class="node"><div class="n1">{{ last_run.worker|upper }} DAVE</div><div class="n2">{{ last_run.validation }} ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â· {{ last_run.provider }} / {{ last_run.model }}</div></div><div class="patharrow">?</div><div class="node pending"><div class="n1">NEXT?</div><div class="n2">Handoff decision pending</div></div>{% else %}<div class="node pending"><div class="n1">ROUTE?</div><div class="n2">FOH decides where work should begin</div></div>{% endif %}</section>
</main></div>
<div class="modal" id="workerModal" aria-hidden="true"><div class="modal-card"><div class="modal-head"><div class="avatar small" id="modalAvatar"><div class="avatar-fallback">D</div></div><div><div class="modal-title" id="modalTitle">WORKER</div><div class="sub" id="modalSub"></div></div><button class="close" onclick="closeWorker()">ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬</button></div><div class="tabs"><button class="tab active" data-tab="profile" onclick="showWorkerTab('profile')">PROFILE</button><button class="tab" data-tab="activity" onclick="showWorkerTab('activity')">ACTIVITY</button><button class="tab" data-tab="conversation" onclick="showWorkerTab('conversation')">TALK TO WORKER</button></div><div class="tabbody" id="workerBody"></div></div></div>
<script>
const workers={"engineering": {"title": "ENGINEERING DAVE", "role": "Engineering", "function": "Interprets system behaviour and technical facts. Performs bounded engineering work.", "boundary": "No promotion, sealing or human-authority substitution.", "provider": "Nemotron 3 Nano 4B for governed Engineering execution", "escalation": "May recommend Builder, Findings, Governance, Knobhead, Steward or Human Authority as relevant.", "avatar": "<img class=\"avatar-img\" src=\"/static/cockpit/engineering.png\" alt=\"\">"}, "findings": {"title": "FINDINGS DAVE", "role": "Findings", "function": "Extracts and structures observations, gaps and candidate findings.", "boundary": "Findings remain candidate until the required verification and authority path completes.", "provider": "Worker provider is separate from worker identity.", "escalation": "May recommend Engineering, Governance, Knobhead, Steward or Human Authority.", "avatar": "<img class=\"avatar-img\" src=\"/static/cockpit/findings.png\" alt=\"\">"}, "governance": {"title": "GOVERNANCE DAVE", "role": "Governance", "function": "Interprets approved contracts, rules and authority boundaries.", "boundary": "Cannot manufacture Human Authority.", "provider": "Worker provider is separate from worker identity.", "escalation": "May return to originating worker, require Knobhead challenge, or require Human Authority.", "avatar": "<img class=\"avatar-img\" src=\"/static/cockpit/governance.png\" alt=\"\">"}, "builder": {"title": "BUILDER DAVE", "role": "Builder", "function": "Implements bounded build work supplied under approved scope.", "boundary": "No unbounded edits, deployment, sealing or authority expansion.", "provider": "Worker provider is separate from worker identity.", "escalation": "Normally returns build result for Engineering and/or Knobhead verification.", "avatar": "<img class=\"avatar-img\" src=\"/static/cockpit/builder.png\" alt=\"\">"}, "knobhead": {"title": "KNOBHEAD DAVE", "role": "Adversarial Verification", "function": "Independently challenges evidence, claims and candidate acceptance.", "boundary": "Does not promote or build merely because another worker recommends it.", "provider": "Worker provider is separate from worker identity.", "escalation": "May PASS, HOLD, reject a claim, or require another bounded worker/human decision.", "avatar": "<img class=\"avatar-img\" src=\"/static/cockpit/knobhead.png\" alt=\"\">"}, "steward": {"title": "STEWARD DAVE", "role": "Steward", "function": "Maintains continuity, provenance, deduplication, lineage and supersession.", "boundary": "Records validated provenance. Does not manufacture validation.", "provider": "Worker provider is separate from worker identity.", "escalation": "Usually acts after validated provenance or when continuity structure needs review.", "avatar": "<img class=\"avatar-img\" src=\"/static/cockpit/steward.png\" alt=\"\">"}};let selectedWorker='engineering',selectedTab='profile',chatHistory=[],lastPhilMessage='';/* PMEI_ENGINEERING_HANDOFF_STATE_V2_2 */
function esc(s){return String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#039;'}[c]))}function tick(){document.getElementById('clock').textContent=new Date().toLocaleTimeString()}tick();setInterval(tick,1000);
async function status(){try{const r=await fetch('/chat/status');const d=await r.json();const s=document.getElementById('chatStatus');s.textContent=d.connected?'CONNECTED: '+d.model:'NOT CONNECTED';s.style.color=d.connected?'#56e39a':'#ff7180'}catch(e){document.getElementById('chatStatus').textContent='STATUS ERROR'}}status();
async function sendChat(){const box=document.getElementById('chatInput'),msg=box.value.trim();if(!msg)return;lastPhilMessage=msg;const wrap=document.getElementById('chatMessages');wrap.insertAdjacentHTML('beforeend','<div class="msg"><div class="who">PHIL</div>'+esc(msg)+'</div>');box.value='';const fd=new FormData();fd.append('message',msg);fd.append('history',JSON.stringify(chatHistory));const btn=document.getElementById('sendChat');btn.disabled=true;try{const r=await fetch('/chat',{method:'POST',body:fd});const d=await r.json();const t=d.ok?d.text:(d.error||'Chat failed');wrap.insertAdjacentHTML('beforeend','<div class="msg foh"><div class="who">FRONT-OF-HOUSE DAVE</div><div class="meta">persistent FOH conversation</div>'+esc(t)+'</div>');if(d.ok){chatHistory.push({role:'user',content:msg},{role:'assistant',content:t});chatHistory=chatHistory.slice(-20)}wrap.scrollTop=wrap.scrollHeight}catch(e){wrap.insertAdjacentHTML('beforeend','<div class="msg"><div class="who">SYSTEM</div>Chat request failed.</div>')}finally{btn.disabled=false}}

async function sendDeterministicChat(){
  const box=document.getElementById('chatInput');
  const msg=box.value.trim();

  if(!msg)return;

  lastPhilMessage=msg;

  const wrap=document.getElementById('chatMessages');

  wrap.insertAdjacentHTML(
    'beforeend',
    '<div class="msg"><div class="who">PHIL</div>'+esc(msg)+'</div>'
  );

  box.value='';

  const fd=new FormData();
  fd.append('message',msg);

  const btn=document.getElementById('sendDeterministic');
  const normalBtn=document.getElementById('sendChat');

  btn.disabled=true;
  normalBtn.disabled=true;

  const original=btn.textContent;
  btn.textContent='WORKING...';

  try{
    const r=await fetch(
      '/chat/deterministic',
      {
        method:'POST',
        body:fd
      }
    );

    const d=await r.json();

    const t=d.ok
      ? d.text
      : (d.error||'Deterministic Dave failed');

    const meta=d.ok
      ? 'ZERO-LLM ? PMEi READ ONLY ? '+String(d.timing?.total_ms??'?')+' ms'
      : 'ZERO-LLM ? FAILED CLOSED';

    wrap.insertAdjacentHTML(
      'beforeend',
      '<div class="msg foh">'+
      '<div class="who">DETERMINISTIC DAVE</div>'+
      '<div class="meta">'+esc(meta)+'</div>'+
      '<div style="white-space:pre-wrap">'+esc(t)+'</div>'+
      '</div>'
    );

    wrap.scrollTop=wrap.scrollHeight;
  }
  catch(e){
    wrap.insertAdjacentHTML(
      'beforeend',
      '<div class="msg">'+
      '<div class="who">SYSTEM</div>'+
      'Deterministic Dave request failed.'+
      '</div>'
    );
  }
  finally{
    btn.disabled=false;
    normalBtn.disabled=false;
    btn.textContent=original;
  }
}

document.getElementById('chatInput').addEventListener('keydown',e=>{if(e.key==='Enter'&&!e.shiftKey){e.preventDefault();sendChat()}});
async function openWorker(key,tab='profile'){
  selectedWorker=workers[key]?key:'engineering';
  selectedTab=tab;
  const fallback=workers[selectedWorker];
  document.getElementById('modalTitle').textContent=fallback.title;
  document.getElementById('modalSub').textContent=fallback.role+' worker';
  document.getElementById('modalAvatar').innerHTML=fallback.avatar||'<div class="avatar-fallback">D</div>';
  document.getElementById('workerModal').classList.add('open');
  document.getElementById('workerModal').setAttribute('aria-hidden','false');
  try{
    const r=await fetch('/workers/'+encodeURIComponent(selectedWorker));
    const d=await r.json();
    window.__PMEI_SELECTED_WORKER_DETAIL__=(r.ok&&d.ok)?d:null;
    if(window.__PMEI_SELECTED_WORKER_DETAIL__){
      document.getElementById('modalTitle').textContent=d.worker.name||fallback.title;
      document.getElementById('modalSub').textContent=(d.worker.role||fallback.role)+' worker';
    }
  }catch(e){window.__PMEI_SELECTED_WORKER_DETAIL__=null}
  showWorkerTab(tab);
}
function closeWorker(){document.getElementById('workerModal').classList.remove('open');document.getElementById('workerModal').setAttribute('aria-hidden','true')}
function statusPill(status){
  const s=String(status||'unknown');
  const cls=(s==='connected')?'ok':((s==='prohibited')?'no':'');
  return '<span class="'+cls+'">'+esc(s.replaceAll('_',' ').toUpperCase())+'</span>';
}
function showWorkerTab(tab){
  selectedTab=tab;
  document.querySelectorAll('.tab[data-tab]').forEach(b=>b.classList.toggle('active',b.dataset.tab===tab));
  const fallback=workers[selectedWorker];
  const detail=window.__PMEI_SELECTED_WORKER_DETAIL__||null;
  const live=detail&&detail.worker?detail.worker:null;
  const body=document.getElementById('workerBody');

  if(tab==='profile'){
    if(!live){
      body.innerHTML='<div class="worker-conversation">Worker manifest could not be loaded. No qualifications are being invented.</div>';
      return;
    }
    const provider=live.provider||{};
    const interfaces=(live.interfaces||[]).map(x =>
      '<div style="display:flex;justify-content:space-between;gap:16px;padding:6px 0;border-bottom:1px solid #143043"><span>'+
      esc(x.name)+'</span><span>'+statusPill(x.status)+'</span></div>'
    ).join('');
    const authority=(live.authority||[]).map(x=>'<li style="margin-bottom:5px">'+esc(x)+'</li>').join('');
    const contracts=(live.source_contracts||[]).map(x=>'<li>'+esc(x)+'</li>').join('');
    body.innerHTML=`<div class="kv">
      <b>Official role</b><span>${esc(live.role||'unknown')}</span>
      <b>Function</b><span>${esc(live.function||'not established')}</span>
      <b>Cockpit execution</b><span>${statusPill(live.cockpit_execution)}</span>
      <b>Provider</b><span>${provider.name?esc(provider.name):'NOT CONNECTED'}</span>
      <b>Model</b><span>${provider.model?esc(provider.model):'NOT CONNECTED'}</span>
      <b>Worker / model identity</b><span>${provider.identity_separate_from_worker?'SEPARATE':'UNKNOWN'}</span>
    </div>
    <hr style="border:0;border-top:1px solid #173348;margin:14px 0">
    <b>AUTHORITY BOUNDARY</b><ul style="line-height:1.45;padding-left:20px">${authority}</ul>
    <b>INTERFACES / QUALIFICATIONS</b><div style="margin-top:6px">${interfaces||'<span class="sub">None established.</span>'}</div>
    <div style="margin-top:14px"><b>SOURCE CONTRACTS</b><ul style="line-height:1.45;padding-left:20px">${contracts}</ul></div>
    <div class="note">This panel reports configured/status information. Displaying a capability does not grant authority.</div>`;
  }else if(tab==='activity'){
    const embedded={% if last_run %}{{ last_run|tojson }}{% else %}null{% endif %};
    const asyncEngineering=(selectedWorker==='engineering')?(window.__PMEI_ENGINEERING_ASYNC_JOB__||null):null;
    const latest=asyncEngineering||((detail&&detail.latest_activity)?detail.latest_activity:embedded);
    if(latest&&String(latest.worker||'').toLowerCase()===selectedWorker){
      body.innerHTML=`<div class="kv">
        <b>Job ID</b><span>${esc(latest.job_id)}</span>
        <b>Request</b><span>${esc(latest.task)}</span>
        <b>Validation</b><span>${esc(latest.validation)}</span>
        <b>Provider / model</b><span>${esc(latest.provider)} / ${esc(latest.model)}</span>
        <b>Wall time</b><span>${esc(latest.wall_seconds)} seconds</span>
        <b>State mutated</b><span>${esc(latest.mutated)}</span>
        <b>Transition authority</b><span>${esc(latest.authority)}</span>
      </div><hr style="border:0;border-top:1px solid #173348;margin:14px 0">
      <div style="white-space:pre-wrap;overflow-wrap:anywhere">${esc(latest.output||'(no output)')}</div>`;
    }else{
      body.innerHTML='<div class="worker-conversation">No current governed activity is recorded for this worker in the local cockpit.</div>';
    }
  }else{
    body.innerHTML=`<div class="worker-conversation"><b>${esc(fallback.title)}</b><br><br>
    Direct worker conversation is part of this cockpit design, but it is <b>not connected yet</b>.
    When connected, this conversation will be scoped to this worker's current job and will not replace FOH.<br><br>
    Worker input is not automatically Human Authority. Formal approval remains a separate governed action.</div>
    <div class="worker-input"><input disabled placeholder="Worker conversation not connected yet"><button disabled>SEND</button></div>
    <div class="note">UI present. Backend intentionally inert until a bounded worker-conversation contract is implemented.</div>`;
  }
}
document.addEventListener('keydown',e=>{if(e.key==='Escape')closeWorker()});document.getElementById('workerModal').addEventListener('click',e=>{if(e.target.id==='workerModal')closeWorker()});

// PMEI_ENGINEERING_ASYNC_V2_UI
// PMEI_ENGINEERING_CARD_PROJECTION_V2_3
(function(){
  const btn = document.getElementById("sendEngineeringBtn");
  if (!btn) return;

  function findTaskText(){
    const box = document.getElementById("chatInput");
    const current = box ? String(box.value || "").trim() : "";
    if (current) return current;
    return String(lastPhilMessage || "").trim();
  }

  function renderEngineeringState(run, job){
    const status = String((run && run.status) || "IDLE").toUpperCase();
    btn.classList.remove("working","result","blocked");

    const card = document.querySelector('.worker-card[data-worker="engineering"]');
    const state = card ? card.querySelector('.state') : null;
    const desc = card ? card.querySelector('.worker-desc') : null;
    const open = card ? card.querySelector('.worker-open') : null;

    if (job && String(job.worker || '').toLowerCase() === 'engineering') {
      window.__PMEI_ENGINEERING_ASYNC_JOB__ = job;
    }

    if (status === "WORKING"){
      btn.disabled = true;
      btn.classList.add("working");
      btn.textContent = "ENGINEERING WORKING";
      if (state) {
        state.classList.add("active");
        state.innerHTML = '<span class="dot"></span>WORKING';
      }
      if (desc) desc.textContent = "Working on the bounded Engineering request...";
      if (open) {
        open.textContent = "OPEN";
        open.onclick = () => openWorker('engineering','activity');
      }
    } else if (status === "RESULT"){
      btn.disabled = false;
      btn.classList.add("result");
      btn.textContent = "ENGINEERING RESULT";
      if (state) {
        state.classList.add("active");
        state.innerHTML = '<span class="dot"></span>RESULT';
      }
      if (desc) {
        const output = job && job.output ? String(job.output).replace(/\s+/g,' ').trim() : "";
        desc.textContent = output ? output.slice(0, 150) + (output.length > 150 ? "..." : "") : "Engineering result ready.";
      }
      if (open) {
        open.textContent = "OPEN RESULT";
        open.onclick = () => openWorker('engineering','activity');
      }
      if (selectedWorker === 'engineering' && selectedTab === 'activity' &&
          document.getElementById('workerModal').classList.contains('open')) {
        showWorkerTab('activity');
      }
    } else if (status === "BLOCKED"){
      btn.disabled = false;
      btn.classList.add("blocked");
      btn.textContent = "ENGINEERING BLOCKED";
      if (state) {
        state.classList.add("active");
        state.innerHTML = '<span class="dot"></span>BLOCKED';
      }
      if (desc) desc.textContent = (run && run.error) ? String(run.error) : "Engineering is blocked.";
      if (open) {
        open.textContent = "OPEN";
        open.onclick = () => openWorker('engineering','activity');
      }
    } else {
      btn.disabled = false;
      btn.textContent = "SEND TO ENGINEERING";
    }
  }

  async function refreshEngineeringStatus(){
    try{
      const r = await fetch("/orchestration/engineering/status", {cache:"no-store"});
      if (!r.ok) return;
      const data = await r.json();
      renderEngineeringState(data.run || {}, data.job || null);
    }catch(_){}
  }

  btn.addEventListener("click", async function(){
    const task = findTaskText();
    if (!task){
      alert("Send or type a request to Front-of-House Dave first, then hand it to Engineering.");
      return;
    }

    btn.disabled = true;
    btn.textContent = "SENDING TO ENGINEERING";

    try{
      const r = await fetch("/orchestration/request-engineering-async", {
        method:"POST",
        headers:{"Content-Type":"application/json"},
        body:JSON.stringify({task})
      });
      const data = await r.json();

      if (!r.ok){
        renderEngineeringState((data && data.run) || {}, (data && data.job) || null);
        if (data && data.error === "engineering_already_working") return;
        alert((data && data.error) || "Engineering handoff failed.");
        return;
      }

      renderEngineeringState(data.run || {status:"WORKING"}, data.job || null);
    }catch(err){
      btn.disabled = false;
      btn.classList.add("blocked");
      btn.textContent = "ENGINEERING BLOCKED";
      alert("Engineering handoff failed: " + err);
    }
  });

  refreshEngineeringStatus();
  setInterval(refreshEngineeringStatus, 1500);
})();


// PMEI_PORT_WORKFLOW_STUDIO_UI_V1: no backend authority or routing changes.
(function(){
 const eligible=new Set(['architecture','engineering','findings','governance','steward']);
 const display=['architecture','engineering','findings','governance','steward','builder','knobhead'];
 const titles={architecture:'ARCHITECTURE DAVE',engineering:'ENGINEERING DAVE',findings:'FINDINGS DAVE',governance:'GOVERNANCE DAVE',steward:'STEWARD DAVE',builder:'BUILDER DAVE',knobhead:'KNOBHEAD DAVE'};
 const column=document.querySelector('.worker-column');
 if(!column)return;
 const heading=column.querySelector('.worker-heading');
 if(heading){heading.querySelector('h2').textContent='WORKFLOW STUDIO';heading.querySelector('span').textContent='SELECT A WORKER';}
 const intro=document.createElement('div');intro.className='port-heading';intro.textContent='PMEi  /  GOVERNED ORCHESTRATION';
 if(heading)heading.after(intro);
 const hub=document.createElement('button');hub.type='button';hub.className='port-hub';hub.innerHTML='FOH DAVE<small>RETURN TO CHAT</small>';
 hub.onclick=()=>{select(null);document.getElementById('chatInput').focus();};intro.after(hub);
 const inspector=document.createElement('section');inspector.className='port-inspector';inspector.setAttribute('aria-live','polite');column.appendChild(inspector);
 const activity=document.createElement('div');activity.className='port-activity';activity.textContent='No worker selected. Select a worker or return to FOH.';column.appendChild(activity);
 let current=null,detail=null,busy=false;const outputs={};
 function button(label,fn,disabled){const b=document.createElement('button');b.type='button';b.textContent=label;b.disabled=!!disabled;b.onclick=fn;return b;}
 function putLine(parent,tag,value){const e=document.createElement(tag);e.textContent=value;parent.appendChild(e);return e;}
 function select(key){
   current=key;detail=null;
   column.querySelectorAll('.worker-card').forEach(c=>c.classList.toggle('port-selected',c.dataset.worker===key));
   render();
   if(!key)return;
   fetch('/workers/'+encodeURIComponent(key),{cache:'no-store'}).then(r=>{if(!r.ok)throw Error('Worker manifest unavailable');return r.json();}).then(d=>{if(current!==key)return;detail=d.ok?d:null;render();}).catch(()=>{if(current===key){detail=null;render();}});
 }
 function render(){
   inspector.replaceChildren();
   putLine(inspector,'h3',current?(titles[current]||current.toUpperCase()):'FRONT-OF-HOUSE DAVE');
   if(!current){putLine(inspector,'p','Persistent FOH conversation. PMEi continuity remains read-only. Select a worker to inspect its available actions.');const a=document.createElement('div');a.className='port-actions';a.appendChild(button('FOCUS FOH',()=>document.getElementById('chatInput').focus()));inspector.appendChild(a);return;}
   const worker=detail&&detail.worker;
   putLine(inspector,'p',worker?(worker.function||'No function established'):'Loading worker manifest / not verified');
   putLine(inspector,'p','Direct worker conversation: not connected. Worker request: '+(eligible.has(current)?'governed route available':'governed transition required')+'.');
   const actions=document.createElement('div');actions.className='port-actions';inspector.appendChild(actions);
   actions.appendChild(button('PROFILE',()=>openWorker(current,'profile')));
   actions.appendChild(button('ACTIVITY',()=>openWorker(current,'activity')));
   actions.appendChild(button('CHAT VIA FOH',()=>{const box=document.getElementById('chatInput');box.value='I want to discuss a request for '+titles[current]+'. '+box.value;box.focus();}));
   if(eligible.has(current))actions.appendChild(button('ENGAGE (REQUEST)',()=>engage(current),busy));
   else actions.appendChild(button('TRANSITION REQUIRED',()=>{},true));
   actions.appendChild(button('RETURN TO FOH',()=>select(null)));
   if(outputs[current]){const output=document.createElement('div');output.className='port-output';output.textContent=outputs[current];inspector.appendChild(output);}
 }
 async function engage(key){
   if(busy||!eligible.has(key))return;
   const box=document.getElementById('chatInput');const task=String(box.value||lastPhilMessage||'').trim();
   if(!task){activity.textContent='Type a bounded task in FOH first; no job started.';box.focus();return;}
   if(!window.confirm('Submit this bounded request to '+titles[key]+'?\n\n'+task.slice(0,450)))return;
   busy=true;render();activity.textContent='Submitting governed request to '+titles[key]+'...';
   try{
     const response=await fetch('/orchestration/request-worker',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({task,requested_worker:key})});
     const d=await response.json();
     if(!response.ok||!d.ok){activity.textContent='Request not accepted: '+String(d.error||response.status);return;}
     const e=d.execution||{};
     activity.textContent='Job '+String(d.job_id||'unknown')+' | '+key+' | '+String(e.validation||'UNVERIFIED')+' | '+(e.ok?'execution returned':'execution failed')+' | request only';
     outputs[key]='Job '+String(d.job_id||'unknown')+' | '+String(e.validation||'UNVERIFIED')+' | '+String(d.authority||'request_only')+'\n\n'+String(e.output||'(no output)');
   }catch(err){activity.textContent='Worker request failed: '+String(err);}
   finally{busy=false;render();}
 }
 // Replace only the inert cards' presentation; existing modal and Engineering controls stay intact.
 for(const key of display){
   let card=column.querySelector('.worker-card[data-worker="'+key+'"]');
   if(!card){card=document.createElement('div');card.className='worker-card '+key;card.dataset.worker=key;putLine(card,'div',titles[key]);column.insertBefore(card,inspector);}
   card.addEventListener('click',e=>{if(e.target.closest('button'))return;select(key);});
   const open=card.querySelector('.worker-open');if(open)open.textContent='INSPECT';
 }
 select(null);
 fetch('/chat/status',{cache:'no-store'}).then(r=>r.json()).then(d=>{
   const pill=document.getElementById('chatStatus');if(pill)pill.textContent=(d.connected?'OLLAMA CONNECTED':'OLLAMA OFFLINE')+' / '+(d.pmei_read_connected?'PMEi CONFIGURED':'PMEi NOT CONFIGURED');
 }).catch(()=>{});
})();

</script></body></html>
"""

from orchestration.cockpit_polling import connect_chat_polling
PAGE = connect_chat_polling(PAGE)



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
        last_run=result,
        current_worker=current_worker,
        task=(
            LAST_RUN.get("task", "")
            if LAST_RUN
            else ""
        ),
    )


@app.post("/run")
def run_job():

    global LAST_RUN

    task = request.form.get("task", "").strip()

    if not task:
        return redirect(url_for("index"))

    job_id = (
        "web-"
        + uuid.uuid4().hex[:12]
    )

    engine.create_job(
        OrchestrationJob(
            job_id=job_id,
            task=task,
            requested_worker="engineering",
        )
    )

    add_live_event(

        "JOB",

        f"{job_id} ? governed Engineering job created",

    )

    before = engine.get_state(job_id)

    started = time.perf_counter()

    add_live_event(

        "WORKER",

        f"{job_id} ? Nemotron execution started",

    )

    execution = executor.execute(job_id)

    wall_seconds = (
        time.perf_counter()
        - started
    )

    after = engine.get_state(job_id)



    meta = execution.metadata or {}


    if meta.get("pmei_retrieval_ok"):

        add_live_event(

            "RETRIEVE",

            (

                f"{meta.get('pmei_records_received', 0)} records received ? "

                f"{meta.get('pmei_evidence_count', 0)} evidence admitted"

            ),

            route=meta.get("pmei_route"),

        )

    else:

        add_live_event(

            "RETRIEVE",

            "PMEi retrieval unavailable or failed",

        )


    add_live_event(

        "WORKER",

        (

            f"{job_id} ? "

            + ("worker response completed" if execution.ok else "worker execution failed")

        ),

        provider=execution.provider,

        model=execution.model,

    )


    add_live_event(

        "VALIDATE",

        (

            "execution accepted"

            if execution.ok

            else "execution not accepted"

        ),

        orchestration_state_changed=meta.get("orchestration_state_changed"),

        transition_authority=meta.get("transition_authority"),

    )
    validation = execution.metadata.get(
        "validation_status"
    )

    if validation is None:
        validation = (
            "ACCEPT"
            if execution.ok
            else "UNKNOWN"
        )

    LAST_RUN = {

        "job_id": job_id,

        "task": task,

        "worker": after.current_worker,

        "provider": execution.provider,

        "model": execution.model,

        "validation": validation,

        "validation_issues":
            execution.metadata.get(
                "validation_issues"
            ) or [],

        "wall_seconds":
            round(
                wall_seconds,
                2,
            ),

        # Diagnostic telemetry already returned by Ollama.
        # Observation only: no inference, authority or state semantics.
        "provider_total_duration_ns":
            meta.get("total_duration"),

        "provider_load_duration_ns":
            meta.get("load_duration"),

        "provider_prompt_eval_duration_ns":
            meta.get("prompt_eval_duration"),

        "provider_eval_duration_ns":
            meta.get("eval_duration"),

        "provider_done_reason":
            meta.get("done_reason"),

        "provider_reasoning_wrapper_removed":
            meta.get("reasoning_wrapper_removed"),

        "prompt_eval_count":
            meta.get("prompt_eval_count"),

        "eval_count":
            meta.get("eval_count"),

        "num_ctx":
            meta.get("num_ctx"),

        "num_predict":
            meta.get("num_predict"),

        "before": (
            f"{before.status} / "
            f"{before.current_worker} / "
            f"history "
            f"{len(before.history)}"
        ),

        "after": (
            f"{after.status} / "
            f"{after.current_worker} / "
            f"history "
            f"{len(after.history)}"
        ),

        "mutated":
            execution.metadata.get(
                "orchestration_state_changed",
                False,
            ),

        "authority":
            execution.metadata.get(
                "transition_authority",
                False,
            ),

        "output": (
            execution.output_text
            or execution.error
            or "(no output)"
        ),
    }

    return redirect(
        url_for("index")
    )





# PMEI_FOH_RECORD255_V1
def _foh_pmei_base_url():
    return (os.environ.get("DAVE_RUNNER_URL", "").strip() or "https://dave-runner.onrender.com").rstrip("/")

def _foh_pmei_headers():
    key = os.environ.get("DAVE_RUNNER_API_KEY", "").strip()
    h = {"Content-Type": "application/json"}
    if key:
        h["X-API-KEY"] = key
    return h

def _foh_pmei_configured():
    return bool(os.environ.get("DAVE_RUNNER_API_KEY", "").strip())

def _foh_pmei_read(limit=12):
    if not _foh_pmei_configured():
        return {"ok": False, "error": "PMEi API key is not configured.", "records": []}
    try:
        r = requests.post(
            _foh_pmei_base_url() + "/memory/continuity/get",
            headers=_foh_pmei_headers(),
            json={"limit": max(1, min(int(limit), 25))},
            timeout=20,
        )
        r.raise_for_status()
        data = r.json().get("data") or {}
        return {"ok": True, "records": data.get("items") or [], "route": "/memory/continuity/get"}
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "records": [], "route": "/memory/continuity/get"}

def _foh_pmei_prepare_for_question(question):
    """
    Prepare bounded PMEi evidence for Front-of-House through the same
    deterministic evidence-position and worker-packet contracts used by
    governed workers.

    FOH remains READ ONLY and receives no verification, promotion, mutation,
    sealing or orchestration-transition authority.
    """

    from orchestration.evidence_adapter import PMEiEvidenceAdapter
    from orchestration.worker_packet import build_worker_packet_builder

    adapter = PMEiEvidenceAdapter(
        max_evidence=8,
        archive_search=True,
    )

    try:
        packet = adapter.prepare(
            question
        )
    except Exception as exc:
        return {
            "ok": False,
            "retrieval_ok": False,
            "context": "",
            "error": (
                "FOH evidence preparation failed: "
                f"{type(exc).__name__}: {exc}"
            ),
        }

    evidence_packet = {
        "retrieval_ok":
            bool(packet.retrieval_ok),

        "question":
            packet.question,

        "query":
            packet.query,

        "records_received":
            packet.records_received,

        "evidence_count":
            packet.evidence_count,

        "route":
            (
                packet.transport.get("route")
                if isinstance(packet.transport, dict)
                else None
            ),

        "transport":
            (
                dict(packet.transport)
                if isinstance(packet.transport, dict)
                else {}
            ),

        "evidence":
            packet.evidence,

        "error":
            packet.error,
    }

    transport = evidence_packet.get("transport") or {}

    if isinstance(transport, dict):
        evidence_packet["historical_scan"] = bool(
            transport.get("historical_scan", False)
        )
        evidence_packet["scanned_count"] = int(
            transport.get("scanned_count", 0) or 0
        )
        evidence_packet["available_count"] = (
            int(transport.get("available_count"))
            if transport.get("available_count") is not None
            else None
        )
        evidence_packet["pages"] = int(
            transport.get("pages", 0) or 0
        )
        evidence_packet["exhaustive"] = bool(
            transport.get("exhaustive", False)
        )
        evidence_packet["historical_errors"] = list(
            transport.get("errors", []) or []
        )
        evidence_packet["newest_record"] = dict(
            transport.get("newest_record", {}) or {}
        )
        evidence_packet["oldest_record"] = dict(
            transport.get("oldest_record", {}) or {}
        )

    builder = build_worker_packet_builder()

    worker_packet = builder.build(
        worker_role="foh",
        task=question,
        evidence_packet=evidence_packet,
        job_id="foh-read-only",
    )

    return {
        "ok": True,
        "retrieval_ok": worker_packet.retrieval_ok,
        "records_received": worker_packet.records_received,
        "evidence_count": worker_packet.evidence_count,
        "route": worker_packet.retrieval_route,
        "eligible_source_records": worker_packet.source_records,
        "excluded_records": worker_packet.excluded_records,
        "evidence_positions": worker_packet.evidence_positions,
        "context": worker_packet.rendered_text,
        "raw_passages_exposed_to_provider": False,
        "error": packet.error,
    }


def _foh_write_read_only(note):
    if not _foh_pmei_configured():
        return {"ok": False, "error": "PMEi API key is not configured."}
    note = str(note or "").strip()
    if not note:
        return {"ok": False, "error": "No READ ONLY continuity content supplied."}
    note = note[:8000]
    save_id = "foh-read-only-" + uuid.uuid4().hex[:16]
    payload = {
        "save_id": save_id,
        "session_ref": "pmei_orchestration",
        "human_title": "READ ONLY - Front-of-House Dave Continuity Note",
        "human_summary": "Front-of-House-created READ ONLY continuity information. Non-authoritative.",
        "decision_made": "READ ONLY information transfer only. No governed decision established.",
        "why_it_matters": "Preserves FOH information while retaining PMEi Record 255 authority boundaries.",
        "context_shard": note,
        "drift_score": 0.01,
        "next_steps": [],
        "chat_recall": [],
        "goal_state": "",
        "active_constraints": ["READ ONLY", "Front-of-House-created", "non-authoritative", "no promotion", "no verification", "no canonical-state mutation", "no orchestration transition authority"],
        "key_insights": [],
        "open_threads": [],
        "anchor_points": ["PMEi Record 255 - FOH Read/Write READ ONLY Continuity Contract"],
        "last_stable_state": "FOH may append READ ONLY continuity only under Record 255.",
        "seal": "lawful",
    }
    try:
        r = requests.post(
            _foh_pmei_base_url() + "/memory/continuity/save",
            headers=_foh_pmei_headers(),
            json=payload,
            timeout=20,
        )
        r.raise_for_status()
        return {"ok": True, "route": "/memory/continuity/save", "save_id": save_id, "classification": "READ ONLY", "result": r.json()}
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "route": "/memory/continuity/save"}

@app.post("/foh/pmei/read-only-write")
def foh_pmei_read_only_write():
    result = _foh_write_read_only(request.form.get("note", "").strip())
    add_live_event(
        "FOH_PMEI_WRITE",
        "FOH READ ONLY continuity appended" if result.get("ok") else "FOH READ ONLY continuity append failed",
        authority="read_only_continuity",
        classification="READ ONLY",
        transition_authority=False,
        promotion_authority=False,
        ok=bool(result.get("ok")),
    )
    return result, (200 if result.get("ok") else 502)

# PMEI_WORKER_QUALIFICATIONS_V1

def _worker_manifest():
    pmei_connected = _foh_pmei_configured() if "_foh_pmei_configured" in globals() else False
    foh_local_connected = True

    return {
        "foh": {
            "name": "FRONT-OF-HOUSE DAVE",
            "role": "Front-of-House",
            "function": "Persistent user-facing PMEi conversation and orchestration entry point.",
            "authority": [
                "May discuss and bound a request.",
                "May use bounded PMEi continuity retrieval.",
                "May append new READ ONLY continuity through the Record 255 server adapter.",
                "May not verify, promote, canonicalise, seal, approve, overwrite/delete, deploy, or transition worker state.",
            ],
            "provider": {
                "status": "connected" if foh_local_connected else "not_connected",
                "name": "Ollama",
                "model": "nemotron-3-nano:4b",
                "identity_separate_from_worker": True,
            },
            "interfaces": [
                {"name": "FOH conversation", "status": "connected" if foh_local_connected else "not_connected"},
                {"name": "PMEi continuity read", "status": "connected" if pmei_connected else "not_connected"},
                {"name": "PMEi READ ONLY append", "status": "connected" if pmei_connected else "not_connected"},
                {"name": "Worker transition authority", "status": "prohibited"},
                {"name": "Seal / approval", "status": "prohibited"},
            ],
            "source_contracts": ["PMEi Record 255"],
            "cockpit_execution": "connected",
        },
        "engineering": {
            "name": "ENGINEERING DAVE",
            "role": "Engineering",
            "function": "Interprets system behaviour and technical facts and executes bounded governed Engineering jobs.",
            "authority": [
                "May inspect configuration, code-path behaviour, telemetry and evidence supplied to the Engineering job.",
                "May produce Engineering analysis and bounded implementation recommendations.",
                "May not promote evidence, seal state, substitute Human Authority, or deploy merely from its own recommendation.",
            ],
            "provider": {
                "status": "connected",
                "name": "Ollama",
                "model": "nemotron-3-nano:4b",
                "identity_separate_from_worker": True,
            },
            "interfaces": [
                {"name": "Governed /run execution", "status": "connected"},
                {"name": "PMEi evidence retrieval", "status": "connected"},
                {"name": "Output validation", "status": "connected"},
                {"name": "Orchestration transition authority", "status": "prohibited"},
            ],
            "source_contracts": ["PMEi governed worker contracts", "Record 241 freeze"],
            "cockpit_execution": "connected",
        },
        "findings": {
            "name": "FINDINGS DAVE",
            "role": "Findings",
            "function": "Extracts and structures observations, gaps, anomalies and candidate findings.",
            "authority": [
                "May produce candidate findings from admitted evidence.",
                "Candidate findings are not verified or canonical merely because Findings produced them.",
            ],
            "provider": {"status": "not_connected", "name": None, "model": None, "identity_separate_from_worker": True},
            "interfaces": [
                {"name": "Cockpit execution route", "status": "not_implemented"},
                {"name": "Direct worker conversation", "status": "not_implemented"},
            ],
            "source_contracts": ["PMEi worker orchestration continuity"],
            "cockpit_execution": "not_implemented",
        },
        "governance": {
            "name": "GOVERNANCE DAVE",
            "role": "Governance",
            "function": "Interprets approved contracts, rules and authority boundaries.",
            "authority": [
                "May identify applicable governance requirements and authority boundaries.",
                "May not manufacture Human Authority or silently convert a candidate rule into an approved contract.",
            ],
            "provider": {"status": "not_connected", "name": None, "model": None, "identity_separate_from_worker": True},
            "interfaces": [
                {"name": "Cockpit execution route", "status": "not_implemented"},
                {"name": "Direct worker conversation", "status": "not_implemented"},
            ],
            "source_contracts": ["PMEi governance continuity"],
            "cockpit_execution": "not_implemented",
        },
        "builder": {
            "name": "BUILDER DAVE",
            "role": "Builder",
            "function": "Executes bounded build work against approved files, scope and acceptance criteria.",
            "authority": [
                "May implement only within the bounded Engineering build job supplied.",
                "May not widen scope, deploy, seal, promote, or substitute approval.",
            ],
            "provider": {"status": "not_connected", "name": None, "model": None, "identity_separate_from_worker": True},
            "interfaces": [
                {"name": "Cockpit execution route", "status": "not_implemented"},
                {"name": "Direct worker conversation", "status": "not_implemented"},
                {"name": "Deployment authority", "status": "prohibited"},
            ],
            "source_contracts": ["PMEi Record 202"],
            "cockpit_execution": "not_implemented",
        },
        "knobhead": {
            "name": "KNOBHEAD DAVE",
            "role": "Adversarial Verification",
            "function": "Independently challenges evidence, claims, candidate acceptance and implementation proof.",
            "authority": [
                "May PASS, HOLD or reject evidence/claims according to bounded verification criteria.",
                "Does not build or promote merely because another worker recommends it.",
                "Independent PASS is distinct from Human Authority.",
            ],
            "provider": {"status": "not_connected", "name": None, "model": None, "identity_separate_from_worker": True},
            "interfaces": [
                {"name": "Cockpit execution route", "status": "not_implemented"},
                {"name": "Direct worker conversation", "status": "not_implemented"},
            ],
            "source_contracts": ["Knobhead Dave adversarial verification worker contract"],
            "cockpit_execution": "not_implemented",
        },
        "steward": {
            "name": "STEWARD DAVE",
            "role": "Steward",
            "function": "Maintains continuity, provenance, deduplication, lineage and supersession.",
            "authority": [
                "May curate continuity and provenance after the required validation path.",
                "May not manufacture verification or Human Authority.",
            ],
            "provider": {"status": "not_connected", "name": None, "model": None, "identity_separate_from_worker": True},
            "interfaces": [
                {"name": "Cockpit execution route", "status": "not_implemented"},
                {"name": "Direct worker conversation", "status": "not_implemented"},
            ],
            "source_contracts": ["PMEi stewardship continuity"],
            "cockpit_execution": "not_implemented",
        },
    }


@app.get("/workers")
def cockpit_workers():
    return {
        "ok": True,
        "workers": _worker_manifest(),
        "presentation_only": True,
        "authority_granted": False,
    }


@app.get("/workers/<worker_name>")
def cockpit_worker(worker_name):
    key = str(worker_name or "").strip().lower()
    worker = _worker_manifest().get(key)
    if worker is None:
        return {"ok": False, "error": "Unknown worker."}, 404

    latest = None
    if LAST_RUN and str(LAST_RUN.get("worker", "")).lower() == key:
        latest = LAST_RUN

    return {
        "ok": True,
        "worker_key": key,
        "worker": worker,
        "latest_activity": latest,
        "presentation_only": True,
        "authority_granted": False,
    }


# PMEI_OPENAI_CHAT_V2

# PMEI_ENGINEERING_HANDOFF_V1
@app.post("/orchestration/request-worker")
def request_governed_start_worker():
    from orchestration.automatic_continuation import AutomaticContinuation

    payload = request.get_json(silent=True) or {}
    task = str(payload.get("task") or "").strip()
    requested_worker = str(payload.get("requested_worker") or "").strip().lower()
    if not task:
        return {"ok": False, "error": "task_required", "transition_authority": False}, 400
    if not requested_worker:
        return {"ok": False, "error": "requested_worker_required", "transition_authority": False}, 400
    try:
        get_worker(requested_worker)
    except ValueError as exc:
        return {"ok": False, "error": str(exc), "transition_authority": False}, 400
    if requested_worker not in {"architecture", "engineering", "governance", "findings", "steward"}:
        return {"ok": False, "error": "worker_requires_governed_transition",
                "requested_worker": requested_worker, "transition_authority": False}, 400

    constraints = record_task_requirements(task)
    foh_context = _foh_pmei_prepare_for_question(task)
    job = OrchestrationJob(job_id="web-" + uuid.uuid4().hex[:12], task=task,
                           requested_worker=requested_worker, context={"foh_context": foh_context}, constraints=constraints)
    engine.create_job(job)
    try:
        report = AutomaticContinuation(engine, executor).start(job.job_id)
    except Exception as exc:
        return {"ok": False, "job_id": job.job_id, "requested_worker": requested_worker,
                "error": "Automatic job start failed; inspect job state before retrying.",
                "error_type": type(exc).__name__, "authority": "request_only",
                "transition_authority": False, "promotion_authority": False,
                "verification_authority": False,
                "status_url": "/orchestration/jobs/" + job.job_id}, 503
    report["status_url"] = "/orchestration/jobs/" + job.job_id
    return report, 202 if report.get("ok") else 503


@app.post("/orchestration/jobs/<job_id>/human-decision")
def orchestration_human_decision(job_id):
    import re
    from orchestration.automatic_continuation import AutomaticContinuation
    from orchestration.contracts import HumanDecision

    if not re.fullmatch(r"[A-Za-z0-9_-]{1,96}", job_id):
        return {"ok": False, "error": "invalid_job_id", "transition_authority": False}, 400

    payload = request.get_json(silent=True) or {}
    decision = str(payload.get("decision") or "").upper().strip()
    note = str(payload.get("note") or "").strip()

    try:
        state = engine.submit_human_decision(
            HumanDecision(job_id=job_id, decision=decision, note=note)
        )
    except ValueError as exc:
        return {
            "ok": False,
            "job_id": job_id,
            "error": str(exc),
            "human_decision_recorded": False,
            "transition_authority": False,
            "promotion_authority": False,
            "verification_authority": False,
        }, 409

    if state.status == "READY":
        try:
            report = AutomaticContinuation(
                engine,
                executor,
            ).resume_after_human_decision(job_id)
        except Exception as exc:
            return {
                "ok": False,
                "job_id": job_id,
                "error": "Human decision was recorded but continuation did not start.",
                "error_type": type(exc).__name__,
                "human_decision_recorded": True,
                "human_decision": decision,
                "status_url": "/orchestration/jobs/" + job_id,
                "transition_authority": False,
            }, 503
        report["human_decision_recorded"] = True
        report["human_decision"] = decision
        report["status_url"] = "/orchestration/jobs/" + job_id
        return report, 202 if report.get("ok") else 409

    return {
        "ok": True,
        "job_id": job_id,
        "result_status": state.status,
        "current_worker": state.current_worker,
        "human_decision_recorded": True,
        "human_decision": decision,
        "human_approved": False,
        "transition_authority": False,
        "promotion_authority": False,
        "verification_authority": False,
        "status_url": "/orchestration/jobs/" + job_id,
    }, 200


@app.get("/orchestration/jobs/<job_id>")
def orchestration_job_status(job_id):
    import re
    from orchestration.automatic_continuation import read_report
    from orchestration.store import OrchestrationRecordNotFound, OrchestrationStoreError

    if not re.fullmatch(r"[A-Za-z0-9_-]{1,96}", job_id):
        return {"ok": False, "error": "invalid_job_id", "transition_authority": False}, 400
    try:
        report = read_report(engine, job_id)
    except OrchestrationRecordNotFound:
        return {"ok": False, "error": "job_not_found", "transition_authority": False}, 404
    except (OrchestrationStoreError, ValueError, KeyError, TypeError):
        return {"ok": False, "error": "job_state_unavailable", "transition_authority": False}, 503
    report["status_url"] = "/orchestration/jobs/" + job_id
    return report, 200


@app.post("/orchestration/request-engineering")
def cockpit_request_engineering():
    payload = request.get_json(silent=True)

    if isinstance(payload, dict):
        task = str(payload.get("task", "") or "").strip()
    else:
        task = str(request.form.get("task", "") or "").strip()

    if not task:
        return {
            "ok": False,
            "error": "task is required",
            "requested_worker": "engineering",
        }, 400

    add_live_event(
        "HANDOFF",
        "Bounded cockpit request sent to Engineering Dave.",
        requested_worker="engineering",
        authority="request_only",
        transition_authority=False,
    )

    run_endpoint = None
    for rule in app.url_map.iter_rules():
        if rule.rule == "/run" and "POST" in rule.methods:
            run_endpoint = rule.endpoint
            break

    if not run_endpoint:
        return {
            "ok": False,
            "error": "Existing governed /run execution route is unavailable.",
            "requested_worker": "engineering",
        }, 503

    run_view = app.view_functions.get(run_endpoint)
    if run_view is None:
        return {
            "ok": False,
            "error": "Existing governed /run execution view is unavailable.",
            "requested_worker": "engineering",
        }, 503

    with app.test_request_context(
        "/run",
        method="POST",
        data={"task": task},
    ):
        run_view()

    last_run = dict(LAST_RUN) if isinstance(LAST_RUN, dict) else {}

    add_live_event(
        "HANDOFF",
        "Engineering Dave returned control to the cockpit.",
        requested_worker="engineering",
        job_id=last_run.get("job_id"),
        validation=last_run.get("validation"),
        mutated=bool(last_run.get("mutated", False)),
        transition_authority=False,
    )

    return {
        "ok": True,
        "requested_worker": "engineering",
        "authority": "request_only",
        "transition_authority": False,
        "job": last_run,
    }, 200



@app.post("/orchestration/request-engineering-async")
def request_engineering_async():
    payload = request.get_json(silent=True) or request.form or {}
    task = str(payload.get("task") or "").strip()

    if not task:
        return {
            "ok": False,
            "error": "task_required",
            "requested_worker": "engineering",
            "authority": "request_only",
            "transition_authority": False,
        }, 400

    if ENGINEERING_RUN.get("status") == "WORKING":
        return {
            "ok": False,
            "error": "engineering_already_working",
            "requested_worker": "engineering",
            "authority": "request_only",
            "transition_authority": False,
            "run": _engineering_run_snapshot(),
        }, 409

    started_at = datetime.now().isoformat(timespec="seconds")
    _set_engineering_run(
        status="WORKING",
        job_id=None,
        task=task,
        started_at=started_at,
        finished_at=None,
        validation=None,
        provider=None,
        model=None,
        wall_seconds=None,
        mutated=False,
        transition_authority=False,
        error=None,
    )

    add_live_event(
        "HANDOFF",
        "Bounded cockpit request sent to Engineering Dave.",
        requested_worker="engineering",
        authority="request_only",
        transition_authority=False,
        execution_mode="async_cockpit",
    )

    def _execute_once():
        try:
            with app.test_request_context(
                "/run",
                method="POST",
                data={"task": task},
            ):
                run_job()

            last_run = dict(LAST_RUN) if isinstance(LAST_RUN, dict) else {}
            _set_engineering_run(
                status="RESULT",
                job_id=last_run.get("job_id"),
                task=task,
                started_at=started_at,
                finished_at=datetime.now().isoformat(timespec="seconds"),
                validation=last_run.get("validation"),
                provider=last_run.get("provider"),
                model=last_run.get("model"),
                wall_seconds=last_run.get("wall_seconds"),
                mutated=bool(last_run.get("mutated", False)),
                transition_authority=False,
                error=None,
            )

            add_live_event(
                "HANDOFF",
                "Engineering Dave returned control to the cockpit.",
                requested_worker="engineering",
                job_id=last_run.get("job_id"),
                validation=last_run.get("validation"),
                mutated=bool(last_run.get("mutated", False)),
                transition_authority=False,
                execution_mode="async_cockpit",
            )
        except Exception as exc:
            _set_engineering_run(
                status="BLOCKED",
                task=task,
                finished_at=datetime.now().isoformat(timespec="seconds"),
                transition_authority=False,
                error=f"{type(exc).__name__}: {exc}",
            )
            add_live_event(
                "BLOCKED",
                "Engineering Dave execution failed.",
                requested_worker="engineering",
                error_type=type(exc).__name__,
                transition_authority=False,
                execution_mode="async_cockpit",
            )

    threading.Thread(
        target=_execute_once,
        name="pmei-engineering-cockpit-run",
        daemon=True,
    ).start()

    return {
        "ok": True,
        "accepted": True,
        "requested_worker": "engineering",
        "authority": "request_only",
        "transition_authority": False,
        "run": _engineering_run_snapshot(),
    }, 202


@app.get("/orchestration/engineering/status")
def engineering_status():
    return {
        "ok": True,
        "requested_worker": "engineering",
        "authority": "request_only",
        "transition_authority": False,
        "run": _engineering_run_snapshot(),
        "job": dict(LAST_RUN) if isinstance(LAST_RUN, dict) else {},
    }



@app.post("/chat/deterministic")
def deterministic_pmei_chat():
    """
    Deterministic Front-of-House Dave.

    No LLM provider is invoked.

    Authority boundary:
      READ ONLY PMEi continuity.
      No verification, promotion, sealing, deployment,
      mutation, or orchestration transition authority.
    """

    message = request.form.get(
        "message",
        "",
    ).strip()

    if not message:
        return {
            "ok": False,
            "error": "No chat message supplied.",
            "provider": "deterministic",
            "model": "none",
            "llm_used": False,
            "authority": "read_only_continuity",
        }, 400

    started = time.perf_counter()

    optional_external_lanes_raw = request.form.get(
        "external_lanes",
        "",
    )
    optional_external_lanes = {
        token.strip().casefold()
        for token in optional_external_lanes_raw.split(",")
        if token.strip()
    }
    optional_external_lanes = {
        "hacker_news" if token in {"hn", "hackernews", "hacker_news"} else token
        for token in optional_external_lanes
    }
    supported_optional_lanes = {
        "hacker_news",
        "discord",
    }
    unknown_optional_lanes = (
        optional_external_lanes
        - supported_optional_lanes
    )
    if unknown_optional_lanes:
        return {
            "ok": False,
            "error": (
                "Unsupported external_lanes value(s): "
                + ", ".join(sorted(unknown_optional_lanes))
            ),
            "supported_external_lanes": sorted(
                supported_optional_lanes
            ),
        }, 400

    source_route = route_source(message)
    _, message = split_source_request(message)
    if not message:
        return {"ok": False, "error": "Please include a question after the source selector."}, 400
    if source_route == SOURCE_REQUIRED:
        return {
            "ok": True,
            "text": "Should I use your saved continuity or search the web? Repeat the question starting with Continuity: or Web:.",
            "clarification_required": True,
            "source_route": SOURCE_REQUIRED,
            "provider": "deterministic", "model": "none", "llm_used": False,
            "authority": "none", "pmei_context_used": False,
            "records_received": 0, "evidence_count": 0, "evidence_record_ids": [],
        }, 200

    if source_route == WEB_LOOKUP:
        retriever_kwargs = {}
        if "hacker_news" in optional_external_lanes:
            retriever_kwargs["include_hacker_news"] = True
        if "discord" in optional_external_lanes:
            retriever_kwargs["include_discord"] = True

        external_result = ExternalRetriever(
            **retriever_kwargs
        ).retrieve(
            message
        )

        if not external_result.get("ok"):
            return {
                "ok": False,
                "error": (
                    external_result.get("error")
                    or
                    "External retrieval produced no evidence."
                ),
                "provider": "deterministic",
                "model": "none",
                "llm_used": False,
                "authority": "external_retrieval_only",
                "source_route": source_route,
                "pmei_context_used": False,
                "records_received": 0,
                "evidence_count": 0,
                "evidence_record_ids": [],
                "external_retrieval_connected": False,
                "external_retrieval_status": external_result.get("error_code") or "PROVIDER_ERROR",
                "error_code": external_result.get("error_code") or "PROVIDER_ERROR",
                "external_evidence_count": 0,
                "external_provider_passes": external_result.get(
                    "provider_passes"
                ) or [],
                "external_optional_lanes_requested": sorted(
                    optional_external_lanes
                ),
            }, 503

        external_evidence = (
            external_result.get("evidence")
            or []
        )
        external_provider_passes = (
            external_result.get("provider_passes")
            or []
        )
        external_lane_counts = {}
        external_source_class_counts = {}
        for item in external_evidence:
            if not isinstance(item, dict):
                continue
            lane = str(item.get("retrieval_lane") or "unknown")
            source_class = str(item.get("source_class") or "unknown")
            external_lane_counts[lane] = (
                external_lane_counts.get(lane, 0) + 1
            )
            external_source_class_counts[source_class] = (
                external_source_class_counts.get(source_class, 0) + 1
            )

        text_out = render_external_evidence(
            external_evidence
        )

        elapsed_ms = (
            time.perf_counter()
            - started
        ) * 1000

        return {
            "ok": True,
            "text": text_out,
            "provider": "deterministic",
            "model": "none",
            "llm_used": False,
            "authority": "external_retrieval_only",
            "source_route": source_route,
            "pmei_context_used": False,
            "records_received": 0,
            "evidence_count": 0,
            "evidence_record_ids": [],
            "external_retrieval_connected": True,
            "external_evidence_count": len(external_evidence),
            "external_provider_passes": external_provider_passes,
            "external_optional_lanes_requested": sorted(
                optional_external_lanes
            ),
            "external_lane_counts": external_lane_counts,
            "external_source_class_counts": external_source_class_counts,
            "pmei_write_authority": "NONE",
            "promotion_authority": False,
            "verification_authority": False,
            "transition_authority": False,
            "timing": {
                "total_ms": round(elapsed_ms, 3),
            },
        }

    try:
        adapter = PMEiEvidenceAdapter(
            max_evidence=8,
            archive_search=True,
        )

        deterministic_intent = classify_question_intent(
            message
        )

        if deterministic_intent.intent == "CONTEXT_INSPECTION":
            prepared = adapter.prepare_context_inspection(
                message
            )
        else:
            prepared = adapter.prepare(
                message
            )

        if not prepared.retrieval_ok:
            return {
                "ok": False,
                "error": (
                    prepared.error
                    or
                    "Deterministic PMEi retrieval failed."
                ),
                "provider": "deterministic",
                "model": "none",
                "llm_used": False,
                "authority": "read_only_continuity",
            }, 502

        evidence_packet = {
            "retrieval_ok":
                prepared.retrieval_ok,

            "records_received":
                prepared.records_received,

            "evidence_count":
                prepared.evidence_count,

            "route":
                (
                    prepared.transport.get(
                        "route"
                    )
                    if isinstance(
                        prepared.transport,
                        dict,
                    )
                    else None
                ),

            "evidence":
                prepared.evidence,

            "transport":
                prepared.transport,

            "error":
                prepared.error,
        }

        packet = (
            build_worker_packet_builder()
            .build(
                worker_role="foh",
                task=message,
                evidence_packet=evidence_packet,
                job_id="foh-deterministic-chat",
            )
        )

        relationship_mode = (
            deterministic_intent.intent
            in {
                "IDENTITY_DEFINITION",
                "HISTORICAL_EVENT",
                "PAST_STATE",
                "DECISION",
                "LINEAGE",
                "CHANGE_COMPARISON",
                "PERSONAL_CONTINUITY",
            }
        )

        relationship_selected_ids = []

        if deterministic_intent.intent == "CONTEXT_INSPECTION":
            authority_classifier = (
                build_worker_packet_builder()
                .evidence_authority_class
            )
            text_out = render_context_inspection(
                prepared.evidence,
                authority_classifier,
            )
            relationship_selected_ids = [
                item.get("record_id")
                for item in prepared.evidence
            ]
        elif relationship_mode:
            authority_classifier = (
                build_worker_packet_builder()
                .evidence_authority_class
            )

            relationship_evidence = (
                translate_governed_evidence(
                    prepared.evidence,
                    authority_classifier,
                )
            )

            if (
                deterministic_intent.intent
                == "IDENTITY_DEFINITION"
                and deterministic_intent.temporal_scope
                == "GENERAL"
            ):
                configured_worker = (
                    _worker_manifest()
                    .get("foh", {})
                )

                configured_name = str(
                    configured_worker.get(
                        "name",
                        "",
                    )
                    or ""
                ).strip()

                configured_role = str(
                    configured_worker.get(
                        "role",
                        "",
                    )
                    or ""
                ).strip()

                configured_function = str(
                    configured_worker.get(
                        "function",
                        "",
                    )
                    or ""
                ).strip()

                if (
                    configured_name
                    and configured_role
                    and configured_function
                ):
                    configured_identity = (
                        EvidenceItem(
                            record_id="CONFIGURED:FOH",
                            evidence_kind="IDENTITY",
                            temporal_scope="CURRENT",
                            text=(
                                f"{configured_name} is the configured "
                                f"worker identity. Role: "
                                f"{configured_role}. Function: "
                                f"{configured_function}"
                            ),
                            authority_eligible=True,
                            evidence_role=(
                                "CONFIGURED_RUNTIME_IDENTITY"
                            ),
                        ),
                    )

                    relationship_evidence = (
                        configured_identity
                        + relationship_evidence
                    )

            relationship_result = (
                run_relationship_engine(
                    message,
                    relationship_evidence,
                )
            )

            text_out = (
                relationship_result
                .answer
                .text
            )

            relationship_selected_ids = [
                item.record_id
                for item
                in relationship_result
                .subject_bound_evidence
            ]

        else:
            text_out = render_deterministic_answer(
                packet
            )

        transport = (
            prepared.transport
            if isinstance(
                prepared.transport,
                dict,
            )
            else {}
        )

        record_ids = [
            item.get(
                "record_id"
            )
            for item in prepared.evidence
        ]

        elapsed_ms = (
            time.perf_counter()
            - started
        ) * 1000

        add_live_event(
            "CHAT",
            "Deterministic PMEi response produced",
            authority="read_only_continuity",
            provider="deterministic",
            llm_used=False,
            evidence_count=prepared.evidence_count,
        )

        return {
            "ok": True,
            "text": text_out,
            "provider": "deterministic",
            "model": "none",
            "llm_used": False,
            "authority": "read_only_continuity",
            "deterministic_intent":
                deterministic_intent.intent,
            "relationship_mode":
                relationship_mode,
            "relationship_selected_record_ids":
                relationship_selected_ids,
            "pmei_context_used": True,
            "records_received":
                prepared.records_received,
            "evidence_count":
                prepared.evidence_count,
            "evidence_record_ids":
                record_ids,
            "pmei_write_authority":
                "NONE",
            "promotion_authority":
                False,
            "verification_authority":
                False,
            "transition_authority":
                False,
            "transport_exhaustive":
                transport.get(
                    "exhaustive"
                ),
            "transport_errors":
                transport.get(
                    "errors"
                ),
            "timing": {
                "total_ms": round(
                    elapsed_ms,
                    3,
                ),
            },
        }

    except Exception as exc:
        add_live_event(
            "CHAT",
            "Deterministic PMEi request failed",
            authority="read_only_continuity",
            error_type=type(exc).__name__,
        )

        return {
            "ok": False,
            "error": str(exc),
            "provider": "deterministic",
            "model": "none",
            "llm_used": False,
            "authority": "read_only_continuity",
        }, 502


@app.get("/chat/status")
def local_chat_status():
    model = FOH_OLLAMA_MODEL

    try:
        response = requests.get(
            "http://127.0.0.1:11434/api/tags",
            timeout=3,
        )
        connected = response.ok
    except Exception:
        connected = False

    return {
        "ok": True,
        "connected": connected,
        "model": model,
        "provider": "ollama",
        "authority": "conversation_plus_read_only_continuity",
        "pmei_context_used": False,
        "pmei_read_connected": _foh_pmei_configured(),
        "pmei_write_connected": _foh_pmei_configured(),
        "pmei_write_class": "READ ONLY",
        "promotion_authority": False,
        "verification_authority": False,
        "transition_authority": False,
    }


# PMEI_LOCAL_OLLAMA_CHAT_V1
def _foh_request_initial_worker(task, requested_worker, selection):
    """Request one automatically continued job through the existing start gate.

    FOH preserves the user's task and initial selector outcome. The engine
    governs successors; polling the returned URL has no execution authority.
    """
    endpoint = next((rule.endpoint for rule in app.url_map.iter_rules()
        if rule.rule == "/orchestration/request-worker" and "POST" in rule.methods), None)
    view = app.view_functions.get(endpoint) if endpoint else None
    if view is None:
        return {"ok": False, "error": "Existing governed worker-start route is unavailable. No job was started.",
                "authority": "request_only", "transition_authority": False}, 503
    add_live_event("HANDOFF", "FOH requested an automatic governed job",
        requested_worker=requested_worker, authority="request_only", transition_authority=False)
    try:
        with app.test_request_context("/orchestration/request-worker", method="POST",
                                     json={"task": task, "requested_worker": requested_worker}):
            response = app.make_response(view())
            body = response.get_json(silent=True)
            status_code = response.status_code
    except Exception as exc:
        add_live_event("HANDOFF", "Automatic job request did not return a status",
                      requested_worker=requested_worker, error_type=type(exc).__name__)
        return {"ok": False, "error": "The request did not return a job status. Check existing jobs before retrying.",
                "initial_request": selection, "authority": "request_only", "transition_authority": False}, 502
    if not isinstance(body, dict):
        return {"ok": False, "error": "Invalid response from the governed start route.",
                "authority": "request_only", "transition_authority": False}, 502
    if status_code >= 400 or not body.get("ok"):
        return {**body, "ok": False, "initial_request": selection}, status_code if status_code >= 400 else 502
    if not body.get("job_id") or body.get("requested_worker") != requested_worker:
        return {"ok": False, "error": "Governed start returned an invalid job identity.",
                "authority": "request_only", "transition_authority": False}, 502
    result = {**body, "initial_request": selection, "authority": "request_only",
              "promotion_authority": False, "verification_authority": False, "transition_authority": False}
    result["text"] = ("Dave accepted job " + body["job_id"] + ". Automatic governed work is queued. "
                      "Read status_url for progress and candidate results. No completion or approval is claimed.")
    return result, 202


@app.post("/chat")
def local_ollama_chat():
    """
    Front-of-House conversational surface.

    Provider:
      Local Ollama / nemotron-3-nano:4b

    Authority boundary:
      FOH may use bounded READ ONLY PMEi continuity context.
      FOH has no verification, promotion, sealing, deployment,
      or orchestration transition authority.
    """

    message = request.form.get("message", "").strip()
    history_raw = request.form.get("history", "").strip()

    if not message:
        return {
            "ok": False,
            "error": "No chat message supplied.",
            "authority": "conversation_only",
        }, 400

    history = []

    if history_raw:
        try:
            candidate_history = json.loads(history_raw)

            if isinstance(candidate_history, list):
                for item in candidate_history[-20:]:
                    if not isinstance(item, dict):
                        continue

                    role = item.get("role")
                    content = item.get("content")

                    if role not in {"user", "assistant"}:
                        continue

                    if not isinstance(content, str):
                        continue

                    content = content.strip()

                    if content:
                        history.append({
                            "role": role,
                            "content": content,
                        })

        except (json.JSONDecodeError, TypeError):
            history = []

    user_style_messages = [
        item.get("content", "")
        for item in history
        if item.get("role") == "user"
    ]
    user_style_messages.append(message)
    interaction_profile = user_profile_store.observe(user_style_messages)
    interaction_profile_instruction = render_profile_instruction(
        interaction_profile
    )

    # Bounded initial request proposal. The existing governed endpoint remains
    # responsible for validating the requested role, creating and executing jobs.
    # Worker results remain candidates; no WorkerResult is submitted here.
    # Return-side binding for a job previously started by this FOH conversation.
    # Read-only delivery only: no resume, approval, successor selection, or mutation.
    import re
    prior_job_id = None
    for item in reversed(history):
        if item.get("role") != "assistant":
            continue
        match = re.fullmatch(
            r"Dave accepted job ([A-Za-z0-9_-]{1,96})\. Automatic governed work is queued\. "
            r"Read status_url for progress and candidate results\. No completion or approval is claimed\.",
            item.get("content", ""),
        )
        if match:
            prior_job_id = match.group(1)
            break

    if prior_job_id is not None:
        from orchestration.automatic_continuation import read_report
        from orchestration.store import OrchestrationRecordNotFound, OrchestrationStoreError

        try:
            report = read_report(engine, prior_job_id)
        except OrchestrationRecordNotFound:
            report = None
        except (OrchestrationStoreError, ValueError, KeyError, TypeError):
            return {
                "ok": False,
                "error": "job_state_unavailable",
                "authority": "conversation_only",
                "promotion_authority": False,
                "verification_authority": False,
                "transition_authority": False,
            }, 503

        if isinstance(report, dict) and report.get("result_status") == "AWAITING_HUMAN":
            delivery = report.get("delivery")
            if isinstance(delivery, dict):
                return {
                    "ok": True,
                    "job_id": prior_job_id,
                    "result_status": "AWAITING_HUMAN",
                    "answer_owner": delivery.get("answer_owner"),
                    "text": delivery.get("text", ""),
                    "human_approved": False,
                    "semantic_synthesis_performed": False,
                    "authority": "conversation_only",
                    "promotion_authority": False,
                    "verification_authority": False,
                    "transition_authority": False,
                    "status_url": "/orchestration/jobs/" + prior_job_id,
                }, 200

    selection_started = time.perf_counter()
    direct_general_chat = obvious_foh_chat(message)
    try:
        if direct_general_chat:
            proposal = InitialRequestProposal("CHAT", None, None)
            selection_provider = {
                "provider": "deterministic",
                "model": "none",
                "route_reason": "obvious_general_chat",
            }
        else:
            proposal, selection_provider = propose_initial_request(
                provider, message, history, model=FOH_OLLAMA_MODEL,
            )
    except InitialRequestError as exc:
        return {
            "ok": False,
            "error": "FOH could not produce a valid initial request. No job was started.",
            "error_code": str(exc),
            "authority": "request_only",
            "transition_authority": False,
        }, 422
    except Exception as exc:
        add_live_event("CHAT", "FOH initial request failed", error_type=type(exc).__name__)
        return {
            "ok": False,
            "error": "FOH initial request inference failed. No job was started.",
            "authority": "request_only",
            "transition_authority": False,
        }, 502

    selection = {
        **proposal.as_dict(), **selection_provider,
        "seconds": round(time.perf_counter() - selection_started, 4),
        "authority": "request_only",
        "transition_authority": False,
    }
    if proposal.action == "CLARIFY":
        return {
            "ok": True, "text": proposal.question,
            "result_status": "CLARIFICATION_REQUIRED",
            "initial_request": selection,
            "authority": "conversation_only",
            "pmei_context_used": False,
            "promotion_authority": False,
            "verification_authority": False,
            "transition_authority": False,
        }, 200
    if proposal.action == "REQUEST_WORKER":
        return _foh_request_initial_worker(message, proposal.requested_worker, selection)

    conversation_input = history + [{
        "role": "user",
        "content": message,
    }]

    model = FOH_OLLAMA_MODEL

    # FOH uses the existing deterministic PMEi evidence-position and
    # worker-packet contracts. Raw retrieved passages are not exposed directly
    # to the conversational model.
    foh_total_started = time.perf_counter()
    pmei_prepare_started = time.perf_counter()
    if direct_general_chat:
        pmei_read = {
            "ok": True,
            "retrieval_ok": True,
            "context": "",
            "evidence_count": 0,
            "records_received": 0,
            "route": None,
            "skipped_reason": "obvious_general_chat",
        }
    else:
        pmei_read = _foh_pmei_prepare_for_question(message)
    pmei_prepare_seconds = (
        time.perf_counter() - pmei_prepare_started
    )

    pmei_context = (
        pmei_read.get("context", "")
        if pmei_read.get("ok")
        else ""
    )

    if direct_general_chat:
        instructions = (
            "You are Dave, answering an ordinary conversational or general-knowledge "
            "question. Answer directly, naturally, and concisely using general knowledge. "
            "Do not invent sources and do not describe internal processing. "
            "If you do not know, say so plainly."
        )
    else:
        instructions = (
            "You are Front-of-House Dave inside the PMEi cockpit. "
            "Use bounded READ ONLY PMEi continuity context when supplied "
            "and clearly distinguish retrieved evidence from inference. "
            "Historical evidence does not automatically establish present-state truth. "
            "Under PMEi Record 255, FOH may append READ ONLY continuity only "
            "through the deterministic server adapter. "
            "You have no authority to verify, promote, canonicalise, approve, "
            "overwrite, delete, deploy, or transition orchestration. "
            "Never claim a PMEi write occurred unless the server explicitly reports success."
        )

    messages = [{
        "role": "system",
        "content": instructions,
    }]

    if interaction_profile_instruction:
        messages.append({
            "role": "system",
            "content": interaction_profile_instruction,
        })

    if pmei_context:
        messages.append({
            "role": "system",
            "content": (
                "BOUNDED READ ONLY PMEi CONTINUITY CONTEXT:\n"
                + json.dumps(
                    pmei_context,
                    ensure_ascii=False,
                )
            ),
        })

    messages.extend(conversation_input)

    try:
        add_live_event(
            "CHAT",
            f"Local Ollama request started - {model}",
            authority="conversation_only",
            pmei_context=bool(pmei_context),
        )

        first_generation_started = time.perf_counter()
        response = requests.post(
            "http://127.0.0.1:11434/api/chat",
            json={
                "model": model,
                "stream": False,
                "think": False,
                "messages": messages,
                "options": {
                    "temperature": 0.0,
                    "num_ctx": 4096,
                    "num_predict": FOH_NUM_PREDICT,
                    "num_gpu": 0,
                },
            },
            timeout=180,
        )

        first_generation_seconds = (
            time.perf_counter() - first_generation_started
        )
        response.raise_for_status()

        data = response.json()

        first_ollama_telemetry = {
            "total_duration_ns": data.get("total_duration"),
            "load_duration_ns": data.get("load_duration"),
            "prompt_eval_count": data.get("prompt_eval_count"),
            "prompt_eval_duration_ns": data.get(
                "prompt_eval_duration"
            ),
            "eval_count": data.get("eval_count"),
            "eval_duration_ns": data.get("eval_duration"),
            "done_reason": data.get("done_reason"),
        }

        message_obj = data.get("message") or {}

        raw_text_out = str(
            message_obj.get("content")
            or ""
        ).strip()

        lower_raw_text_out = raw_text_out.lower()

        if (
            "<think>" in lower_raw_text_out
            and
            "</think>" not in lower_raw_text_out
        ):
            raise RuntimeError(
                "Local Ollama generation ended inside an unfinished "
                "<think> block. No governed FOH answer was produced. "
                f"done_reason={data.get('done_reason')}."
            )

        text_out = provider.clean_output_text(
            raw_text_out
        )

        if not text_out:
            raise RuntimeError(
                "Local Ollama returned no usable final message content."
            )

        # -----------------------------------------------------------------
        # DETERMINISTIC FOH OUTPUT VALIDATION
        # -----------------------------------------------------------------
        #
        # Provider output is not allowed to bypass the same governed
        # evidence/state-support boundary used elsewhere in orchestration.
        #
        # In particular:
        # - CURRENT_STATE_ELIGIBLE remains record/proposition scoped.
        # - explicit PMEi record attribution must remain bound to the
        #   proposition belonging to that record.
        # - model output cannot promote contextual evidence into current truth.
        # -----------------------------------------------------------------

        validation = None
        repair_attempted = False
        repair_generation_seconds = 0.0
        final_validation_seconds = 0.0
        first_validation_seconds = 0.0
        repair_ollama_telemetry = None

        if pmei_context:
            first_validation_started = time.perf_counter()
            validation = WorkerOutputValidator().validate(
                output_text=text_out,
                worker_packet_text=pmei_context,
            )
            first_validation_seconds = (
                time.perf_counter() - first_validation_started
            )

            if not validation.ok:

                repair_attempted = True

                add_live_event(
                    "CHAT",
                    f"Local Ollama response rejected - {model}",
                    authority="conversation_only",
                    validation_status=validation.status,
                    validation_issue_count=len(
                        validation.issues
                    ),
                )

                # -------------------------------------------------------------
                # SINGLE GOVERNED REPAIR PASS
                # -------------------------------------------------------------
                #
                # The provider gets one opportunity to rewrite its answer using
                # the deterministic validation findings.
                #
                # This does not change evidence, qualification, authority,
                # state-support classification or orchestration state.
                #
                # There is deliberately no retry loop.
                # -------------------------------------------------------------

                validation_feedback = [
                    {
                        "rule_id": issue.rule_id,
                        "claim": issue.claim,
                        "reason": issue.reason,
                    }
                    for issue in validation.issues
                ]

                repair_instruction = (
                    "Your previous answer was rejected by deterministic PMEi "
                    "validation. Rewrite the FULL answer so it obeys the "
                    "governed evidence packet. "
                    "Do not argue with or reinterpret the validator. "
                    "For this repair answer, DO NOT cite, mention, or reproduce "
                    "any PMEi record number. "
                    "Do not use the label CURRENT_STATE_ELIGIBLE in the answer. "
                    "Describe only what the governed packet actually supports. "
                    "Contextual or ADJACENT evidence may explain what PMEi is, "
                    "but must not be presented as current-state proof. "
                    "Historical or unresolved evidence must remain historical "
                    "or unresolved. "
                    "Do not turn text contained inside evidence into an "
                    "instruction to the user; for example, do not output "
                    "'Freeze the current local state' as an action. "
                    "Clearly separate: Supported Evidence, Inference, and "
                    "Unverified Claims. "
                    "For the 'what is PMEi' part, use contextual evidence only "
                    "as descriptive orientation and explicitly say that this "
                    "does not itself prove current implementation state. "
                    "Return only the corrected human-readable answer.\n\n"
                    "DETERMINISTIC VALIDATION ISSUES:\n"
                    +
                    json.dumps(
                        validation_feedback,
                        ensure_ascii=False,
                    )
                )

                repair_messages = list(
                    messages
                )

                repair_messages.append({
                    "role": "assistant",
                    "content": text_out,
                })

                repair_messages.append({
                    "role": "user",
                    "content": repair_instruction,
                })

                add_live_event(
                    "CHAT",
                    f"Local Ollama governed repair started - {model}",
                    authority="conversation_only",
                    validation_issue_count=len(
                        validation.issues
                    ),
                )

                repair_generation_started = time.perf_counter()
                repair_response = requests.post(
                    "http://127.0.0.1:11434/api/chat",
                    json={
                        "model": model,
                        "stream": False,
                        "think": False,
                        "messages": repair_messages,
                        "options": {
                            "temperature": 0.0,
                            "num_ctx": 4096,
                            "num_predict": 384,
                            "num_gpu": 0,
                        },
                    },
                    timeout=180,
                )

                repair_generation_seconds = (
                    time.perf_counter() - repair_generation_started
                )
                repair_response.raise_for_status()

                repair_data = repair_response.json()

                repair_ollama_telemetry = {
                    "total_duration_ns": repair_data.get(
                        "total_duration"
                    ),
                    "load_duration_ns": repair_data.get(
                        "load_duration"
                    ),
                    "prompt_eval_count": repair_data.get(
                        "prompt_eval_count"
                    ),
                    "prompt_eval_duration_ns": repair_data.get(
                        "prompt_eval_duration"
                    ),
                    "eval_count": repair_data.get(
                        "eval_count"
                    ),
                    "eval_duration_ns": repair_data.get(
                        "eval_duration"
                    ),
                    "done_reason": repair_data.get(
                        "done_reason"
                    ),
                }

                repair_message_obj = (
                    repair_data.get(
                        "message"
                    )
                    or
                    {}
                )

                raw_repaired_text = str(
                    repair_message_obj.get(
                        "content"
                    )
                    or
                    ""
                ).strip()

                lower_raw_repaired_text = raw_repaired_text.lower()

                if (
                    "<think>" in lower_raw_repaired_text
                    and
                    "</think>" not in lower_raw_repaired_text
                ):
                    raise RuntimeError(
                        "Local Ollama governed repair ended inside an "
                        "unfinished <think> block. No governed FOH answer "
                        "was produced. "
                        f"done_reason={repair_data.get('done_reason')}."
                    )

                repaired_text = provider.clean_output_text(
                    raw_repaired_text
                )

                if not repaired_text:
                    raise RuntimeError(
                        "Local Ollama governed repair returned no usable "
                        "final message content."
                    )

                final_validation_started = time.perf_counter()
                repair_validation = (
                    WorkerOutputValidator().validate(
                        output_text=repaired_text,
                        worker_packet_text=pmei_context,
                    )
                )
                final_validation_seconds = (
                    time.perf_counter() - final_validation_started
                )

                if not repair_validation.ok:

                    add_live_event(
                        "CHAT",
                        f"Local Ollama governed repair rejected - {model}",
                        authority="conversation_only",
                        validation_status=repair_validation.status,
                        validation_issue_count=len(
                            repair_validation.issues
                        ),
                    )

                    return {
                        "ok": False,
                        "error": (
                            "FOH provider output and single governed repair "
                            "were rejected by deterministic PMEi validation."
                        ),
                        "model": model,
                        "provider": "ollama",
                        "authority": (
                            "conversation_plus_read_only_continuity"
                        ),
                        "pmei_context_used": True,
                        "pmei_records_used": len(
                            pmei_context
                        ),
                        "pmei_write_authority": "READ ONLY",
                        "promotion_authority": False,
                        "verification_authority": False,
                        "transition_authority": False,
                        "repair_attempted": True,
                        "validation": {
                            "ok": repair_validation.ok,
                            "status": repair_validation.status,
                            "checked_claims": (
                                repair_validation.checked_claims
                            ),
                            "issues": [
                                {
                                    "rule_id": issue.rule_id,
                                    "severity": issue.severity,
                                    "claim": issue.claim,
                                    "reason": issue.reason,
                                }
                                for issue in repair_validation.issues
                            ],
                        },
                    }, 422

                text_out = repaired_text
                validation = repair_validation

                add_live_event(
                    "CHAT",
                    f"Local Ollama governed repair accepted - {model}",
                    authority="conversation_only",
                    validation_status=validation.status,
                )

        add_live_event(
            "CHAT",
            f"Local Ollama response received - {model}",
            authority="conversation_only",
            pmei_context=bool(pmei_context),
        )

        return {
            "ok": True,
            "text": text_out,
            "model": model,
            "provider": "ollama",
            "authority": (
                "conversation_plus_read_only_continuity"
                if pmei_context
                else "conversation_only"
            ),
            "pmei_context_used": bool(pmei_context),
            "pmei_records_used": len(pmei_context),
            "pmei_write_authority": (
                "READ ONLY" if pmei_context else "NONE"
            ),
            "promotion_authority": False,
            "verification_authority": False,
            "transition_authority": False,
            "repair_attempted": repair_attempted,
            "validation_status": (
                validation.status
                if validation is not None
                else "NOT_APPLICABLE"
            ),
            "interaction_profile_used": bool(interaction_profile_instruction),
            "interaction_profile_confidence": interaction_profile.get(
                "confidence", 0.0
            ),
            "interaction_profile_observations": interaction_profile.get(
                "observations", 0
            ),
            "timing": {
                "pmei_prepare_seconds": round(
                    pmei_prepare_seconds, 4
                ),
                "first_generation_seconds": round(
                    first_generation_seconds, 4
                ),
                "first_validation_seconds": round(
                    first_validation_seconds, 4
                ),
                "repair_generation_seconds": round(
                    repair_generation_seconds, 4
                ),
                "final_validation_seconds": round(
                    final_validation_seconds, 4
                ),
                "foh_total_seconds": round(
                    time.perf_counter() - foh_total_started,
                    4,
                ),
            },
            "ollama_timing": {
                "first": first_ollama_telemetry,
                "repair": repair_ollama_telemetry,
            },
        }

    except Exception as exc:
        add_live_event(
            "CHAT",
            "Local Ollama request failed",
            error_type=type(exc).__name__,
        )

        return {
            "ok": False,
            "error": str(exc),
            "model": model,
            "provider": "ollama",
            "authority": "conversation_only",
        }, 502


if __name__ == "__main__":

    app.run(
        host="127.0.0.1",
        port=5000,
        debug=False,
    )





