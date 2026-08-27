# DAVE-RUNNER

DAVE-RUNNER is a PMEi service layer providing persistent continuity storage,
retrieval, governance-supporting operations and API access to PMEi data.

The repository is part of the wider PMEi architecture. It is not itself an
autonomous agent and should not be treated as the authority for worker
decisions, human approval or self-modification.

## Current architectural position

PMEi separates several concerns that historically existed closer together:

1. Persistent continuity and evidence
2. Retrieval and state reconstruction
3. Tool/API exposure
4. Worker orchestration and reasoning
5. Human authority

DAVE-RUNNER primarily occupies the persistence, retrieval and service/API
layers.

Worker orchestration is being developed separately and must not be assumed to
be an existing responsibility of DAVE-RUNNER merely because workers consume
its data.

Phil remains final human authority.

## Repository components

### server.py

Primary Flask service.

This contains the main DAVE-RUNNER HTTP API and the server-side implementation
for PMEi persistence, retrieval and related continuity operations.

Production deployment currently starts this service with Gunicorn via the
repository Procfile.

### pmei_mcp_server.py

MCP exposure layer.

This exposes selected DAVE-RUNNER capabilities as MCP tools. It should remain
a thin interface boundary rather than becoming the owner of PMEi worker
orchestration.

MCP transport/exposure and orchestration are separate architectural concerns.

### phil_continuity_harness.py

Legacy/experimental continuity validation harness.

This script exercises DAVE-RUNNER endpoints including health, memory scanning,
context scanning and memory saving.

IMPORTANT:

This harness can write/archive results through the memory API. Its behaviour
must not be interpreted as the current authority model for PMEi Workbench or
other read-only workflows.

It is retained as historical/experimental implementation unless deliberately
reviewed and promoted.

### photo_continuity.py

Experimental photo-continuity schema/configuration material.

Despite the `.py` filename, the current file is declarative YAML-like content
rather than executable Python.

It describes proposed local photo scanning, evidence extraction and candidate
continuity behaviour.

It must not be treated as active runtime behaviour or current governance
authority without separate review.

A future cleanup should move this material to an appropriately named schema or
configuration file if it remains part of the architecture.

### phil_continuity_harness.py and experimental files

Experimental or historical files can remain useful evidence of PMEi's
development, but repository presence does not make them authoritative current
architecture.

Current behaviour must be established from the active implementation and
approved contracts.

## Deployment

The current Procfile launches:

```text
gunicorn server:app --bind 0.0.0.0:$PORT --timeout 120 --graceful-timeout 20 --keep-alive 5