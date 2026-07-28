# Hermes Agent adapter conformance probe

This directory preserves one real released-Hermes Agent adapter run, not a
comparative benchmark or a Harness-effect result.

- Date: 2026-07-28
- Host: macOS `aarch64`
- Official release: `v2026.7.20`
- Annotated tag object: `c7d08de287556b3d339df336b180a39d4980ebd7`
- Release commit:
  [`3ef6bbd`](https://github.com/NousResearch/hermes-agent/tree/3ef6bbd201263d354fd83ec55b3c306ded2eb72a)
- Python package version: `hermes-agent==0.19.0`
- Official `uv.lock` SHA-256:
  `456f76d5396df0f543d1035c2d05173cae1882c290ba585cc926a79958b9d7fe`
- Official `pyproject.toml` SHA-256:
  `7fc0552a6bfdd8d58632a9164e3432c868fc4d928170f8c8a545421134c5952f`
- Environment builder: `uv 0.11.32`, archive SHA-256
  `ed336d0ba49db8ef89b2b41fffa372ce63bd032f22a56f001c265891aec32829`
- Managed Python: `3.13.14`
- Hermes console-launcher SHA-256:
  `2bec840a476717c67d5ccf5b0fe4d76d63c58825a2523e22057f3e62113d2928`
- Adapter track: `adapter_conformance`
- Tools: disabled
- Claim eligible: no

The exact adapter input is [`spec.json`](spec.json), and its format-6 output is
[`result.json`](result.json). The source checkout was installed with:

```bash
uv sync --locked --no-dev --python 3.13
```

The released `hermes --oneshot` process loaded the explicit Provider and Model
coordinates, called the deterministic loopback [`provider.mjs`](provider.mjs),
and returned exactly `YH-HERMES-ADAPTER-OK`. Its validated usage sidecar
reported one API call, one input token, one output token, the settled
`local-deterministic` Model, the `openrouter` Provider, and estimated cost
`0.0`. Format 6 correctly leaves actual cost unavailable.

[`provider-request.jsonl`](provider-request.jsonl) is the sanitized request
captured by the loopback process. It corroborates streaming mode, the labeled
user-level instruction, settled request Model, and absence of Tool schemas,
but format 6 does not bind this sidecar's digest into `result.json`. The
literal `fixture-token` used during the local run is a test value, not a
credential.

The run began with an empty workspace, Hermes home, and usage directory. The
workspace remained empty. Hermes created its documented isolated session,
cache, log, memory, and state paths; the adapter removed its private usage file
after settlement. The adapter maps both POSIX `HOME` and Windows
`USERPROFILE` to the isolated Hermes home. The retained Provider request
therefore contains
`User home directory: /private/tmp/yh-hermes-final.op07ju/home`, not the
operator's real home.

The product still adds its own system prompt and host/workspace metadata, the
requested benchmark instruction has only user-message authority, and the
prompt is visible in process arguments. The Python launcher digest does not
identify the installed package graph, the loopback request is not
report-hash-bound, and process isolation is unrestricted. These boundaries
remain explicit rather than being promoted into stronger evidence.

This fixed-output cell proves released-CLI and adapter protocol conformance
only. It disables Tools, exercises no recovery or stateful Agent task, uses no
comparable Model, measures no answer quality, and supports no claim that
either Harness is better.
