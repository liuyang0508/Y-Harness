# Pi adapter conformance probe

This directory preserves one real released-Pi adapter run, not a comparative
benchmark or a Harness-effect result.

- Date: 2026-07-28
- Host: macOS `aarch64`
- Released package: `@earendil-works/pi-coding-agent@0.82.1`
- Source coordinate:
  [`cee5ff7`](https://github.com/earendil-works/pi/tree/cee5ff7520d8828bed9955ef00419e995d1f91e0)
- Package integrity:
  `sha512-zbkAhoIuDPMF3pKuja0ajZabrMWU29FUMV9A/XMXT/XC1yXs5xt6t6t13GogQFsDrDqbFP4DkZQO1w8rWRAzYA==`
- Product entry-point SHA-256:
  `af302f231437eaf6f37691bce4b34234fcb626bcb5eb3910d4fc3f6519bf78ca`
- Adapter track: `adapter_conformance`
- Tools: disabled
- Claim eligible: no

The exact adapter input is [`spec.json`](spec.json), and the adapter output is
[`result.json`](result.json). Pi loaded the isolated product-profile
[`models.json`](models.json), called the deterministic loopback
[`provider.mjs`](provider.mjs), emitted its JSONL Agent lifecycle, and returned
the exact requested text `YH-PI-ADAPTER-OK`.

[`provider-request.jsonl`](provider-request.jsonl) is the sanitized request
captured by that loopback process. It corroborates the Model, prompt, streaming
mode, and absence of Tools, but format 4 does not bind this sidecar's digest
into `result.json`. The literal `fixture-token` in `models.json` is a local
test value, not a credential.

The run used the `product` profile because Pi discovers custom Providers from
`PI_CODING_AGENT_DIR/models.json`; the adapter therefore inherited both
`PI_CODING_AGENT_DIR` and the `PATH` required by the entry point's
`/usr/bin/env node` launcher. Those environment values, launcher dependency,
Provider routing, unrestricted process authority, and ambient-configuration
boundary are explicitly unsupported controls in the report.

The deterministic response proves released-CLI and adapter protocol
conformance only. It disables Tools, exercises no recovery or stateful Agent
work, uses no comparable Model, measures no answer quality, and supports no
claim that either Harness is better.
