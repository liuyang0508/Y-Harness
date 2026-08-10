# Y-Harness Aquaculture Domain Pack

This workspace crate specializes Y-Harness for the智慧渔业 project without
changing Core Agent Loop semantics.

The first release provides:

- a registry for all eight business Journeys;
- deterministic intent and pond-scope resolution;
- a structured `ContextPackage` with trusted tenant/user scope, time window,
  provenance, and unresolved questions;
- synthetic, tenant-fenced IoT and ERP connector tools with evidence claims;
- multi-dimensional evidence scoring where source type is not a fixed rank;
- a structured answer contract and Y-Harness completion verifier;
- eight systemic POC evaluation cases;
- an immutable Domain Pack snapshot pinning exact components.

Only `AQ-JR-001` is marked `poc_ready`. The remaining Journeys are designed and
kept visible so implementation progresses against the full product map rather
than a single demo case.

Synthetic data is always labeled `data_origin: synthetic` and must never be
presented as customer production truth.

Run the deterministic end-to-end replay through the real Y-Harness Agent Loop:

```bash
cargo run -p y-harness-aquaculture --example poc_replay
```

The replay model is a test fixture, not a production LLM adapter. It proves the
host wiring and governance chain while keeping model and retrieval providers
replaceable.
