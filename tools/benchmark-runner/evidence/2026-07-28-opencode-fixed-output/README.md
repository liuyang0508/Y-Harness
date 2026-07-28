# OpenCode adapter conformance probe

This directory preserves one real released-OpenCode adapter run, not a
comparative benchmark or a Harness-effect result.

- Date: 2026-07-28
- Host: macOS `aarch64`
- Released package: `opencode-ai@1.18.5`
- Native package: `opencode-darwin-arm64@1.18.5`
- Source coordinate:
  [`7534d23`](https://github.com/anomalyco/opencode/tree/7534d23551f665e65080809975b4ca5c7d63807b)
- Wrapper-package integrity:
  `sha512-Q0jlX4ihn7veMeYsLX3c4PYFAKIURU3GIpXt1FnhNxNn3v8+RpIZ8z9umG5D0r8g8Smp9fZLGjgLe/9mJ4NyYw==`
- Native-package integrity:
  `sha512-an4t+aOHTREf1f1Z8mNIyEoy1iJL+434BuB+2UKFuAarMUaV37SWkPiS4oW8nvJjXyb8LC0AFSeVA8AeBy44cg==`
- Native executable SHA-256:
  `45922f63cf068f5c72d44cf18d1cde9816f359668258608b0272e54c304106c1`
- Adapter track: `adapter_conformance`
- Tools: denied
- Claim eligible: no

The exact adapter input is [`spec.json`](spec.json), and the adapter output is
[`result.json`](result.json). OpenCode loaded the explicit
[`opencode.json`](opencode.json), called the deterministic loopback
[`provider.mjs`](provider.mjs), emitted one ordered
`step_start → text → step_finish` lifecycle, and returned the exact requested
text `YH-OPENCODE-ADAPTER-OK`.

[`provider-request.jsonl`](provider-request.jsonl) is the sanitized request
captured by that loopback process. It corroborates the Model, prompt, streaming
mode, and absence of Tools, but format 5 does not bind this sidecar's digest
into `result.json`. The literal `fixture-token` in `opencode.json` is a local
test value, not a credential.

The run used an initially empty bare home and an empty workspace. OpenCode
created only its isolated config/cache/data/state paths during the run. The
adapter inherited the name `OPENCODE_CONFIG` so the released product could
load the exact custom Provider file. The runtime copy was mode `0444`; its
SHA-256 remained
`b49fc14ae270cbfe11f5a7ce90b483b4bdb7f004b47cdb1db54252f503b36f39`
after OpenCode attempted its ordinary configuration normalization. The
environment value, Provider routing, unrestricted process authority, and
launcher dependencies remain explicit unsupported controls.

The format keeps `observed_models` empty because OpenCode's run JSONL does not
expose settled Model identity. The request sidecar is not promoted into
product-settlement evidence.

The deterministic response proves released-CLI and adapter protocol
conformance only. It denies Tools, exercises no recovery or stateful Agent
work, uses no comparable Model, measures no answer quality, and supports no
claim that either Harness is better.
