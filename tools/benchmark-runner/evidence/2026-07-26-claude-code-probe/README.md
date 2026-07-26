# Claude Code adapter conformance probe

This directory preserves one real adapter-format run, not a comparative
benchmark.

- Date: 2026-07-26
- Host: macOS `aarch64`
- Released CLI: Claude Code `2.1.143`
- Adapter source revision: `3b92c7d`
- Adapter track: `adapter_conformance`
- Tools: disabled
- Claim eligible: no

The exact input is [`spec.json`](spec.json); the adapter output is
[`result.json`](result.json). The product returned the exact requested text,
but the requested `haiku` alias settled under observed model
`MiniMax-M2.7`. Product-profile ambient configuration was intentionally
retained and is declared as an unsupported control in the result.

An earlier exploratory call requested a 0.02 USD maximum and returned
`error_max_budget_usd` after reporting 0.056875 USD. That shape is retained
only as the sanitized parser fixture; it is not presented as this adapter run.

The report fingerprints the exact debug adapter executable used for the run,
but that binary is not stored in Git. Rebuilding a debug artifact after
workspace metadata changes is not expected to reproduce its byte hash. This is
another reason the record is conformance evidence rather than a comparison
result; claim-eligible runs must retain a released artifact and build
provenance.
