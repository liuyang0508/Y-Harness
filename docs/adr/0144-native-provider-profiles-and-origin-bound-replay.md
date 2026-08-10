# ADR 0144: native Provider Profiles and origin-bound replay

- Status: accepted
- Date: 2026-08-02

## Context

The first direct Provider adapter implemented OpenAI Responses and later
allowed explicitly compatible endpoints. That supported configuration-driven
Model aliases but did not make Anthropic Messages or Gemini
`generateContent` compatible with the Responses wire. Repeating credentials,
timeouts, byte limits, and endpoint policy in every Model entry also made a
large catalog harder to audit and rotate.

Provider breadth must not weaken the existing Engine boundary. Vendor Tools
remain model proposals rather than execution authority; private reasoning or
continuation state must be replayed exactly and only by the Model identity and
origin that produced it; credentials must remain short-lived Secret values;
and a client catalog must remain credential-free.

## Decision

1. Service-schema 1 additively introduces `provider_profiles`. A Profile owns
   one exact native protocol, Secret reference and environment mapping,
   endpoint/base URL, API version when applicable, request/connect timeout,
   response-byte limit, concurrency ceiling, and output-token ceiling.
   `provider_model` entries select a stable Harness Model identity, one exact
   Profile, and one vendor Model ID. Legacy Model entries remain valid.
2. Three first-party direct protocol families are supported:
   `openai_responses`, `anthropic_messages`, and
   `gemini_generate_content`. All use pooled HTTPS-only clients, TLS 1.2 or
   later, no redirects, no ambient proxy, no implicit retry, bounded response
   retention, bounded stream events, explicit concurrency, and sensitive
   credential headers.
3. Anthropic request assembly uses native Messages content blocks,
   `tool_use`, `tool_result`, version headers, named SSE event sequencing, and
   native usage. Mixed non-Tool blocks required before a Tool decision enter
   `anthropic.messages.content.v1` continuation state.
4. Gemini request assembly uses native `Content.parts`, function declarations,
   `functionCall`, `functionResponse`, `usageMetadata`, and data-only SSE.
   Every response part preceding a Tool decision is retained in
   `google.gemini.parts.v1`, including `thoughtSignature`. Missing provider
   call IDs receive deterministic content-derived Harness correlation IDs;
   replay validates the continuation against the separately durable Tool call.
5. Startup and `yh doctor` validate every Profile shape and exact references
   before accepting requests. Only referenced Models resolve credentials.
   Runtime Catalog exposes adapter family and endpoint but never the Secret
   reference, environment name, value, request headers, or Provider-private
   continuation.
6. Profile changes use ADR 0143's settled-Turn restart boundary. A running Turn
   never changes Provider generation in place.

## Consequences

- Operators may rotate an API-key mapping or add another concrete Model for a
  supported Provider without changing Rust code.
- Provider-specific behavior stays at the adapter boundary instead of leaking
  vendor enums into Agent Loop, Tool Runtime, State, Policy, or clients.
- Exact opaque replay costs durable bytes, but avoids silently discarding
  Anthropic content or Gemini signatures required for a valid next request.
- Config schema, Client Protocol, State schema, and Model Gateway coordinates
  do not change because all additions are defaulted service assembly and
  existing origin-bound continuation data.

## Non-claims

- This is not the full Provider catalog of Pi, Claude Code, Codex, OpenCode, or
  a hosted model router. OpenAI Chat Completions is added separately by
  [ADR 0146](0146-chat-completions-compatible-and-loopback-provider-boundary.md);
  Azure-specific auth, Bedrock, Vertex AI, local inference discovery,
  price/load routing, and OAuth remain outside this decision.
- A configured endpoint must implement the selected exact native contract.
  Endpoint substitution never performs protocol guessing or semantic
  translation.
- Provider-reported usage is evidence, not independently verified billing.
  Live ignored tests require operator-supplied credentials; deterministic
  contract tests do not claim production availability or quality superiority.
