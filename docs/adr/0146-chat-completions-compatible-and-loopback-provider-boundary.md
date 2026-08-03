# ADR 0146: Chat Completions compatibility and loopback Provider boundary

- Status: accepted
- Date: 2026-08-02

## Context

Source review showed that much of Pi's Provider breadth is composed through an
OpenAI Chat Completions-compatible protocol used by several hosted and local
inference services. Y-Harness already had strict native Responses, Anthropic,
and Gemini adapters, but routing a Chat endpoint through Responses would be a
false compatibility claim. Requiring public HTTPS also left local Ollama/vLLM
hosts behind a custom Broker despite their otherwise compatible wire format.

## Decision

1. Add a distinct `open_ai_chat_completions` Provider Profile and
   `OpenAiChatCompletionsModel`; it never aliases the Responses adapter.
2. Map ordered messages, function Tools, parallel Tool calls, non-stream and
   SSE responses, interleaved text/Tool deltas, usage-only terminal chunks,
   cached/reasoning usage details, and typed finish failures.
3. Preserve the normalized assistant message that proposed Tool calls in an
   origin-bound `openai.chat.message.v1` continuation. On replay, every
   continuation call must exactly match its separately durable Tool call before
   the Tool result is sent.
4. Let a Profile choose `max_completion_tokens` or legacy `max_tokens`, and
   whether to request a final streaming usage chunk. These are wire
   compatibility choices, not Agent policy.
5. Keep public endpoints HTTPS-only. An explicit `allow_loopback_http` may
   permit HTTP only when the URL host is a literal loopback IP. Hostnames,
   userinfo, query credentials, fragments, redirects, ambient proxies, implicit
   retries, and non-loopback plaintext endpoints remain rejected.

## Consequences

- One adapter covers official OpenAI Chat Completions plus services that
  implement that exact contract, without adding vendor branches to Agent Loop.
- Local inference is configurable but not automatically discovered, and still
  requires an explicit credential mapping (a local host may ignore its value).
- Streaming text remains provisional; the final decoded message or Tool batch
  is the only authoritative Model decision.
- Provider Profiles, State, Client Protocol, Policy, and Tool authority retain
  their existing coordinates and responsibilities.

## Non-claims

- Compatibility is a wire-contract claim, not certification of every endpoint,
  model, rate limit, prompt-cache extension, multimodal variant, or reasoning
  field.
- Azure deployment paths/authentication, AWS SigV4/Bedrock, Vertex service
  accounts, OAuth, price/load routing, and local model discovery remain open.
- Deterministic contract tests and an ignored credentialed live probe do not
  establish comparative answer quality or production availability.
