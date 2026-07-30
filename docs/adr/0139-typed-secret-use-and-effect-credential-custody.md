# ADR 0139: Typed Secret use and Effect Connector credential custody

- Status: accepted
- Date: 2026-07-30

## Context

Secret Provider API 2 required every `SecretRequest` to contain a Thread and
Turn. That was exact for Model requests, but it was not a truthful universal
credential scope:

- a Governed Effect owns an `EffectId`, immutable operation, attempt, lease,
  phase, and trusted `AuthorityContext`; it does not own an Agent Turn;
- Doctor and service startup validate deployment credentials without a Turn;
- the shared HTTPS MCP transport resolves its configured bearer for a
  transport request, not for a fabricated conversation.

The reference Effect consumer also supported only `environment_from_host`.
That map is an eagerly loaded, long-lived plain environment projection. Calling
it a Secret manager would be false, and using it for credentials would retain
plain `String` values in process configuration.

## Decision

- Advance the Secret Provider API from 2 to 3.
- Replace mandatory Thread/Turn fields in `SecretRequest` with one typed
  `SecretUseContext`:
  - `agent_turn {thread_id, turn_id}`;
  - `governed_effect {effect_id, operation, phase, attempt, lease_id}`;
  - `service {use_case}`, currently bounded to startup probes and configured
    transport requests.
- Keep trusted actor and tenant identity exclusively in
  `SecretProvider::resolve_as(request, authority)`. A serialized use context is
  correlation evidence, never an authority selector.
- Advance the exact client protocol from v29 to v30 because `initialize`
  advertises Secret Provider API 3. Protocol commands, durable schemas, the
  Model Gateway coordinate, and JSON Effect Connector protocol 1 do not
  otherwise change.
- Add `EffectSecretEnvironment` as an optional adapter boundary. It stores only
  child variable names, opaque `SecretReference` values, and a host-supplied
  `SecretProvider`. It resolves every value:
  - under the exact Effect `AuthorityContext`;
  - after Policy approval and durable claim/selection;
  - immediately before each execution or reconciliation dispatch;
  - inside the existing Connector cancellation and timeout future.
- Add a separate non-cloneable `ProcessRequest.secret_environment` containing
  `SecretValue` buffers. The local broker clears inherited environment, applies
  plain and Secret maps with collision checks, and retains the zeroizing
  buffers through process spawn.
- Add optional reference-service process configuration:

  ```json
  {
    "secret_environment": {
      "TARGET_API_TOKEN": {
        "reference": "effect/notification-primary",
        "host_environment": "NOTIFICATION_API_TOKEN"
      }
    }
  }
  ```

  The reference service selects its existing explicit unscoped or fixed-tenant
  environment Provider, performs a content-free startup/Doctor availability
  probe, and resolves again on every dispatch. Other JSON-command adapters
  reject this field until they define an equally truthful usage context.
- Keep `environment_from_host` as a plain, eagerly loaded configuration
  projection. It remains useful for non-secret flags and paths but carries no
  Secret-custody claim.
- Report only counts of credential-scoped Connectors and variables in Doctor.
  Provider errors, references, host variable names, and values never enter
  Effect errors or health diagnostics.

## Consequences and non-claims

- Effect credentials no longer require fake Thread or Turn identities and do
  not enter Effect input, the JSON Connector envelope, durable State, config
  values, Trace, or default diagnostics.
- `SecretValue` zeroization covers only its own buffers. The process launcher,
  operating system, child process, target SDK, and network stack necessarily
  receive copies that Y-Harness cannot prove erased.
- An external Connector is still trusted not to echo a credential into stdout,
  a receipt, or its target system. Output validation and content-free errors
  reduce accidental leakage; they do not certify arbitrary Connector code.
- Credential-bearing adapters require dispatch SHA-256 evidence and perform an
  additional bounded preflight before Provider resolution. The Broker still
  remeasures before child entry. This prevents Provider lookup for drift
  already present at call entry, but a change racing after the preflight can
  still cause issuance before the second measurement rejects it. See
  [ADR 0140](0140-secret-gated-effect-command-integrity-preflight.md).
- The built-in reference service still offers environment-backed custody, not a
  vault, KMS, OAuth refresh service, rotation controller, or revocation feed.
  Embedded hosts can supply another API-3 `SecretProvider` without changing
  Effect or Process contracts.
- A custom `ProcessBroker` or `SecretProvider` is a trusted host component. The
  Engine cannot prove that an implementation does not copy or log values.
- `SecretRequest` and `ProcessRequest` are public pre-1.0 Rust structs. External
  struct literals must migrate to `use_context` and
  `secret_environment`. Their serialized durable formats are unchanged because
  neither request is persisted by the Engine.

## Rejected alternatives

- Invent a `doctor-thread`, `doctor-turn`, or Effect Turn: fabricated identity
  makes audit evidence ambiguous and invites Policy decisions on false facts.
- Put optional Thread, Turn, Effect, and service fields in one flat request:
  that creates invalid combinations and moves validation into every Provider.
- Resolve credentials during configuration assembly and store plain Strings:
  that lengthens credential lifetime and prevents per-dispatch rotation.
- Put Secret references or values in Effect input or the JSON Connector
  envelope: both are durable or externally observable data planes.
- Let every JSON command use `secret_environment` immediately: Tool, Grader,
  Verifier, and Model operations require their own exact authority and usage
  contracts before receiving credentials.

## Evidence

- `execution::effect::tests::secret_environment_uses_typed_effect_context_and_never_enters_json`
- `execution::effect::tests::secret_resolution_failure_and_precancellation_block_process_entry`
- `execution::effect::tests::secret_environment_rejects_invalid_or_overlapping_child_names`
- `reference_cli::service::tests::effect_consumer_requires_explicit_exact_authority_and_bounded_timeouts`
- `configured_effect_consumer_degrades_recovers_stops_and_does_not_replay_terminal_effects`
- `protocol::tests::protocol_thirty_wire_envelopes_and_permissions_are_stable`
