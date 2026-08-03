#![cfg(feature = "https-model")]

use std::{collections::BTreeMap, env, sync::Arc, time::Duration};

use y_harness::{
    AnthropicMessagesModel, AnthropicMessagesModelConfig, EnvironmentSecretProvider, Item,
    ItemKind, LanguageModel, ModelOutput, ModelRequest, SecretReference, ThreadId, TurnId,
};

#[tokio::test]
#[ignore = "requires YH_ANTHROPIC_MODEL and ANTHROPIC_API_KEY"]
async fn direct_anthropic_messages_round_trip() {
    let model = env::var("YH_ANTHROPIC_MODEL").expect("set YH_ANTHROPIC_MODEL");
    let reference = SecretReference::new("anthropic/live").expect("secret reference");
    let secrets = Arc::new(
        EnvironmentSecretProvider::new(
            "anthropic-live-test",
            BTreeMap::from([(reference.clone(), "ANTHROPIC_API_KEY".to_owned())]),
        )
        .expect("secret provider"),
    );
    let config = AnthropicMessagesModelConfig::new(model, reference)
        .expect("config")
        .with_limits(
            Duration::from_secs(120),
            Duration::from_secs(10),
            4_194_304,
            1,
        )
        .expect("limits");
    let provider =
        AnthropicMessagesModel::new("anthropic/live", config, secrets).expect("provider");
    let response = provider
        .complete_with_metadata(ModelRequest {
            thread_id: ThreadId::from_static("anthropic-live-thread"),
            turn_id: TurnId::from_static("anthropic-live-turn"),
            authority: y_harness::AuthorityContext::local_process(),
            items: vec![Item::new(ItemKind::UserMessage {
                content: "Reply with a short greeting.".to_owned(),
            })],
            context: Vec::new(),
            tools: Vec::new(),
        })
        .await
        .expect("Anthropic response");
    assert!(matches!(
        response.output,
        ModelOutput::Message { ref content } if !content.trim().is_empty()
    ));
    assert!(response.provider_model.is_some());
    assert!(response.provider_request_id.is_some());
}
