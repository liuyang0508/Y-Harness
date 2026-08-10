#![cfg(feature = "https-model")]

use std::{collections::BTreeMap, env, sync::Arc, time::Duration};

use y_harness::{
    EnvironmentSecretProvider, Item, ItemKind, LanguageModel, ModelOutput, ModelRequest,
    OpenAiChatCompletionsModel, OpenAiChatCompletionsModelConfig, SecretReference, ThreadId,
    TurnId,
};

#[tokio::test]
#[ignore = "requires YH_OPENAI_CHAT_MODEL and OPENAI_API_KEY"]
async fn direct_openai_chat_completions_round_trip() {
    let model = env::var("YH_OPENAI_CHAT_MODEL").expect("set YH_OPENAI_CHAT_MODEL");
    let reference = SecretReference::new("openai-chat/live").expect("secret reference");
    let secrets = Arc::new(
        EnvironmentSecretProvider::new(
            "openai-chat-live-test",
            BTreeMap::from([(reference.clone(), "OPENAI_API_KEY".to_owned())]),
        )
        .expect("secret provider"),
    );
    let config = OpenAiChatCompletionsModelConfig::new(model, reference)
        .expect("config")
        .with_limits(
            4_096,
            Duration::from_secs(120),
            Duration::from_secs(10),
            4_194_304,
            1,
        )
        .expect("limits");
    let provider = OpenAiChatCompletionsModel::new("openai-chat/live", config, secrets)
        .expect("Chat Completions provider");
    let response = provider
        .complete_with_metadata(ModelRequest {
            thread_id: ThreadId::from_static("openai-chat-live-thread"),
            turn_id: TurnId::from_static("openai-chat-live-turn"),
            authority: y_harness::AuthorityContext::local_process(),
            items: vec![Item::new(ItemKind::UserMessage {
                content: "Reply with a short greeting.".to_owned(),
            })],
            context: Vec::new(),
            tools: Vec::new(),
        })
        .await
        .expect("Chat Completions response");
    assert!(matches!(
        response.output,
        ModelOutput::Message { ref content } if !content.trim().is_empty()
    ));
    assert!(response.provider_model.is_some());
    assert!(response.provider_request_id.is_some());
}
