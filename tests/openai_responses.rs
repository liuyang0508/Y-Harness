#![cfg(feature = "https-model")]

use std::{collections::BTreeMap, env, sync::Arc, time::Duration};

use y_harness::{
    EnvironmentSecretProvider, Item, ItemKind, LanguageModel, ModelOutput, ModelRequest,
    OpenAiResponsesModel, OpenAiResponsesModelConfig, SecretReference, ThreadId, TurnId,
};

#[tokio::test]
#[ignore = "requires YH_OPENAI_MODEL and OPENAI_API_KEY"]
async fn direct_openai_responses_round_trip() {
    let model = env::var("YH_OPENAI_MODEL").expect("set YH_OPENAI_MODEL");
    let reference = SecretReference::new("openai/live".to_owned()).expect("secret reference");
    let secrets = Arc::new(
        EnvironmentSecretProvider::new(
            "openai-live-test",
            BTreeMap::from([(reference.clone(), "OPENAI_API_KEY".to_owned())]),
        )
        .expect("secret provider"),
    );
    let config = OpenAiResponsesModelConfig::new(model, reference)
        .expect("config")
        .with_limits(
            Duration::from_secs(120),
            Duration::from_secs(10),
            4_194_304,
            1,
        )
        .expect("limits");
    let provider =
        OpenAiResponsesModel::new("openai/live", config, secrets).expect("OpenAI provider");
    let response = provider
        .complete_with_metadata(ModelRequest {
            thread_id: ThreadId::from_static("openai-live-thread"),
            turn_id: TurnId::from_static("openai-live-turn"),
            items: vec![Item::new(ItemKind::UserMessage {
                content: "Reply with a short greeting.".to_owned(),
            })],
            context: Vec::new(),
            tools: Vec::new(),
        })
        .await
        .expect("OpenAI response");
    assert!(matches!(
        response.output,
        ModelOutput::Message { ref content } if !content.trim().is_empty()
    ));
    assert!(response.provider_model.is_some());
    assert!(response.provider_request_id.is_some());
}
