#![cfg(feature = "https-model")]

use std::{collections::BTreeMap, env, sync::Arc, time::Duration};

use y_harness::{
    EnvironmentSecretProvider, GeminiGenerateContentModel, GeminiGenerateContentModelConfig, Item,
    ItemKind, LanguageModel, ModelOutput, ModelRequest, SecretReference, ThreadId, TurnId,
};

#[tokio::test]
#[ignore = "requires YH_GEMINI_MODEL and GEMINI_API_KEY"]
async fn direct_gemini_generate_content_round_trip() {
    let model = env::var("YH_GEMINI_MODEL").expect("set YH_GEMINI_MODEL");
    let reference = SecretReference::new("gemini/live").expect("secret reference");
    let secrets = Arc::new(
        EnvironmentSecretProvider::new(
            "gemini-live-test",
            BTreeMap::from([(reference.clone(), "GEMINI_API_KEY".to_owned())]),
        )
        .expect("secret provider"),
    );
    let config = GeminiGenerateContentModelConfig::new(model, reference)
        .expect("config")
        .with_limits(
            Duration::from_secs(120),
            Duration::from_secs(10),
            4_194_304,
            1,
        )
        .expect("limits");
    let provider =
        GeminiGenerateContentModel::new("gemini/live", config, secrets).expect("provider");
    let response = provider
        .complete_with_metadata(ModelRequest {
            thread_id: ThreadId::from_static("gemini-live-thread"),
            turn_id: TurnId::from_static("gemini-live-turn"),
            authority: y_harness::AuthorityContext::local_process(),
            items: vec![Item::new(ItemKind::UserMessage {
                content: "Reply with a short greeting.".to_owned(),
            })],
            context: Vec::new(),
            tools: Vec::new(),
        })
        .await
        .expect("Gemini response");
    assert!(matches!(
        response.output,
        ModelOutput::Message { ref content } if !content.trim().is_empty()
    ));
    assert!(response.provider_model.is_some());
    assert!(response.provider_request_id.is_some());
}
