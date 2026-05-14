use crate::{ProviderDecision, ProviderRequest};
use async_trait::async_trait;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderErrorKind {
    Timeout,
    Transport,
    RateLimited,
    InvalidResponse,
    Configuration,
    Internal,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("{kind:?}: {message}")]
pub struct ProviderError {
    pub kind: ProviderErrorKind,
    pub message: String,
    pub retryable: bool,
}

impl ProviderError {
    pub fn new(kind: ProviderErrorKind, message: impl Into<String>, retryable: bool) -> Self {
        Self {
            kind,
            message: message.into(),
            retryable,
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ProviderErrorKind::Internal, message, false)
    }
}

#[async_trait]
pub trait LlmProvider: Send + Sync {
    async fn generate_action(
        &self,
        input: ProviderRequest,
    ) -> Result<ProviderDecision, ProviderError>;
}

type DynamicResponder =
    dyn Fn(ProviderRequest) -> Result<ProviderDecision, ProviderError> + Send + Sync;

#[derive(Clone)]
pub struct MockLlmProvider {
    responder: Arc<DynamicResponder>,
    observed_inputs: Arc<Mutex<Vec<ProviderRequest>>>,
}

impl MockLlmProvider {
    pub fn scripted(actions: Vec<crate::AgentAction>) -> Self {
        let queue = Arc::new(Mutex::new(VecDeque::from(actions)));
        Self::dynamic(move |_input| {
            let action = queue
                .lock()
                .expect("script queue poisoned")
                .pop_front()
                .ok_or_else(|| ProviderError::internal("mock script exhausted"))?;
            Ok(ProviderDecision {
                action,
                usage: None,
                cache: None,
            })
        })
    }

    pub fn dynamic<F>(responder: F) -> Self
    where
        F: Fn(ProviderRequest) -> Result<ProviderDecision, ProviderError> + Send + Sync + 'static,
    {
        Self {
            responder: Arc::new(responder),
            observed_inputs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn observed_inputs(&self) -> Vec<ProviderRequest> {
        self.observed_inputs
            .lock()
            .expect("observed input lock poisoned")
            .clone()
    }
}

#[async_trait]
impl LlmProvider for MockLlmProvider {
    async fn generate_action(
        &self,
        input: ProviderRequest,
    ) -> Result<ProviderDecision, ProviderError> {
        self.observed_inputs
            .lock()
            .expect("observed input lock poisoned")
            .push(input.clone());
        (self.responder)(input)
    }
}
