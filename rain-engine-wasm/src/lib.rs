//! Wasmtime-backed executor for untrusted RainEngine skills.
//!
//! WASM skills receive explicit JSON inputs and only the host capabilities
//! declared in their manifest.

use async_trait::async_trait;
use rain_engine_core::{
    SkillCapability, SkillExecutionError, SkillExecutor, SkillFailureKind, SkillInvocation,
    SkillManifest,
};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;
use tokio::task;
use url::Url;
use wasmtime::{
    AsContextMut, Caller, Config, Engine, Extern, Instance, Linker, Memory, Module, Store,
    StoreLimits, StoreLimitsBuilder,
};

#[derive(Clone)]
pub struct WasmSkillConfig {
    pub manifest: SkillManifest,
    pub wasm_bytes: Arc<Vec<u8>>,
    pub capabilities: Arc<dyn WasmCapabilityHost>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WasmSkillRequest {
    pub invocation: SkillInvocation,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WasmSkillResponse {
    pub ok: bool,
    pub value: Value,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HttpCapabilityRequest {
    pub url: String,
    #[serde(default = "default_method")]
    pub method: String,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default)]
    pub body: Option<String>,
}

fn default_method() -> String {
    "GET".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HttpCapabilityResponse {
    pub status: u16,
    pub body: String,
    pub headers: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KvCapabilityRequest {
    pub namespace: String,
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KvCapabilityResponse {
    pub found: bool,
    pub value: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StructuredLogEntry {
    pub level: String,
    pub message: String,
    #[serde(default)]
    pub fields: HashMap<String, Value>,
}

#[derive(Debug, Error)]
pub enum WasmError {
    #[error("module error: {0}")]
    Module(String),
}

#[derive(Debug, Error, Clone, PartialEq)]
#[error("{message}")]
pub struct CapabilityError {
    pub kind: SkillFailureKind,
    pub message: String,
}

impl CapabilityError {
    fn denied(message: impl Into<String>) -> Self {
        Self {
            kind: SkillFailureKind::CapabilityDenied,
            message: message.into(),
        }
    }
}

pub trait WasmCapabilityHost: Send + Sync {
    fn kv_get(
        &self,
        _request: KvCapabilityRequest,
    ) -> Result<KvCapabilityResponse, CapabilityError> {
        Err(CapabilityError::denied(
            "key/value capability host is not configured",
        ))
    }

    fn http_fetch(
        &self,
        _request: HttpCapabilityRequest,
    ) -> Result<HttpCapabilityResponse, CapabilityError> {
        Err(CapabilityError::denied(
            "http capability host is not configured",
        ))
    }

    fn log(&self, _entry: StructuredLogEntry) -> Result<(), CapabilityError> {
        Err(CapabilityError::denied(
            "structured log capability host is not configured",
        ))
    }
}

#[derive(Default)]
pub struct NoopCapabilityHost;

impl WasmCapabilityHost for NoopCapabilityHost {}

#[derive(Default)]
pub struct InMemoryCapabilityHost {
    values: HashMap<(String, String), Value>,
    logs: Mutex<Vec<StructuredLogEntry>>,
    http_enabled: bool,
}

impl InMemoryCapabilityHost {
    pub fn with_value(mut self, namespace: &str, key: &str, value: Value) -> Self {
        self.values
            .insert((namespace.to_string(), key.to_string()), value);
        self
    }

    pub fn with_http_client(mut self) -> Self {
        self.http_enabled = true;
        self
    }

    pub fn logs(&self) -> Vec<StructuredLogEntry> {
        self.logs.lock().expect("logs lock").clone()
    }
}

impl WasmCapabilityHost for InMemoryCapabilityHost {
    fn kv_get(
        &self,
        request: KvCapabilityRequest,
    ) -> Result<KvCapabilityResponse, CapabilityError> {
        Ok(KvCapabilityResponse {
            found: self
                .values
                .contains_key(&(request.namespace.clone(), request.key.clone())),
            value: self.values.get(&(request.namespace, request.key)).cloned(),
        })
    }

    fn http_fetch(
        &self,
        request: HttpCapabilityRequest,
    ) -> Result<HttpCapabilityResponse, CapabilityError> {
        if !self.http_enabled {
            return Err(CapabilityError::denied("http client is disabled"));
        }
        let client = Client::new();
        let method = request
            .method
            .parse::<reqwest::Method>()
            .map_err(|err| CapabilityError {
                kind: SkillFailureKind::InvalidResponse,
                message: err.to_string(),
            })?;
        let mut builder = client.request(method, &request.url);
        for (name, value) in &request.headers {
            builder = builder.header(name, value);
        }
        if let Some(body) = request.body {
            builder = builder.body(body);
        }
        let response = builder.send().map_err(|err| CapabilityError {
            kind: SkillFailureKind::Internal,
            message: err.to_string(),
        })?;
        let status = response.status().as_u16();
        let headers = response
            .headers()
            .iter()
            .map(|(name, value)| {
                (
                    name.to_string(),
                    value.to_str().unwrap_or_default().to_string(),
                )
            })
            .collect::<HashMap<_, _>>();
        let body = response.text().map_err(|err| CapabilityError {
            kind: SkillFailureKind::Internal,
            message: err.to_string(),
        })?;
        Ok(HttpCapabilityResponse {
            status,
            body,
            headers,
        })
    }

    fn log(&self, entry: StructuredLogEntry) -> Result<(), CapabilityError> {
        self.logs.lock().expect("logs lock").push(entry);
        Ok(())
    }
}

struct StoreState {
    limits: StoreLimits,
    manifest: SkillManifest,
    capabilities: Arc<dyn WasmCapabilityHost>,
}

pub struct WasmSkillExecutor {
    engine: Engine,
    module: Module,
    manifest: SkillManifest,
    capabilities: Arc<dyn WasmCapabilityHost>,
}

impl WasmSkillExecutor {
    pub fn new(config: WasmSkillConfig) -> Result<Self, WasmError> {
        let mut wasmtime_config = Config::new();
        wasmtime_config.consume_fuel(true);
        wasmtime_config.epoch_interruption(true);

        let engine =
            Engine::new(&wasmtime_config).map_err(|err| WasmError::Module(err.to_string()))?;
        let module = Module::from_binary(&engine, &config.wasm_bytes)
            .map_err(|err| WasmError::Module(err.to_string()))?;

        Ok(Self {
            engine,
            module,
            manifest: config.manifest,
            capabilities: config.capabilities,
        })
    }

    fn build_store(&self) -> Result<Store<StoreState>, SkillExecutionError> {
        let mut store = Store::new(
            &self.engine,
            StoreState {
                limits: StoreLimitsBuilder::new()
                    .memory_size(self.manifest.resource_policy.max_memory_bytes)
                    .build(),
                manifest: self.manifest.clone(),
                capabilities: self.capabilities.clone(),
            },
        );
        store.limiter(|state| &mut state.limits);
        if let Some(fuel) = self.manifest.resource_policy.max_fuel {
            store.set_fuel(fuel).map_err(|err| {
                SkillExecutionError::new(SkillFailureKind::Internal, err.to_string())
            })?;
        }
        Ok(store)
    }
}

#[async_trait]
impl SkillExecutor for WasmSkillExecutor {
    async fn execute(&self, invocation: SkillInvocation) -> Result<Value, SkillExecutionError> {
        let timeout = Duration::from_millis(self.manifest.resource_policy.timeout_ms);
        let engine = self.engine.clone();
        let module = self.module.clone();
        let manifest = self.manifest.clone();
        let capabilities = self.capabilities.clone();
        let encoded = serde_json::to_vec(&WasmSkillRequest { invocation }).map_err(|err| {
            SkillExecutionError::new(SkillFailureKind::InvalidResponse, err.to_string())
        })?;

        let join = task::spawn_blocking(move || {
            let executor = WasmSkillExecutor {
                engine,
                module,
                manifest,
                capabilities,
            };
            executor.execute_blocking(encoded)
        });

        match tokio::time::timeout(timeout + Duration::from_millis(50), join).await {
            Ok(join_result) => join_result.map_err(|err| {
                SkillExecutionError::new(SkillFailureKind::Internal, err.to_string())
            })?,
            Err(_) => {
                self.engine.increment_epoch();
                Err(SkillExecutionError::new(
                    SkillFailureKind::Timeout,
                    "skill execution exceeded timeout",
                ))
            }
        }
    }

    fn executor_kind(&self) -> &'static str {
        "wasm"
    }
}

impl WasmSkillExecutor {
    fn execute_blocking(&self, encoded: Vec<u8>) -> Result<Value, SkillExecutionError> {
        let mut store = self.build_store()?;
        store.set_epoch_deadline(1);

        let mut linker = Linker::new(&self.engine);
        register_capabilities(&mut linker)?;

        let instance = linker
            .instantiate(&mut store, &self.module)
            .map_err(|err| classify_trap(err.to_string()))?;

        let memory = extract_memory(&mut store, &instance)?;
        let alloc = instance
            .get_typed_func::<i32, i32>(&mut store, "alloc")
            .map_err(|err| SkillExecutionError::new(SkillFailureKind::Internal, err.to_string()))?;
        let run = instance
            .get_typed_func::<(i32, i32), i64>(&mut store, "run")
            .map_err(|err| SkillExecutionError::new(SkillFailureKind::Internal, err.to_string()))?;
        let dealloc = instance
            .get_typed_func::<(i32, i32), ()>(&mut store, "dealloc")
            .map_err(|err| SkillExecutionError::new(SkillFailureKind::Internal, err.to_string()))?;

        let input_ptr = alloc
            .call(&mut store, i32::try_from(encoded.len()).unwrap_or(i32::MAX))
            .map_err(|err| classify_trap(err.to_string()))?;
        memory
            .write(&mut store, input_ptr as usize, &encoded)
            .map_err(|err| classify_memory(err.to_string()))?;

        let packed = run
            .call(
                &mut store,
                (input_ptr, i32::try_from(encoded.len()).unwrap_or(i32::MAX)),
            )
            .map_err(|err| classify_trap(err.to_string()))?;
        let output_ptr = packed as u32;
        let output_len = (packed >> 32) as u32;

        let mut output = vec![0u8; output_len as usize];
        memory
            .read(&store, output_ptr as usize, &mut output)
            .map_err(|err| classify_memory(err.to_string()))?;
        let _ = dealloc.call(&mut store, (input_ptr, encoded.len() as i32));
        let _ = dealloc.call(&mut store, (output_ptr as i32, output_len as i32));

        if let Ok(decoded) = serde_json::from_slice::<WasmSkillResponse>(&output) {
            if decoded.ok {
                return Ok(decoded.value);
            }
            let message = decoded
                .error
                .unwrap_or_else(|| "wasm module returned failure".to_string());
            let kind = if message.contains("capability") {
                SkillFailureKind::CapabilityDenied
            } else {
                SkillFailureKind::Internal
            };
            return Err(SkillExecutionError::new(kind, message));
        }

        serde_json::from_slice::<Value>(&output).map_err(|err| {
            SkillExecutionError::new(SkillFailureKind::InvalidResponse, err.to_string())
        })
    }
}

fn register_capabilities(linker: &mut Linker<StoreState>) -> Result<(), SkillExecutionError> {
    linker
        .func_wrap(
            "env",
            "host_log",
            |mut caller: Caller<'_, StoreState>,
             ptr: i32,
             len: i32|
             -> Result<i32, anyhow::Error> {
                ensure_capability(&caller.data().manifest, CapabilityKind::Log)
                    .map_err(anyhow::Error::msg)?;
                let bytes = read_guest_bytes(&mut caller, ptr, len).map_err(anyhow::Error::msg)?;
                let entry: StructuredLogEntry =
                    serde_json::from_slice(&bytes).map_err(anyhow::Error::msg)?;
                caller
                    .data()
                    .capabilities
                    .log(entry)
                    .map_err(|err| anyhow::Error::msg(err.message))?;
                Ok(0)
            },
        )
        .map_err(|err| SkillExecutionError::new(SkillFailureKind::Internal, err.to_string()))?;

    linker
        .func_wrap(
            "env",
            "host_kv_get",
            |mut caller: Caller<'_, StoreState>,
             ptr: i32,
             len: i32|
             -> Result<i64, anyhow::Error> {
                let bytes = read_guest_bytes(&mut caller, ptr, len).map_err(anyhow::Error::msg)?;
                let request: KvCapabilityRequest =
                    serde_json::from_slice(&bytes).map_err(anyhow::Error::msg)?;
                let response_bytes = match ensure_capability(
                    &caller.data().manifest,
                    CapabilityKind::Kv(&request.namespace),
                ) {
                    Ok(()) => {
                        let response = caller
                            .data()
                            .capabilities
                            .kv_get(request)
                            .map_err(|err| anyhow::Error::msg(err.message))?;
                        serde_json::to_vec(&response).map_err(anyhow::Error::msg)?
                    }
                    Err(err) => {
                        serialize_error_response(err.message).map_err(anyhow::Error::msg)?
                    }
                };
                let (ptr, len) =
                    write_guest_bytes(&mut caller, &response_bytes).map_err(anyhow::Error::msg)?;
                Ok(pack_ptr_len(ptr, len))
            },
        )
        .map_err(|err| SkillExecutionError::new(SkillFailureKind::Internal, err.to_string()))?;

    linker
        .func_wrap(
            "env",
            "host_http_fetch",
            |mut caller: Caller<'_, StoreState>,
             ptr: i32,
             len: i32|
             -> Result<i64, anyhow::Error> {
                let bytes = read_guest_bytes(&mut caller, ptr, len).map_err(anyhow::Error::msg)?;
                let request: HttpCapabilityRequest =
                    serde_json::from_slice(&bytes).map_err(anyhow::Error::msg)?;
                let response_bytes = match ensure_capability(
                    &caller.data().manifest,
                    CapabilityKind::Http(&request.url),
                ) {
                    Ok(()) => {
                        let response = caller
                            .data()
                            .capabilities
                            .http_fetch(request)
                            .map_err(|err| anyhow::Error::msg(err.message))?;
                        serde_json::to_vec(&response).map_err(anyhow::Error::msg)?
                    }
                    Err(err) => {
                        serialize_error_response(err.message).map_err(anyhow::Error::msg)?
                    }
                };
                let (ptr, len) =
                    write_guest_bytes(&mut caller, &response_bytes).map_err(anyhow::Error::msg)?;
                Ok(pack_ptr_len(ptr, len))
            },
        )
        .map_err(|err| SkillExecutionError::new(SkillFailureKind::Internal, err.to_string()))?;

    Ok(())
}

enum CapabilityKind<'a> {
    Log,
    Kv(&'a str),
    Http(&'a str),
}

fn ensure_capability(
    manifest: &SkillManifest,
    requested: CapabilityKind<'_>,
) -> Result<(), CapabilityError> {
    match requested {
        CapabilityKind::Log => manifest
            .capability_grants
            .iter()
            .any(|capability| matches!(capability, SkillCapability::StructuredLog))
            .then_some(())
            .ok_or_else(|| CapabilityError::denied("structured log capability not granted")),
        CapabilityKind::Kv(namespace) => manifest
            .capability_grants
            .iter()
            .find_map(|capability| match capability {
                SkillCapability::KeyValueRead { namespaces }
                    if namespaces.iter().any(|allowed| allowed == namespace) =>
                {
                    Some(())
                }
                _ => None,
            })
            .ok_or_else(|| {
                CapabilityError::denied(format!(
                    "key/value capability not granted for namespace `{namespace}`"
                ))
            }),
        CapabilityKind::Http(url) => {
            let parsed = Url::parse(url).map_err(|err| CapabilityError {
                kind: SkillFailureKind::InvalidResponse,
                message: err.to_string(),
            })?;
            let host = parsed.host_str().unwrap_or_default();
            manifest
                .capability_grants
                .iter()
                .find_map(|capability| match capability {
                    SkillCapability::HttpOutbound { allow_hosts }
                        if allow_hosts.iter().any(|allowed| allowed == host) =>
                    {
                        Some(())
                    }
                    _ => None,
                })
                .ok_or_else(|| {
                    CapabilityError::denied(format!(
                        "http capability not granted for host `{host}`"
                    ))
                })
        }
    }
}

fn pack_ptr_len(ptr: i32, len: i32) -> i64 {
    ((len as i64) << 32) | (ptr as u32 as i64)
}

fn serialize_error_response(message: String) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&WasmSkillResponse {
        ok: false,
        value: Value::Null,
        error: Some(message),
    })
}

fn read_guest_bytes(
    caller: &mut Caller<'_, StoreState>,
    ptr: i32,
    len: i32,
) -> Result<Vec<u8>, SkillExecutionError> {
    let memory = match caller.get_export("memory") {
        Some(Extern::Memory(memory)) => memory,
        _ => {
            return Err(SkillExecutionError::new(
                SkillFailureKind::Internal,
                "wasm module must export memory",
            ));
        }
    };
    let mut output = vec![0u8; len as usize];
    memory
        .read(caller.as_context_mut(), ptr as usize, &mut output)
        .map_err(|err| classify_memory(err.to_string()))?;
    Ok(output)
}

fn write_guest_bytes(
    caller: &mut Caller<'_, StoreState>,
    bytes: &[u8],
) -> Result<(i32, i32), SkillExecutionError> {
    let alloc = caller
        .get_export("alloc")
        .and_then(|export| export.into_func())
        .ok_or_else(|| {
            SkillExecutionError::new(SkillFailureKind::Internal, "wasm module must export alloc")
        })?;
    let alloc = alloc
        .typed::<i32, i32>(caller.as_context_mut())
        .map_err(|err| SkillExecutionError::new(SkillFailureKind::Internal, err.to_string()))?;
    let memory = match caller.get_export("memory") {
        Some(Extern::Memory(memory)) => memory,
        _ => {
            return Err(SkillExecutionError::new(
                SkillFailureKind::Internal,
                "wasm module must export memory",
            ));
        }
    };
    let len = i32::try_from(bytes.len()).unwrap_or(i32::MAX);
    let ptr = alloc
        .call(caller.as_context_mut(), len)
        .map_err(|err| classify_trap(err.to_string()))?;
    memory
        .write(caller.as_context_mut(), ptr as usize, bytes)
        .map_err(|err| classify_memory(err.to_string()))?;
    Ok((ptr, len))
}

fn extract_memory(
    store: &mut Store<StoreState>,
    instance: &Instance,
) -> Result<Memory, SkillExecutionError> {
    match instance.get_export(store.as_context_mut(), "memory") {
        Some(Extern::Memory(memory)) => Ok(memory),
        _ => Err(SkillExecutionError::new(
            SkillFailureKind::Internal,
            "wasm module must export memory",
        )),
    }
}

fn classify_memory(message: String) -> SkillExecutionError {
    SkillExecutionError::new(SkillFailureKind::MemoryLimitExceeded, message)
}

fn classify_trap(message: String) -> SkillExecutionError {
    let kind = if message.contains("all fuel consumed") {
        SkillFailureKind::Timeout
    } else if message.contains("out of bounds memory access")
        || message.contains("memory")
        || message.contains("limit")
    {
        SkillFailureKind::MemoryLimitExceeded
    } else if message.contains("capability") {
        SkillFailureKind::CapabilityDenied
    } else {
        SkillFailureKind::Trap
    };
    SkillExecutionError::new(kind, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, routing::get};
    use rain_engine_core::{AgentContextSnapshot, AgentId, AgentStateSnapshot, ResourcePolicy};
    use serde_json::json;

    fn manifest(timeout_ms: u64, max_memory_bytes: usize) -> SkillManifest {
        SkillManifest {
            name: "echo".to_string(),
            description: "echo".to_string(),
            input_schema: json!({"type": "object"}),
            required_scopes: vec!["tool:run".to_string()],
            capability_grants: vec![SkillCapability::StructuredLog],
            resource_policy: ResourcePolicy {
                timeout_ms,
                max_memory_bytes,
                max_fuel: Some(1_000_000),
                priority_class: 0,
                max_retries: 0,
                retry_backoff_ms: 250,
                dry_run_supported: false,
            },
            approval_required: false,
        }
    }

    fn invocation(manifest: SkillManifest) -> SkillInvocation {
        SkillInvocation {
            call_id: "call-1".to_string(),
            manifest,
            args: json!({"value": 1}),
            dry_run: false,
            context: AgentContextSnapshot {
                session_id: "session".to_string(),
                granted_scopes: vec!["tool:run".to_string()],
                trigger_id: "trigger".to_string(),
                idempotency_key: None,
                current_step: 0,
                max_steps: 4,
                history: Vec::new(),
                prior_tool_results: Vec::new(),
                session_cost_usd: 0.0,
                state: AgentStateSnapshot {
                    agent_id: AgentId("session".to_string()),
                    profile: None,
                    goals: Vec::new(),
                    tasks: Vec::new(),
                    observations: Vec::new(),
                    artifacts: Vec::new(),
                    resources: Vec::new(),
                    relationships: Vec::new(),
                    pending_wake: None,
                },
            },
        }
    }

    #[tokio::test]
    async fn executes_successful_wasm_skill() {
        let module = wat::parse_str(
            r#"
            (module
              (memory (export "memory") 1)
              (global $heap (mut i32) (i32.const 4096))
              (func (export "alloc") (param $len i32) (result i32)
                (local $ptr i32)
                global.get $heap
                local.set $ptr
                global.get $heap
                local.get $len
                i32.add
                global.set $heap
                local.get $ptr)
              (func (export "dealloc") (param i32 i32))
              (data (i32.const 0) "{\"ok\":true,\"value\":{\"status\":\"ok\"},\"error\":null}")
              (func (export "run") (param i32 i32) (result i64)
                i64.const 206158430208)
            )
            "#,
        )
        .expect("wat");

        let manifest = manifest(1_000, 65_536);
        let executor = WasmSkillExecutor::new(WasmSkillConfig {
            manifest: manifest.clone(),
            wasm_bytes: Arc::new(module),
            capabilities: Arc::new(NoopCapabilityHost),
        })
        .expect("executor");

        let output = executor.execute(invocation(manifest)).await.expect("value");

        assert_eq!(output, json!({"status": "ok"}));
    }

    #[tokio::test]
    async fn allowed_http_capability_only_reaches_allowlisted_hosts() {
        let app = Router::new().route("/ok", get(|| async { Json(json!({"ok": true})) }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("server");
        });

        let request_json = format!(
            "{{\"url\":\"http://{}/ok\",\"method\":\"GET\",\"headers\":{{}},\"body\":null}}",
            addr
        );
        let wat_request_json = request_json.replace('\\', "\\\\").replace('"', "\\\"");
        let module = wat::parse_str(format!(
            r#"
            (module
              (import "env" "host_http_fetch" (func $host_http_fetch (param i32 i32) (result i64)))
              (memory (export "memory") 1)
              (global $heap (mut i32) (i32.const 4096))
              (func (export "alloc") (param $len i32) (result i32)
                (local $ptr i32)
                global.get $heap
                local.set $ptr
                global.get $heap
                local.get $len
                i32.add
                global.set $heap
                local.get $ptr)
              (func (export "dealloc") (param i32 i32))
              (data (i32.const 0) "{wat_request_json}")
              (func (export "run") (param i32 i32) (result i64)
                i32.const 0
                i32.const {len}
                call $host_http_fetch)
            )
            "#,
            len = request_json.len()
        ))
        .expect("wat");

        let allowed_manifest = SkillManifest {
            capability_grants: vec![
                SkillCapability::HttpOutbound {
                    allow_hosts: vec!["127.0.0.1".to_string()],
                },
                SkillCapability::StructuredLog,
            ],
            ..manifest(1_000, 65_536)
        };
        let executor = WasmSkillExecutor::new(WasmSkillConfig {
            manifest: allowed_manifest.clone(),
            wasm_bytes: Arc::new(module.clone()),
            capabilities: Arc::new(InMemoryCapabilityHost::default().with_http_client()),
        })
        .expect("executor");
        let output = executor
            .execute(invocation(allowed_manifest))
            .await
            .expect("http output");
        assert_eq!(output["status"], json!(200));

        let denied_manifest = SkillManifest {
            capability_grants: vec![SkillCapability::StructuredLog],
            ..manifest(1_000, 65_536)
        };
        let denied = WasmSkillExecutor::new(WasmSkillConfig {
            manifest: denied_manifest.clone(),
            wasm_bytes: Arc::new(module),
            capabilities: Arc::new(InMemoryCapabilityHost::default().with_http_client()),
        })
        .expect("executor");
        let error = denied
            .execute(invocation(denied_manifest))
            .await
            .expect_err("capability denied");
        assert_eq!(error.kind, SkillFailureKind::CapabilityDenied);
    }

    #[tokio::test]
    async fn wasm_traps_are_contained() {
        let module = wat::parse_str(
            r#"
            (module
              (memory (export "memory") 1)
              (func (export "alloc") (param i32) (result i32) (i32.const 0))
              (func (export "dealloc") (param i32 i32))
              (func (export "run") (param i32 i32) (result i64)
                unreachable)
            )
            "#,
        )
        .expect("wat");

        let manifest = manifest(1_000, 65_536);
        let executor = WasmSkillExecutor::new(WasmSkillConfig {
            manifest: manifest.clone(),
            wasm_bytes: Arc::new(module),
            capabilities: Arc::new(NoopCapabilityHost),
        })
        .expect("executor");

        let err = executor
            .execute(invocation(manifest))
            .await
            .expect_err("trap");

        assert_eq!(err.kind, SkillFailureKind::Trap);
    }

    #[tokio::test]
    async fn wasm_timeout_is_reported() {
        let module = wat::parse_str(
            r#"
            (module
              (memory (export "memory") 1)
              (func (export "alloc") (param i32) (result i32) (i32.const 0))
              (func (export "dealloc") (param i32 i32))
              (func (export "run") (param i32 i32) (result i64)
                (loop br 0)
                i64.const 0)
            )
            "#,
        )
        .expect("wat");

        let manifest = manifest(10, 65_536);
        let executor = WasmSkillExecutor::new(WasmSkillConfig {
            manifest: manifest.clone(),
            wasm_bytes: Arc::new(module),
            capabilities: Arc::new(NoopCapabilityHost),
        })
        .expect("executor");

        let err = executor
            .execute(invocation(manifest))
            .await
            .expect_err("timeout");

        assert!(matches!(
            err.kind,
            SkillFailureKind::Timeout | SkillFailureKind::Trap
        ));
    }
}
