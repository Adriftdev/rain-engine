use crate::traits::SkillStore;
use crate::types::SkillManifest;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

type StoredSkill = (SkillManifest, Vec<u8>);
type SkillMap = HashMap<String, StoredSkill>;

pub struct InMemorySkillStore {
    skills: Arc<RwLock<SkillMap>>,
}

impl InMemorySkillStore {
    pub fn new() -> Self {
        Self {
            skills: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl Default for InMemorySkillStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SkillStore for InMemorySkillStore {
    async fn store_skill(
        &self,
        manifest: SkillManifest,
        wasm_bytes: Vec<u8>,
    ) -> Result<(), String> {
        let mut skills = self.skills.write().await;
        skills.insert(manifest.name.clone(), (manifest, wasm_bytes));
        Ok(())
    }

    async fn list_skills(&self) -> Result<Vec<(SkillManifest, Vec<u8>)>, String> {
        let skills = self.skills.read().await;
        Ok(skills.values().cloned().collect())
    }

    async fn remove_skill(&self, name: &str) -> Result<(), String> {
        let mut skills = self.skills.write().await;
        skills.remove(name);
        Ok(())
    }
}
