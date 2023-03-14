use std::sync::Arc;

use datafusion::execution::context::SessionState;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::{SessionConfig, SessionContext};
use models::auth::user::User;
use models::oid::Oid;
use models::schema::OptimizerRuleConfig;

use crate::service::protocol::Context;
use crate::Result;

#[derive(Clone)]
pub struct SessionCtx {
    // todo
    // ...
    user: User,

    tenant_id: Oid,
    tenant: String,
    default_database: String,

    inner: SessionContext,
}

impl SessionCtx {
    pub fn inner(&self) -> &SessionContext {
        &self.inner
    }

    pub fn tenant_id(&self) -> &Oid {
        &self.tenant_id
    }

    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    pub fn default_database(&self) -> &str {
        &self.default_database
    }

    pub fn user(&self) -> &User {
        &self.user
    }
}

#[derive(Default)]
pub struct SessionCtxFactory {}

impl SessionCtxFactory {
    pub fn create_session_ctx(
        &self,
        context: Context,
        tenant_id: Oid,
        memory_pool: Arc<dyn MemoryPool>,
    ) -> Result<SessionCtx> {
        let ctx = context.session_config().to_owned();
        let mut rt_config = RuntimeConfig::new();
        rt_config.memory_pool = Some(memory_pool);
        let rt = RuntimeEnv::new(rt_config)?;
        let df_session_state = SessionState::with_config_rt(ctx.inner, Arc::new(rt));
        let df_session_ctx = SessionContext::with_state(df_session_state);

        Ok(SessionCtx {
            user: context.user_info().to_owned(),
            tenant_id,
            tenant: context.tenant().to_owned(),
            default_database: context.database().to_owned(),
            inner: df_session_ctx,
        })
    }
}

#[derive(Clone)]
pub struct CnosSessionConfig {
    inner: SessionConfig,
}

impl Default for CnosSessionConfig {
    fn default() -> Self {
        let inner =
            SessionConfig::default().set_bool("datafusion.optimizer.skip_failed_rules", false);

        Self { inner }
    }
}

impl CnosSessionConfig {
    pub fn to_df_config(&self) -> &SessionConfig {
        &self.inner
    }

    /// Customize target_partitions
    /// partition count must be greater than zero
    pub fn with_target_partitions(mut self, n: usize) -> Self {
        self.inner = self.inner.with_target_partitions(n);
        self
    }

    pub fn with_precision(mut self, precision: String) -> Self {
        let mut extension = OptimizerRuleConfig::default();
        extension.input_precision = precision;
        self.inner.config_options_mut().extensions.insert(extension);
        self
    }
}
