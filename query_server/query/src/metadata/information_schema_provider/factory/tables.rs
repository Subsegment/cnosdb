use std::sync::Arc;

use datafusion::{datasource::MemTable, logical_expr::TableType};
use meta::{error::MetaError, MetaClientRef};
use models::{auth::user::User, oid::Identifier};

use crate::{
    dispatcher::query_tracker::QueryTracker,
    metadata::information_schema_provider::{
        builder::tables::InformationSchemaTablesBuilder, InformationSchemaTableFactory,
    },
};

const INFORMATION_SCHEMA_TABLES: &str = "TABLES";

/// This view only displays table information under the database for which the current user has Read permission or higher.
pub struct TablesFactory {}

#[async_trait::async_trait]
impl InformationSchemaTableFactory for TablesFactory {
    fn table_name(&self) -> &'static str {
        INFORMATION_SCHEMA_TABLES
    }

    async fn create(
        &self,
        user: &User,
        metadata: MetaClientRef,
        _query_tracker: Arc<QueryTracker>,
    ) -> std::result::Result<Arc<MemTable>, MetaError> {
        let mut builder = InformationSchemaTablesBuilder::default();

        let metadata_r = metadata.read().await;
        let dbs = metadata_r.list_databases()?;
        let tenant = metadata_r.tenant();
        let tenant_id = tenant.id();
        let tenant_name = tenant.name();

        for db in dbs {
            // Check if the current user has at least read permission on this db, skip if not
            if !user.can_read_database(*tenant_id, &db) {
                continue;
            }

            let tables = metadata.read().await.list_tables(&db)?;
            for table in tables {
                if let Some(table) = metadata.read().await.get_table_schema(&db, &table)? {
                    builder.append_row(
                        tenant_name,
                        &db,
                        table.name(),
                        TableType::Base,
                        table.engine_name(),
                        "TODO",
                    );
                }
            }
        }

        let mem_table = MemTable::try_from(builder)
            .map_err(|e| MetaError::CommonError { msg: e.to_string() })?;
        Ok(Arc::new(mem_table))
    }
}
