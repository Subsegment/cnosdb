use std::sync::Arc;

use datafusion::datasource::MemTable;
use meta::{error::MetaError, MetaClientRef};
use models::{
    auth::user::User,
    oid::Identifier,
    schema::{ColumnType, ExternalTableSchema, TableSchema, TskvTableSchema},
    ValueType,
};

use crate::{
    dispatcher::query_tracker::QueryTracker,
    metadata::information_schema_provider::{
        builder::columns::InformationSchemaColumnsBuilder, InformationSchemaTableFactory,
    },
};

const INFORMATION_SCHEMA_COLUMNS: &str = "COLUMNS";

/// This view only displays the column information of tables under the database that the current user has Read permission or higher.
pub struct ColumnsFactory {}

#[async_trait::async_trait]
impl InformationSchemaTableFactory for ColumnsFactory {
    fn table_name(&self) -> &'static str {
        INFORMATION_SCHEMA_COLUMNS
    }

    async fn create(
        &self,
        user: &User,
        metadata: MetaClientRef,
        _query_tracker: Arc<QueryTracker>,
    ) -> std::result::Result<Arc<MemTable>, MetaError> {
        let mut builder = InformationSchemaColumnsBuilder::default();

        let metadata_r = metadata.read().await;
        let tenant = metadata_r.tenant();

        let dbs = metadata_r.list_databases()?;
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
                    match table.as_ref().clone() {
                        TableSchema::TsKvTableSchema(t) => {
                            append_tskv_table(tenant_name, &db, t, &mut builder);
                        }
                        TableSchema::ExternalTableSchema(t) => {
                            append_external_table(tenant_name, &db, t, &mut builder);
                        }
                    }
                }
            }
        }

        let mem_table = MemTable::try_from(builder)
            .map_err(|e| MetaError::CommonError { msg: e.to_string() })?;
        Ok(Arc::new(mem_table))
    }
}

fn append_tskv_table(
    tenant_name: &str,
    database_name: &str,
    table: TskvTableSchema,
    builder: &mut InformationSchemaColumnsBuilder,
) {
    for (idx, col) in table.columns().iter().enumerate() {
        builder.append_row(
            tenant_name,
            database_name,
            &table.name,
            &col.name,
            col.column_type.as_column_type_str(),
            idx as u64,
            "NULL",
            col.nullable(),
            col.column_type.to_sql_type_str(),
            Some(col.encoding.as_str()),
        );
    }
}

fn append_external_table(
    tenant_name: &str,
    database_name: &str,
    table: ExternalTableSchema,
    builder: &mut InformationSchemaColumnsBuilder,
) {
    for (idx, col) in table.schema.all_fields().iter().enumerate() {
        builder.append_row(
            tenant_name,
            database_name,
            &table.name,
            col.name(),
            // The fields of the external table are all type FIELD
            ColumnType::Field(ValueType::Unknown).as_column_type_str(),
            idx as u64,
            "NULL",
            col.is_nullable(),
            col.data_type().to_string(),
            None::<String>,
        );
    }
}
