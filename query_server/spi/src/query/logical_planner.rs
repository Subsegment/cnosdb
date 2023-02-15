use crate::{catalog::MetadataError, service::protocol::QueryId};
use std::sync::Arc;
use super::{
    ast::{ExtStatement, ObjectType},
    session::IsiphoSessionCtx,
    AFFECTED_ROWS,
};
use lazy_static::lazy_static;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::type_coercion::aggregates::DATES;
use datafusion::logical_expr::type_coercion::aggregates::NUMERICS;
use datafusion::logical_expr::type_coercion::aggregates::STRINGS;
use datafusion::logical_expr::type_coercion::aggregates::TIMES;
use datafusion::logical_expr::type_coercion::aggregates::TIMESTAMPS;
use datafusion::logical_expr::ReturnTypeFunction;
use datafusion::logical_expr::ScalarUDF;
use datafusion::logical_expr::Signature;
use datafusion::logical_expr::Volatility;
use datafusion::physical_plan::functions::make_scalar_function;
use datafusion::{
    error::DataFusionError,
    logical_expr::expr::AggregateFunction as AggregateFunctionExpr,
    logical_expr::{AggregateFunction, CreateExternalTable, LogicalPlan as DFPlan},
    prelude::{col, Expr},
};
use models::schema::DatabaseOptions;
use models::{define_result, schema::TableColumn};
use snafu::Snafu;

define_result!(LogicalPlannerError);

pub const MISSING_COLUMN: &str = "Insert column name does not exist in target table: ";
pub const DUPLICATE_COLUMN_NAME: &str = "Insert column name is specified more than once: ";
pub const MISMATCHED_COLUMNS: &str = "Insert columns and Source columns not match";

lazy_static! {
    static ref TABLE_WRITE_UDF: Arc<ScalarUDF> = Arc::new(ScalarUDF::new(
        "not_exec_only_prevent_col_prune",
        &Signature::variadic(
            STRINGS
                .iter()
                .chain(NUMERICS)
                .chain(TIMESTAMPS)
                .chain(DATES)
                .chain(TIMES)
                .cloned()
                .collect::<Vec<_>>(),
            Volatility::Immutable
        ),
        &(Arc::new(move |_: &[DataType]| Ok(Arc::new(DataType::UInt64))) as ReturnTypeFunction),
        &make_scalar_function(|args: &[ArrayRef]| Ok(Arc::clone(&args[0]))),
    ));
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum LogicalPlannerError {
    #[snafu(display("External err: {}", source))]
    External { source: DataFusionError },

    #[snafu(display("Semantic err: {}", err))]
    Semantic { err: String },

    #[snafu(display("Metadata err: {}", source))]
    Metadata { source: MetadataError },

    #[snafu(display("This feature is not implemented: {}", err))]
    NotImplemented { err: String },
}

#[derive(Clone)]
pub enum Plan {
    /// Query plan
    Query(QueryPlan),
    /// Query plan
    DDL(DDLPlan),
    /// Query plan
    SYSTEM(SYSPlan),
}

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub df_plan: DFPlan,
}

#[derive(Clone)]
pub enum DDLPlan {
    Drop(DropPlan),
    /// Create external table. such as parquet\csv...
    CreateExternalTable(CreateExternalTable),

    CreateTable(CreateTable),

    CreateDatabase(CreateDatabase),

    DescribeTable(DescribeTable),

    DescribeDatabase(DescribeDatabase),

    ShowTables(Option<String>),

    ShowDatabases(),

    AlterDatabase(AlterDatabase),

    AlterTable(AlterTable),
}

#[derive(Debug, Clone)]
pub enum SYSPlan {
    ShowQueries,
    KillQuery(QueryId),
}

#[derive(Debug, Clone)]
pub struct DropPlan {
    /// Table name
    pub object_name: String,
    /// If exists
    pub if_exist: bool,
    ///ObjectType
    pub obj_type: ObjectType,
}

// #[derive(Debug, Clone)]
// pub struct CreateExternalTable {
//     /// The table schema
//     pub schema: DFSchemaRef,
//     /// The table name
//     pub name: String,
//     /// The physical location
//     pub location: String,
//     /// The file type of physical file
//     pub file_descriptor: FileDescriptor,
//     /// Partition Columns
//     pub table_partition_cols: Vec<String>,
//     /// Option to not error if table already exists
//     pub if_not_exists: bool,
// }

// #[derive(Debug, Clone, Copy, PartialEq, Eq)]
// pub enum FileDescriptor {
//     /// Newline-delimited JSON
//     NdJson,
//     /// Apache Parquet columnar storage
//     Parquet,
//     /// Comma separated values
//     CSV(CSVOptions),
//     /// Avro binary records
//     Avro,
// }

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CSVOptions {
    /// Whether the CSV file contains a header
    pub has_header: bool,
    /// Delimiter for CSV
    pub delimiter: char,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTable {
    /// The table schema
    pub schema: Vec<TableColumn>,
    /// The table name
    pub name: String,
    /// Option to not error if table already exists
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateDatabase {
    pub name: String,

    pub if_not_exists: bool,

    pub options: DatabaseOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeDatabase {
    pub database_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeTable {
    pub table_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowTables {
    pub database_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterDatabase {
    pub database_name: String,
    pub database_options: DatabaseOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterTable {
    pub table_name: String,
    pub alter_action: AlterTableAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterTableAction {
    AddColumn {
        table_column: TableColumn,
    },
    AlterColumn {
        column_name: String,
        new_column: TableColumn,
    },
    DropColumn {
        column_name: String,
    },
}

pub trait LogicalPlanner {
    fn create_logical_plan(
        &self,
        statement: ExtStatement,
        session: &IsiphoSessionCtx,
    ) -> Result<Plan>;
}

/// Additional output information
pub fn affected_row_expr(args: Vec<Expr>) -> Expr {
    Expr::ScalarUDF {
        fun: TABLE_WRITE_UDF.clone(),
        args,
    }
        .alias(AFFECTED_ROWS.0)
}

pub fn merge_affected_row_expr() -> Expr {
    // lit(ScalarValue::Null).alias("COUNT")
    Expr::AggregateFunction(AggregateFunctionExpr {
        fun: AggregateFunction::Sum,
        args: vec![col(AFFECTED_ROWS.0)],
        distinct: false,
        filter: None,
    })
        .alias(AFFECTED_ROWS.0)
}
