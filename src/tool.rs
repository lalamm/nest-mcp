use crate::duckdb::DuckDB;
use rmcp::{
    ErrorData as McpError, RoleServer, ServerHandler,
    handler::server::{router::tool::ToolRouter, tool::Parameters},
    model::*,
    schemars,
    service::RequestContext,
    tool, tool_handler, tool_router,
};

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct QueryRequest {
    pub sql: String,
}

#[derive(Clone)]
pub struct Tool {
    tool_router: ToolRouter<Tool>,
}

#[tool_router]
impl Tool {
    pub fn new() -> Self {
        Self {
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        name = "company",
        description = "Execute SQL queries against the company database.",
        annotations(title = "Companies", read_only_hint = true)
    )]
    pub async fn company(
        &self,
        Parameters(QueryRequest { sql }): Parameters<QueryRequest>,
    ) -> Result<CallToolResult, McpError> {
        let db = DuckDB::new_default().await.map_err(|e| {
            McpError::internal_error(format!("Failed to connect to database: {}", e), None)
        })?;

        let result = db.query_all_json(&sql).map_err(|e| {
            McpError::internal_error(format!("Failed to execute query: {}", e), None)
        })?;

        Ok(CallToolResult::success(vec![Content::text(result)]))
    }

    #[tool(
        name = "company_annual_report",
        description = "Execute SQL queries against the company annual report database",
        annotations(title = "Company Annual Reports", read_only_hint = true)
    )]
    pub async fn company_annual_report(
        &self,
        Parameters(QueryRequest { sql }): Parameters<QueryRequest>,
    ) -> Result<CallToolResult, McpError> {
        let db = DuckDB::new_default().await.map_err(|e| {
            McpError::internal_error(format!("Failed to connect to database: {}", e), None)
        })?;

        let result = db.query_all_json(&sql).map_err(|e| {
            McpError::internal_error(format!("Failed to execute query: {}", e), None)
        })?;

        Ok(CallToolResult::success(vec![Content::text(result)]))
    }
}

#[tool_handler]
impl ServerHandler for Tool {
    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: Implementation::from_build_env(),
            instructions: Some(
                "This server provides SQL query tools for company database access.".to_string(),
            ),
        }
    }

    async fn initialize(
        &self,
        _request: InitializeRequestParam,
        context: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, McpError> {
        if let Some(http_request_part) = context.extensions.get::<axum::http::request::Parts>() {
            let initialize_headers = &http_request_part.headers;
            let initialize_uri = &http_request_part.uri;
            tracing::info!(?initialize_headers, %initialize_uri, "initialize from http server");
        }
        Ok(self.get_info())
    }
}
