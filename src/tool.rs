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

#[derive(Debug, Default, serde::Deserialize, schemars::JsonSchema)]
#[serde(default)]
#[schemars(description = "Search for Swedish companies with various filters")]
pub struct SearchRequest {
    #[schemars(
        description = "Swedish company name to search for (supports partial matching, case-insensitive)",
        example = "\"Scania\""
    )]
    pub company_name: Option<String>,

    #[schemars(
        description = "Foundation year range as [min_year, max_year] tuple (both inclusive). Use same year twice for exact year match, e.g., [2010, 2010]",
        example = "[2000, 2024]"
    )]
    pub foundation_year: Option<(i64, i64)>,

    #[schemars(
        description = "Swedish NACE industry categories to filter by (supports partial matching). Multiple categories can be provided to match any of them.",
        example = "[\"43320 Byggnadssnickeriarbeten\", \"78200 Personaluthyrning\", \"73111 Reklambyråverksamhet\"]"
    )]
    pub nace_categories: Option<Vec<String>>,
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
        name = "company-sql",
        description = r#"
        Execute SQL queries (duckdb dialect) against the company database.

        # Schema
        -- Define the complete financial metrics structure (used across all years)
        FINANCIAL_METRICS_BASE STRUCT(
            "Allocation dividends" DOUBLE,
            "Bank deposits cash etc." DOUBLE,
            "Board and CEO salaries" DOUBLE,
            "Changes in work in progress" DOUBLE,
            "Cost per employee" DOUBLE,
            "Debt ratio" DOUBLE,
            "Employees from accounting" DOUBLE,
            "Equity-to-asset ratio / solvency ratio" DOUBLE,
            "Extraordinary expenses" DOUBLE,
            "Financial expenses" DOUBLE,
            "Financial income" DOUBLE,
            "Group contribution" DOUBLE,
            "Minority interests" DOUBLE,
            "Operating margin" DOUBLE,
            "Operating result" DOUBLE,
            "Ordinary result before taxes" DOUBLE,
            "Other operating expenses" DOUBLE,
            "Other operating revenues" DOUBLE,
            "Other wages and salaries" DOUBLE,
            "Profitability (Total profitability)" DOUBLE,
            "Result before depreciation" DOUBLE,
            "Result before financial net" DOUBLE,
            "Return on equity" DOUBLE,
            "Return on total capital" DOUBLE,
            "Revenue per employee" DOUBLE,
            "Sales revenues" DOUBLE,
            "Share capital" DOUBLE,
            "Short-term liabilities to group companies (internal)" DOUBLE,
            "Tax on ordinary result" DOUBLE,
            "Total assets" DOUBLE,
            "Total current assets" DOUBLE,
            "Total equity" DOUBLE,
            "Total equity deposits" DOUBLE,
            "Total financial fixed assets" DOUBLE,
            "Total fixed assets" DOUBLE,
            "Total intangible fixed assets" DOUBLE,
            "Total inventories" DOUBLE,
            "Total liabilities and equity" DOUBLE,
            "Total long-term liabilities" DOUBLE,
            "Total operating expenses" DOUBLE,
            "Total operating revenues" DOUBLE,
            "Total provisions for liabilities and charges" DOUBLE,
            "Total receivable" DOUBLE,
            "Total short-term liabilities" DOUBLE,
            "Trade creditors" DOUBLE
        );

        'hello_nest.parquet' (
            company_id BIGINT,
            name VARCHAR,
            organization_number BIGINT,
            company_type VARCHAR,
            company_purpose VARCHAR,
            established_date VARCHAR,
            foundation_year BIGINT,
            registered_for_payroll_tax BOOLEAN,
            homepage VARCHAR,
            postal_address VARCHAR,
            visitor_address VARCHAR,
            nace_categories VARCHAR,
            location VARCHAR,

            -- Nested financial data: each year contains the same base metrics with minor type variations
            financial_data STRUCT(
                -- Years 2016-2017: Standard metrics but "Allocation dividends" is INTEGER
                "2016" STRUCT(
                    ...FINANCIAL_METRICS_BASE,  -- 44 standard financial metrics
                    -- Override: "Allocation dividends" INTEGER (instead of DOUBLE)
                    -- Missing: "Minority interests" (not present in 2016-2018)
                ),
                "2017" STRUCT(
                    ...FINANCIAL_METRICS_BASE,  -- 44 standard financial metrics
                    -- Override: "Allocation dividends" INTEGER (instead of DOUBLE)
                    -- Missing: "Minority interests" (not present in 2016-2018)
                ),

                -- Year 2018: Standard metrics but no "Minority interests" yet
                "2018" STRUCT(
                    ...FINANCIAL_METRICS_BASE,  -- 44 standard financial metrics
                    -- Missing: "Minority interests" (not present in 2016-2018)
                ),

                -- Years 2019-2020: Standard metrics but "Minority interests" is INTEGER
                "2019" STRUCT(
                    ...FINANCIAL_METRICS_BASE,  -- 44 standard financial metrics
                    -- Override: "Minority interests" INTEGER (instead of DOUBLE)
                ),
                "2020" STRUCT(
                    ...FINANCIAL_METRICS_BASE,  -- 44 standard financial metrics
                    -- Override: "Minority interests" INTEGER (instead of DOUBLE)
                ),

                -- Years 2021-2024: Fully standard structure (all DOUBLE types)
                "2021" STRUCT(
                    ...FINANCIAL_METRICS_BASE   -- 44 standard financial metrics
                ),
                "2022" STRUCT(
                    ...FINANCIAL_METRICS_BASE   -- 44 standard financial metrics
                ),
                "2023" STRUCT(
                    ...FINANCIAL_METRICS_BASE   -- 44 standard financial metrics
                ),
                "2024" STRUCT(
                    ...FINANCIAL_METRICS_BASE   -- 44 standard financial metrics
                )
            )
        );

        -- Schema Evolution Summary:
        -- • 2016-2017: 43 metrics (missing "Minority interests", "Allocation dividends" is INTEGER)
        -- • 2018:      43 metrics (missing "Minority interests", "Allocation dividends" becomes DOUBLE)
        -- • 2019-2020: 44 metrics (adds "Minority interests" as INTEGER)
        -- • 2021-2024: 44 metrics (all fields present, all DOUBLE types - fully standardized)
        "#,
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
        name = "company-search",
        description = r#"
            Search for companies in the company database.

            # Schema

            -- Define the complete financial metrics structure (used across all years)
            FINANCIAL_METRICS_BASE STRUCT(
                "Allocation dividends" DOUBLE,
                "Bank deposits cash etc." DOUBLE,
                "Board and CEO salaries" DOUBLE,
                "Changes in work in progress" DOUBLE,
                "Cost per employee" DOUBLE,
                "Debt ratio" DOUBLE,
                "Employees from accounting" DOUBLE,
                "Equity-to-asset ratio / solvency ratio" DOUBLE,
                "Extraordinary expenses" DOUBLE,
                "Financial expenses" DOUBLE,
                "Financial income" DOUBLE,
                "Group contribution" DOUBLE,
                "Minority interests" DOUBLE,
                "Operating margin" DOUBLE,
                "Operating result" DOUBLE,
                "Ordinary result before taxes" DOUBLE,
                "Other operating expenses" DOUBLE,
                "Other operating revenues" DOUBLE,
                "Other wages and salaries" DOUBLE,
                "Profitability (Total profitability)" DOUBLE,
                "Result before depreciation" DOUBLE,
                "Result before financial net" DOUBLE,
                "Return on equity" DOUBLE,
                "Return on total capital" DOUBLE,
                "Revenue per employee" DOUBLE,
                "Sales revenues" DOUBLE,
                "Share capital" DOUBLE,
                "Short-term liabilities to group companies (internal)" DOUBLE,
                "Tax on ordinary result" DOUBLE,
                "Total assets" DOUBLE,
                "Total current assets" DOUBLE,
                "Total equity" DOUBLE,
                "Total equity deposits" DOUBLE,
                "Total financial fixed assets" DOUBLE,
                "Total fixed assets" DOUBLE,
                "Total intangible fixed assets" DOUBLE,
                "Total inventories" DOUBLE,
                "Total liabilities and equity" DOUBLE,
                "Total long-term liabilities" DOUBLE,
                "Total operating expenses" DOUBLE,
                "Total operating revenues" DOUBLE,
                "Total provisions for liabilities and charges" DOUBLE,
                "Total receivable" DOUBLE,
                "Total short-term liabilities" DOUBLE,
                "Trade creditors" DOUBLE
            );

            'hello_nest.parquet' (
                company_id BIGINT,
                name VARCHAR,
                organization_number BIGINT,
                company_type VARCHAR,
                company_purpose VARCHAR,
                established_date VARCHAR,
                foundation_year BIGINT,
                registered_for_payroll_tax BOOLEAN,
                homepage VARCHAR,
                postal_address VARCHAR,
                visitor_address VARCHAR,
                nace_categories VARCHAR,
                location VARCHAR,

                -- Nested financial data: each year contains the same base metrics with minor type variations
                financial_data STRUCT(
                    -- Years 2016-2017: Standard metrics but "Allocation dividends" is INTEGER
                    "2016" STRUCT(
                        ...FINANCIAL_METRICS_BASE,  -- 44 standard financial metrics
                        -- Override: "Allocation dividends" INTEGER (instead of DOUBLE)
                        -- Missing: "Minority interests" (not present in 2016-2018)
                    ),
                    "2017" STRUCT(
                        ...FINANCIAL_METRICS_BASE,  -- 44 standard financial metrics
                        -- Override: "Allocation dividends" INTEGER (instead of DOUBLE)
                        -- Missing: "Minority interests" (not present in 2016-2018)
                    ),

                    -- Year 2018: Standard metrics but no "Minority interests" yet
                    "2018" STRUCT(
                        ...FINANCIAL_METRICS_BASE,  -- 44 standard financial metrics
                        -- Missing: "Minority interests" (not present in 2016-2018)
                    ),

                    -- Years 2019-2020: Standard metrics but "Minority interests" is INTEGER
                    "2019" STRUCT(
                        ...FINANCIAL_METRICS_BASE,  -- 44 standard financial metrics
                        -- Override: "Minority interests" INTEGER (instead of DOUBLE)
                    ),
                    "2020" STRUCT(
                        ...FINANCIAL_METRICS_BASE,  -- 44 standard financial metrics
                        -- Override: "Minority interests" INTEGER (instead of DOUBLE)
                    ),

                    -- Years 2021-2024: Fully standard structure (all DOUBLE types)
                    "2021" STRUCT(
                        ...FINANCIAL_METRICS_BASE   -- 44 standard financial metrics
                    ),
                    "2022" STRUCT(
                        ...FINANCIAL_METRICS_BASE   -- 44 standard financial metrics
                    ),
                    "2023" STRUCT(
                        ...FINANCIAL_METRICS_BASE   -- 44 standard financial metrics
                    ),
                    "2024" STRUCT(
                        ...FINANCIAL_METRICS_BASE   -- 44 standard financial metrics
                    )
                )
            );

            -- Schema Evolution Summary:
            -- • 2016-2017: 43 metrics (missing "Minority interests", "Allocation dividends" is INTEGER)
            -- • 2018:      43 metrics (missing "Minority interests", "Allocation dividends" becomes DOUBLE)
            -- • 2019-2020: 44 metrics (adds "Minority interests" as INTEGER)
            -- • 2021-2024: 44 metrics (all fields present, all DOUBLE types - fully standardized)
        "#,
        annotations(title = "Company Search", read_only_hint = true)
    )]
    pub async fn company_search(
        &self,
        Parameters(search_request): Parameters<SearchRequest>,
    ) -> Result<CallToolResult, McpError> {
        if search_request.company_name.is_none()
            && search_request.foundation_year.is_none()
            && search_request.nace_categories.is_none()
        {
            return Err(McpError::invalid_params(
                "At least one search parameter must be provided".to_string(),
                None,
            ));
        }

        let db = DuckDB::new_default().await.map_err(|e| {
            McpError::internal_error(format!("Failed to connect to database: {}", e), None)
        })?;

        let sql = build_company_search_query(&search_request)?;

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

fn build_company_search_query(search_request: &SearchRequest) -> Result<String, McpError> {
    let mut sql = "SELECT * FROM 'hello_nest.parquet' WHERE 1=1".to_string();
    let mut conditions = Vec::new();

    if let Some(company_name) = &search_request.company_name {
        let trimmed_name = company_name.trim();
        if !trimmed_name.is_empty() {
            // Basic SQL injection protection
            if trimmed_name.contains("'")
                || trimmed_name.contains(";")
                || trimmed_name.contains("--")
            {
                return Err(McpError::invalid_params(
                    "Invalid characters in company name".to_string(),
                    None,
                ));
            }
            conditions.push(format!("name ILIKE '%{}%'", trimmed_name));
        }
    }

    if let Some((min_year, max_year)) = search_request.foundation_year {
        if min_year > max_year {
            return Err(McpError::invalid_params(
                "Minimum year cannot be greater than maximum year".to_string(),
                None,
            ));
        }
        if min_year < 1800 || max_year > 2024 {
            return Err(McpError::invalid_params(
                "Years must be between 1800 and 2024".to_string(),
                None,
            ));
        }
        conditions.push(format!(
            "foundation_year BETWEEN {} AND {}",
            min_year, max_year
        ));
    }

    if let Some(nace_categories) = &search_request.nace_categories {
        if !nace_categories.is_empty() {
            let mut category_conditions = Vec::new();

            for category in nace_categories {
                let trimmed_category = category.trim();
                if !trimmed_category.is_empty() {
                    // Basic SQL injection protection
                    if trimmed_category.contains("'")
                        || trimmed_category.contains(";")
                        || trimmed_category.contains("--")
                    {
                        return Err(McpError::invalid_params(
                            "Invalid characters in NACE categories".to_string(),
                            None,
                        ));
                    }
                    category_conditions
                        .push(format!("nace_categories ILIKE '%{}%'", trimmed_category));
                }
            }

            if !category_conditions.is_empty() {
                conditions.push(format!("({})", category_conditions.join(" OR ")));
            }
        }
    }

    if !conditions.is_empty() {
        sql.push_str(" AND ");
        sql.push_str(&conditions.join(" AND "));
    }

    sql.push_str(" ORDER BY name LIMIT 1000");

    Ok(sql)
}
