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

    #[schemars(
        description = "Company purpose text to search for (full text search, case-insensitive)",
        example = "\"byggverksamhet\""
    )]
    pub company_purpose: Option<String>,

    #[schemars(
        description = "Revenue range as [min_revenue, max_revenue] tuple in SEK (both inclusive)",
        example = "[1000000, 10000000]"
    )]
    pub revenue_range: Option<(f64, f64)>,

    #[schemars(
        description = "Employee number range as [min_employees, max_employees] tuple (both inclusive)",
        example = "[10, 100]"
    )]
    pub employee_range: Option<(f64, f64)>,
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

        'hello_nest' table (
            company_id BIGINT,
            company_name VARCHAR,
            organization_number BIGINT,
            company_type VARCHAR,
            company_purpose VARCHAR,
            established_date DATE,
            foundation_year BIGINT,
            registered_for_payroll_tax BOOLEAN,
            homepage VARCHAR,
            postal_address VARCHAR,
            visitor_address VARCHAR,
            nace_categories VARCHAR[],
            location STRUCT(
                county VARCHAR,
                coordinates STRUCT(
                    XCoordinate DOUBLE,
                    YCoordinate DOUBLE,
                    coordinateSystem VARCHAR
                ),
                countryPart VARCHAR,
                municipality VARCHAR
            ),

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

            'hello_nest' (
                company_id BIGINT,
                company_name VARCHAR,
                organization_number BIGINT,
                company_type VARCHAR,
                company_purpose VARCHAR,
                established_date DATE,
                foundation_year BIGINT,
                registered_for_payroll_tax BOOLEAN,
                homepage VARCHAR,
                postal_address VARCHAR,
                visitor_address VARCHAR,
                nace_categories VARCHAR[],
                location STRUCT(
                    county VARCHAR,
                    coordinates STRUCT(
                        XCoordinate DOUBLE,
                        YCoordinate DOUBLE,
                        coordinateSystem VARCHAR
                    ),
                    countryPart VARCHAR,
                    municipality VARCHAR
                ),

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
        // All filters are now optional - if none provided, return all companies (limited)
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
    let mut sql = "SELECT * FROM hello_nest WHERE 1=1".to_string();
    let mut conditions = Vec::new();
    let mut has_text_search = false;

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
            conditions.push(format!("company_name ILIKE '%{}%'", trimmed_name));
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
                    // Use array functions for searching in nace_categories array
                    category_conditions
                        .push(format!("'{}' = ANY(nace_categories)", trimmed_category));
                }
            }

            if !category_conditions.is_empty() {
                conditions.push(format!("({})", category_conditions.join(" OR ")));
            }
        }
    }

    if let Some(company_purpose) = &search_request.company_purpose {
        let trimmed_purpose = company_purpose.trim();
        if !trimmed_purpose.is_empty() {
            // Basic SQL injection protection
            if trimmed_purpose.contains("'")
                || trimmed_purpose.contains(";")
                || trimmed_purpose.contains("--")
            {
                return Err(McpError::invalid_params(
                    "Invalid characters in company purpose".to_string(),
                    None,
                ));
            }
            // Use DuckDB's full-text search with BM25 ranking for better performance
            // The FTS index was created with company_id as the unique key and company_purpose as the text column
            conditions.push(format!(
                "fts_main_hello_nest.match_bm25(company_id, '{}') IS NOT NULL",
                trimmed_purpose
            ));
            has_text_search = true;
        }
    }

    if let Some((min_revenue, max_revenue)) = search_request.revenue_range {
        if min_revenue > max_revenue {
            return Err(McpError::invalid_params(
                "Minimum revenue cannot be greater than maximum revenue".to_string(),
                None,
            ));
        }
        if min_revenue < 0.0 || max_revenue < 0.0 {
            return Err(McpError::invalid_params(
                "Revenue values must be non-negative".to_string(),
                None,
            ));
        }
        // Search in financial_data for Sales revenues across all years
        // Use STRUCT access for better performance than JSON functions
        conditions.push(format!(
            "EXISTS (SELECT 1 FROM (VALUES
                (financial_data['2016']['Sales revenues']),
                (financial_data['2017']['Sales revenues']),
                (financial_data['2018']['Sales revenues']),
                (financial_data['2019']['Sales revenues']),
                (financial_data['2020']['Sales revenues']),
                (financial_data['2021']['Sales revenues']),
                (financial_data['2022']['Sales revenues']),
                (financial_data['2023']['Sales revenues']),
                (financial_data['2024']['Sales revenues'])
            ) AS revenue_data(revenue)
            WHERE revenue IS NOT NULL AND revenue BETWEEN {} AND {})",
            min_revenue, max_revenue
        ));
    }

    if let Some((min_employees, max_employees)) = search_request.employee_range {
        if min_employees > max_employees {
            return Err(McpError::invalid_params(
                "Minimum employee count cannot be greater than maximum employee count".to_string(),
                None,
            ));
        }
        if min_employees < 0.0 || max_employees < 0.0 {
            return Err(McpError::invalid_params(
                "Employee count values must be non-negative".to_string(),
                None,
            ));
        }
        // Search in financial_data for Employees from accounting across all years
        // Use STRUCT access for better performance than JSON functions
        conditions.push(format!(
            "EXISTS (SELECT 1 FROM (VALUES
                (financial_data['2016']['Employees from accounting']),
                (financial_data['2017']['Employees from accounting']),
                (financial_data['2018']['Employees from accounting']),
                (financial_data['2019']['Employees from accounting']),
                (financial_data['2020']['Employees from accounting']),
                (financial_data['2021']['Employees from accounting']),
                (financial_data['2022']['Employees from accounting']),
                (financial_data['2023']['Employees from accounting']),
                (financial_data['2024']['Employees from accounting'])
            ) AS employee_data(employees)
            WHERE employees IS NOT NULL AND employees BETWEEN {} AND {})",
            min_employees, max_employees
        ));
    }

    if !conditions.is_empty() {
        sql.push_str(" AND ");
        sql.push_str(&conditions.join(" AND "));
    }

    // Order by relevance (BM25 score) when company_purpose search is used, otherwise by company name
    if has_text_search {
        sql.push_str(" ORDER BY fts_main_hello_nest.match_bm25(company_id, '");
        if let Some(company_purpose) = &search_request.company_purpose {
            sql.push_str(&company_purpose.trim().replace("'", "''")); // Escape single quotes
        }
        sql.push_str("') DESC, company_name LIMIT 1000");
    } else {
        sql.push_str(" ORDER BY company_name LIMIT 1000");
    }

    Ok(sql)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_company_search_query_basic() {
        let search_request = SearchRequest {
            company_name: Some("Test Company".to_string()),
            foundation_year: Some((2020, 2023)),
            nace_categories: None,
            company_purpose: None,
            revenue_range: None,
            employee_range: None,
        };

        let query = build_company_search_query(&search_request).unwrap();

        assert!(query.contains("company_name ILIKE '%Test Company%'"));
        assert!(query.contains("foundation_year BETWEEN 2020 AND 2023"));
        assert!(query.contains("ORDER BY company_name LIMIT 1000"));
    }

    #[test]
    fn test_build_company_search_query_nace_array() {
        let search_request = SearchRequest {
            company_name: None,
            foundation_year: None,
            nace_categories: Some(vec!["62010".to_string(), "62020".to_string()]),
            company_purpose: None,
            revenue_range: None,
            employee_range: None,
        };

        let query = build_company_search_query(&search_request).unwrap();

        // Should use ANY() syntax for VARCHAR[] array search
        assert!(query.contains("'62010' = ANY(nace_categories)"));
        assert!(query.contains("'62020' = ANY(nace_categories)"));
        assert!(query.contains(" OR "));
    }

    #[test]
    fn test_build_company_search_query_revenue_struct_access() {
        let search_request = SearchRequest {
            company_name: None,
            foundation_year: None,
            nace_categories: None,
            company_purpose: None,
            revenue_range: Some((1000000.0, 5000000.0)),
            employee_range: None,
        };

        let query = build_company_search_query(&search_request).unwrap();

        // Should use proper STRUCT access for financial_data
        assert!(query.contains("financial_data['2016']['Sales revenues']"));
        assert!(query.contains("financial_data['2024']['Sales revenues']"));
        assert!(query.contains("revenue BETWEEN 1000000 AND 5000000"));
    }

    #[test]
    fn test_build_company_search_query_employee_struct_access() {
        let search_request = SearchRequest {
            company_name: None,
            foundation_year: None,
            nace_categories: None,
            company_purpose: None,
            revenue_range: None,
            employee_range: Some((10.0, 100.0)),
        };

        let query = build_company_search_query(&search_request).unwrap();

        // Should use proper STRUCT access for employee data
        assert!(query.contains("financial_data['2016']['Employees from accounting']"));
        assert!(query.contains("financial_data['2024']['Employees from accounting']"));
        assert!(query.contains("employees BETWEEN 10 AND 100"));
    }

    #[test]
    fn test_sql_injection_protection_company_name() {
        let search_request = SearchRequest {
            company_name: Some("'; DROP TABLE hello_nest; --".to_string()),
            foundation_year: None,
            nace_categories: None,
            company_purpose: None,
            revenue_range: None,
            employee_range: None,
        };

        let result = build_company_search_query(&search_request);
        assert!(result.is_err());
    }

    #[test]
    fn test_sql_injection_protection_nace_categories() {
        let search_request = SearchRequest {
            company_name: None,
            foundation_year: None,
            nace_categories: Some(vec!["'; DELETE FROM hello_nest; --".to_string()]),
            company_purpose: None,
            revenue_range: None,
            employee_range: None,
        };

        let result = build_company_search_query(&search_request);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_search_parameters() {
        let search_request = SearchRequest {
            company_name: None,
            foundation_year: None,
            nace_categories: None,
            company_purpose: None,
            revenue_range: None,
            employee_range: None,
        };

        let query = build_company_search_query(&search_request).unwrap();

        // Should return all companies with basic ordering
        assert!(query.contains("SELECT * FROM hello_nest WHERE 1=1"));
        assert!(query.contains("ORDER BY company_name LIMIT 1000"));
        assert!(!query.contains(" AND ")); // No additional conditions
    }

    #[test]
    fn test_schema_types_in_queries() {
        // Test that queries use proper types for the schema
        let search_request = SearchRequest {
            company_name: Some("AB".to_string()),
            foundation_year: Some((2020, 2024)),
            nace_categories: Some(vec!["62010".to_string()]),
            company_purpose: None,
            revenue_range: Some((1000000.0, 10000000.0)),
            employee_range: Some((10.0, 100.0)),
        };

        let query = build_company_search_query(&search_request).unwrap();

        // Check that it properly handles:
        // - VARCHAR[] for nace_categories with ANY()
        // - STRUCT access for financial_data
        // - DATE type for established_date (implicitly tested by foundation_year)
        assert!(query.contains("'62010' = ANY(nace_categories)"));
        assert!(query.contains("financial_data['2024']['Sales revenues']"));
        assert!(query.contains("financial_data['2024']['Employees from accounting']"));
        assert!(query.contains("foundation_year BETWEEN 2020 AND 2024"));
    }

    // Integration tests that require the actual database
    #[tokio::test]
    #[ignore] // Run with: cargo test -- --ignored
    async fn integration_test_company_sql_schema_validation() {
        use crate::duckdb::{DuckDB, DuckDbConfig};
        use rmcp::handler::server::tool::Parameters;

        let mut config = DuckDbConfig::default();
        config.access_mode = duck::AccessMode::ReadOnly;
        let _db = DuckDB::new(config).await.expect("Database connection");

        let tool = Tool::new();

        // Test DATE type for established_date
        let query_request = Parameters(QueryRequest {
            sql: "SELECT company_name, established_date FROM hello_nest WHERE established_date > DATE '2020-01-01' LIMIT 1".to_string(),
        });
        let result = tool.company(query_request).await;
        assert!(result.is_ok(), "DATE query should work");

        // Test VARCHAR[] type for nace_categories
        let query_request = Parameters(QueryRequest {
            sql: "SELECT company_name, array_length(nace_categories) FROM hello_nest WHERE nace_categories IS NOT NULL LIMIT 1".to_string(),
        });
        let result = tool.company(query_request).await;
        assert!(result.is_ok(), "VARCHAR[] query should work");

        // Test STRUCT type for location
        let query_request = Parameters(QueryRequest {
            sql: "SELECT company_name, location.county, location.coordinates.XCoordinate FROM hello_nest WHERE location IS NOT NULL LIMIT 1".to_string(),
        });
        let result = tool.company(query_request).await;
        assert!(result.is_ok(), "STRUCT query should work");
    }

    #[tokio::test]
    #[ignore] // Run with: cargo test -- --ignored
    async fn integration_test_company_search_with_real_data() {
        use crate::duckdb::{DuckDB, DuckDbConfig};
        use rmcp::handler::server::tool::Parameters;

        let mut config = DuckDbConfig::default();
        config.access_mode = duck::AccessMode::ReadOnly;
        let _db = DuckDB::new(config).await.expect("Database connection");

        let tool = Tool::new();

        // Test search by common Swedish company suffix
        let search_request = Parameters(SearchRequest {
            company_name: Some("AB".to_string()),
            foundation_year: None,
            nace_categories: None,
            company_purpose: None,
            revenue_range: None,
            employee_range: None,
        });
        let result = tool.company_search(search_request).await;
        assert!(result.is_ok(), "Company name search should work");

        // Test search by foundation year range
        let search_request = Parameters(SearchRequest {
            company_name: None,
            foundation_year: Some((2000, 2024)),
            nace_categories: None,
            company_purpose: None,
            revenue_range: None,
            employee_range: None,
        });
        let result = tool.company_search(search_request).await;
        assert!(result.is_ok(), "Foundation year search should work");

        // Test search by NACE categories (common construction code)
        let search_request = Parameters(SearchRequest {
            company_name: None,
            foundation_year: None,
            nace_categories: Some(vec!["43".to_string()]), // Construction
            company_purpose: None,
            revenue_range: None,
            employee_range: None,
        });
        let result = tool.company_search(search_request).await;
        assert!(result.is_ok(), "NACE category search should work");

        // Test revenue range search
        let search_request = Parameters(SearchRequest {
            company_name: None,
            foundation_year: None,
            nace_categories: None,
            company_purpose: None,
            revenue_range: Some((100000.0, 50000000.0)),
            employee_range: None,
        });
        let result = tool.company_search(search_request).await;
        assert!(result.is_ok(), "Revenue range search should work");
    }

    #[tokio::test]
    #[ignore] // Run with: cargo test -- --ignored
    async fn integration_test_financial_data_structure() {
        use crate::duckdb::DuckDB;
        use rmcp::handler::server::tool::Parameters;

        let mut config = crate::duckdb::DuckDbConfig::default();
        config.access_mode = duck::AccessMode::ReadOnly;
        let _db = DuckDB::new(config).await.expect("Database connection");

        let tool = Tool::new();

        // Test accessing different years of financial data
        let years = vec!["2020", "2021", "2022", "2023", "2024"];

        for year in years {
            let query_request = Parameters(QueryRequest {
                sql: format!(
                    r#"SELECT company_name, financial_data."{}"."Sales revenues", financial_data."{}"."Total assets"
                       FROM hello_nest
                       WHERE financial_data."{}" IS NOT NULL
                       LIMIT 1"#,
                    year, year, year
                ),
            });
            let result = tool.company(query_request).await;
            assert!(
                result.is_ok(),
                "Financial data query for {} should work",
                year
            );
        }

        // Test financial evolution query (2016 vs 2024)
        let query_request = Parameters(QueryRequest {
            sql: r#"SELECT
                        company_name,
                        financial_data."2016"."Sales revenues" as revenue_2016,
                        financial_data."2024"."Sales revenues" as revenue_2024
                     FROM hello_nest
                     WHERE financial_data."2016" IS NOT NULL
                       AND financial_data."2024" IS NOT NULL
                     LIMIT 5"#
                .to_string(),
        });
        let result = tool.company(query_request).await;
        assert!(
            result.is_ok(),
            "Multi-year financial comparison should work"
        );
    }

    #[tokio::test]
    #[ignore] // Run with: cargo test -- --ignored
    async fn integration_test_complex_location_queries() {
        use crate::duckdb::DuckDB;
        use rmcp::handler::server::tool::Parameters;

        let mut config = crate::duckdb::DuckDbConfig::default();
        config.access_mode = duck::AccessMode::ReadOnly;
        let _db = DuckDB::new(config).await.expect("Database connection");

        let tool = Tool::new();

        // Test location filtering by county
        let query_request = Parameters(QueryRequest {
            sql: "SELECT company_name, location.county FROM hello_nest WHERE location.county = 'Stockholm' LIMIT 3".to_string(),
        });
        let result = tool.company(query_request).await;
        assert!(result.is_ok(), "County filtering should work");

        // Test coordinate access (companies with GPS coordinates)
        let query_request = Parameters(QueryRequest {
            sql: "SELECT company_name, location.coordinates.XCoordinate, location.coordinates.YCoordinate FROM hello_nest WHERE location.coordinates.XCoordinate IS NOT NULL LIMIT 3".to_string(),
        });
        let result = tool.company(query_request).await;
        assert!(result.is_ok(), "Coordinate access should work");

        // Test municipality grouping
        let query_request = Parameters(QueryRequest {
            sql: "SELECT location.municipality, COUNT(*) as company_count FROM hello_nest WHERE location.municipality IS NOT NULL GROUP BY location.municipality ORDER BY company_count DESC LIMIT 5".to_string(),
        });
        let result = tool.company(query_request).await;
        assert!(result.is_ok(), "Municipality grouping should work");
    }

    #[tokio::test]
    #[ignore] // Run with: cargo test -- --ignored
    async fn integration_test_error_handling() {
        use rmcp::handler::server::tool::Parameters;

        let tool = Tool::new();

        // Test malformed SQL
        let query_request = Parameters(QueryRequest {
            sql: "SELECT * FROM nonexistent_table".to_string(),
        });
        let result = tool.company(query_request).await;
        assert!(result.is_err(), "Malformed SQL should fail");

        // Test SQL injection through company_search
        let search_request = Parameters(SearchRequest {
            company_name: Some("'; DROP TABLE hello_nest; --".to_string()),
            foundation_year: None,
            nace_categories: None,
            company_purpose: None,
            revenue_range: None,
            employee_range: None,
        });
        let result = tool.company_search(search_request).await;
        assert!(result.is_err(), "SQL injection should be blocked");
    }
}
