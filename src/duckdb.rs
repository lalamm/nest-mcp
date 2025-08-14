use anyhow::{Context, Result};
use duck::{AccessMode, Config, Connection};
use serde_json::Value;
use std::{env, path::PathBuf, time::Duration};

#[derive(Debug)]
pub struct DuckDbConfig {
    pub db_filename: String,
    pub http_timeout: Duration,
    pub http_keep_alive: bool,
    pub http_retries: u32,
    pub s3_uploader_thread_limit: u32,
    pub temp_directory: PathBuf,
    pub max_temp_directory_size: String,
    pub access_mode: AccessMode,
}

impl Default for DuckDbConfig {
    fn default() -> Self {
        Self {
            db_filename: "nest_mcp.db".into(),
            http_timeout: Duration::from_secs(15 * 60),
            http_keep_alive: true,
            http_retries: 3,
            s3_uploader_thread_limit: 64,
            temp_directory: std::env::current_dir().unwrap_or_else(|_| env::temp_dir()),
            max_temp_directory_size: "10 GB".into(),
            access_mode: AccessMode::Automatic,
        }
    }
}

pub struct DuckDB {
    conn: Connection,
}

impl DuckDB {
    pub async fn new(config: DuckDbConfig) -> Result<Self> {
        let db_path = config.temp_directory.join(&config.db_filename);
        let duck_config = Config::default().access_mode(config.access_mode)?;
        let conn = Connection::open_with_flags(db_path, duck_config)?;

        conn.pragma_update(
            None,
            "http_timeout",
            &(config.http_timeout.as_millis() as i64).to_string(),
        )?;
        conn.pragma_update(None, "http_keep_alive", &config.http_keep_alive.to_string())?;
        conn.pragma_update(None, "http_retries", &config.http_retries.to_string())?;
        conn.pragma_update(
            None,
            "s3_uploader_thread_limit",
            &config.s3_uploader_thread_limit.to_string(),
        )?;
        conn.pragma_update(
            None,
            "max_temp_directory_size",
            &config.max_temp_directory_size,
        )?;

        conn.execute(
            r#"
            CREATE SECRET (
                TYPE s3,
                PROVIDER credential_chain,
                REFRESH auto
            );
            "#,
            [],
        )
        .context("Failed to create s3 credentials")?;

        Ok(Self { conn })
    }

    pub async fn new_default() -> Result<Self> {
        let mut config = DuckDbConfig::default();
        config.access_mode = AccessMode::ReadOnly;
        return Self::new(config).await;
    }

    /// Inspect the parquet file schema
    pub fn inspect_parquet_schema(&self) -> Result<String> {
        let schema_sql = "DESCRIBE SELECT * FROM 'hello_nest.parquet' LIMIT 1";
        self.query_all_json(schema_sql)
    }

    /// Create the hello_nest table from the parquet file with proper schema
    pub fn create_hello_nest_table(&self) -> Result<()> {
        // Drop existing table if it exists
        self.conn.execute("DROP TABLE IF EXISTS hello_nest", [])?;

        // First, let's inspect the parquet file structure
        match self.inspect_parquet_schema() {
            Ok(_) => {}
            Err(_) => {}
        }

        // Create table with basic structure first - debug JSON fields later
        let create_sql = r#"
        CREATE TABLE hello_nest AS
        SELECT
            company_id,
            name AS company_name,
            organization_number,
            company_type,
            company_purpose,
            CASE
                WHEN established_date IS NULL OR established_date = '' THEN NULL
                ELSE TRY_CAST(established_date AS DATE)
            END AS established_date,
            foundation_year,
            registered_for_payroll_tax,
            homepage,
            postal_address,
            visitor_address,
            CASE
                WHEN nace_categories IS NULL OR nace_categories = '' OR nace_categories = '[]' OR nace_categories = 'null' THEN NULL
                ELSE nace_categories
            END AS nace_categories,
            CASE
                WHEN location IS NULL OR location = '' OR location = '{}' THEN NULL
                ELSE STRUCT_PACK(
                    county := json_extract_string(location, '$.county'),
                    countryPart := json_extract_string(location, '$.countryPart'),
                    municipality := json_extract_string(location, '$.municipality'),
                    coordinates := CASE
                        WHEN json_extract(location, '$.coordinates') IS NULL THEN NULL
                        ELSE STRUCT_PACK(
                            XCoordinate := CAST(json_extract(location, '$.coordinates[0].XCoordinate') AS DOUBLE),
                            YCoordinate := CAST(json_extract(location, '$.coordinates[0].YCoordinate') AS DOUBLE),
                            coordinateSystem := json_extract_string(location, '$.coordinates[0].coordinateSystem')
                        )
                    END
                )
            END AS location,
            "financiaL_data" AS financial_data
        FROM 'hello_nest.parquet'
        "#;

        self.conn
            .execute(create_sql, [])
            .context("Failed to create hello_nest table")?;

        Ok(())
    }

    /// Get table info to verify schema
    pub fn get_table_info(&self, table_name: &str) -> Result<String> {
        let sql = format!("DESCRIBE {}", table_name);
        let result = self.query_all_json(&sql)?;
        Ok(result)
    }

    pub fn execute(&self, sql: &str) -> Result<usize> {
        self.conn
            .execute(sql, [])
            .context("Failed to execute query")
    }

    pub fn query_all<T, F>(&self, sql: &str, row_mapper: F) -> Result<Vec<T>>
    where
        F: Fn(&duck::Row) -> Result<T>,
    {
        let mut stmt = self
            .conn
            .prepare(sql)
            .with_context(|| format!("Failed to prepare query: {}", sql))?;
        let mut rows = stmt.query([])?;

        let mut results = Vec::new();
        while let Some(row) = rows.next()? {
            results.push(row_mapper(&row)?);
        }
        Ok(results)
    }

    pub fn query_one<T, F>(&self, sql: &str, row_mapper: F) -> Result<Option<T>>
    where
        F: Fn(&duck::Row) -> Result<T>,
    {
        let mut stmt = self
            .conn
            .prepare(sql)
            .with_context(|| format!("Failed to prepare query: {}", sql))?;
        let mut rows = stmt.query([])?;

        match rows.next()? {
            Some(row) => Ok(Some(row_mapper(&row)?)),
            None => Ok(None),
        }
    }

    pub fn query_all_json(&self, sql: &str) -> Result<String> {
        let json_sql = format!(
            "SELECT COALESCE(json_group_array(to_json(row_data)), '[]') FROM ({}) as row_data",
            sql.trim_end_matches([';', '\n']).trim()
        );

        let mut stmt = self
            .conn
            .prepare(&json_sql)
            .with_context(|| format!("Failed to prepare JSON query: {}", json_sql))?;

        let result: String = stmt
            .query_row([], |row| row.get(0))
            .with_context(|| format!("Failed to execute JSON query: {}", json_sql))?;
        let value: Value = serde_json::from_str(&result).context("Failed to parse JSON result")?;
        serde_json::to_string_pretty(&value).context("Failed to format JSON")
    }

    /// Query all results as JSON - same as query_all_json since no normalization
    pub fn query_all_json_normalized(&self, sql: &str) -> Result<String> {
        self.query_all_json(sql)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn create_test_db(test_name: &str) -> Result<DuckDB> {
        let db_path = format!("/tmp/test_duck_{}.db", test_name);
        let _ = fs::remove_file(&db_path);
        let conn = Connection::open(&db_path)?;
        Ok(DuckDB { conn })
    }

    fn cleanup_test_db(test_name: &str) {
        let db_path = format!("/tmp/test_duck_{}.db", test_name);
        let _ = fs::remove_file(&db_path);
    }

    #[tokio::test]
    async fn test_duckdb_connection() -> Result<()> {
        let db = create_test_db("connection")?;

        db.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER, name VARCHAR)")?;
        db.execute("INSERT INTO test VALUES (1, 'hello')")?;

        let results = db.query_all("SELECT id, name FROM test", |row| {
            Ok((row.get::<_, i32>(0)?, row.get::<_, String>(1)?))
        })?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0], (1, "hello".to_string()));

        cleanup_test_db("connection");
        Ok(())
    }

    #[tokio::test]
    async fn test_query_one() -> Result<()> {
        let db = create_test_db("query_one")?;

        db.execute("CREATE TABLE IF NOT EXISTS test_one (id INTEGER, name VARCHAR)")?;
        db.execute("INSERT INTO test_one VALUES (1, 'first'), (2, 'second')")?;

        let result = db.query_one("SELECT name FROM test_one WHERE id = 1", |row| {
            Ok(row.get::<_, String>(0)?)
        })?;

        assert_eq!(result, Some("first".to_string()));

        let no_result = db.query_one("SELECT name FROM test_one WHERE id = 999", |row| {
            Ok(row.get::<_, String>(0)?)
        })?;

        assert_eq!(no_result, None);

        cleanup_test_db("query_one");
        Ok(())
    }

    #[tokio::test]
    async fn test_query_all_json() -> Result<()> {
        let db = create_test_db("query_json")?;

        db.execute("CREATE TABLE test_json_unique (id INTEGER, name VARCHAR, active BOOLEAN, score DOUBLE)")?;
        db.execute(
            "INSERT INTO test_json_unique VALUES (1, 'Alice', true, 95.5), (2, 'Bob', false, 87.2)",
        )?;

        let result =
            db.query_all_json("SELECT id, name, active, score FROM test_json_unique ORDER BY id")?;

        // Verify it's a formatted JSON string
        assert!(result.contains("{\n"));
        assert!(result.contains("  \"id\": 1"));
        assert!(result.contains("  \"name\": \"Alice\""));
        assert!(result.contains("  \"active\": true"));
        assert!(result.contains("  \"score\": 95.5"));
        assert!(result.contains("  \"name\": \"Bob\""));
        assert!(result.contains("  \"active\": false"));
        assert!(result.contains("  \"score\": 87.2"));

        cleanup_test_db("query_json");
        Ok(())
    }

    #[tokio::test]
    async fn test_access_mode_configuration() -> Result<()> {
        let mut config = DuckDbConfig::default();
        config.access_mode = AccessMode::ReadWrite;
        let db_rw = DuckDB::new(config).await?;
        db_rw.execute("DROP TABLE IF EXISTS test_access")?;
        db_rw.execute("CREATE TABLE test_access (id INTEGER, name VARCHAR)")?;
        db_rw.execute("INSERT INTO test_access VALUES (1, 'test')")?;

        let results = db_rw.query_all("SELECT id, name FROM test_access", |row| {
            Ok((row.get::<_, i32>(0)?, row.get::<_, String>(1)?))
        })?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0], (1, "test".to_string()));

        let mut custom_config = DuckDbConfig::default();
        custom_config.access_mode = AccessMode::ReadWrite;
        custom_config.db_filename = "custom_test.db".to_string();
        let db_custom = DuckDB::new(custom_config).await?;
        db_custom.execute("DROP TABLE IF EXISTS test_custom")?;
        db_custom.execute("CREATE TABLE test_custom (id INTEGER)")?;
        db_custom.execute("INSERT INTO test_custom VALUES (42)")?;

        let custom_results =
            db_custom.query_all(
                "SELECT id FROM test_custom",
                |row| Ok(row.get::<_, i32>(0)?),
            )?;

        assert_eq!(custom_results.len(), 1);
        assert_eq!(custom_results[0], 42);

        let _ = std::fs::remove_file(std::env::temp_dir().join("duck.db"));
        let _ = std::fs::remove_file(std::env::temp_dir().join("custom_test.db"));

        Ok(())
    }
}
