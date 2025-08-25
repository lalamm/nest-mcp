# AGENTS.md - Development Guidelines for nest-mcp

This file provides development guidelines for agentic coding assistants working on the nest-mcp Rust project.

## Build/Lint/Test Commands

### Core Commands
- `cargo build` - Build the project in debug mode
- `cargo build --release` - Build the project in release mode
- `cargo run serve` - Start the MCP server (default port 8000)
- `cargo check` - Check code without building (fast feedback)

### Testing
- `cargo test` - Run all tests
- `cargo test duckdb` - Run DuckDB-specific tests
- `cargo test -- --ignored` - Run integration tests (requires database)
- `cargo test test_name` - Run a specific test function

### Single Test Execution
To run a specific test, use: `cargo test test_function_name`

Examples:
- `cargo test test_duckdb_connection`
- `cargo test test_build_company_search_query_basic`
- `cargo test test_query_all_json`

## Code Style Guidelines

### Imports
- Group imports logically: standard library, external crates, local modules
- Use explicit imports rather than glob imports (`*`)
- Example:
```rust
use anyhow::{Context, Result};
use std::{env, path::PathBuf};
use tokio::time::Duration;
```

### Formatting
- Use `rustfmt` for consistent formatting
- Run `cargo fmt` before committing
- Follow standard Rust formatting conventions

### Types and Naming
- **Structs/Enums**: PascalCase (e.g., `DuckDB`, `Command`, `SearchRequest`)
- **Functions/Methods**: snake_case (e.g., `new_default`, `query_all_json`)
- **Variables**: snake_case (e.g., `db_path`, `search_request`)
- **Constants**: SCREAMING_SNAKE_CASE
- Use strong typing with explicit types for clarity
- Prefer structs over tuples for complex data

### Error Handling
- Use `anyhow::Result<T>` for functions that can fail
- Add context with `.context("description")` for better error messages
- Example:
```rust
pub fn create_hello_nest_table(&self) -> Result<()> {
    // ... implementation
    self.conn
        .execute(create_sql, [])
        .context("Failed to create hello_nest table")?;
    Ok(())
}
```

### Async/Await
- Use `#[tokio::main]` for main functions
- Use `async fn` for async functions
- Prefer async methods over blocking operations

### Documentation
- Use `///` for public API documentation
- Document complex logic and business rules
- Include parameter descriptions and examples where helpful

### Testing Patterns
- Use `#[tokio::test]` for async tests
- Use `#[test]` for synchronous tests
- Create test-specific resources (e.g., temp databases)
- Clean up test resources in test functions
- Use descriptive test names: `test_functionality_scenario`

### Security Considerations
- Implement basic SQL injection protection for user inputs
- Validate input parameters and ranges
- Use parameterized queries when possible
- Log security-relevant events

### Database Patterns
- Use connection pooling where appropriate
- Implement proper resource cleanup
- Handle database schema evolution carefully
- Test with realistic data volumes

### MCP Tool Patterns
- Use descriptive tool names and annotations
- Provide comprehensive schema documentation
- Implement proper error handling for tool calls
- Validate parameters before processing

## Development Workflow

1. **Before coding**: Run `cargo check` to ensure current code compiles
2. **During development**: Use `cargo test` to verify functionality
3. **Before commit**: Run `cargo fmt` and `cargo test`
4. **Integration testing**: Use `cargo test -- --ignored` for database tests

## Project-Specific Notes

- Server runs on port 8000 by default (configurable via PORT env var)
- Uses DuckDB for SQL queries with read-only access by default
- Implements Claude-specific authentication middleware
- Query results limited to 1000 records for performance
- Financial data spans 2016-2024 with evolving schema