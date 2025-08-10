# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Build and Run
- `cargo build` - Build the project
- `cargo run serve` - Start the MCP server (serves on 127.0.0.1:8000)
- `cargo test` - Run tests
- `cargo check` - Check code without building

### Testing
- `cargo test` - Run all tests
- `cargo test duckdb` - Run DuckDB-specific tests

## Project Architecture

This is a Rust-based MCP (Model Context Protocol) server that provides SQL query tools for accessing a Swedish company database. The server uses Server-Sent Events (SSE) for client communication.

### Core Components

**Main Application (`src/main.rs`)**
- Command-line interface with `serve` command
- Entry point for the MCP server

**Server Implementation (`src/lib.rs`)**
- SSE-based MCP server using rmcp crate
- Binds to 127.0.0.1:8000
- Routes: `/sse` (SSE endpoint), `/message` (POST endpoint)
- Uses Axum for HTTP handling

**MCP Tools (`src/tool.rs`)**
- `company-sql`: Execute raw SQL queries against the company database
- `company-search`: Search companies with filters (name, foundation year, NACE categories)
- All tools use DuckDB dialect SQL
- Query results returned as JSON

**Database Layer (`src/duckdb.rs`)**
- DuckDB wrapper with connection management
- S3 credentials configured for cloud data access
- JSON query helpers for MCP tool responses
- Comprehensive test coverage

### Database Schema

The `hello_nest.parquet` file contains Swedish company data with:
- Basic company info (name, organization_number, company_type, etc.)
- Financial data by year (2016-2024) with 44+ financial metrics
- NACE industry categories
- Location and contact information

**Financial Data Evolution:**
- 2016-2017: 43 metrics, "Allocation dividends" as INTEGER
- 2018: 43 metrics, "Allocation dividends" becomes DOUBLE  
- 2019-2020: 44 metrics, adds "Minority interests" as INTEGER
- 2021-2024: 44 metrics, fully standardized with all DOUBLE types

### Key Dependencies
- `rmcp` - MCP server implementation with SSE transport
- `duckdb` - SQL engine for parquet data analysis
- `axum` - HTTP server framework
- `tokio` - Async runtime
- `serde/serde_json` - JSON serialization
- `schemars` - JSON schema generation for MCP tools

### Data Files
- `hello_nest.parquet` - Main company database
- `raw/` - Source CSV files for data processing
- `converter.ipynb` - Jupyter notebook for CSV to Parquet conversion

### Important Notes
- Server runs on localhost:8000 by default
- All SQL queries use DuckDB dialect
- Company search tool requires at least one search parameter
- Basic SQL injection protection implemented for search queries
- Query results limited to 1000 records for performance