
# TODO
- [ ] Full text search on company_purpose
- [ ] Full text search on nace_categories (it should be a varchar not varchar[]) This also means the filters should have an or between them
- [ ] remove and recreate hello_nest.db

- [x] Scaffold MCP server code
- [x] Convert csv to parquet and
- [x] infer table schema (make sure structs are inferred correctly)
- [x] Create sql tool
- [x] Create a search tool to find companies by name, industry code, revenue range, employee number range
- [x] Add correct path to .parquet
- [x] Deploy the MCP as a serverless function
- [x] create a db file inside the folder using duckdb and create the hello_nest table
- [x] Make sure filters are optional in company search tool
- [x] Location as struct not varchar
- [x] Allocation dividends always as double
- [x] nacecategories as array of strings, not varchar
