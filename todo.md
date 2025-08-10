# TODO
- [x] Scaffold MCP server code
- [x] Convert csv to parquet and
- [x] infer table schema (make sure structs are inferred correctly)
- [x] Create sql tool
- [x] Create a search tool to find companies by name, industry code, revenue range, employee number range
- [ ] Add correct path to .parquet
- [ ] Make sure filters are optional in company search tool
- [ ] Deploy the MCP as a serverless function
- [ ] Create s3 bucket and upload the parquet file

# Backlog
- [ ] Location as struct not varchar
- [ ] Allocation dividends always as double
- [ ] nacecategories as array of strings, not varchar
- [ ] Full text search on company_purpose
