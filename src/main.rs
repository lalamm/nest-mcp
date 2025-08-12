use nest_mcp::{duckdb::DuckDB, serve};
use std::env;

#[derive(Debug)]
enum Command {
    Serve,
    CreateDb,
    VerifyDb,
    InspectData,
    TestParsed,
}

impl Command {
    fn from_args() -> Result<Self, String> {
        let args: Vec<String> = env::args().collect();

        if args.len() < 2 {
            return Err(
                "No command provided. Available commands: serve, create-db, verify-db, inspect-data, test-parsed".to_string(),
            );
        }

        match args[1].as_str() {
            "serve" => Ok(Command::Serve),
            "create-db" => Ok(Command::CreateDb),
            "verify-db" => Ok(Command::VerifyDb),
            "inspect-data" => Ok(Command::InspectData),
            "test-parsed" => Ok(Command::TestParsed),
            cmd => Err(format!(
                "Unknown command: {}. Available commands: serve, create-db, verify-db, inspect-data, test-parsed",
                cmd
            )),
        }
    }
}

#[tokio::main]
async fn main() {
    let command = Command::from_args().unwrap();

    match command {
        Command::Serve => {
            println!("Starting server...");
            serve().await.unwrap();
        }
        Command::CreateDb => {
            println!("Creating database table...");
            let db = DuckDB::new_default().await.unwrap();
            db.create_hello_nest_table().unwrap();
            println!("Database table 'hello_nest' created successfully!");
        }
        Command::VerifyDb => {
            println!("Verifying database table...");
            let db = DuckDB::new_default().await.unwrap();

            // Get table info
            match db.get_table_info("hello_nest") {
                Ok(info) => {
                    println!("Table schema:");
                    println!("{}", info);
                }
                Err(e) => {
                    println!("Error getting table info: {}", e);
                    return;
                }
            }

            // Get row count
            match db.query_all_json("SELECT COUNT(*) as row_count FROM hello_nest") {
                Ok(count) => {
                    println!("\nRow count:");
                    println!("{}", count);
                }
                Err(e) => {
                    println!("Error getting row count: {}", e);
                }
            }

            // Show sample data
            match db.query_all_json(
                "SELECT company_name, organization_number, company_type FROM hello_nest LIMIT 5",
            ) {
                Ok(sample) => {
                    println!("\nSample data:");
                    println!("{}", sample);
                }
                Err(e) => {
                    println!("Error getting sample data: {}", e);
                }
            }
        }
        Command::InspectData => {
            println!("Inspecting actual data values...");
            let db = DuckDB::new_default().await.unwrap();

            // Check what nace_categories looks like
            match db.query_all_json(
                "SELECT nace_categories FROM hello_nest WHERE nace_categories IS NOT NULL LIMIT 3",
            ) {
                Ok(nace_data) => {
                    println!("\nNACE categories sample:");
                    println!("{}", nace_data);
                }
                Err(e) => println!("Error getting NACE data: {}", e),
            }

            // Check what location looks like
            match db.query_all_json(
                "SELECT location FROM hello_nest WHERE location IS NOT NULL LIMIT 3",
            ) {
                Ok(location_data) => {
                    println!("\nLocation sample:");
                    println!("{}", location_data);
                }
                Err(e) => println!("Error getting location data: {}", e),
            }

            // Check financial_data structure
            match db.query_all_json(
                "SELECT financial_data FROM hello_nest WHERE financial_data IS NOT NULL LIMIT 1",
            ) {
                Ok(financial_data) => {
                    println!("\nFinancial data sample:");
                    println!("{}", financial_data);
                }
                Err(e) => println!("Error getting financial data: {}", e),
            }
        }
        Command::TestParsed => {
            println!("Testing parsed view with complex types...");
            let db = DuckDB::new_default().await.unwrap();

            // Test parsed view schema
            match db.get_table_info("hello_nest_parsed") {
                Ok(schema) => {
                    println!("\nParsed view schema:");
                    println!("{}", schema);
                }
                Err(e) => println!("Error getting parsed view schema: {}", e),
            }

            // Test NACE categories parsing
            match db.query_all_json(
                "SELECT company_name, nace_categories FROM hello_nest_parsed WHERE nace_categories IS NOT NULL LIMIT 3",
            ) {
                Ok(nace_data) => {
                    println!("\nParsed NACE categories:");
                    println!("{}", nace_data);
                }
                Err(e) => println!("Error getting parsed NACE data: {}", e),
            }

            // Test location parsing
            match db.query_all_json(
                "SELECT company_name, location FROM hello_nest_parsed WHERE location IS NOT NULL LIMIT 3",
            ) {
                Ok(location_data) => {
                    println!("\nParsed location data:");
                    println!("{}", location_data);
                }
                Err(e) => println!("Error getting parsed location data: {}", e),
            }
        }
    }
}
