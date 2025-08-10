use nest_mcp::serve;
use std::env;

#[derive(Debug)]
enum Command {
    Serve,
}

impl Command {
    fn from_args() -> Result<Self, String> {
        let args: Vec<String> = env::args().collect();

        if args.len() < 2 {
            return Err("No command provided. Available commands: serve".to_string());
        }

        match args[1].as_str() {
            "serve" => Ok(Command::Serve),
            cmd => Err(format!(
                "Unknown command: {}. Available commands: serve",
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
    }
}
