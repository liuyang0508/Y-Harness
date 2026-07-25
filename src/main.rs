use std::{env, error::Error};

mod reference_cli;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("init") => {
            let directory = args.next().unwrap_or_else(|| ".".to_owned());
            reject_extra_argument(args.next())?;
            reference_cli::run_init(directory)
        }
        Some("doctor") => {
            let config = args.next().unwrap_or_else(|| "y-harness.json".to_owned());
            reject_extra_argument(args.next())?;
            reference_cli::run_doctor(config).await
        }
        Some("serve") => {
            let config = args.next().unwrap_or_else(|| "y-harness.json".to_owned());
            reject_extra_argument(args.next())?;
            reference_cli::run_service(config).await
        }
        Some("demo") => reference_cli::run_demo(args.collect()).await,
        Some("tui-demo") => {
            reject_extra_argument(args.next())?;
            reference_cli::run_tui_demo().await
        }
        Some("serve-demo") => {
            reject_extra_argument(args.next())?;
            reference_cli::run_demo_server().await
        }
        Some("eval-smoke") => {
            reject_extra_argument(args.next())?;
            reference_cli::run_eval_smoke().await
        }
        Some("state-migrate") => {
            let database = required_argument(args.next(), "database")?;
            let backup = required_argument(args.next(), "backup")?;
            reject_extra_argument(args.next())?;
            reference_cli::run_state_migrate(database, backup).await
        }
        Some("approval-migrate") => {
            let database = required_argument(args.next(), "database")?;
            let backup = required_argument(args.next(), "backup")?;
            reject_extra_argument(args.next())?;
            reference_cli::run_approval_migrate(database, backup).await
        }
        Some("-V" | "--version") => {
            reject_extra_argument(args.next())?;
            println!("yh {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Some("-h" | "--help") | None => {
            reference_cli::print_help();
            Ok(())
        }
        Some(command) => Err(format!("unknown command {command:?}; run `yh --help`").into()),
    }
}

fn required_argument(argument: Option<String>, name: &str) -> Result<String, Box<dyn Error>> {
    argument.ok_or_else(|| format!("missing {name}; run `yh --help`").into())
}

fn reject_extra_argument(argument: Option<String>) -> Result<(), Box<dyn Error>> {
    match argument {
        Some(argument) => Err(format!("unexpected argument {argument:?}; run `yh --help`").into()),
        None => Ok(()),
    }
}
