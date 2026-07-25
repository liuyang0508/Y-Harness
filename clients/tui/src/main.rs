//! Optional full-screen client for the headless Y-Harness Engine.

#![warn(missing_docs)]

mod app;
mod protocol;
mod ui;

use std::{
    env,
    error::Error,
    ffi::OsString,
    io::{self, IsTerminal},
    path::PathBuf,
    process::ExitCode,
};

use app::App;
use protocol::{EngineMode, ProtocolClient};
use ui::TerminalSession;

type MainResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

enum Action {
    Help,
    Version,
    Run(Options),
}

struct Options {
    engine: OsString,
    mode: Mode,
    thread: Option<String>,
}

enum Mode {
    Demo,
    Config(PathBuf),
}

#[tokio::main]
async fn main() -> ExitCode {
    match entry().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn entry() -> MainResult<()> {
    match parse_options(env::args_os().skip(1))? {
        Action::Help => {
            print_help();
            Ok(())
        }
        Action::Version => {
            println!("yh-tui {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Action::Run(options) => run(options).await,
    }
}

async fn run(options: Options) -> MainResult<()> {
    if !io::stdin().is_terminal() || !io::stderr().is_terminal() {
        return Err(io::Error::other(
            "yh-tui requires an interactive terminal on stdin and stderr",
        )
        .into());
    }
    let mode = match &options.mode {
        Mode::Demo => EngineMode::Demo,
        Mode::Config(config) => EngineMode::Config(config),
    };
    let mut client = ProtocolClient::spawn(&options.engine, mode)?;
    let mut app = App::bootstrap(&mut client, options.thread).await?;
    let result = {
        let mut terminal = TerminalSession::enter()?;
        app.run(terminal.terminal_mut(), &mut client).await
    };
    let shutdown = client.shutdown().await;
    result?;
    shutdown
}

fn parse_options(arguments: impl Iterator<Item = OsString>) -> MainResult<Action> {
    let mut engine = env::var_os("YH_BIN").unwrap_or_else(|| OsString::from("yh"));
    let mut config = None;
    let mut demo = false;
    let mut thread = None;
    let mut arguments = arguments.peekable();
    while let Some(argument) = arguments.next() {
        match argument.to_str() {
            Some("-h" | "--help") => return Ok(Action::Help),
            Some("-V" | "--version") => return Ok(Action::Version),
            Some("--demo") => demo = true,
            Some("--engine") => {
                engine = arguments.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "--engine requires a path")
                })?;
            }
            Some("--config") => {
                let value = arguments.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "--config requires a path")
                })?;
                config = Some(PathBuf::from(value));
            }
            Some("--thread") => {
                let value = arguments.next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "--thread requires an id")
                })?;
                thread = Some(value.into_string().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "Thread id must be UTF-8")
                })?);
            }
            Some(option) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown option {option:?}; run `yh-tui --help`"),
                )
                .into());
            }
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "options must be valid UTF-8",
                )
                .into());
            }
        }
    }
    if demo && config.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "--demo and --config are mutually exclusive",
        )
        .into());
    }
    let mode = if demo {
        Mode::Demo
    } else if let Some(config) = config {
        Mode::Config(config)
    } else {
        let default = PathBuf::from("y-harness.json");
        if default.is_file() {
            Mode::Config(default)
        } else {
            Mode::Demo
        }
    };
    Ok(Action::Run(Options {
        engine,
        mode,
        thread,
    }))
}

fn print_help() {
    println!(
        "Y-Harness TUI\n\n\
         Usage:\n  \
           yh-tui [--demo | --config <path>] [--engine <path>] [--thread <id>]\n  \
           yh-tui --version\n\n\
         Options:\n  \
           --demo           start `yh serve-demo`\n  \
           --config <path>  start `yh serve <path>`\n  \
           --engine <path>  Engine executable; defaults to $YH_BIN or `yh`\n  \
           --thread <id>    attach to an existing authoritative Thread\n\n\
         With no mode, y-harness.json is used when present; otherwise demo mode.\n\
         The TUI is an optional Protocol v10 client and owns no Engine state."
    );
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;

    use super::{Action, Mode, parse_options};

    #[test]
    fn options_keep_demo_and_config_explicit()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let action = parse_options([OsString::from("--demo")].into_iter())?;
        assert!(matches!(
            action,
            Action::Run(super::Options {
                mode: Mode::Demo,
                ..
            })
        ));

        let error = match parse_options(
            [
                OsString::from("--demo"),
                OsString::from("--config"),
                OsString::from("project.json"),
            ]
            .into_iter(),
        ) {
            Ok(_) => return Err("conflicting modes were accepted".into()),
            Err(error) => error,
        };
        assert!(error.to_string().contains("mutually exclusive"));
        Ok(())
    }
}
