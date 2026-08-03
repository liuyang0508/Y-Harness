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
        Some("serve-demo") => {
            reject_extra_argument(args.next())?;
            reference_cli::run_demo_server().await
        }
        Some("eval-smoke") => {
            reject_extra_argument(args.next())?;
            reference_cli::run_eval_smoke().await
        }
        Some("eval") => {
            let suite = required_argument(args.next(), "Evaluation suite")?;
            let baseline = required_argument(args.next(), "Evaluation baseline")?;
            let config = args.next().unwrap_or_else(|| "y-harness.json".to_owned());
            reject_extra_argument(args.next())?;
            reference_cli::run_evaluation(suite, baseline, config).await
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
        Some("task-migrate") => {
            let database = required_argument(args.next(), "database")?;
            let backup = required_argument(args.next(), "backup")?;
            reject_extra_argument(args.next())?;
            reference_cli::run_task_migrate(database, backup).await
        }
        Some("skill" | "package") => match args.next().as_deref() {
            Some("install") => {
                let package = required_argument(args.next(), "Skill package")?;
                let config = args.next().unwrap_or_else(|| "y-harness.json".to_owned());
                reject_extra_argument(args.next())?;
                reference_cli::run_skill_install(package, config)
            }
            Some("install-external") => {
                let package = required_argument(args.next(), "signed Skill package")?;
                let config = args.next().unwrap_or_else(|| "y-harness.json".to_owned());
                reject_extra_argument(args.next())?;
                reference_cli::run_skill_install_external(package, config)
            }
            Some("install-https") => {
                let endpoint = required_argument(args.next(), "HTTPS Skill URL")?;
                let identity = required_argument(args.next(), "Skill identity name@version")?;
                let expected_sha256 = required_argument(args.next(), "Skill content SHA-256")?;
                let config = args.next().unwrap_or_else(|| "y-harness.json".to_owned());
                reject_extra_argument(args.next())?;
                reference_cli::run_skill_install_https(endpoint, identity, expected_sha256, config)
                    .await
            }
            Some("search-https") => {
                let endpoint = required_argument(args.next(), "HTTPS Skill catalog URL")?;
                let expected_sha256 = required_argument(args.next(), "Skill catalog SHA-256")?;
                let query = required_argument(args.next(), "Skill catalog query or *")?;
                let config = args.next().unwrap_or_else(|| "y-harness.json".to_owned());
                reject_extra_argument(args.next())?;
                reference_cli::run_skill_search_catalog(endpoint, expected_sha256, query, config)
                    .await
            }
            Some("install-catalog") => {
                let endpoint = required_argument(args.next(), "HTTPS Skill catalog URL")?;
                let expected_sha256 = required_argument(args.next(), "Skill catalog SHA-256")?;
                let identity = required_argument(args.next(), "Skill identity name@version")?;
                let config = args.next().unwrap_or_else(|| "y-harness.json".to_owned());
                reject_extra_argument(args.next())?;
                reference_cli::run_skill_install_catalog(
                    endpoint,
                    expected_sha256,
                    identity,
                    config,
                )
                .await
            }
            Some("upgrade-catalog") => {
                let endpoint = required_argument(args.next(), "HTTPS Skill catalog URL")?;
                let expected_sha256 = required_argument(args.next(), "Skill catalog SHA-256")?;
                let identity = required_argument(args.next(), "Skill identity name@version")?;
                let config = args.next().unwrap_or_else(|| "y-harness.json".to_owned());
                reject_extra_argument(args.next())?;
                reference_cli::run_skill_upgrade_catalog(
                    endpoint,
                    expected_sha256,
                    identity,
                    config,
                )
                .await
            }
            Some("registry-search") => {
                let registry = required_argument(args.next(), "Skill Registry identity")?;
                let expected_sha256 = required_argument(args.next(), "Skill catalog SHA-256")?;
                let query = required_argument(args.next(), "Skill catalog query or *")?;
                let config = args.next().unwrap_or_else(|| "y-harness.json".to_owned());
                reject_extra_argument(args.next())?;
                reference_cli::run_skill_search_registry(registry, expected_sha256, query, config)
                    .await
            }
            Some("registry-install") => {
                let registry = required_argument(args.next(), "Skill Registry identity")?;
                let expected_sha256 = required_argument(args.next(), "Skill catalog SHA-256")?;
                let identity = required_argument(args.next(), "Skill identity name@version")?;
                let config = args.next().unwrap_or_else(|| "y-harness.json".to_owned());
                reject_extra_argument(args.next())?;
                reference_cli::run_skill_install_registry(
                    registry,
                    expected_sha256,
                    identity,
                    config,
                )
                .await
            }
            Some("registry-upgrade") => {
                let registry = required_argument(args.next(), "Skill Registry identity")?;
                let expected_sha256 = required_argument(args.next(), "Skill catalog SHA-256")?;
                let identity = required_argument(args.next(), "Skill identity name@version")?;
                let config = args.next().unwrap_or_else(|| "y-harness.json".to_owned());
                reject_extra_argument(args.next())?;
                reference_cli::run_skill_upgrade_registry(
                    registry,
                    expected_sha256,
                    identity,
                    config,
                )
                .await
            }
            Some("list") => {
                let config = args.next().unwrap_or_else(|| "y-harness.json".to_owned());
                reject_extra_argument(args.next())?;
                reference_cli::run_skill_list(config)
            }
            Some("activate") => {
                let identity = required_argument(args.next(), "Skill identity name@version")?;
                let config = args.next().unwrap_or_else(|| "y-harness.json".to_owned());
                reject_extra_argument(args.next())?;
                reference_cli::run_skill_activate(identity, config).await
            }
            Some("deactivate") => {
                let identity = required_argument(args.next(), "Skill identity name@version")?;
                let config = args.next().unwrap_or_else(|| "y-harness.json".to_owned());
                reject_extra_argument(args.next())?;
                reference_cli::run_skill_deactivate(identity, config).await
            }
            Some("verify") => {
                let config = args.next().unwrap_or_else(|| "y-harness.json".to_owned());
                reject_extra_argument(args.next())?;
                reference_cli::run_skill_verify(config)
            }
            Some("history") => {
                let config = args.next().unwrap_or_else(|| "y-harness.json".to_owned());
                reject_extra_argument(args.next())?;
                reference_cli::run_skill_history(config)
            }
            Some("rollback") => {
                let revision = required_argument(args.next(), "configuration revision SHA-256")?;
                let config = args.next().unwrap_or_else(|| "y-harness.json".to_owned());
                reject_extra_argument(args.next())?;
                reference_cli::run_skill_rollback(revision, config).await
            }
            Some("remove") => {
                let identity = required_argument(args.next(), "Skill identity name@version")?;
                let config = args.next().unwrap_or_else(|| "y-harness.json".to_owned());
                reject_extra_argument(args.next())?;
                reference_cli::run_skill_remove(identity, config).await
            }
            Some(command) => {
                Err(format!("unknown skill command {command:?}; run `yh --help`").into())
            }
            None => Err("missing skill command; run `yh --help`".into()),
        },
        Some("thread") => match args.next().as_deref() {
            Some("export") => {
                let thread_id = required_argument(args.next(), "Thread identity")?;
                let archive = required_argument(args.next(), "archive path")?;
                let config = args.next().unwrap_or_else(|| "y-harness.json".to_owned());
                reject_extra_argument(args.next())?;
                reference_cli::run_thread_export(thread_id, archive, config).await
            }
            Some("import") => {
                let archive = required_argument(args.next(), "archive path")?;
                let target_thread_id = required_argument(args.next(), "target Thread identity")?;
                let config = args.next().unwrap_or_else(|| "y-harness.json".to_owned());
                reject_extra_argument(args.next())?;
                reference_cli::run_thread_import(archive, target_thread_id, config).await
            }
            Some(command) => {
                Err(format!("unknown thread command {command:?}; run `yh --help`").into())
            }
            None => Err("missing thread command; run `yh --help`".into()),
        },
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
