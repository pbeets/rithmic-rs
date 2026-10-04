//! First-use agreements on an optional repository connection. Lists pending
//! agreements by default. Accepting and self-certifying require explicit commands.
//!
//! cargo run --example repository_agreements
//! cargo run --example repository_agreements -- accepted
//! cargo run --example repository_agreements -- show AGREEMENT_ID ./agreements
//! cargo run --example repository_agreements -- accept AGREEMENT_ID [professional|non-professional]
//! cargo run --example repository_agreements -- certify AGREEMENT_ID professional|non-professional

use std::{env, path::Path};

use rithmic_rs::{
    ConnectStrategy, MarketDataUsageCapacity, RithmicConfig, RithmicEnv, RithmicRepositoryPlant,
    RithmicRepositoryPlantHandle, RithmicResponse, rti::messages::RithmicMessage,
};

const ENV: RithmicEnv = RithmicEnv::Demo;
type ExampleResult<T> = Result<T, Box<dyn std::error::Error>>;

fn capacity(value: &str) -> ExampleResult<MarketDataUsageCapacity> {
    match value {
        "professional" => Ok(MarketDataUsageCapacity::Professional),
        "non-professional" => Ok(MarketDataUsageCapacity::NonProfessional),
        _ => Err("capacity must be professional or non-professional".into()),
    }
}

fn check(response: &RithmicResponse) -> ExampleResult<()> {
    if let Some(error) = &response.error {
        return Err(error.clone().into());
    }
    Ok(())
}

enum Command {
    Pending,
    Accepted,
    Show(String, String),
    Accept(String, Option<MarketDataUsageCapacity>),
    Certify(String, MarketDataUsageCapacity),
}

fn command() -> ExampleResult<Command> {
    let args: Vec<String> = env::args().skip(1).collect();
    match args.iter().map(String::as_str).collect::<Vec<_>>().as_slice() {
        [] | ["pending"] => Ok(Command::Pending),
        ["accepted"] => Ok(Command::Accepted),
        ["show", id, directory] => Ok(Command::Show((*id).into(), (*directory).into())),
        ["accept", id] => Ok(Command::Accept((*id).into(), None)),
        ["accept", id, value] => Ok(Command::Accept((*id).into(), Some(capacity(value)?))),
        ["certify", id, value] => Ok(Command::Certify((*id).into(), capacity(value)?)),
        _ => Err("usage: pending | accepted | show ID DIRECTORY | accept ID [CAPACITY] | certify ID CAPACITY".into()),
    }
}

async fn run(handle: &RithmicRepositoryPlantHandle, command: Command) -> ExampleResult<()> {
    let responses = match command {
        Command::Pending => handle.list_unaccepted_agreements().await?,
        Command::Accepted => handle.list_accepted_agreements().await?,
        Command::Show(id, directory) => {
            let responses = handle.show_agreement(&id).await?;
            // Check every frame before writing any content.
            for response in &responses {
                check(response)?;
            }
            std::fs::create_dir_all(&directory)?;
            for (index, response) in responses.iter().enumerate() {
                if let RithmicMessage::ResponseShowAgreement(agreement) = &response.message {
                    println!(
                        "Title: {:?}; mandatory: {:?}; status: {:?}; acceptance request: {:?}",
                        agreement.agreement_title,
                        agreement.agreement_mandatory_flag,
                        agreement.agreement_status,
                        agreement.agreement_acceptance_request
                    );
                    // Preserve each frame's bytes; the protocol does not promise UTF-8.
                    for (extension, bytes) in [
                        ("bin", &agreement.agreement),
                        ("html", &agreement.agreement_html),
                    ] {
                        if let Some(bytes) = bytes {
                            let path = Path::new(&directory)
                                .join(format!("agreement-{index}.{extension}"));
                            std::fs::write(&path, bytes)?;
                            println!("Saved {}", path.display());
                        }
                    }
                }
            }
            return Ok(());
        }
        Command::Accept(id, capacity) => vec![handle.accept_agreement(&id, capacity).await?],
        Command::Certify(id, capacity) => vec![
            handle
                .set_market_data_self_cert_status(&id, capacity)
                .await?,
        ],
    };
    for response in responses {
        check(&response)?;
        println!("{:#?}", response.message);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> ExampleResult<()> {
    let command = command()?;
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().init();
    let config = RithmicConfig::from_env(ENV)?;
    let plant = RithmicRepositoryPlant::connect(&config, ConnectStrategy::Simple).await?;
    let handle = plant.get_handle();
    if let Err(error) = handle.login().await {
        handle.abort();
        plant.await_shutdown().await?;
        return Err(error.into());
    }
    let result = run(&handle, command).await;
    let logout = handle.disconnect().await;
    plant.await_shutdown().await?;
    result?;
    check(&logout?)?;
    Ok(())
}
