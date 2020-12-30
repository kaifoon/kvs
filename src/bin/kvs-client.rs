use clap::{App, AppSettings, Arg, SubCommand};
use kvs::{KvsClient, Result};

fn main() -> Result<()> {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .setting(AppSettings::DisableHelpSubcommand)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .arg(
            Arg::with_name("addr")
                .takes_value(true)
                .short("a")
                .long("addr")
                .help("An IP address with the format IP:PORT.default 127.0.0.1:4000."),
        )
        .subcommand(
            SubCommand::with_name("set")
                .about("Set the value of a string key to a string")
                .arg(Arg::with_name("KEY").help("A string key").required(true))
                .arg(
                    Arg::with_name("VALUE")
                        .help("The string value of the key")
                        .required(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get the string value of a given string key")
                .arg(Arg::with_name("KEY").help("A string key").required(true)),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove a given key")
                .arg(Arg::with_name("KEY").help("A string key").required(true)),
        )
        .get_matches();

    // IP Addr
    let addr = matches.value_of("addr").unwrap_or("127.0.0.1:4000");
    let mut client = KvsClient::connect(addr)?;

    match matches.subcommand() {
        ("set", Some(sub_m)) => {
            let key = sub_m.value_of("KEY").expect("KEY argument missing");
            let value = sub_m.value_of("VALUE").expect("VALUE argument missing");

            client.set(key.to_string(), value.to_string())?;
        }
        ("get", Some(sub_m)) => {
            let key = sub_m.value_of("KEY").expect("KEY argument missing");

            if let Some(value) = client.get(key.to_string())? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        ("rm", Some(sub_m)) => {
            let key = sub_m.value_of("KEY").expect("KEY argument missing");
            client.remove(key.to_string())?;
        }
        _ => unreachable!(),
    }
    Ok(())
}
