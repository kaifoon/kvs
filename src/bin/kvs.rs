extern crate clap;
use clap::{App, AppSettings, Arg, SubCommand};
use kvs::{KvsError, KvStore, Result};
use std::env::current_dir;
use std::process::exit;

fn main() -> Result<()> {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .setting(AppSettings::DisableHelpSubcommand)
        .setting(AppSettings::SubcommandRequiredElseHelp)
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

    match matches.subcommand() {
        ("set", Some(sub_m)) => {
            let key = sub_m.value_of("KEY").expect("KEY argument missing");
            let value = sub_m.value_of("VALUE").expect("VALUE argument missing");

            let mut store = KvStore::open(current_dir()?)?;
            store.set(key.to_owned(), value.to_owned())?;
        }
        ("get", Some(sub_m)) => {
            let key = sub_m.value_of("KEY").expect("KEY argument missing");

            let mut store = KvStore::open(current_dir()?)?;
            
            match store.get(key.to_owned())? {
              Some(val) => println!("{}", val),
              None => println!("Key not found"),
            }

        }
        ("rm", Some(sub_m)) => {
            let key = sub_m.value_of("KEY").expect("KEY argument missing");

            let mut store = KvStore::open(current_dir()?)?;
            match store.remove(key.to_owned()) {
              Ok(()) => {},
              Err(KvsError::KeyNotFound) => {
                println!("Key not found");
                exit(1);
              },
              Err(e) => return Err(e),
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}
