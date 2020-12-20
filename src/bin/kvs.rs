extern crate clap;
use clap::{App, AppSettings, Arg, SubCommand};

fn main() {
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
        ("set", Some(sub_m)) => 
           unimplemented!("unimplemented"),
          /* println!(
           *   "{} -> {}",
           *   sub_m.value_of("KEY").expect("KEY argument missing"),
           *   sub_m.value_of("VALUE").expect("VALUE argument missing")) */
        ("get", Some(sub_m)) => {
           unimplemented!("unimplemented")
//            println!("{}", sub_m.value_of("KEY").expect("KEY argument missing"))
        }
        ("rm", Some(sub_m)) => unimplemented!("unimplemented"),
          //println!("{}", sub_m.value_of("KEY").expect("KEY argument missing")),
        _ => unreachable!(),
    }
}
