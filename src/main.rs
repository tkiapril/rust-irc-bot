#[macro_use(bson, doc)]
extern crate bson;
extern crate irc;
extern crate mongodb;
extern crate time;

use std::collections::HashMap;
use std::io;
use std::process;
use std::str::FromStr;
use std::sync::mpsc;
use std::thread;

use irc::client::prelude::*;
use mongodb::ThreadedClient;
use mongodb::db::ThreadedDatabase;

struct DateMessage {
    time: i64,
    message: String,
}

struct DbConfig<'a> {
    host: &'a str,
    port: u16,
    name: &'a str,
    user: &'a str,
    pass: &'a str,
}

impl<'a> DbConfig<'a> {
    fn from(options: &HashMap<String, String>)
    -> io::Result<DbConfig> {
        Ok(DbConfig {
            host: try!(unwrap_config(&options, "db_host")),
            port: match FromStr::from_str(
                try!(unwrap_config(&options, "db_port")),
            ) {
                Ok(port) => port,
                Err(_) => return Err(
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "db_port is not valid",
                    ),
                ),
            },
            name: try!(unwrap_config(&options, "db_name")),
            user: try!(unwrap_config(&options, "db_user")),
            pass: try!(unwrap_config(&options, "db_pass")),
        })
    }
}

fn unwrap_config<'a>(
    options: &'a HashMap<String, String>,
    key: &str,
) -> io::Result<&'a str> {
    let err_not_found = Err(
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("{} was not found in config", key)
        ),
    );
    match options.get(key) {
        Some(val) => Ok(val),
        None => err_not_found,
    }
}

fn main_() -> io::Result<()> {
    let server = try!(IrcServer::new("config.json"));
    let options = match server.config().options {
        Some(ref options) => options,
        None => return Err(
            io::Error::new(
                io::ErrorKind::NotFound,
                "DB config is not found",
            ),
        ),
    };
    let db_config = try!(DbConfig::from(options));
    let debug: bool = match FromStr::from_str(
        try!(unwrap_config(&options, "debug")),
    ) {
        Ok(debug) => debug,
        Err(_) => false,
    };

    let db = try!(mongodb::Client::connect(
        db_config.host,
        db_config.port,
    )).db(db_config.name);
    if db_config.user != "" && db_config.pass != "" {
        try!(db.auth(db_config.user, db_config.pass))
    };

    let (tx_error, rx_error) = mpsc::channel();
    let (tx_log, rx_log) = mpsc::channel();
    let server = server.clone();
    thread::spawn(move || {
        let mut id = false;
        for message in server.iter() {
            let time = time::now().to_timespec().sec;
            let message = match message {
                Ok(message) => message,
                Err(e) => { tx_error.send(Err(e)).unwrap(); return },
            };
            let message_str = format!("{}", message);
            tx_log.send(
                DateMessage { time: time, message: message_str }
            ).unwrap();
            
            if !id { id = server.identify().is_ok() };

            if debug { println!("{:?}", message) };
        };
    });

    thread::spawn(move || {
        loop {
            let datemessage = rx_log.recv().unwrap();
            let time = datemessage.time;
            let message = datemessage.message;
            match db.collection("raw").insert_one(
                doc!{
                    "time" => time,
                    "line" => message
                },
                None,
            ) {
                Err(e) => println!("Cannot insert to DB due to error: {}", e),
                _ => (),
            };
        };
    });

    loop {
        return rx_error.recv().unwrap()
    };
}

fn main() {
    if let Err(e) = main_() {
        println!("{}", e);
        process::exit(e.raw_os_error().unwrap_or(1));
    }
}
