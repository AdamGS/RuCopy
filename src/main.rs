extern crate futures;
extern crate rusoto_core;
extern crate rusoto_s3;

use clap::{App, Arg};
use futures::{Future, Stream};
use rayon::prelude::*;
use rusoto_core::credential::ChainProvider;
use rusoto_core::{HttpClient, ProvideAwsCredentials, Region};
use rusoto_s3::{GetObjectRequest, ListObjectsV2Request, Object, S3Client, S3};
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::str::FromStr;
use xmltree::Element;

use crossbeam::channel::{bounded, unbounded, Receiver};
use crossbeam_utils::sync::WaitGroup;
use std::thread;

const REGION: &str = "REGION";
const BUCKET: &str = "BUCKET";
const ENDPOINT: &str = "ENDPOINT";
const PREFIX: &str = "PREFIX";
const LOCAL_PATH: &str = "LOCAL_PATH";
const DELIMITER: &str = "DELIMITER";

fn main() -> std::io::Result<()> {
    let args = App::new("RuCopy")
        .version("0.0.1")
        .about("Trying to make high-performance cp from s3 in Rust.")
        .arg(
            Arg::with_name(BUCKET)
                .short("b")
                .takes_value(true)
                .required(true)
                .help("Name of your bucket"),
        )
        .arg(
            Arg::with_name(REGION)
                .short("r")
                .takes_value(true)
                .required(true)
                .help("Specify AWS region"),
        )
        .arg(
            Arg::with_name(ENDPOINT)
                .short("e")
                .takes_value(true)
                .required(false)
                .help(
                    "Your s3 endpoint (for use when proxy or non-aws s3 implementation are used)",
                ),
        )
        .arg(
            Arg::with_name(PREFIX)
                .default_value("/")
                .required(false)
                .short("p")
                .help("The path inside the specified bucket"),
        )
        .arg(
            Arg::with_name(LOCAL_PATH)
                .default_value("./")
                .short("l")
                .takes_value(true)
                .help("Local target path, created if does not exist."),
        )
        .arg(
            Arg::with_name(DELIMITER)
                .default_value("/")
                .short("d")
                .takes_value(true)
                .help("Delimiter to group keys by in s3"),
        )
        .get_matches();

    if !args.is_present(REGION) || !args.is_present(BUCKET) {
        eprintln!("Error! Both region and bucket must be specified!");
        return Ok(());
    }

    let region_arg = args.value_of(REGION).unwrap();
    let region = match Region::from_str(region_arg) {
        Ok(region) => region,
        Err(_) => {
            let endpoint_arg = args
                .value_of(ENDPOINT)
                .expect("s3endpoint must be specified for non-aws region");
            let bucket_arg = args.value_of("BUCKET").unwrap_or("test");

            Region::Custom {
                name: region_arg.to_owned(),
                endpoint: format!("{}/{}", endpoint_arg.to_owned(), bucket_arg),
            }
        }
    };

    let local_path = args.value_of(LOCAL_PATH).unwrap();
    create_dir_all(local_path)
        .unwrap_or_else(|_| panic!("Failed to create new directory: {}", local_path));

    let some_prefix = Some(
        args.value_of(PREFIX)
            .unwrap_or_else(|| panic!("Error with given Prefix!"))
            .to_owned(),
    );

    download_with_channels(region, local_path.to_string(), some_prefix)
}

fn download_with_channels(
    region: Region,
    local_path: String,
    prefix: Option<String>,
) -> std::io::Result<()> {
    let (s, r) = unbounded();
    let provider = ChainProvider::new();
    println!("Provider created!");
    let s3client = S3Client::new_with(HttpClient::new().unwrap(), provider, region);
    println!("S3Client created!");

    let list_objects: ListObjectsV2Request = ListObjectsV2Request {
        prefix,
        ..Default::default()
    };
    let s3objects = match s3client.list_objects_v2(list_objects).sync() {
        Ok(output) => output.contents,
        Err(err) => {
            let mut elements = Element::parse(err.to_string().as_bytes()).unwrap();
            let message = elements.get_child("Message").unwrap().text.as_ref();
            eprintln!("Error: {:?}", message.expect("General Error"));
            return Ok(());
        }
    }
        .unwrap();

    let r2: Receiver<Object> = r.clone();

    thread::spawn(move || {
        while !r2.is_empty() {
            let x = r2.recv();
            println!("Recived!");
            match x {
                Ok(o) => {
                    let result = s3client
                        .get_object(GetObjectRequest {
                            key: o.key.as_ref().expect("Error getting key!").to_string(),
                            ..Default::default()
                        })
                        .sync()
                        .expect("Error GETting object");

                    let path_parts: Vec<&str> = o
                        .key
                        .as_ref()
                        .expect("Error getting object as ref")
                        .split_terminator('/')
                        .collect();
                    let filename = path_parts
                        .last()
                        .expect("Error with getting the filename from path_parts");

                    println!("The file's name is: {}", filename);

                    let stream = result.body.expect("Error getting body from result");
                    let body = stream
                        .concat2()
                        .wait()
                        .expect("Error getting body from stream");

                    println!("Finished reading file to memory");

                    let mut file = File::create(format!("{}/{}", local_path, filename))
                        .expect("Failed to create file");
                    file.write_all(&body).expect("Failed to write to file");
                    println!("Finished writing file!");
                }
                Err(_) => {}
            }
        }
    });

    for o in s3objects {
        println!("Sent!");
        s.send(o).unwrap();
    }

    Ok(())
}

fn download_bucket_with_prefix(
    region: Region,
    local_path: String,
    prefix: Option<String>,
) -> std::io::Result<()> {
    let provider = ChainProvider::new();
    println!("Provider created!");
    let s3client = S3Client::new_with(HttpClient::new().unwrap(), provider, region);
    println!("S3Client created!");

    let list_objects: ListObjectsV2Request = ListObjectsV2Request {
        prefix,
        ..Default::default()
    };
    let s3objects = match s3client.list_objects_v2(list_objects).sync() {
        Ok(output) => output.contents,
        Err(err) => {
            let mut elements = Element::parse(err.to_string().as_bytes()).unwrap();
            let message = elements.get_child("Message").unwrap().text.as_ref();
            eprintln!("Error: {:?}", message.expect("General Error"));
            return Ok(());
        }
    };

    println!("Objects listed correctly");

    s3objects
        .expect("Error getting objects from s3")
        .par_iter()
        .for_each(|o| {
            let result = s3client
                .get_object(GetObjectRequest {
                    key: o.key.as_ref().expect("Error getting key!").to_string(),
                    ..Default::default()
                })
                .sync()
                .expect("Error GETting object");

            let path_parts: Vec<&str> = o
                .key
                .as_ref()
                .expect("Error getting object as ref")
                .split_terminator('/')
                .collect();
            let filename = path_parts
                .last()
                .expect("Error with getting the filename from path_parts");

            println!("The file's name is: {}", filename);

            let stream = result.body.expect("Error getting body from result");
            let body = stream
                .concat2()
                .wait()
                .expect("Error getting body from stream");

            println!("Finished reading file to memory");

            let mut file = File::create(format!("{}/{}", local_path, filename))
                .expect("Failed to create file");
            file.write_all(&body).expect("Failed to write to file");
            println!("Finished writing file!");
        });

    Ok(())
}
