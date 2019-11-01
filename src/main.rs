extern crate futures;
extern crate rusoto_core;
extern crate rusoto_s3;

use clap::{App, Arg};
use futures::{Future, Stream};
use rayon::prelude::*;
use rusoto_core::credential::ChainProvider;
use rusoto_core::{HttpClient, Region};
use rusoto_s3::{GetObjectRequest, ListObjectsV2Request, S3Client, S3};
use std::fs::File;
use std::io::Write;
use std::str::FromStr;

fn main() {
    let args = App::new("Rucopy")
        .version("0.0.1")
        .about("Trying to make high-performance cp from s3 in Rust.")
        .arg(
            Arg::with_name("BUCKET")
                .short("b")
                .takes_value(true)
                .required(false)
                .help("Name of your bucket"),
        )
        .arg(
            Arg::with_name("REGION")
                .short("r")
                .takes_value(true)
                .required(true)
                .default_value("ap-east-1")
                .help("Specify AWS region"),
        )
        .arg(
            Arg::with_name("S3_ENDPOINT")
                .short("e")
                .takes_value(true)
                .required(false)
                .help("Your s3 endpoint"),
        )
        .arg(
            Arg::with_name("REMOTE_PATH")
                .default_value("/")
                .required(false)
                .short("p")
                .help("The path inside the specified bucket"),
        )
        .arg(
            Arg::with_name("LOCAL_PATH")
                .default_value("./")
                .short("l")
                .help("Local target path, created if does not exist."),
        )
        .get_matches();

    let region_arg = args.value_of("REGION").unwrap();

    let region = match Region::from_str(region_arg) {
        Ok(region) => region,
        Err(_) => {
            let endpoint_arg = args
                .value_of("S3_ENDPOINT")
                .expect("s3endpoint must be specified for non-aws region");
            let bucket = args.value_of("BUCKET").unwrap_or("test");

            Region::Custom {
                name: region_arg.to_owned(),
                endpoint: format!("{}/{}", endpoint_arg.to_owned(), bucket),
            }
        }
    };

    let provider = ChainProvider::new();

    let s3client = S3Client::new_with(HttpClient::new().unwrap(), provider, region);

    let list_objects: ListObjectsV2Request = Default::default();
    let s3objects = match s3client.list_objects_v2(list_objects).sync() {
        Ok(output) => output.contents,
        Err(err) => {
            eprintln!("Error right here?: {:?}", err);
            None
        }
    };

    s3objects.unwrap().par_iter().for_each(|o| {
        let result = s3client
            .get_object(GetObjectRequest {
                key: o.key.as_ref().unwrap().to_string(),
                ..Default::default()
            })
            .sync()
            .expect("Error GETting object");

        let stream = result.body.unwrap();
        let body = stream.concat2().wait().unwrap();

        let local_path = args.value_of("LOCAL_PATH").unwrap();

        let mut file = File::create(format!(
            "{}/{}",
            local_path,
            o.key.as_ref().unwrap().to_string()
        ))
        .expect("Failed to create file");
        file.write_all(&body).expect("Failed to write to file");
    });
}
