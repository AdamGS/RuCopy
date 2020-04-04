#![feature(async_closure)]

extern crate rusoto_core;
extern crate rusoto_s3;

use clap::{App, Arg};
use rayon::prelude::*;
use rusoto_core::credential::ChainProvider;
use rusoto_core::{HttpClient, HttpConfig, Region};
use rusoto_s3::{Bucket, GetObjectRequest, ListObjectsV2Request, Object, S3Client, S3};
use std::borrow::Borrow;
use std::fs::create_dir_all;
use std::fs::OpenOptions;
use std::io::{BufWriter, Error, Read, Write};
use std::str::FromStr;

const BUCKET: &str = "BUCKET";
const REGION: &str = "REGION";
const ENDPOINT: &str = "ENDPOINT";
const PREFIX: &str = "PREFIX";
const LOCAL_PATH: &str = "LOCAL_PATH";
const DELIMITER: &str = "DELIMITER";

#[tokio::main]
async fn main() {
    let args = App::new("RuCopy")
        .version("0.1.0")
        .about("Trying to make high-performance cp from s3 in Rust.")
        .arg(
            Arg::with_name(BUCKET)
                .short("b")
                .takes_value(true)
                .required(false)
                .help("Name of your bucket"),
        )
        .arg(
            Arg::with_name(REGION)
                .short("r")
                .takes_value(true)
                .required(false)
                .default_value("eu-central-1")
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

    let local_path = args.value_of(LOCAL_PATH).unwrap();
    create_dir_all(local_path)
        .unwrap_or_else(|_| panic!("Failed to create new directory: {}", local_path));

    let region_arg = args.value_of(REGION).unwrap();
    let bucket_arg = args.value_of(BUCKET).unwrap_or("test");
    let region = match Region::from_str(region_arg) {
        Ok(region) => region,
        Err(_) => {
            let endpoint_arg = args
                .value_of(ENDPOINT)
                .expect("s3endpoint must be specified for non-aws region");

            Region::Custom {
                name: region_arg.to_owned(),
                endpoint: format!("{}/{}", endpoint_arg.to_owned(), bucket_arg),
            }
        }
    };

    let some_prefix = Some(args.value_of(PREFIX).unwrap().to_owned());

    let provider = ChainProvider::new();
    let mut http_provider_config = HttpConfig::new();
    http_provider_config.read_buf_size(1024 * 1024 * 4); // Set buffer size to 4MB
    let s3client = S3Client::new_with(
        HttpClient::new_with_config(http_provider_config).unwrap(),
        provider,
        region,
    );

    let list_objects: ListObjectsV2Request = ListObjectsV2Request {
        prefix: some_prefix,
        bucket: bucket_arg.to_string(),
        ..Default::default()
    };

    let s3objects = match s3client.list_objects_v2(list_objects).await {
        Ok(objects) => objects.contents,
        Err(err) => panic!("Error: {:?}", err),
    };

    let object_iter = s3objects.expect("Got empty results, exiting!");
    println!("Downloading!");
    //for o in  object_iter.iter() {
    while let Some(o) = object_iter.iter().next() {
        tokio::spawn(async move  {
            if o.size.unwrap() == 0 {
                let path_parts: Vec<&str> = o.key.as_ref().unwrap().split_terminator('/').collect();
                let filename = path_parts.last().unwrap();
                println!("File {} length is 0! doing nothing", filename);
            } else {
                let mut result = s3client
                    .get_object(GetObjectRequest {
                        bucket: bucket_arg.to_string(),
                        key: o.key.as_ref().unwrap().to_string(),
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                //.expect("Error Getting object from remote storage endpoint");

                let path_parts: Vec<&str> = o.key.as_ref().unwrap().split_terminator('/').collect();
                let filename = path_parts.last().unwrap();
                println!("Filename: {}", filename);

                let mut stream = result.body.unwrap();
                let mut file = BufWriter::new(
                    OpenOptions::new()
                        .write(true)
                        .create(true)
                        .open(format!("{}/{}", local_path, filename))
                        .expect("Failed to create file"),
                );

                tokio::task::spawn_blocking(|| {
                    std::io::copy(&mut stream.into_blocking_read(), &mut file).unwrap();
                    file.flush();
                });


            }
        });
        //});
    }

    // download_bucket_with_prefix(
    //     region,
    //     bucket_arg.to_string(),
    //     local_path.to_string(),
    //     some_prefix,
    // );
}

// async fn download_bucket_with_prefix(
//     region: Region,
//     bucket: String,
//     local_path: String,
//     prefix: Option<String>,
// ) -> std::io::Result<()> {
//
//
//     Ok(())
// }

// let futures = stream.for_each(move |b| {
//     println!("Filename: {}, chunk_size: {}", filename, b.len());
//     for chunk in b.iter() {
//         file.write_all(chunk).unwrap();
//     }
//     file.flush().unwrap();
// });
//
// let mut runtime = tokio::runtime::Runtime::new().expect("Unable to start runtime!");
// runtime.block_on(futures).unwrap();

//file.flush().unwrap();
//file.get_mut().flush();

//println!("{} - Done!", filename);
