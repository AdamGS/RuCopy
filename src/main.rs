extern crate fibers;
extern crate futures;
extern crate rusoto_core;
extern crate rusoto_s3;

use rusoto_core::Region;
use rusoto_s3::S3Client;

fn main() {
    let s3client = S3Client::new(Region::ApEast1);
}
