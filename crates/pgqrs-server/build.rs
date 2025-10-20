use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:warning=pgqrs-server build.rs starting");

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let proto_dir = manifest_dir.join("../../proto");
    let queue_proto = proto_dir.join("queue.proto");

    println!("cargo:warning=proto_dir = {}", proto_dir.display());
    println!("cargo:warning=queue_proto = {}", queue_proto.display());
    println!("cargo:rerun-if-changed={}", queue_proto.display());
    println!("cargo:rerun-if-changed={}", proto_dir.display());
    println!("cargo:rerun-if-changed={}", queue_proto.display());
    println!("cargo:rerun-if-changed={}", proto_dir.display());

        // TEMP: prove OUT_DIR & stop
    let out_dir = env::var("OUT_DIR")?;
    println!("cargo:warning=OUT_DIR = {out_dir}");

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        . compile_protos(&[queue_proto], &[proto_dir])?;

    println!("cargo:warning=pgqrs-server build.rs finished");
    Ok(())
}
