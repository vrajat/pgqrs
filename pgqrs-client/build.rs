fn main() {
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(true)
        .out_dir(std::env::var("OUT_DIR").unwrap())
        .compile_protos(&["../proto/queue.proto"], &["../proto"])
        .unwrap();
}
