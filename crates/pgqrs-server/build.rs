fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir(std::env::var("OUT_DIR").unwrap())
        .compile(&["../../proto/queue.proto"], &["../../proto"])
        .unwrap();
}
