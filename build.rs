fn main() {
    tonic_build::configure()
        .compile(
            &[
                "proto/node_protocol.proto",
                "proto/client_protocol.proto",
                "proto/datatransfer_protocol.proto",
            ],
            &["proto"],
        )
        .unwrap();
}
