fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true) // peer fan-out uses the generated client
        .compile_protos(
            &["proto/pathlockd.proto", "proto/pathlockd_raft.proto"],
            &["proto"],
        )?;
    println!("cargo:rerun-if-changed=proto/pathlockd.proto");
    println!("cargo:rerun-if-changed=proto/pathlockd_raft.proto");
    Ok(())
}
