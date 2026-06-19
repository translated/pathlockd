fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true) // peer fan-out uses the generated client
        // The REST/HTTP-3 facade (src/web) speaks JSON over the same message
        // types the gRPC service uses. Derive serde directly on the generated
        // prost types so the web layer is a thin JSON<->proto bridge with no
        // duplicate DTOs. `serde(default)` is messages-only (it is invalid on
        // the C-like enums `type_attribute` also covers), so a JSON body may
        // omit fields and get proto3 defaults.
        .type_attribute(".", "#[derive(::serde::Serialize, ::serde::Deserialize)]")
        .message_attribute(".", "#[serde(default, rename_all = \"camelCase\")]")
        .compile_protos(
            &["proto/pathlockd.proto", "proto/pathlockd_raft.proto"],
            &["proto"],
        )?;
    println!("cargo:rerun-if-changed=proto/pathlockd.proto");
    println!("cargo:rerun-if-changed=proto/pathlockd_raft.proto");
    Ok(())
}
