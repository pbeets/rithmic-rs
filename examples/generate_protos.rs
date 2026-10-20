//! Regenerates `src/rti.rs` from a directory of vendor `.proto` files.
//! Run via: `cargo run --example generate_protos -- /path/to/release/proto && cargo fmt`
//! With no argument, reads `src/raw-proto/`.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").map_err(
        |_| "CARGO_MANIFEST_DIR not set: run this via `cargo run --example generate_protos`",
    )?);
    let src_dir = manifest_dir.join("src");
    let proto_dir = std::env::args_os()
        .nth(1)
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| src_dir.join("raw-proto"));
    // 0.90 no longer ships otps_proto_pool.proto. Compile every supplied schema
    // directly, including agreement and MessageType definitions omitted by older pools.
    let mut protos = std::fs::read_dir(&proto_dir)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;
    protos.retain(|path| path.extension().is_some_and(|ext| ext == "proto"));
    // These administrative password schemas have no template IDs in the
    // published reference guide and are outside this client's supported surface.
    protos.retain(|path| {
        !matches!(
            path.file_name().and_then(|name| name.to_str()),
            Some(
                "request_change_password_on_behalf_of.proto"
                    | "response_change_password_on_behalf_of.proto"
            )
        )
    });
    protos.sort();
    if protos.is_empty() {
        return Err(format!("no .proto files in {}", proto_dir.display()).into());
    }
    // Keep the checked-in bindings intact until compilation and postprocessing
    // both succeed, and avoid sharing temporary output with other generator runs.
    let output_dir = tempfile::tempdir()?;

    let mut config = prost_build::Config::new();
    // Rithmic adds fields to existing messages in most template releases, so a
    // generated type without this makes every proto refresh a semver-major event here.
    config.type_attribute(".", "#[non_exhaustive]");
    // Rithmic deprecated entitlement_flag in template version 5.42 in favor of
    // the level_1_market_data / level_2_market_data fields.
    config.field_attribute(
        ".rti.ResponseListExchangePermissions.entitlement_flag",
        "#[deprecated = \"use level_1_market_data and level_2_market_data instead\"]",
    );
    config.out_dir(output_dir.path());
    config.compile_protos(&protos, &[&proto_dir])?;

    let pool_path = src_dir.join("rti.rs");
    let code = std::fs::read_to_string(output_dir.path().join("rti.rs"))?;
    let combined = format!("pub mod messages;\n\n{code}");

    // Some Rithmic comments contain tabs, which prost copies into doc comments
    // and clippy rejects. Strip them here so a proto refresh cannot break the
    // build. Code is untouched; prost indents with spaces.
    let mut detabbed = String::with_capacity(combined.len());

    for line in combined.lines() {
        let trimmed = line.trim_start();

        if trimmed.starts_with("///") || trimmed.starts_with("//!") {
            detabbed.push_str(&line.replace('\t', " "));
        } else {
            detabbed.push_str(line);
        }

        detabbed.push('\n');
    }

    std::fs::write(&pool_path, detabbed)?;

    println!("Generated src/rti.rs. Run `cargo fmt` to format it.");
    Ok(())
}
