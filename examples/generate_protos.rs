//! Regenerates `src/rti.rs` from the `.proto` files in `src/raw-proto/`.
//! Run via: `cargo run --example generate_protos && cargo fmt`

// Fields Rithmic deprecated, left out of the generated code. Template version
// 5.42 replaced entitlement_flag; 5.55 gave each 32-bit size a `_64` twin.
const RETIRED_FIELDS: &[&str] = &[
    "ResponseListExchangePermissions.entitlement_flag",
    "BestBidOffer.bid_size",
    "BestBidOffer.bid_implicit_size",
    "BestBidOffer.ask_size",
    "BestBidOffer.ask_implicit_size",
    "DepthByOrder.depth_size",
    "ExchangeOrderNotification.quantity",
    "ExchangeOrderNotification.confirmed_size",
    "ExchangeOrderNotification.modified_size",
    "ExchangeOrderNotification.cancelled_size",
    "ExchangeOrderNotification.fill_size",
    "ExchangeOrderNotification.total_fill_size",
    "ExchangeOrderNotification.total_unfilled_size",
    "LastTrade.trade_size",
    "OrderBook.bid_size",
    "OrderBook.impl_bid_size",
    "OrderBook.ask_size",
    "OrderBook.impl_ask_size",
    "RequestBracketOrder.quantity",
    "RequestBracketOrder.target_quantity",
    "RequestBracketOrder.stop_quantity",
    "RequestModifyOrder.quantity",
    "RequestNewOrder.quantity",
    "RequestOCOOrder.quantity",
    "ResponseDepthByOrderSnapshot.depth_size",
    "ResponseGetVolumeAtPrice.volume_at_price",
    "ResponseVolumeProfileMinuteBars.profile_no_aggressor_volume",
    "ResponseVolumeProfileMinuteBars.profile_bid_volume",
    "ResponseVolumeProfileMinuteBars.profile_ask_volume",
    "ResponseVolumeProfileMinuteBars.profile_no_aggressor_trades",
    "ResponseVolumeProfileMinuteBars.profile_bid_aggressor_trades",
    "ResponseVolumeProfileMinuteBars.profile_ask_aggressor_trades",
    "RithmicOrderNotification.quantity",
    "RithmicOrderNotification.quan_release_pending",
    "RithmicOrderNotification.total_fill_size",
    "RithmicOrderNotification.total_unfilled_size",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").map_err(
        |_| "CARGO_MANIFEST_DIR not set: run this via `cargo run --example generate_protos`",
    )?);
    let src_dir = manifest_dir.join("src");
    let proto_dir = src_dir.join("raw-proto");
    let mut protos = std::fs::read_dir(&proto_dir)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;
    protos.retain(|path| path.extension().is_some_and(|ext| ext == "proto"));
    // Change password has no template ids in the Reference Guide; order session
    // config (3502/3503) is deprecated in favor of session config (24/25).
    const SKIPPED: &[&str] = &[
        "request_change_password_on_behalf_of.proto",
        "response_change_password_on_behalf_of.proto",
        "request_order_session_config.proto",
        "response_order_session_config.proto",
    ];
    protos.retain(|path| !SKIPPED.iter().any(|name| path.ends_with(name)));
    protos.sort();

    let mut config = prost_build::Config::new();
    // Rithmic adds fields to existing messages in most template releases, so a
    // generated type without this makes every proto refresh a semver-major event here.
    config.type_attribute(".", "#[non_exhaustive]");
    config.out_dir(&src_dir);
    let mut fds = config.load_fds(&protos, &[&proto_dir])?;

    for path in RETIRED_FIELDS {
        let (message, field) = path.split_once('.').expect("Message.field");
        let mut removed = false;

        for file in &mut fds.file {
            let Some(m) = file.message_type.iter().position(|d| d.name() == message) else {
                continue;
            };
            let fields = &mut file.message_type[m].field;
            let Some(f) = fields.iter().position(|d| d.name() == field) else {
                continue;
            };
            fields.remove(f);
            removed = true;

            // Doc comments are matched to fields by index, at path [4, m, 2, f, ..].
            let (m, f) = (m as i32, f as i32);
            if let Some(info) = file.source_code_info.as_mut() {
                info.location.retain(|l| !l.path.starts_with(&[4, m, 2, f]));
                for l in &mut info.location {
                    if l.path.len() >= 4 && l.path[..3] == [4, m, 2] && l.path[3] > f {
                        l.path[3] -= 1;
                    }
                }
            }
        }

        assert!(removed, "{path} is no longer in the protos");
    }

    config.compile_fds(fds)?;

    let rti_path = src_dir.join("rti.rs");
    let code = std::fs::read_to_string(&rti_path)?;
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

    std::fs::write(&rti_path, detabbed)?;

    println!("Generated src/rti.rs. Run `cargo fmt` to format it.");
    Ok(())
}
