//! Regenerates `src/rti.rs` from the `.proto` files in `src/raw-proto/`.
//! Run via: `cargo run --example generate_protos && cargo fmt`

// Template version 5.55 deprecated these 32-bit fields. Each has a `_64` twin.
const DEPRECATED_32_BIT_FIELDS: &[&str] = &[
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
    // No template ids in the Reference Guide, so the receiver could not decode them.
    protos.retain(|path| {
        !path.ends_with("request_change_password_on_behalf_of.proto")
            && !path.ends_with("response_change_password_on_behalf_of.proto")
    });
    protos.sort();

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

    for path in DEPRECATED_32_BIT_FIELDS {
        let (_, field) = path.split_once('.').expect("Message.field");
        let note = format!("#[deprecated = \"use {field}_64 instead\"]");
        config.field_attribute(format!(".rti.{path}"), note);
    }

    config.out_dir(&src_dir);
    config.compile_protos(&protos, &[&proto_dir])?;

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
