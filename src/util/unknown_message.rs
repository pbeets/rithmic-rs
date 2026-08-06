//! Frames whose `template_id` this crate doesn't map.

use prost::bytes::Bytes;
use std::fmt;

/// Payload bytes rendered by the [`fmt::Display`] and [`fmt::Debug`] impls
/// before eliding. [`UnknownTemplateMessage::payload_hex`] is the full form.
const MAX_RENDERED_BYTES: usize = 32;

/// A frame whose `template_id` this crate has no message definition for.
///
/// The payload is kept as received, so a template this crate doesn't map can
/// still be handled downstream: [`decode_as`](Self::decode_as) decodes it into a
/// type you generate yourself, and [`payload_hex`](Self::payload_hex) dumps it
/// for later.
///
/// # Examples
///
/// A frame off a subscription stream arrives as
/// [`RithmicMessage::UnknownTemplate`](crate::rti::messages::RithmicMessage::UnknownTemplate)
/// with `error: None`. Log it, then decode it into a type generated in your own
/// crate from the `.proto`; [`crate::prost`] is re-exported so the generated
/// code can't drift from the version this crate decodes with.
///
/// ```
/// use rithmic_rs::prost;
/// use rithmic_rs::rti::messages::RithmicMessage;
///
/// // Generated in your crate by prost-build, once you know what 358 maps to.
/// #[derive(Clone, PartialEq, prost::Message)]
/// pub struct Template358 {
///     #[prost(string, optional, tag = "110100")]
///     pub symbol: Option<String>,
/// }
///
/// fn on_message(message: &RithmicMessage) {
///     let RithmicMessage::UnknownTemplate(frame) = message else {
///         return;
///     };
///
///     // template_id=358 (84 bytes) a2e135054d45535536aae13503434d45…+52B
///     tracing::warn!(payload = %frame.payload_hex(), "unmapped template: {frame}");
///
///     if frame.template_id == 358 {
///         if let Ok(decoded) = frame.decode_as::<Template358>() {
///             println!("{:?}", decoded.symbol);
///         }
///     }
/// }
/// ```
///
/// [`payload_hex`](Self::payload_hex) is untruncated, so a frame captured in
/// production can be replayed in a test through
/// [`from_payload_hex`](Self::from_payload_hex).
#[derive(Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct UnknownTemplateMessage {
    /// The `template_id` read from the frame header.
    pub template_id: i32,
    /// The complete message body, exactly as received.
    ///
    /// Only the 4-byte big-endian length prefix is stripped, as every other
    /// decoder here does; it carries nothing beyond `payload.len()`.
    pub payload: Bytes,
}

impl UnknownTemplateMessage {
    /// Build a frame from its `template_id` and body.
    pub fn new(template_id: i32, payload: Bytes) -> Self {
        Self {
            template_id,
            payload,
        }
    }

    /// Decode the payload into a caller-supplied protobuf type.
    ///
    /// Generate the type from the `.proto` in your own crate and decode into it.
    /// Uses the [`crate::prost`] re-export, so types generated against
    /// `rithmic_rs::prost` are compatible by construction.
    ///
    /// # Errors
    ///
    /// [`prost::DecodeError`] when the bytes are structurally incompatible with
    /// `M`, such as a wire-type conflict on a field `M` declares.
    ///
    /// `Ok` is not proof the type was guessed right. Protobuf skips fields the
    /// target doesn't declare, so an unrelated payload usually decodes into a
    /// mostly-empty value.
    pub fn decode_as<M: prost::Message + Default>(&self) -> Result<M, prost::DecodeError> {
        M::decode(self.payload.clone())
    }

    /// The whole payload as lowercase hex, no truncation, no `0x` prefix.
    ///
    /// `Display` elides the payload to stay readable in a log line; this
    /// doesn't.
    pub fn payload_hex(&self) -> String {
        use fmt::Write;

        let mut hex = String::with_capacity(self.payload.len() * 2);

        for byte in &self.payload {
            // Writing to a String is infallible.
            let _ = write!(hex, "{byte:02x}");
        }

        hex
    }

    /// Rebuild a frame from a [`payload_hex`](Self::payload_hex) dump.
    ///
    /// Ignores a leading `0x` and any whitespace, including newlines from a
    /// wrapped log line. `None` unless `hex` holds an even number of hex
    /// digits; empty input yields an empty payload.
    pub fn from_payload_hex(template_id: i32, hex: &str) -> Option<Self> {
        let hex = hex.trim();
        let hex = hex
            .strip_prefix("0x")
            .or(hex.strip_prefix("0X"))
            .unwrap_or(hex);

        let digits: Vec<u8> = hex
            .chars()
            .filter(|character| !character.is_whitespace())
            .map(|character| character.to_digit(16).map(|digit| digit as u8))
            .collect::<Option<_>>()?;

        if digits.len() % 2 != 0 {
            return None;
        }

        let payload: Vec<u8> = digits
            .chunks_exact(2)
            .map(|pair| (pair[0] << 4) | pair[1])
            .collect();

        Some(Self::new(template_id, Bytes::from(payload)))
    }
}

impl fmt::Display for UnknownTemplateMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "template_id={} ({} bytes)",
            self.template_id,
            self.payload.len()
        )?;

        if self.payload.is_empty() {
            return Ok(());
        }

        write!(f, " {}", Hex(&self.payload))
    }
}

impl fmt::Debug for UnknownTemplateMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The derive would dump the payload as a list of byte literals.
        f.debug_struct("UnknownTemplateMessage")
            .field("template_id", &self.template_id)
            .field("payload_len", &self.payload.len())
            .field("payload", &format_args!("{}", Hex(&self.payload)))
            .finish()
    }
}

/// Renders bytes as hex, capped at [`MAX_RENDERED_BYTES`].
struct Hex<'a>(&'a [u8]);

impl fmt::Display for Hex<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0.iter().take(MAX_RENDERED_BYTES) {
            write!(f, "{byte:02x}")?;
        }

        let remaining = self.0.len().saturating_sub(MAX_RENDERED_BYTES);

        if remaining > 0 {
            write!(f, "…+{remaining}B")?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;
    use crate::rti::{RequestCancelAllOrders, RithmicOrderNotification};

    fn frame<M: Message>(template_id: i32, message: &M) -> UnknownTemplateMessage {
        UnknownTemplateMessage {
            template_id,
            payload: Bytes::from(message.encode_to_vec()),
        }
    }

    fn notification() -> RithmicOrderNotification {
        RithmicOrderNotification {
            template_id: 358,
            basket_id: Some("9214-2".to_string()),
            symbol: Some("MESU6".to_string()),
            price: Some(6412.25),
            ..RithmicOrderNotification::default()
        }
    }

    #[test]
    fn decodes_into_a_caller_supplied_type() {
        let original = notification();

        let decoded: RithmicOrderNotification = frame(358, &original)
            .decode_as()
            .expect("payload round-trips into the matching type");

        assert_eq!(decoded, original);
    }

    #[test]
    fn decode_as_reports_structurally_incompatible_bytes() {
        // template_id (154467) is an int32 everywhere, so sending it as
        // length-delimited conflicts with what the target declares.
        // Key = (154467 << 3) | 2, varint-encoded, then a 2-byte string.
        let payload = vec![0x9a, 0xb6, 0x4b, 0x02, b'h', b'i'];

        let frame = UnknownTemplateMessage {
            template_id: 358,
            payload: Bytes::from(payload),
        };

        assert!(frame.decode_as::<RequestCancelAllOrders>().is_err());
    }

    #[test]
    fn decode_as_can_succeed_against_the_wrong_type() {
        // Guards the documented caveat: unknown fields are skipped, so this
        // decodes fine and drops everything but template_id.
        let decoded = frame(358, &notification())
            .decode_as::<RequestCancelAllOrders>()
            .expect("unknown fields are skipped, so this decodes");

        assert_eq!(decoded.template_id, 358);
        assert_eq!(decoded.account_id, None);
    }

    #[test]
    fn payload_hex_round_trips_verbatim() {
        let captured = frame(358, &notification());
        let hex = captured.payload_hex();

        // Complete, unlike Display.
        assert_eq!(hex.len(), captured.payload.len() * 2);
        assert!(hex.chars().all(|character| character.is_ascii_hexdigit()));

        let replayed = UnknownTemplateMessage::from_payload_hex(358, &hex)
            .expect("payload_hex output parses back");

        assert_eq!(replayed, captured);
    }

    #[test]
    fn from_payload_hex_tolerates_copy_paste() {
        let expected = UnknownTemplateMessage {
            template_id: 358,
            payload: Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]),
        };

        for input in [
            "deadbeef",
            "DEADBEEF",
            "0xdeadbeef",
            " dead beef\n",
            "dead\nbeef",
        ] {
            assert_eq!(
                UnknownTemplateMessage::from_payload_hex(358, input).as_ref(),
                Some(&expected),
                "{input:?}"
            );
        }
    }

    #[test]
    fn from_payload_hex_rejects_malformed_input() {
        // Odd digit count, and a non-hex character.
        assert_eq!(UnknownTemplateMessage::from_payload_hex(358, "abc"), None);
        assert_eq!(UnknownTemplateMessage::from_payload_hex(358, "zz"), None);
    }

    #[test]
    fn display_elides_a_long_payload() {
        let frame = UnknownTemplateMessage {
            template_id: 358,
            payload: Bytes::from(vec![0xab; MAX_RENDERED_BYTES + 20]),
        };

        let rendered = frame.to_string();

        assert!(
            rendered.starts_with("template_id=358 (52 bytes) "),
            "{rendered}"
        );
        assert!(rendered.ends_with("…+20B"), "{rendered}");
        assert_eq!(frame.payload_hex().len(), 104);
    }
}
