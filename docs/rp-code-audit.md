# rp_code Audit

Rithmic's `rp_code` is a **protocol-level** response code attached to request
responses. It signals whether a request succeeded, returned no results, or was
rejected at the application layer. It is NOT a transport or connection-health
signal. Reconnect-driving callers must key exclusively off
`RithmicResponse::is_connection_issue()` and
`RithmicMessage::{ConnectionError, HeartbeatTimeout, ForcedLogout}`, plus
`RithmicError::{ConnectionFailed, ConnectionClosed, SendFailed}`.

## Invariant

Request `rp_code` outcomes are not connection failures. Only explicit
transport failures (WebSocket send errors, ping/heartbeat timeouts, reader
stream errors, forced logout) are actor-fatal. A non-zero `rp_code` surfaces
via `RithmicResponse::request_error() -> Option<RithmicError::RequestRejected(RithmicRequestError)>`;
the raw payload is preserved in `RithmicRequestError.rp_code: Vec<String>`
with derived `code: Option<String>` (first element) and `message: String`
(second element, empty if absent). The raw slice is also reachable via
`RithmicResponse::rp_code()` / `rp_code_first()` / `rp_code_text()`, and the
flat human message remains on `RithmicResponse::error`.

## Current allowlist

`classify_rp_code` normalizes exactly one tuple as a benign empty result:

| rp_code tuple            | Classification | Notes                                         |
| ------------------------ | -------------- | --------------------------------------------- |
| `[]`                     | `Success`      | No response code attached.                    |
| `["0"]`                  | `Success`      | Explicit success.                             |
| `["7", "no data"]` (ci)  | `KnownBenignEmpty` | Case-insensitive on message only.         |
| anything else            | `RequestRejected(RithmicRequestError { rp_code, code: Option<String>, message })` | Surfaces via `RithmicResponse::request_error()`; `response.error` carries the same human message. |

The match on `["7", "no data"]` is exact on the message string (case-insensitive).
Any other message paired with code `"7"` is a real error — see watchpoints below.

## Current captured evidence

See [`rp_code_observations.tsv`](./rp_code_observations.tsv) for the fixtures
this audit is based on. Summary:

- The only `known_benign_empty` tuple observed in captured data is
  `7 / "no data"` — this is the sole allowlist entry. Everything else in the
  captured set rejects as `RithmicError::RequestRejected`.
- `invalid_argument`, `wrong_plant`, and `protocol_error` rows are all
  treated as `RequestRejected` (non-fatal, non-reconnect). They must not tear
  down the actor or drain pending requests.
- Specifically `["7", "an error occurred while parsing data."]`
  (`ResponseOrderSessionConfig`) shares the benign-empty code `"7"` but is a
  real error — our classifier matches the message exactly to avoid swallowing
  it. A unit test covers this fixture.

## Watchpoints — candidates NOT silently normalized

Each of the following response families may, in the wild, legitimately return
a zero-row result that the server encodes as a non-zero `rp_code` tuple. If a
captured fixture proves such a tuple is legitimately-empty → extend
`classify_rp_code` AND add a decode test mirroring `list_accounts_no_data_decodes_as_ok`.
Do NOT widen the allowlist speculatively.

- Symbol / product / instrument discovery (`ResponseSearchSymbols`,
  `ResponseProductCodes`, `ResponseGetInstrumentByUnderlying`,
  `ResponseReferenceData`, `ResponseAuxilliaryReferenceData`,
  `ResponseFrontMonthContract`).
- Account / trade-route / permission / agreement listings
  (`ResponseAccountList`, `ResponseTradeRoutes`,
  `ResponseListExchangePermissions`, `ResponseListAcceptedAgreements`,
  `ResponseListUnacceptedAgreements`, `ResponseShowAgreement`).
- Order / bracket / history listings (`ResponseShowOrders`,
  `ResponseShowOrderHistory`, `ResponseShowOrderHistoryDates`,
  `ResponseShowOrderHistoryDetail`, `ResponseShowOrderHistorySummary`,
  `ResponseShowBrackets`, `ResponseShowBracketStops`,
  `ResponseSubscribeForOrderUpdates`, `ResponseSubscribeToBracketUpdates`).
- Replay / history / snapshot queries (`ResponseReplayExecutions`,
  `ResponseTickBarReplay`, `ResponseTimeBarReplay`,
  `ResponseVolumeProfileMinuteBars`, `ResponseDepthByOrderSnapshot`,
  `ResponseGetVolumeAtPrice`).
- PnL / account / reference lookups that may return zero rows
  (`ResponsePnLPositionSnapshot`, `ResponsePnLPositionUpdates`,
  `ResponseAccountRmsInfo`, `ResponseProductRmsInfo`,
  `ResponseAccountRmsUpdates`).

## Multipart framing — `rq_handler_rp_code` vs `rp_code`

Resolved via §3 ("Responses From Server") of the Rithmic R|Protocol Reference
Guide (`rapi/0.84.0.0/doc/Reference_Guide.pdf`, p. 19):

> Clients should use the following logic to determine end of responses in the
> same sequence. First, check for the presence of `rq_hndlr_rp_code` which
> indicates there are more response messages to receive. In the absence of
> this field, clients should check field `rp_code`, presence of this field
> indicates there are NO more response messages to receive.

The **presence** of `rq_handler_rp_code` — not any particular value inside it —
is what signals "more frames follow". proto3 `repeated string` has no
distinction between "absent" and "empty" on the wire, so our `has_multiple`
keys on non-empty. An intermediate multipart frame may legitimately carry a
non-`"0"` value in `rq_handler_rp_code` (captured samples include `"7"`); keying
the multipart decision on `[0] == "0"` would silently truncate those responses.

The two fields are mutually exclusive on the wire: intermediate frames carry
`rq_handler_rp_code`, the terminal frame carries `rp_code`. `classify_rp_code`
runs only against `rp_code` on the terminal frame.
