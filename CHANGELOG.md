# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.1.0]

### Requests no longer time out

The library no longer times out requests. A reply can arrive after the timeout
has passed, and handling that added more complexity. It is up to the consumer
to add their own timeout logic — see `examples/request_timeout.rs` for one way
to do it.

### Deprecated

These still compile and are still accepted, but the library ignores them.
They go away in 4.0.0.

- `RithmicConfig::request_timeout`
- `RithmicConfigBuilder::request_timeout()`
- `DEFAULT_REQUEST_TIMEOUT`
- `RithmicError::RequestTimeout` — never returned any more
- `RITHMIC_REQUEST_TIMEOUT_SECS` — still checked for a bad value, then ignored

### Added

- `examples/request_timeout.rs` — how to add your own timeout to a request.

### Fixed

- When a frame fails to decode, the log now names the request it belonged to,
  the template it came from, and where it was routed. It used to log the decode
  error on its own, which left you unable to tell which call lost its reply.
- When part of a multi-part response arrives for a request that is no longer
  waiting, it is now logged. It used to be thrown away silently.

## [3.0.0]

Order commands are now built with `::new()` and chained setters, the generated
protobuf enums are replaced by crate-owned ones, and every order call takes a
command struct.

**[MIGRATING.md](MIGRATING.md) has before/after code for every change below.**

### Behavior changes the compiler will not catch

Nothing here needs a code change. Read it before the first run against a live
account.

- **Orders route off the exchange's published trade route** instead of a hardcoded
  `"globex"`/`"simulator"`. With no route published and none set on the command,
  the order is **not sent** — you get `RithmicError::NoTradeRoute`.
- **`cancel_all_orders` is attributed `Auto`, not `Manual`.** Set
  `.manual_or_auto(ManualOrAutoEntry::Manual)` to keep the old attribution.
- **A target-only bracket now sends `TARGET_ONLY_STATIC`** rather than
  `TargetAndStopStatic`, which 2.x sent whatever legs you supplied.
- **`get_account_list` and `get_account_rms_info` may return fewer accounts**, now
  that requests are scoped with the real login info instead of a hardcoded `Trader`
  user type.
- **Market orders no longer go out priced at `0.0`** — an unset price is omitted.
- **An empty `user_tag` echoes back as `None`, not `Some("")`.**

### Breaking Changes

- **The `load_*` replay methods validate before sending.** A missing symbol or
  exchange, a `bar_length` or `bar_type_period` below 1, a non-positive
  timestamp, or an `end_time_sec` before `start_time_sec` returns
  `RithmicError::InvalidArgument` without a round trip.

- **Order command types are their own builders and are `#[non_exhaustive]`.**
  `..Default::default()` no longer compiles on them outside this crate. `new()`
  takes no arguments, every field has a setter of the same name, and `build()`
  runs `validate()` and returns `Result<_, RithmicError>`.

  ```rust
  let order = RithmicOrder::new()
      .symbol("ESM6")
      .exchange("CME")
      .quantity(1)
      .transaction_type(OrderSide::Buy)
      .price_type(OrderType::Limit)
      .price(5000.0)
      .build()?;
  ```

  Fields stay public, so direct assignment still works — but the handles send what
  they are given, so a command that skips `build()` skips validation and the
  derived `bracket_type`.

- **`RithmicConfig`, `RithmicAccount`, `LoginConfig`, `InstrumentInfo`,
  `InstrumentInfoError`, `RithmicEnv`, `TrailingStop`, `RithmicIfTouchedTrigger`
  and all 246 generated protobuf types are `#[non_exhaustive]`.** Struct literals
  stop compiling downstream. Use the builder where there is one, `Default::default()`
  plus field assignment on the generated types, and a `_` arm on matches over
  generated enums.

- **Fourteen generated enum re-exports leave the crate root, replaced by
  crate-owned enums.** Variant names are unchanged, so the arms port over as
  written. Two things do not: the crate-owned enums are `#[non_exhaustive]`, so
  every `match` needs a `_` arm, and `OrderType` carries six variants where
  `OcoPriceType` had four. The prost-specific surface (`as i32`, `try_from`,
  `from_str_name`, derived `Ord`) does not carry over; `as_str_name` does.

  | removed | use instead |
  |---|---|
  | `BracketTransactionType`, `OcoTransactionType`, `NewOrderTransactionType` | `OrderSide` |
  | `BracketPriceType`, `OcoPriceType`, `NewOrderPriceType`, `ModifyPriceType` | `OrderType` |
  | `BracketDuration`, `OcoDuration`, `NewOrderDuration` | `TimeInForce` |
  | `BracketCondition` | `OrderCondition` |
  | `BracketPriceField` | `OrderPriceField` |

  The remaining two, `BracketType` and `EasyToBorrowRequest`, keep their names but now resolve to
  crate-owned enums rather than `rti::request_bracket_order::BracketType` and
  `rti::request_easy_to_borrow_list::Request`.

- **Seven handle method names change, covering ten methods** (`list_system_info`
  exists on all four plants). Each is now named after the request it sends.
  Nothing was added or removed, and no signatures changed except `adjust_target`,
  which also takes a command struct (below).

  | old | new | handle |
  |---|---|---|
  | `list_system_info` | `get_system_info` | all four plants |
  | `list_exchanges` | `list_exchange_permissions` | ticker |
  | `request_depth_by_order_snapshot` | `get_depth_by_order_snapshot` | ticker |
  | `subscribe_order_book` | `subscribe_depth_by_order_update` | ticker |
  | `unsubscribe_order_book` | `unsubscribe_depth_by_order_update` | ticker |
  | `pnl_position_snapshots` | `get_pnl_position_snapshot` | pnl |
  | `adjust_profit` | `adjust_target` | order |

- **`RithmicAdvancedBracketOrder` is removed; `RithmicBracketOrder` does everything
  it did.** One bracket type now carries every venue-native field, and
  `place_advanced_bracket_order` is gone — call `place_bracket_order`. On the plain
  bracket, `profit_ticks: i32` and `stop_ticks: i32` become `target_ticks: Vec<i32>`
  and `stop_ticks: Vec<i32>`, one entry per exit leg. An unset `bracket_type` is
  derived from the legs present, which changes a target-only bracket to send
  `TARGET_ONLY_STATIC`. See [MIGRATING.md](MIGRATING.md) §5.

- **`price` on `RithmicOrder`, `RithmicOcoOrderLeg` and `RithmicModifyOrder` is
  `Option<f64>`.** Wrap existing values in `Some(..)` and pass `None` for market
  orders, which previously went out at `0.0`. An unset price is now omitted
  entirely, so it can no longer reach the wire as `0.0` or stand in as a `0.0`
  trigger.

- **Order calls take command structs.** `cancel_all_orders()` takes a
  `RithmicCancelAllOrders` and `exit_position(symbol, exchange)` takes a
  `RithmicExitPosition`, matching every other order call. Origination moves onto
  the command as a `.manual_or_auto(..)` field, defaulting to `Auto`.

- **`adjust_target`/`adjust_stop` take a `RithmicBracketLevelAdjustment`** instead
  of `(id, ticks)`. The new `level` field selects which bracket leg to adjust;
  `level: None` keeps the old behavior.

- **`load_volume_profile_minute_bars` takes a `VolumeProfileMinuteBarsRequest`**
  instead of seven positional arguments. See [MIGRATING.md](MIGRATING.md) §6.

- **`TrailingStop` and `RithmicIfTouchedTrigger` are built with `new()`, chained
  setters and `build()`** — they have no separate `validate()` — and neither
  implements `Default`; `TrailingStop` had one at 2.0.0.
  `TrailingStop` gains a required `trail_by_price_id: i32`; `build()` refuses a
  zero id and a `trail_by_ticks` below one.
  `RithmicIfTouchedTrigger::price` is `Option<f64>` and its `build()` requires a
  symbol, an exchange and the price. An unset price is omitted rather than sent as
  `0.0`, which the default `GreaterThanEqualTo`/`TradePrice` condition would fire
  on immediately.

  ```rust
  let stop = TrailingStop::new().trail_by_ticks(15).trail_by_price_id(7).build()?;
  ```

- **`RithmicConfig` gains a required `request_timeout: Duration`.** Build it with
  `RithmicConfig::builder(env)` or `RithmicConfigBuilder::from_env(env)` — the type
  is `#[non_exhaustive]`, so struct literals no longer compile downstream anyway.

- **`subscribe_account_rms_updates` gains a required `update_bits` parameter.**
  Pass `vec![]` for the old behavior.

- **The bundled protos move to R | Protocol API 0.89.0.0 (template version 5.42),
  and `RithmicMessage::AccountListUpdates` is removed.** Rithmic dropped
  `account_list_updates.proto` (template 354) and points to the account RMS updates
  stream instead. `UserAccountUpdate::update_type` and `access_type` are now
  `Option<String>`, and the generated `UpdateType`/`AccessType` enums are gone.

- **The `ws` module is private.** `ConnectStrategy` was its only public item and
  stays at the crate root: spell `rithmic_rs::ws::ConnectStrategy` as
  `rithmic_rs::ConnectStrategy`.

- **Smaller changes.** All mechanical.

  | change | what to do |
  |---|---|
  | `RithmicModifyOrder::qty` renamed to `quantity` | rename field and setter |
  | `RithmicOrder::duration` is `TimeInForce`, not `Option<TimeInForce>` | unwrap; `None` already sent `Day`, so the wire is unchanged |
  | `ConfigError::InvalidValue` is `#[non_exhaustive]` at the variant | destructuring `{ var, reason }` needs a `..` |
  | new `Option` fields on the command types | leave `None` to keep old behavior |

### Added

#### New calls

- **`show_fill_history(range, max_record_count)`** on the order handle — the
  account's fill history (templates 3512/3513), one response per fill. Pass a range
  in either format the request accepts, and `None` for an uncapped count; anything
  outside 0–10,000 is refused with `RithmicError::InvalidArgument`.

  ```rust
  let fills = handle
      .show_fill_history(FillHistoryRange::trade_date(20260801, 20260809), None)
      .await?;
  ```

- **`get_user_info(user)`** on the order handle — a user's profile, entitlement
  status and session limits (templates 3510/3511); pass `None` for the logged-in
  user. The unsolicited **`RithmicMessage::UserInfoUpdate`** (template 357) carries
  user-level changes as they happen; it previously surfaced as `Unknown` with a
  `ProtocolError`.

- **`load_ticks_all`, `load_tick_bars_all` and `load_time_bars_all`** — replay
  loaders that return the whole window. The plain loaders stop at 10,000 records
  and give no sign they did, so prefer these unless you want the cap.

  ```rust
  let bars = handle
      .load_time_bars_all(symbol, exchange, TimeBarType::MinuteBar, 5, start, end)
      .await?;
  ```

  The window is buffered before the call returns — a 23-hour ES session runs to
  hundreds of thousands of records in memory.

#### Command types and fields

- **`RithmicCancelAllOrders`, `RithmicExitPosition`, `RithmicLinkOrders`,
  `RithmicModifyOrderReferenceData` and `RithmicBracketLevelAdjustment`** — command
  structs for the order calls that previously took loose arguments. Every order
  call now takes one.

- **New command fields.** `window_name` on `RithmicOrder` and the bracket, modify,
  cancel, exit and OCO-leg types; `release_at_*`, `cancel_at_*`, `cancel_after_secs`
  and `if_touched` on `RithmicOrder`; group-level cancel timing on `RithmicOcoOrder`;
  `trigger_price`, `trail_by_ticks` and `if_touched` on `RithmicModifyOrder`; and
  `trading_algorithm` on `RithmicExitPosition`. With these, every field of the
  eleven order request messages that describes the order is reachable from a
  command type. The session-scoped fields — `template_id`, `user_msg`, `fcm_id`,
  `ib_id`, `account_id` and `user_type` — still come from the login and the
  account. On the OCO leg,
  `window_name` is all-or-none across legs like `user_tag`: set it on one leg and
  every leg gets a slot.

- **`validate()` on every command type that has something to check**, public as
  well as called by `build()`. The commands that carry an instrument —
  `RithmicOrder`, `RithmicBracketOrder`, `RithmicOcoOrderLeg` and
  `RithmicModifyOrder` — need a symbol, an exchange and a positive quantity. The
  ones that name an existing order — `RithmicModifyOrder`, `RithmicCancelOrder`,
  `RithmicBracketLevelAdjustment` and `RithmicModifyOrderReferenceData` — need its
  basket id, and `RithmicLinkOrders` needs at least two. On top of that, `Limit`,
  `StopLimit` and `LimitIfTouched` need a price, and the stop and if-touched types
  need a trigger; `Market` needs neither. On `RithmicModifyOrder` a `price` stands
  in for a missing `trigger_price`, matching the fallback the sender applies.
  Failures return `RithmicError::InvalidArgument`. Nothing beyond this is checked
  locally.

- **`RithmicExitPosition` can flatten the whole account.** `symbol` and `exchange`
  are `Option<String>` and come as a pair: set both to exit one instrument, set
  neither to exit every position on the account.

  ```rust
  handle.exit_position(RithmicExitPosition::new().build()?).await?; // flatten all
  ```

- **Multi-leg OCO orders.** A `RithmicOcoOrder` takes two or more legs instead of a
  fixed pair, each able to carry its own trailing stop. Fewer than two legs is
  rejected.

- **`RithmicBracketOrder::operation_type` and the `BracketOperationType` enum** —
  the `order_operation_type` field added in 5.37, selecting which event on one
  order of the bracket cancels the rest. Left unset it stays off the wire.

- **Per-order trade routes.** The new `trade_route` field on `RithmicOrder`,
  `RithmicBracketOrder` and `RithmicOcoOrderLeg` overrides the route the crate
  would pick, including one the server never published. On the order handle,
  `trade_route_for(exchange)` reports the route an order would take now and
  `record_trade_route(update)` applies a `TradeRoute` update to the cache.
  `RithmicError::NoTradeRoute` comes back when no route exists and the order set
  none.

  ```rust
  let route = handle.trade_route_for("CME").await?;      // what an order would use
  let order = RithmicOrder::new().trade_route("globex"); // or pick one yourself
  ```

#### Types and re-exports

- **Crate-owned `ManualOrAutoEntry`, `OrderCondition`, `OrderPriceField` and
  `RmsUpdateBits`** at the crate root, joining `OrderSide`, `OrderType` and
  `TimeInForce`. `ManualOrAutoEntry` sets order origination and defaults to `Auto`
  on every command. `OrderCondition` defaults to `GreaterThanEqualTo` and
  `OrderPriceField` to `TradePrice` — what an unconfigured `RithmicIfTouchedTrigger`
  sends.

- **`TickBarReplayRequest` and `TimeBarReplayRequest`**, the replay counterparts
  of `VolumeProfileMinuteBarsRequest`. Same shape as the order commands: `new()`,
  chained setters, `validate()` and `build()`. Set `user_max_count` to cap a
  replay or `resume_bars` to uncap it.

- **`TimeBarType`**, an alias for the generated `request_time_bar_replay::BarType`.

- **`RithmicMessage::UnknownTemplate(UnknownTemplateMessage)`** — a frame whose
  `template_id` has no definition here, body kept as received, with
  **`decode_as<M>()`, `payload_hex()` and `from_payload_hex()`** to decode it into
  your own `prost` type or capture it for replay. `Ok` from `decode_as` is not
  proof the type was guessed right. **`RithmicMessage::RequestHeartbeat`**
  (template 18) is now mapped too; the library does not reply to it.

- **Trait impls and re-exports.**
  - `Clone` on the order and PnL handles (ticker and history already had it). A
    clone's `subscription_receiver` starts at the clone and does not replay
    earlier updates.
  - `PartialEq` on the command types and on `RithmicMessage`, `RithmicResponse`,
    `LoginConfig` and `ConfigError`.
  - `#[must_use]` on the command, trigger and request types.
  - `SubscriptionFilter`, `rithmic_rs::prost` and the new order enums at the
    crate root.

- **The generated types pick up the fields Rithmic added through 5.42.**

  | type | new fields |
  |---|---|
  | `ResponseLoginInfo` | contact, address, `order_copy_status`, per-plant session counts |
  | `ResponseAccountList` | `loss_limit`, creation timestamps |
  | `ExchangeOrderNotification` | `source_*` |
  | `ResponseListExchangePermissions` | `level_1_market_data`, `level_2_market_data` (`entitlement_flag` now `#[deprecated]`) |
  | `TickBar`, `ResponseTickBarReplay` | `data_bar_seq_num` |

#### Connection and config

- **Request timeouts.** `rithmic_rs::DEFAULT_REQUEST_TIMEOUT` (30 seconds),
  `RithmicConfigBuilder::request_timeout` and the `RITHMIC_REQUEST_TIMEOUT_SECS`
  environment variable control how long a request waits before failing with the new
  `RithmicError::RequestTimeout`.

- **`RithmicConfigBuilder::from_env(env)`** — a construction path for a type that
  lost struct-literal syntax, pre-filled from the same environment variables
  `RithmicConfig::from_env` reads so a single field can be overridden.

- **Reconnect backoff is jittered** by a clock-derived factor in `[0.5, 1.5)`, applied
  after the cap, so plants that lost the same connection no longer retry in
  lockstep. The schedule is otherwise unchanged — 500 ms more per attempt, capped
  at 60 seconds — giving 30–90 s delays at the cap.

- **RMS auto-liquidation streaming** — pass `RmsUpdateBits::AutoLiqThresholdCurrentValue`
  to `subscribe_account_rms_updates`. An empty `Vec` omits `update_bits` rather
  than sending `0`.

- **`serde` derives on every order command type** behind the `serde` feature, so a
  strategy can persist and replay any command.

### Changed

- **An unrecognized `template_id` is no longer a decode failure.** It used to
  return `Err` with `ProtocolError` and discard the payload; it now returns `Ok`
  with `RithmicMessage::UnknownTemplate` and logs a `warn`. Code matching
  `RithmicMessage::Unknown` still compiles but no longer sees these frames —
  `Unknown` now means only that a frame failed to decode.

- **A `Reject` is always surfaced as a rejection**, with `rp_code` passed through
  element for element, so codes like `["0"]` no longer make a rejected request look
  like a success. An unsolicited `Reject` — one echoing no `user_msg` — is logged
  at `warn` and dropped instead of reaching the request handler.

- **Documentation pass across the crate.** The "Error Handling" docs now cover
  every way an error surfaces, the rustdoc examples on the command types compile
  instead of being fenced as `ignore`, docs.rs builds with all features, and the
  README samples show correct API usage. The order handle gains examples on
  `place_order`, `place_bracket_order`, `place_oco_order`, `cancel_order`,
  `exit_position` and `subscribe_order_updates`. Four doc claims were wrong and are
  corrected: the handle types named `connect()` as their constructor rather than
  `get_handle()`; `connect()` documented an error it cannot return under
  `ConnectStrategy::Retry`, which loops until it connects rather than failing;
  `place_order`'s example had its call commented out; and `place_oco_order`'s
  two-leg minimum was documented only in a source comment. No API changed.

### Fixed

- **Login declared template version `5.30`** while the bundled protos were 5.42.
  It declares `5.42`. Nothing about the session changes — the field is a client
  declaration, not a negotiation.

- **Market orders were sent with `price = 0.0`**, and multi-leg OCO orders
  zero-filled `price` and `trigger_price`. Both now follow the all-or-none rule the
  trailing-stop fields already used: absent when no leg carries one, index-aligned
  once any leg does.

- **Every order went out on `"globex"` or `"simulator"` regardless of exchange**,
  ignoring the routes the server publishes. The plant now uses the published route
  for the order's exchange, preferring one marked default, read once at `login()`.

- **Requests carried a hardcoded `Trader` user type**, and the account list request
  carried no `fcm_id`/`ib_id`, so FCM and IB logins saw no accounts. `login()` now
  scopes requests with the real login info. As a result, **`get_account_list` and
  `get_account_rms_info` may return fewer accounts than before**, including for
  `Trader` logins.

- **Requests could wait forever.** A request whose response never arrives now fails
  after 30 seconds with `RithmicError::RequestTimeout`. Reconcile a timed-out order
  rather than re-sending it.

- **Order origination was attributed inconsistently** — orders, modifies, cancels
  and exits sent the bare literal `2` while cancel-all sent `Manual`, so one session
  reported two different originators. All commands now use the typed enum of their
  own request module.

- **Empty `user_tag` and `localid` were sent as `""`** instead of being omitted.
  `request_modify_order_reference_data` still sends what it is given — an empty tag
  there is how a tag is cleared.

- **A frame that failed to decode was discarded, leaving its caller waiting.** The
  echoed `user_msg` is now read off the wire even when the body will not decode, so
  the failure resolves the matching request with `ProtocolError`.

- **A response that ended a request without being marked `multi_response` leaked
  the parts accumulated under its id.** They were kept for the life of the
  connection and prepended to a later request that reused the id.

- **An order sent from a cloned handle alongside `disconnect()` could reach Rithmic
  while its caller saw `ConnectionClosed`** — a recorded failure for an order that
  was live at the exchange. Every plant now rejects queued commands once a
  disconnect is in flight.

- **A failed logout made `disconnect()` return early without sending `Close`**,
  leaving the actor with `close_requested` set: no heartbeats, every later command
  dropped, pending requests never drained. It now always sends `Close`.

- **A server `ForcedLogout` (template 77) left the plant heartbeating a session the
  server had already ended.** It now stops the actor and fails pending requests with
  `ConnectionClosed`.

- **Plants clamped the heartbeat period with `hb.max(60)`**, so a server asking for
  a heartbeat every 30 seconds got one every 60 and dropped the connection as idle.
  The `heartbeat_interval` from the login response is now used as sent.

- **A `MarketIfTouched` or `LimitIfTouched` modify omitted `trigger_price`.** The
  fallback to the order's own price covered only the two stop types; it now covers
  the same four types `validate()` requires a trigger for.

- **Single-order trailing stops omitted `trail_by_price_id`**, so the trailing
  stop was sent without the price-id it trails against.

- **Bracket target/stop adjustments omitted the `level` field**, so on a multi-leg
  bracket every adjustment landed on the server's default leg and the other legs
  were unreachable.

- **`request_account_rms_updates` sent `update_bits: None`**, so
  `auto_liq_threshold_current_value` never streamed even when subscribed.

- **Example fixes.** `examples/bracket_order.rs` no longer exits its listener on a
  recoverable error, and the samples now check `resp.error` for a rejected
  subscribe instead of an `Err` arm that can never match.

## [2.0.0]

### Breaking Changes

- **`RithmicError::ServerError(String)` removed.** Replaced by `RequestRejected` and `ProtocolError` variants that preserve the server/transport distinction. `RithmicError` now derives `PartialEq`, and `source()` returns the inner `RithmicRequestError` for `RequestRejected`.
- **`RithmicResponse::rp_code_error` field removed.** Use `response.error` directly, or the new rp_code accessors (`rp_code()`, `rp_code_num()`, `rp_code_text()`) for the raw payload.
- **`RithmicRequestError` shape changed.** `code: String` → `code: Option<String>`; `message: String` → `message: Option<String>` (symmetric with `code`; single-element rp_codes like `["5"]` now produce `message = None`); new `rp_code: Vec<String>` field preserves the full raw payload; struct is now `#[non_exhaustive]`. Accesses via `err.code` / `err.message` must update to `err.code.as_deref().unwrap_or("?")` and `err.message.as_deref().unwrap_or("")`.
- **`buf_to_message` no longer returns `Err(RithmicResponse)` for rp_code rejections.** Protocol-level outcomes now always come out as `Ok(response)` with `response.error` populated. `Err(RithmicResponse)` now exclusively means decode failure.
- **`RithmicConfig` no longer includes `account_id`, `fcm_id`, or `ib_id`** — those fields moved to `RithmicAccount`
- **`RithmicOrderPlant::get_handle()` and `RithmicPnlPlant::get_handle()` now require `&RithmicAccount`**
  - Create a `RithmicAccount` with `RithmicAccount::from_env(env)` or build one directly
  - For multi-account workflows, create one `RithmicAccount` per account and call `get_handle(&account)` for each
- **`subscription_receiver` on order and PnL handles is now `SubscriptionFilter`** instead of `broadcast::Receiver<RithmicResponse>`

#### Migrating from `RithmicError::ServerError`
Before (≤ 1.x):
```rust
match handle.subscribe("ESH6", "CME").await {
    Ok(_) => { /* ... */ }
    Err(RithmicError::ServerError(msg)) => {
        eprintln!("server error: {msg}");
        // unclear whether this is a rejection or a decode failure —
        // callers often used the message text to guess
    }
    Err(e) => eprintln!("{e}"),
}
```

After (2.0):
```rust
match handle.subscribe("ESH6", "CME").await {
    Ok(resp) => match &resp.error {
        // A rejection now arrives here, not in an `Err` arm. Decode failures
        // populate `error` too. Neither is a reconnect signal.
        Some(err) => eprintln!("request failed: {err}"),
        None => { /* success */ }
    },
    Err(RithmicError::ConnectionClosed | RithmicError::SendFailed) => {
        // Transport failure — reconnect.
    }
    Err(e) => eprintln!("{e}"),
}

// `login` is the one call that returns this as `Err`:
if let Err(RithmicError::RequestRejected(err)) = handle.login().await {
    eprintln!(
        "login rejected code={} msg={}",
        err.code.as_deref().unwrap_or("?"),
        err.message.as_deref().unwrap_or(""),
    );
}
```

### Added

- **`RithmicError::ProtocolError(String)`** — non-transport failures that don't carry `rp_code` (decode errors, missing response).
- **`RithmicError::InvalidArgument(String)`** variant for rejecting invalid caller-supplied arguments before a request is sent
- **`RithmicResponse::rp_code() -> Option<&[String]>`** — raw payload slice.
- **`RithmicResponse::rp_code_num() -> Option<&str>`** — numeric code (first element).
- **`RithmicResponse::rp_code_text() -> Option<&str>`** — human message (second element).
- **`RithmicAccount`** — account-scoped type for order and PnL operations
  - `RithmicAccount::from_env(RithmicEnv)` loads `RITHMIC_<ENV>_ACCOUNT_ID`, `FCM_ID`, `IB_ID`
- **`load_tick_bars(symbol, exchange, bar_length, start_time_sec, end_time_sec)`** on `RithmicHistoryPlantHandle`
  - Fetches historical N-tick bars (e.g., 5-tick, 10-tick) for a symbol
  - `bar_length` controls the number of ticks aggregated into each bar
  - Returns `RithmicError::InvalidArgument` when `bar_length` is 0
- **`RithmicAdvancedBracketOrder`** — full raw bracket order request exposing all venue-native fields
  - Supports triggered entry, break-even, trailing-stop, timed release/cancel, and if-touched entry conditions
  - Re-exported from crate root
- **`RithmicIfTouchedTrigger`** — conditional trigger for advanced bracket order entry (`if_touched_*` fields)
  - Re-exported from crate root
- **New bracket order enums** re-exported from crate root: `BracketType`, `BracketCondition`, `BracketPriceField`
- **Semantic ticker market-data subscription helpers** on `RithmicTickerPlantHandle` (all accept `symbol, exchange`):
  - `subscribe_instrument_status` / `unsubscribe_instrument_status` — market mode updates
  - `subscribe_order_book_summary` / `unsubscribe_order_book_summary` — aggregated bid/ask summary (proto 100, distinct from depth-by-order)
  - `subscribe_session_prices` / `unsubscribe_session_prices` — high/low/open trade statistics
  - `subscribe_quote_statistics` / `unsubscribe_quote_statistics` — quote-related statistics
  - `subscribe_indicator_prices` / `unsubscribe_indicator_prices` — settlement and projected settlement prices
  - `subscribe_open_interest` / `unsubscribe_open_interest` — open interest updates
  - `subscribe_end_of_day_prices` / `unsubscribe_end_of_day_prices` — end-of-day price data
  - `subscribe_order_price_limits` / `unsubscribe_order_price_limits` — high/low price limits
  - `subscribe_symbol_margin_rate` / `unsubscribe_symbol_margin_rate` — margin rate updates
- **Internal `rp_code_response_variants!` macro** enumerating every `RithmicMessage` variant whose inner proto carries `rp_code`. Keep in sync when new `Response*` templates are added.

### Changed

- **Plant login helpers simplified** — all four plants check `response.error` directly.
- **Ping/heartbeat SEND transport failures** broadcast as `RithmicMessage::HeartbeatTimeout` (same signal as a true heartbeat timeout) instead of `ConnectionError`.
- **`send_or_fail` timeout now drains all pending requests** and broadcasts `ConnectionError` before the next ping/heartbeat stops the actor. Previously only the single failing request was notified; remaining pending oneshots could hang on a half-open TCP connection since the poisoned sink is not guaranteed to surface through the reader.
- **`RithmicError::SendFailed`** now also covers send timeouts — all plant WebSocket sends are bounded to 10 seconds; a hung sink surfaces as `SendFailed` rather than blocking the actor indefinitely
- **`classify_rp_code` accepts `["0", <trailing>]` as success.** Only the first element decides success; a trailing element does not change it. Previously `["0", "ok"]` was mis-classified as a rejection.
- **`has_multiple` (multipart framing) now keys on presence, not value.** The presence of `rq_handler_rp_code` marks an intermediate frame; the value inside is not the multipart signal. Previously keying on `[0] == "0"` silently truncated multipart responses whose intermediate frames carried a non-`"0"` value.
- **`load_ticks`** now delegates to `load_tick_bars` with `bar_length = 1` — no behavioral change for existing callers
- **`request_tick_bar_replay`** on `RithmicSenderApi` now accepts a `bar_type_specifier` parameter instead of hard-coding `"1"`
- **`examples/reconnect.rs` handles broadcast `RecvError::Lagged` explicitly** — a slow consumer that drops a connection-health frame through buffer wrap now logs and reconnects instead of silently exiting the read loop.
- **Unknown `template_id` responses route as subscription updates** (`is_update: true`) instead of going to the request handler. Previously they surfaced as "no responder found" noise on the per-request path. Subscribers now observe `RithmicMessage::Unknown` frames with a populated `error` describing the unknown `template_id`.

### Fixed

- **`rp_code = ["7", "no data"]`** is now treated as a successful empty result (not an error) across all list/replay/search responses — previously this caused methods like `replay_executions` to return `ServerError("no data")` when the query matched zero records

### Known behaviors

- `RithmicMessage::ForcedLogout` surfaces via subscription updates and `is_connection_issue()` returns `true` for it.
- A protobuf decode failure on a `ResponseHeartbeat` frame is routed to the subscription channel as a generic update (not a synthetic `HeartbeatTimeout`). Unchanged from prior behavior.

## [1.0.0]

### Breaking Changes

#### Typed Error Handling
- **`RithmicError`** enum replaces `String` errors across the entire API
  - `ConnectionFailed` — WebSocket connection could not be established
  - `ConnectionClosed` — plant's WebSocket connection is gone
  - `SendFailed` — WebSocket send failed after the request was registered
  - `EmptyResponse` — server returned empty response where at least one was expected
  - `ServerError(String)` — protocol-level rejection from Rithmic
- **`connect()`** on all plants now returns `Result<Plant, RithmicError>` instead of `Result<Plant, Box<dyn std::error::Error>>`
- All plant handle methods now return `Result<_, RithmicError>` instead of `Result<_, String>`

#### API Renames
- **`RithmicBracketOrder::qty`** renamed to **`quantity`** for clarity and consistency

#### Visibility Changes
- **`connection_handle`** on all plant structs is now `pub(crate)` (was `pub`)
  - Use the new **`await_shutdown()`** method instead to wait for the plant to stop

#### Dependency Changes
- **Prost** upgraded from `0.13` to `0.14` — if you depend on generated protobuf types, this is a breaking change
- **`async-trait`** removed — all async traits now use native Rust async trait support (requires Rust 1.85+)

#### Removed
- **`place_new_order()`** removed from `RithmicOrderPlantHandle` — use `place_order(RithmicOrder)` instead
- Protobuf codegen removed from build script — moved to standalone example binary

### Added

- **`LoginConfig`** struct for advanced login options (`aggregated_quotes`, `mac_addr`, `os_version`, `os_platform`)
- **`login_with_config(LoginConfig)`** method on all plant handles for customized login
- **`await_shutdown()`** method on all plant structs to wait for clean shutdown
- **`RithmicConfigBuilder`** re-exported from crate root
- **`InstrumentInfoError`** re-exported from crate root
- **`#[non_exhaustive]`** on `RithmicResponse`, `RithmicMessage`, `RithmicError`, `RithmicOrder`, `TrailingStop`, `ConnectStrategy`, `OrderStatus`, and `ConfigError` for forward compatibility
- **`Debug`** impl on all plant structs and plant handle structs
- **`RithmicConfig`** `Debug` output now redacts the `password` field

### Changed

- Set MSRV (minimum supported Rust version) to **1.85**
- Relaxed `futures-util` version constraint from `0.3.32` to `0.3`
- Replaced `serial_test` + unsafe `env::set_var` in tests with `temp-env` crate
- Added `#![warn(missing_docs)]` to enforce documentation coverage
- Feature flags section added to crate-level documentation

## [0.7.2] - 2026-02-07

### Added

#### New Order API
- **`RithmicOrder`**: New struct for placing standalone orders with advanced features
  - Supports trigger prices for stop orders (StopLimit, StopMarket)
  - Supports trailing stops via `TrailingStop` configuration
  - Ergonomic API using `Default` trait for optional fields
  - Comprehensive documentation with examples
- **`TrailingStop`**: Configuration struct for trailing stop orders
  - `trail_by_ticks`: Number of ticks to trail behind market price
- **`place_order(RithmicOrder)`**: New method on `RithmicOrderPlantHandle`
  - Preferred method for placing standalone orders
  - Supports all order types including stop orders and trailing stops

#### Ticker Plant Unsubscribe Methods
- **`unsubscribe(symbol, exchange)`**: Unsubscribe from market data for a symbol
- **`unsubscribe_order_book(symbol, exchange)`**: Unsubscribe from order book depth-by-order updates

#### Serde-Compatible Order Types
- **`OrderSide`**, **`OrderType`**, **`TimeInForce`**: New enums with optional serde support
  - Flexible parsing via `FromStr` (e.g., `"buy"`, `"BUY"`, `"B"` all parse to `OrderSide::Buy`)
  - `From` impls for conversion to protobuf request types
- **`OrderStatus::Expired`**: New variant added to the `OrderStatus` enum
- `OrderStatus` now supports optional serde serialization/deserialization

### Removed
- **`place_new_order()`**: Replaced by `place_order(RithmicOrder)` which supports trigger prices and trailing stops

## [0.7.1] - 2026-01-23

### Added

#### New Utility Module (`util`)
- **`InstrumentInfo`**: Parsed instrument reference data from Rithmic
  - Converts `ResponseReferenceData` to a structured type via `TryFrom`
  - `price_precision()`: Calculate decimal places based on tick size
  - `size_precision()`: Returns 0 for futures (whole contracts)
  - Fields include: symbol, exchange, name, tick_size, point_value, is_tradable, and more
- **`OrderStatus`**: Order status enum with helper methods
  - Parses case-insensitively with common variations ("filled" → Complete, "canceled" → Cancelled)
  - `is_terminal()`: Returns true for Complete, Cancelled, Rejected
  - `is_active()`: Returns true for Open, Pending, Partial
  - Implements `FromStr`, `Display`, `Default` (Unknown)
- **`rithmic_to_unix_nanos(ssboe, usecs)`**: Convert Rithmic timestamps to Unix nanoseconds
- **`rithmic_to_unix_nanos_precise(ssboe, usecs, nsecs)`**: Convert with optional nanosecond precision

#### RithmicResponse Helper Methods
- **`is_error()`**: Returns true if response has an error or connection issue
- **`is_connection_issue()`**: Returns true for ConnectionError, HeartbeatTimeout, ForcedLogout
- **`is_market_data()`**: Returns true for BestBidOffer, LastTrade, DepthByOrder, OrderBook, etc.

#### Optional Serde Support
- Added `serde` feature flag for serialization/deserialization support
- `RithmicEnv` derives `Serialize`/`Deserialize` when enabled with lowercase rename
- Enable with: `rithmic-rs = { version = "2.0", features = ["serde"] }`

#### New Example
- **`bracket_order.rs`**: Demonstrates placing bracket orders with typed enums

#### CI/CD
- Added GitHub Actions CI workflow for automated testing

### Fixed

#### Error Handling Improvements
- Replaced `.unwrap()` panics with proper error handling in all plant handles
  - `RithmicTickerPlantHandle`: `subscribe`, `unsubscribe`, `get_front_month_contract`, and other methods now handle channel send failures gracefully
  - `RithmicOrderPlantHandle`: `place_bracket_order`, `modify_order`, `cancel_order`, and other methods now handle channel send failures gracefully
  - `RithmicHistoryPlantHandle`: `load_time_bars`, `load_ticks`, and other methods now handle channel send failures gracefully
  - `RithmicPnlPlantHandle`: `subscribe_pnl_updates`, `pnl_position_snapshots`, and other methods now handle channel send failures gracefully

#### Code Quality
- Addressed clippy lints in util module
- Cleaned up util module documentation

## [0.7.0] - 2026-01-08

### Breaking Changes

#### Order Types Now Use Enums Instead of Raw Integers
- **`RithmicBracketOrder`**: Field types and names changed
  - `action: i32` → `action: BracketTransactionType` (enum)
  - `ordertype: i32` → `price_type: BracketPriceType` (enum, **renamed**)
  - `duration: i32` → `duration: BracketDuration` (enum)
- **`RithmicModifyOrder`**: Field type changed
  - `ordertype: i32` → `price_type: ModifyPriceType` (enum, **renamed**)

**Migration example:**
```rust
// Old (0.6.x)
let order = RithmicBracketOrder {
    action: 1,      // Buy
    ordertype: 1,   // Limit
    duration: 2,    // Day
    // ...
};

// New
use rithmic_rs::{BracketTransactionType, BracketPriceType, BracketDuration};
let order = RithmicBracketOrder {
    action: BracketTransactionType::Buy,
    price_type: BracketPriceType::Limit,
    duration: BracketDuration::Day,
    // ...
};
```

### Added

#### Cleaner Public API
- All order-related types and enums now re-exported from crate root:
  - `RithmicBracketOrder`, `RithmicModifyOrder`, `RithmicCancelOrder`, `RithmicOcoOrderLeg`
  - `BracketTransactionType`, `BracketDuration`, `BracketPriceType`
  - `ModifyPriceType`
  - `RithmicResponse`, `RithmicStream`
- Internal implementation details hidden with `pub(crate)` visibility
- Users can now import all types from `rithmic_rs::*` instead of deep module paths

#### Improved Documentation
- Added comprehensive doc comments and examples for all order types
- Simplified `ConnectionError` and `HeartbeatTimeout` documentation
- Added module-level documentation for `api`, `plants`, and `rti` modules
- Added `.env.blank` reference to `RithmicConfig::from_env()` docs
- Streamlined README with clearer quick start and architecture sections

#### Reorganized Examples
- Added `ticker.rs`: Market data subscription and symbol discovery
- Added `pnl.rs`: P&L monitoring example
- Added `reconnect.rs`: Reconnection handling with subscription tracking
- Removed `market_data.rs` (replaced by `ticker.rs`)

### Removed
- Removed unused `HEARTBEAT_TIMEOUT_SECS` constant (dead code from removed HeartbeatManager)

## [0.6.2] - 2025-12-20

### Added

#### New Sender API Methods

##### Ticker Plant
- `request_rithmic_system_gateway_info()`: Get gateway-specific information
- `request_get_instrument_by_underlying()`: Get all instruments for an underlying symbol
- `request_market_data_update_by_underlying()`: Subscribe to market data by underlying
- `request_give_tick_size_type_table()`: Get tick size table for a tick size type
- `request_product_codes()`: Get available product codes for an exchange
- `request_get_volume_at_price()`: Get volume profile for a symbol
- `request_auxilliary_reference_data()`: Get additional reference data for a symbol
- `request_volume_profile_minute_bars()`: Get minute bars with volume profile
- `request_resume_bars()`: Resume a truncated bars request
- `request_depth_by_order_snapshot()`: Get depth by order snapshot
- `request_depth_by_order_update()`: Subscribe to depth by order updates

##### Order Plant
- `request_login_info()`: Get current login session information
- `request_oco_order()`: Place OCO (One Cancels Other) order pairs
- `request_link_orders()`: Link multiple orders together
- `request_easy_to_borrow_list()`: Get easy-to-borrow list for short selling
- `request_modify_order_reference_data()`: Update user tag on existing order
- `request_order_session_config()`: Get/set order session configuration
- `request_replay_executions()`: Replay historical execution data

##### Repository Plant (Agreements)
- `request_list_unaccepted_agreements()`: List agreements not yet accepted
- `request_list_accepted_agreements()`: List already accepted agreements
- `request_accept_agreement()`: Accept a specific agreement
- `request_show_agreement()`: Get full agreement details
- `request_set_rithmic_mrkt_data_self_cert_status()`: Set market data self-certification status

#### API Ergonomics
- Re-exported `RithmicOcoOrderLeg` and related OCO order enums from `api` module:
  - `OcoTransactionType`: Buy/Sell transaction type
  - `OcoDuration`: Day/GTC/IOC/FOK duration
  - `OcoPriceType`: Limit/Market/StopLimit/StopMarket price type
- Changed `RithmicOcoOrderLeg.trigger_price` from `f64` to `Option<f64>` since it's only required for stop orders

#### New Market Data Messages (Ticker Plant)
- `TradeStatistics`: High/low/open price statistics
- `QuoteStatistics`: Quote-related statistics  
- `IndicatorPrices`: Settlement, projected settlement prices
- `EndOfDayPrices`: End of day price data
- `MarketMode`: Market trading mode updates
- `OpenInterest`: Open interest updates
- `FrontMonthContractUpdate`: Front month contract changes
- `DepthByOrderEndEvent`: Depth by order stream end marker
- `SymbolMarginRate`: Symbol margin rate updates
- `OrderPriceLimits`: Price limit updates

#### New Order Plant Messages
- `UserAccountUpdate`: Account permission/access changes
- `AccountListUpdates`: Account list change notifications
- `AccountRmsUpdates`: Real-time RMS limit updates

#### New RithmicMessage Variants
- `ResponseReferenceData`: Symbol reference data
- `ResponseFrontMonthContract`: Front month contract info
- `ResponseTimeBarUpdate`: Time bar subscription confirmation
- `ResponseTickBarUpdate`: Tick bar subscription confirmation
- `ResponseAccountRmsUpdates`: RMS updates subscription confirmation

### Fixed
- Fixed clippy warning: use `is_multiple_of()` instead of modulo check in connection retry logic

## [0.6.1] - 2025-11-24

> **⚠️ Breaking Change:** Environment variable names have changed. See migration guide below.

### Breaking Changes

#### Environment Variable Structure
- **Environment-specific configuration variables** for better multi-environment support
  - All configuration variables now include environment prefix (DEMO, LIVE, TEST)
  - Account variables: `RITHMIC_<ENV>_ACCOUNT_ID`, `RITHMIC_<ENV>_FCM_ID`, `RITHMIC_<ENV>_IB_ID`
  - Connection variables: `RITHMIC_<ENV>_URL`, `RITHMIC_<ENV>_ALT_URL`
  - User credentials: `RITHMIC_<ENV>_USER`, `RITHMIC_<ENV>_PW`
  - Enables separate configurations for each environment
  - Example: `RITHMIC_DEMO_ACCOUNT_ID`, `RITHMIC_LIVE_ACCOUNT_ID`, `RITHMIC_TEST_ACCOUNT_ID`

#### Migration from Previous Versions
**Old variable names (no longer supported):**
- `RITHMIC_ACCOUNT_ID` → `RITHMIC_<ENV>_ACCOUNT_ID`
- `FCM_ID` → `RITHMIC_<ENV>_FCM_ID`
- `IB_ID` → `RITHMIC_<ENV>_IB_ID`

**Example for Demo environment:**
```bash
# Old (0.6.0 and earlier)
RITHMIC_ACCOUNT_ID=account123
FCM_ID=fcm123
IB_ID=ib123
RITHMIC_DEMO_USER=user
RITHMIC_DEMO_PW=pass

# New (0.6.1)
RITHMIC_DEMO_ACCOUNT_ID=account123
RITHMIC_DEMO_FCM_ID=fcm123
RITHMIC_DEMO_IB_ID=ib123
RITHMIC_DEMO_USER=user
RITHMIC_DEMO_PW=pass
RITHMIC_DEMO_URL=<provided_by_rithmic>
RITHMIC_DEMO_ALT_URL=<provided_by_rithmic>
```

See `examples/.env.blank` for complete template with all required variables.

### Fixed
- Fixed rustfmt compliance issues with long error messages
- Fixed clippy warning: use `.first()` instead of `.get(0)` for idiomatic array access

## [0.6.0] - 2025-11-23

### Breaking Changes

- **Removed `connection_info` module** - deprecated types removed (use `RithmicConfig` instead)
- **Removed `RithmicConfig::from_dotenv()` method** - consumers call `dotenvy::dotenv()` themselves
- **Removed `return_heartbeat_response()` method** from all plant handles
- **Updated to `dotenvy` crate** - moved to dev-dependencies (from deprecated `dotenv`)

### Changed

- **Connection health monitoring** now fully automatic via WebSocket ping/pong
  - Heartbeats sent automatically for protocol compliance
  - Successful responses silently dropped
  - Errors delivered as `HeartbeatTimeout` messages
- **Environment variable loading** now consumer-controlled
  - Library no longer forces approach for loading env vars
  - Examples demonstrate using `dotenvy`, but any method works
- **Reduced code complexity** - removed 500+ lines of deprecated code

### Documentation

- Removed dotenv/`.env` references from library docs (examples still show usage)
- Updated README with clearer examples and breaking changes summary

## [0.5.3] - 2025-11-22

### Added

#### Order Management APIs
- **New `cancel_all_orders()` method** on `RithmicOrderPlantHandle`
  - Cancels all active orders across all symbols and exchanges for the account
  - Returns cancellation confirmation response
- **New order history methods** on `RithmicOrderPlantHandle`
  - `show_order_history_dates()`: Get dates for which order history is available
  - `show_order_history_summary(date)`: Get order summary for a specific date (YYYYMMDD format)
  - `show_order_history_detail(basket_id, date)`: Get detailed history for a specific order
  - `show_order_history(basket_id)`: Get general order history with optional basket_id filter
  - Enables comprehensive order audit trails and historical analysis

#### Risk Management APIs
- **New RMS information methods** on `RithmicOrderPlantHandle`
  - `get_account_rms_info()`: Retrieve account-level risk management limits and settings
  - `get_product_rms_info()`: Retrieve product-specific risk management limits
  - `get_trade_routes(subscribe_for_updates)`: Get available trade routes with optional update subscription
  - Critical for monitoring trading limits and route availability

#### Symbol Search and Discovery APIs
- **New `search_symbols()` method** on `RithmicTickerPlantHandle`
  - Search for symbols by text pattern with optional filters
  - Supports filtering by exchange, product code, and instrument type
  - Configurable search pattern (EQUALS or CONTAINS)
  - Returns list of matching symbols for dynamic symbol discovery
- **New `list_exchanges()` method** on `RithmicTickerPlantHandle`
  - Lists exchanges available to the specified user
  - Useful for determining trading permissions

#### Protocol Message Support
- **New `TradeRoute` message type** added to `RithmicMessage` enum
  - Handles template ID 310 for trade route information
  - Delivered as update message (`is_update: true`)
  - Supports trade route subscription updates

#### Sender API Methods
- Added 10 new request methods to `RithmicSenderApi`:
  - `request_cancel_all_orders()`: Template 346
  - `request_account_rms_info()`: Template 304
  - `request_product_rms_info()`: Template 306
  - `request_trade_routes(subscribe_for_updates)`: Template 310
  - `request_search_symbols(...)`: Template 109 with extensive search filters
  - `request_list_exchanges(user)`: Template 342
  - `request_show_order_history_dates()`: Template 318
  - `request_show_order_history_summary(date)`: Template 324
  - `request_show_order_history_detail(basket_id, date)`: Template 326
  - `request_show_order_history(basket_id)`: Template 322

### Changed

#### Internal Improvements
- Extended `OrderPlantCommand` enum with 8 new command variants for order history and RMS operations
- Extended `TickerPlantCommand` enum with 2 new command variants for symbol search and exchange listing
- Updated receiver API to handle TradeRoute message type (template ID 310)
- Added new imports for request types: `RequestCancelAllOrders`, `RequestAccountRmsInfo`, `RequestProductRmsInfo`, `RequestSearchSymbols`, `RequestTradeRoutes`, and order history request types

### Known Issues

#### Error Handling
- New TradeRoute message handler uses `.unwrap()` on protobuf decode (line 438 in receiver_api.rs)
- New plant handle methods use multiple `.unwrap()` calls that could panic on channel failures
- These follow existing patterns in the codebase but should be addressed in future releases
- Users should be aware that malformed messages or actor failures may cause panics

## [0.5.2] - 2025-11-20

### Added

#### Optional Heartbeat Response Handling
- **New `return_heartbeat_response()` method** on all plant handles (ticker, order, pnl, history)
  - Controls whether heartbeat responses are delivered through subscription channel
  - Default behavior: heartbeats use request/response pattern (not sent to channel)
  - Call `handle.return_heartbeat_response(true)` to enable heartbeat monitoring
  - Useful for explicit connection health monitoring during trading hours
  - Can be disabled during off-market hours to avoid false alarms

#### Heartbeat Timeout Detection
- **New `HeartbeatManager`** for tracking heartbeat response timeouts
  - Monitors pending heartbeats when responses are expected
  - Detects timeouts after 30 seconds (configurable via `HEARTBEAT_TIMEOUT_SECS`)
  - Integrated into all plant actors (ticker, order, pnl, history)
  - Non-blocking implementation using tokio `sleep_until()` with efficient select! loop integration
- **New `RithmicMessage::HeartbeatTimeout` variant** for timeout notifications
  - Sent as an update message when heartbeat response does not arrive within timeout period
  - Includes error context: "Heartbeat response timeout"
  - Only active when heartbeat responses are expected (`return_heartbeat_response(false)`)
  - Helps detect connection degradation without requiring manual timeout tracking
  - Comprehensive documentation with usage examples
- **Timeout constant `HEARTBEAT_TIMEOUT_SECS`** in `ws.rs`
  - Set to 30 seconds (half the 60-second heartbeat interval)
  - Provides balance between detecting issues and avoiding false positives

### Changed

#### Internal Refactoring
- Renamed internal field `ignore_heartbeat_response` to `expect_heartbeat_response` in all plants
  - Improves code clarity with explicit naming and positive boolean logic
  - Added documentation explaining the setting's purpose and when to use it
  - No API changes - public interface remains the same

#### Heartbeat Response Delivery
- **Reverted heartbeat behavior to request/response pattern** (no longer sent through subscription channel by default)
  - Heartbeats sent automatically on interval but responses not delivered to subscription channel
  - Previous behavior (0.5.0): All heartbeat responses delivered through subscription channel as updates
  - New behavior: Heartbeat responses only delivered if explicitly enabled via `return_heartbeat_response(true)`
  - Reduces noise in subscription channel for applications that don't need heartbeat monitoring
  - Provides flexibility: enable during trading hours, disable during off-hours
- **Internal improvements** to `request_handler.rs`
  - Now handles heartbeat responses when callbacks are registered
  - Refactored response sending into helper method for better error handling
  - Improved logging for failed response deliveries

### Fixed

#### Heartbeat Response Handling
- Fixed ResponseHeartbeat request_id extraction in `src/api/receiver_api.rs`
  - Now correctly extracts request_id from `user_msg[0]` instead of using empty string
  - Enables proper matching of heartbeat responses to pending requests in timeout detection
- Fixed ResponseHeartbeat routing in all plants
  - Successful heartbeat responses are never delivered to subscription channel (silent when connection is healthy)
  - When `expect_heartbeat_response = true`, only `HeartbeatTimeout` messages are sent on failure
  - Purpose: connection health verification - report only when heartbeat fails, not when it succeeds

#### Code Quality
- Fixed clippy warning `tabs_in_doc_comments` in `src/rti.rs`
  - Replaced tab character with spaces in documentation comment

## [0.5.1]

### Added

#### Connection Error Handling
- **New `RithmicMessage::ConnectionError` variant** for WebSocket connection failures
  - Provides unified error handling for all connection-related failures
  - Enables consumers to implement reconnection logic via pattern matching
  - Includes comprehensive documentation with examples
- **Comprehensive WebSocket error detection** across all plants (ticker, order, pnl, history):
  - `ConnectionClosed`: Normal WebSocket closure
  - `AlreadyClosed`: Attempted use of closed connection
  - `Io` errors: Network/socket I/O failures (connection lost, timeout)
  - `ResetWithoutClosingHandshake`: Connection reset without proper WebSocket close
  - `SendAfterClosing`: Attempted to send data after closing frame sent
  - `ReceivedAfterClosing`: Received data after closing frame sent
- **Automatic error notifications** sent through subscription channel when connection fails
  - `RithmicResponse` with `message: ConnectionError` and `is_update: true`
  - `error` field contains specific error description
  - `source` field identifies which plant failed
  - Enables consumers to detect and handle connection failures in real-time

#### Documentation
- Added comprehensive documentation to `RithmicMessage::ConnectionError`
  - Lists all handled error types
  - Step-by-step guidance for handling connection errors
  - Complete code examples showing pattern matching
  - Notes on behavioral details and channel lifecycle
- Added detailed documentation to `RithmicResponse` struct
  - Explains error handling for both protocol and connection errors
  - Examples showing how to handle different error scenarios
  - Cross-references to related documentation

### Changed
- **Improved logging consistency**: Changed `ConnectionClosed` log level from `info!` to `error!` across all plants
  - Ensures all connection termination events are logged at error level
  - Makes connection issues more visible in production logs
- Replace `event!` macro with specific logging macros (`info!`, `error!`, `warn!`) across library code for better code clarity and idiomatic Rust logging
  - Updated: all plant files, `src/api/receiver_api.rs`, `src/request_handler.rs`

### Fixed
- **Connection error handling**: Plants now properly stop and notify consumers on all WebSocket connection failures
  - Previously, most connection errors fell through to catch-all warning and left plants in undefined state
  - Now all connection errors trigger clean shutdown with error notification
  - Prevents resource leaks and zombie plant instances

## [0.5.0]

### Breaking Changes

#### Connection API Changes
- **Plant constructors renamed**: `new()` → `connect()` across all plants
- **Return type changed**: `connect()` now returns `Result<Plant, Box<dyn std::error::Error>>`
- **Required parameter**: All plants now require a `ConnectStrategy` parameter
- Enables proper error handling instead of panics and explicit connection strategy selection

#### Configuration API Changes
- **New unified configuration**: `RithmicConfig` replaces separate account/connection info types
  - Old types (`AccountInfo`, `RithmicConnectionInfo`, `RithmicConnectionSystem`) are deprecated
  - Migration path provided via `From`/`TryFrom` trait implementations
- **Environment handling**: `RithmicEnv` replaces `RithmicConnectionSystem`
  - More idiomatic enum naming
  - Better integration with configuration builder

#### Error Handling Changes
- **Heartbeat error visibility**: Heartbeat responses now delivered through subscription channel
  - `ResponseHeartbeat` changed from `is_update: false` → `is_update: true`
  - Consumers must check `error` field on heartbeat responses to detect connection issues
  - Breaking for applications that assumed heartbeats wouldn't appear in subscriptions
- **Forced logout events**: Now delivered through subscription channel for visibility
  - `ForcedLogout` changed from `is_update: false` → `is_update: true`
  - Applications must handle forced logout events to implement reconnection logic
- **No more panics**: Error responses from server no longer panic, sent to subscription channel instead

### Added

#### Connection Strategies
- New `ConnectStrategy` enum with three modes:
  - **`Simple`**: Single connection attempt (recommended default, fast-fail)
  - **`Retry`**: Indefinite retries with exponential backoff on same URL
  - **`AlternateWithRetry`**: Alternates between primary and beta URLs with retries
- Retry strategies now retry indefinitely instead of limiting to 15 attempts
- Maximum backoff capped at 60 seconds to ensure at most one login attempt per minute
- Prevents excessive load on Rithmic servers during extended outages

#### Unified Configuration API
- `RithmicConfig`: Modern, ergonomic configuration type combining account and connection fields
- `RithmicEnv`: Environment selection enum (Demo, Live, Test)
- `ConfigError`: Type-safe error handling for configuration operations
- `from_env()`: Load configuration from environment variables with proper error handling
- `from_dotenv()`: Load configuration from .env file (requires `dotenv` feature)
- `RithmicConfigBuilder`: Builder pattern for programmatic configuration
- Comprehensive unit tests (15 tests) covering all configuration scenarios

#### Connection Health Monitoring
- Heartbeat responses now include error information in subscription channel
- Forced logout events delivered through subscription channel
- Applications can monitor connection health in real-time
- Examples added showing proper heartbeat timeout tracking

#### Documentation
- Comprehensive documentation for connection strategies
- Connection timeout and retry behavior documented
- Migration guide for deprecated types in `connection_info` module
- Real-world examples showing proper error handling and connection monitoring
- Examples updated to demonstrate new unified configuration API

### Fixed

#### Critical Panic Fixes
- Fixed panic on unknown message types by adding proper error handling (#3)
  - Unknown message types now logged and gracefully handled
  - Added `UnknownMessage` variant to handle unexpected protocol messages
- Fixed panic on error responses in ticker plant (#2)
  - Error responses from `buf_to_message()` now handled gracefully
  - Errors sent through subscription channel for consumer handling
- Fixed panic on heartbeat errors across all plants
  - Broadcast send errors now handled gracefully instead of unwrapping
  - No more crashes on channel receiver drops

#### Consistency Fixes
- Fixed inconsistent heartbeat logic across plants (#9)
  - All plants (ticker, order, pnl, history) now only send heartbeats after login
  - Prevents protocol violations from pre-login heartbeats
  - Unified behavior across all plant implementations
- Fixed MessageType decode unwrap with proper error handling (#4)
  - Removed `.unwrap()` calls in message decoding
  - Proper error propagation through Result types

#### Code Quality
- Removed `#[allow(dead_code)]` annotations from valid public API methods (#11)
  - `request_new_order`, `request_exit_position`, `request_show_brackets`, `request_show_bracket_stops`
  - Added comprehensive documentation for these public API methods
  - Improved library API clarity

### Deprecated

The following types are deprecated and will be removed in a future version:
- `AccountInfo` - Use `RithmicConfig` instead
- `RithmicConnectionInfo` - Use `RithmicConfig` instead
- `RithmicConnectionSystem` - Use `RithmicEnv` instead
- `get_config()` function - Use `RithmicConfig::from_env()` or builder pattern

Migration helpers provided via trait implementations maintain backward compatibility.

### Changed

#### API Consistency
- Unified error handling pattern across all plants
  - Consistent routing based on `is_update` flag
  - Simplified message handling logic
  - No panics in production code

#### Internal Improvements
- Updated `RithmicSenderApi` to use `RithmicConfig` and `RithmicEnv`
- Simplified routing logic using `is_update` flag instead of message type checks
- Improved type safety by replacing panics with proper error types

## [0.4.2] - 2025-11-15

Previous stable release. See git history for earlier changes.

---

## Version History Summary

- **3.0.0** (2026-08-09): Breaking changes - order commands built with `new()` + setters, crate-owned enums replace fourteen generated re-exports, every order call takes a command struct, prices are `Option<f64>`, seven handle methods renamed, required `request_timeout`; orders route off the exchange's published trade route, uncapped replay loaders, protos at 0.89.0.0
- **2.0.0**: Breaking changes - typed `RithmicError::RequestRejected`/`ProtocolError` replace `ServerError`, `RithmicResponse::rp_code_error` removed, `RithmicAccount` split from `RithmicConfig`, account-scoped `get_handle()`, `SubscriptionFilter`; advanced bracket orders, semantic ticker subscriptions, bounded WebSocket sends
- **1.0.0**: Breaking changes - typed `RithmicError` enum, prost 0.14, async-trait removed, `LoginConfig` for advanced login, `await_shutdown()`, non_exhaustive annotations, MSRV 1.85
- **0.7.2** (2026-02-07): New RithmicOrder API with trigger prices and trailing stops, ticker plant unsubscribe methods, serde-compatible order types
- **0.7.1** (2026-01-23): New utility module (InstrumentInfo, OrderStatus, timestamp helpers), RithmicResponse helper methods, optional serde support, improved error handling
- **0.7.0** (2026-01-08): Breaking changes - Order types now use enums instead of raw integers, cleaner public API exports
- **0.6.2** (2025-12-20): Expanded plant handle APIs, additional message types, OCO order support, and new sender methods
- **0.6.1** (2025-11-24): Environment-specific configuration variables
- **0.6.0** (2025-11-23): Major breaking changes - Removed deprecated code, simplified heartbeat handling, updated to dotenvy
- **0.5.3** (2025-11-22): API expansion - Order history, RMS info, symbol search, trade routes, cancel all orders
- **0.5.2** (2025-11-20): Heartbeat improvements - Optional heartbeat response handling, heartbeat timeout detection, internal refactoring
- **0.5.1** (2025-11-18): Connection error handling improvements - ConnectionError variant, comprehensive WebSocket error detection, automatic error notifications
- **0.5.0** (2025-11-16): Major stability and API improvements - Connection strategies, unified config, panic fixes, connection health monitoring
- **0.4.2** (2025-11-15): Previous stable release

[Unreleased]: https://github.com/pbeets/rithmic-rs/compare/v3.1.0...HEAD
[3.1.0]: https://github.com/pbeets/rithmic-rs/compare/v3.0.0...v3.1.0
[3.0.0]: https://github.com/pbeets/rithmic-rs/compare/v2.0.0...v3.0.0
[2.0.0]: https://github.com/pbeets/rithmic-rs/compare/v1.0.0...v2.0.0
[1.0.0]: https://github.com/pbeets/rithmic-rs/compare/v0.7.2...v1.0.0
[0.7.2]: https://github.com/pbeets/rithmic-rs/compare/v0.7.1...v0.7.2
[0.7.1]: https://github.com/pbeets/rithmic-rs/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/pbeets/rithmic-rs/compare/v0.6.2...v0.7.0
[0.6.2]: https://github.com/pbeets/rithmic-rs/compare/v0.6.1...v0.6.2
[0.6.1]: https://github.com/pbeets/rithmic-rs/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/pbeets/rithmic-rs/compare/v0.5.3...v0.6.0
[0.5.3]: https://github.com/pbeets/rithmic-rs/compare/v0.5.2...v0.5.3
[0.5.2]: https://github.com/pbeets/rithmic-rs/compare/v0.5.1...v0.5.2
[0.5.1]: https://github.com/pbeets/rithmic-rs/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/pbeets/rithmic-rs/compare/v0.4.2...v0.5.0
[0.4.2]: https://github.com/pbeets/rithmic-rs/releases/tag/v0.4.2
