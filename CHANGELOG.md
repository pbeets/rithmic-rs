# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Order commands are now built with `::new()` and chained setters, and the command types are `#[non_exhaustive]`, so this ships as a major release.

### Breaking Changes

- **The bundled protos are updated to R | Protocol API 0.89.0.0 (template version 5.42), and `RithmicMessage::AccountListUpdates` is removed.** Rithmic dropped `account_list_updates.proto` (template 354) from the proto pool; its release notes say to use the account RMS updates stream instead. Subscribe with `subscribe_account_rms_updates` and match `RithmicMessage::AccountRmsUpdates`. Rithmic also changed two enum fields on `UserAccountUpdate` to strings: `update_type` and `access_type` are now `Option<String>` (values like `add_account`, `modify_account`), and the generated `UpdateType`/`AccessType` enums no longer exist.
- **Order command types are now their own builders and are `#[non_exhaustive]`.** `..Default::default()` no longer compiles on them outside this crate; use `new()` plus chained setters instead. There is no separate builder type: `new()` takes no arguments and returns the command with its defaults filled in, every field has a setter of the same name, and `build()` runs `validate()` and returns the command as `Result<_, RithmicError>`. The fields are still public, so direct assignment works too: `let mut order = RithmicOrder::new(); order.symbol = "ESH6".into();`.

  ```rust
  // Before
  let order = RithmicOrder { symbol: "ESM6".into(), quantity: 1, ..Default::default() };
  // After
  let order = RithmicOrder::new()
      .symbol("ESM6")
      .exchange("CME")
      .quantity(1)
      .transaction_type(OrderSide::Buy)
      .price_type(OrderType::Limit)
      .price(5000.0)
      .build()?;
  ```

- **`RithmicAdvancedBracketOrder` is removed; `RithmicBracketOrder` now does everything it did.** One bracket type carries every venue-native field — trailing stops, break-even, timed release/cancel, if-touched entry. `place_advanced_bracket_order` is gone; call `place_bracket_order`. Migration notes:
  - `profit_ticks`/`stop_ticks` are replaced by `target_quantity`/`target_ticks` and `stop_quantity`/`stop_ticks`, all `Vec<i32>`, one entry per exit leg.
  - The single-value **setters** are renamed: `.profit_ticks(n)` is now `.target(n)` and `.stop_ticks(n)` is now `.stop(n)`. The *fields* `target_ticks`/`stop_ticks` keep their names, so reading `bracket.stop_ticks` still works.
  - `.target(n)`/`.stop(n)` size their leg from the entry quantity at the moment they are called, so set `.quantity()` first — `build()` rejects the zero-sized leg you get in the other order. The plural `.targets(..)`/`.stops(..)` take explicit `(quantity, ticks)` pairs and can be called before or after `.quantity()`.
  - `bracket_type` is now `Option<BracketType>`. Left unset, `build()` derives it from which exit legs are present (as async_rithmic does) and omits it from the request when there are none. This changes what a target-only bracket sends: `TARGET_ONLY_STATIC`, where `RithmicAdvancedBracketOrder` defaulted the field to `TargetAndStopStatic` and always sent it. Set `.bracket_type(..)` to override the derivation; `validate()` rejects a value that does not match the legs supplied.
- **The `manual_or_auto` parameters are gone from handle methods.** Origination is now a field on the command struct, set with `.manual_or_auto(..)`. `exit_position_with_placement` and `cancel_all_orders_with_placement` are removed — they existed only to pass that argument.
- **`cancel_all_orders` and `exit_position` now take command structs** (`RithmicCancelAllOrders`, `RithmicExitPosition`) instead of loose arguments, matching every other order call.
- **`RithmicConfig`, `RithmicAccount`, `LoginConfig`, `InstrumentInfo`, `InstrumentInfoError`, `RithmicEnv`, `TrailingStop`, `RithmicIfTouchedTrigger` and all 246 generated protobuf types are now `#[non_exhaustive]`.** Rithmic added fields to existing messages in 24 of the 35 template releases in its change log, so without this attribute every proto refresh would be a major version bump here. Downstream, struct literals and `..Default::default()` stop compiling on these types: use `RithmicConfigBuilder::from_env(env)`, the `new()`/setters/`build()` chain, or `Default::default()` followed by field assignment. Matches on generated enums need a `_` arm. The struct variants `ConfigError::InvalidValue`, `RithmicError::NoTradeRoute`, `FillHistoryRange::Ssboe` and `FillHistoryRange::TradeDate` carry the attribute too, so their fields cannot be matched exhaustively or built as literals downstream.
- **`price` on `RithmicOrder`, `RithmicOcoOrderLeg` and `RithmicModifyOrder` is now `Option<f64>`.** Wrap existing values in `Some(..)`; pass `None` for market orders, which previously went out with `price = 0.0`. A modify still restates the whole order, so set `price` to the order's current price when only the quantity is changing. An unset price is now left out of the request entirely — it can no longer reach the wire as `0.0` or stand in as a `0.0` trigger.
- **Order command types gain a `manual_or_auto` field,** typed as the crate's own `ManualOrAutoEntry` enum and defaulting to `Auto`. The crate defines its own enum, with `From` conversions into each of the seven generated per-request `OrderPlacement` enums, because those have no `Default` — a struct holding one by value cannot derive `Default`.
- **`RithmicOrder` gains `window_name`, `release_at_ssboe`/`release_at_usecs`, `cancel_at_ssboe`/`cancel_at_usecs`/`cancel_after_secs` and `if_touched`.** All are `Option` and left off the wire when unset, so an order that does not use them is unchanged on the wire.
- **`cancel_all_orders` is now attributed as `Auto` instead of `Manual`,** which changes what the server records as the origination. Set `.manual_or_auto(ManualOrAutoEntry::Manual)` on the command to keep the old attribution.
- **`TrailingStop` gains a required `trail_by_price_id: i32` and is built like the order commands:** `new()` takes no arguments, every field has a setter, and `build()` returns `Result<Self, RithmicError>` — `TrailingStop::new().trail_by_ticks(15).trail_by_price_id(7).build()?`. Rithmic rejects a zero price id with rp_code 1112, so `build()` refuses it before it reaches the wire, along with a zero `trail_by_ticks`. **`RithmicIfTouchedTrigger` follows the same pattern:** `new()` no longer takes positional arguments; its `price` is `Option<f64>`, and `build()` requires a symbol, an exchange and the price. A trigger sent without a price omits `if_touched_price` from the wire rather than sending `0.0`, which the default `GreaterThanEqualTo`/`TradePrice` condition would have fired on immediately. Neither type implements `Default` — the all-unset value is not a usable trailing stop or trigger, so `new()` is the only zero-argument constructor. `build()` is opt-in: `trailing_stop_by(ticks, price_id)` and direct field assignment still send exactly what they are given, unchanged from 2.0.0 except that an unset trigger price is now absent instead of `0.0`.
- **`RithmicOrder`, `RithmicBracketOrder` and `RithmicOcoOrderLeg` gain a `trade_route: Option<String>` field.** Leave it `None` to keep the old behavior. Orders now use the route the server publishes for their exchange instead of a hardcoded `"globex"`/`"simulator"`, and fail with `RithmicError::NoTradeRoute` when there is none.
- **`RithmicOcoOrderLeg` gains a `trailing_stop: Option<TrailingStop>` field and `RithmicModifyOrder` a `trigger_price: Option<f64>`.** Set them to `None` to keep the old behavior.
- **`RithmicOrder::duration` is now `TimeInForce` instead of `Option<TimeInForce>`,** matching `RithmicBracketOrder` and `RithmicOcoOrderLeg`. Unwrap existing values. `None` was already sent as `Day`, which is `TimeInForce::default()`, so nothing changes on the wire.
- **`RithmicConfig` gains a required `request_timeout: Duration` field.** Build the config with `RithmicConfig::builder(env)` or `RithmicConfigBuilder::from_env(env)` — the type is now `#[non_exhaustive]`, so struct literals no longer compile downstream anyway.
- **`RithmicOrderPlantHandle::subscribe_account_rms_updates` gains a required `update_bits` parameter.** Pass `vec![]` for the old behavior.
- **`RithmicOrderPlantHandle::adjust_target` (renamed from `adjust_profit`) and `adjust_stop` now take a `RithmicBracketLevelAdjustment`** instead of `(id, ticks)`. The struct adds a `level` field selecting which bracket leg to adjust; `level: None` keeps the old behavior.
- **`RithmicHistoryPlantHandle::load_volume_profile_minute_bars` takes a `VolumeProfileMinuteBarsRequest`** instead of seven positional arguments, so future fields Rithmic adds to the request land as setters rather than signature changes. Build it with the same `new()`/setters/`build()` chain as the order commands; `build()` requires a symbol, an exchange, a bar period of at least one minute and a time window whose end does not precede its start.
- **`RithmicModifyOrder::qty` is renamed to `quantity`,** field and setter. 1.0.0 made the same rename on `RithmicBracketOrder`; this was the last field spelling it the short way.
- **An order placed with an empty `user_tag` now echoes back as `None` instead of `Some("")`,** because the field is no longer sent as `""`.
- **The `ws` module is private.** `ConnectStrategy` was the only public item in it and stays available at the crate root; spell `rithmic_rs::ws::ConnectStrategy` as `rithmic_rs::ConnectStrategy`. The rest of the module was connection plumbing that was never meant as API.
- **Seven handle method names change, covering ten methods** (`list_system_info` exists on all four plants). Each method is now named after the request it sends: the template's own verb where it has one, and `get_` only where the template name has none. Nothing was added or removed, and no signatures changed.

  | old | new | handle |
  |---|---|---|
  | `list_system_info` | `get_system_info` | all four plants |
  | `list_exchanges` | `list_exchange_permissions` | ticker |
  | `request_depth_by_order_snapshot` | `get_depth_by_order_snapshot` | ticker |
  | `subscribe_order_book` | `subscribe_depth_by_order_update` | ticker |
  | `unsubscribe_order_book` | `unsubscribe_depth_by_order_update` | ticker |
  | `pnl_position_snapshots` | `get_pnl_position_snapshot` | pnl |
  | `adjust_profit` | `adjust_target` | order |

  Notes: `list_exchange_permissions` already existed on the order plant — the ticker method was renamed to match it, and both remain, since they are two plants' handles onto the same request. `subscribe_order_book_summary`/`unsubscribe_order_book_summary` is a different request pair and is deliberately unchanged. `adjust_stop` was already named correctly. `RithmicSenderApi::request_depth_by_order_snapshot` keeps its name — every sender-api method is `request_*`, and only the handle method was renamed.

- **Fifteen generated enum re-exports are removed from the crate root,** replaced by crate-owned enums. The generated per-request enums were incompatible with each other despite naming the same concepts — the same order could not be expressed against two request types. `BracketType` keeps its name but now resolves to the crate's own enum instead of `rti::request_bracket_order::BracketType`: the variant names are unchanged, so matches keep compiling, but prost-specific uses (`as i32`, `try_from`) do not. `EasyToBorrowRequest` and `RmsUpdateBits` likewise keep their names but now resolve to crate-owned enums instead of `rti::request_easy_to_borrow_list::Request` and `rti::request_account_rms_updates::UpdateBits` — the last two raw generated types on the curated surface, so a proto refresh can no longer change the crate's own API. Variant names are unchanged there too; prost-specific uses (`as i32`, `try_from`, `from_str_name`) and the derived `Ord`/`PartialOrd` orderings do not carry over, while `as_str_name` does.

  | removed | use instead |
  |---|---|
  | `BracketTransactionType`, `OcoTransactionType`, `NewOrderTransactionType` | `OrderSide` |
  | `BracketPriceType`, `OcoPriceType`, `NewOrderPriceType`, `ModifyPriceType` | `OrderType` |
  | `BracketDuration`, `OcoDuration`, `NewOrderDuration` | `TimeInForce` |
  | `BracketCondition` | `OrderCondition` |
  | `BracketPriceField` | `OrderPriceField` |

### Added

- **`RithmicOrderPlantHandle::show_fill_history(range, max_record_count)`** — fetches the account's fill history (templates 3512/3513, new in template version 5.42), one response per fill. The new `FillHistoryRange` expresses the window in either format the request takes: seconds since the epoch (`FillHistoryRange::ssboe(start, finish)`) or trade dates as CCYYMMDD (`FillHistoryRange::trade_date(start, finish)`). A `max_record_count` above 10,000 is refused with `RithmicError::InvalidArgument`, since Rithmic rejects it.
- **`RithmicOrderPlantHandle::get_user_info(user)`** — fetches a user's profile, entitlement status and session limits (templates 3510/3511); pass `None` for the logged-in user. The unsolicited **`RithmicMessage::UserInfoUpdate`** (template 357) carries user-level changes as they happen; it previously arrived as `UnknownTemplate`.
- **`RithmicHistoryPlantHandle::load_ticks_all`, `load_tick_bars_all` and `load_time_bars_all`** — replay loaders that auto-paginate. Rithmic truncates a large replay and puts a `request_key` on its closing response; the existing loaders return that page as-is and leave `resume_bars` to the caller, while these follow each key until a page closes without one, returning every page's responses in order. Resumption uses the server's key rather than re-requesting from the last timestamp: same-timestamp ticks are routine (one aggressor filling several resting orders), so a timestamp restart would skip or duplicate the ticks at a page boundary. A `max_pages` cap (`None` = unbounded) bounds a runaway replay; when it cuts the replay short, the last returned response still carries its key — readable through the new **`RithmicResponse::resume_key()`** — so `resume_bars` can pick up where it stopped.
- **`RithmicBracketOrder::operation_type`** and the `BracketOperationType` enum — the `order_operation_type` field Rithmic added to bracket requests in template version 5.37. It selects which event on one order of the bracket cancels the rest. Rithmic documents only the wire spellings (`AFOCCA`, `FOCCA`, `CCA`, `FCA`, `OCA`); the rustdoc on each variant carries async_rithmic's reading of them (`FCA` = "fill cancels all", and so on), attributed as such. When unset, the field stays off the wire as before — async_rithmic tried defaulting it to `OCA` and reverted after it broke bracket orders.
- **The generated types pick up every field Rithmic added through template 5.42:** the contact, address, `order_copy_status` and per-plant session-count fields on `ResponseLoginInfo` (also on the new `ResponseGetUserInfo` and `UserInfoUpdate`); `loss_limit` and the account-creation timestamps on `ResponseAccountList`; `source_ssboe`/`source_usecs`/`source_nsecs` on `ExchangeOrderNotification`; `level_1_market_data`/`level_2_market_data` on `ResponseListExchangePermissions` (Rithmic deprecates `entitlement_flag` in favor of these, and the generated field now carries `#[deprecated]`); `min_qprice_change_precision` on `ResponseReferenceData`; `data_bar_seq_num` on `TickBar` and `ResponseTickBarReplay`; and `rms_updates_only` on `RequestPnLPositionUpdates`, which the PnL subscription leaves unset to keep streaming every update.
- **`validate()` on `RithmicOrder`, `RithmicBracketOrder`, `RithmicModifyOrder`, `RithmicOcoOrder` and `RithmicOcoOrderLeg`** — checks that the command carries the prices its price type requires, returning `RithmicError::InvalidArgument` when it does not. The rules are async_rithmic's: `Limit`, `StopLimit` and `LimitIfTouched` need a price; the stop and if-touched types need a trigger; `Market` needs nothing. On `RithmicModifyOrder` the price may stand in for the trigger, since moving a stop by its price alone predates `trigger_price`. On `RithmicBracketOrder` the exit legs are also checked: quantities and tick distances must pair up, every leg's quantity must be positive, and a hand-set `bracket_type` must match the legs supplied. `build()` calls `validate()`, and it is also public, so a command assembled another way can be checked too. Market rules beyond this are deliberately not validated: Rithmic is the authority on what it accepts, and stricter rules compiled in here would refuse orders the server would have taken.
- **`validate()` covers identity, not just prices.** `build()` on the order commands now also requires a symbol, an exchange and a positive quantity — and the `basket_id` on `RithmicModifyOrder`. The remaining command types gain a `validate()` of their own: `RithmicCancelOrder`, `RithmicModifyOrderReferenceData` and `RithmicBracketLevelAdjustment` require their basket id, `RithmicExitPosition` its symbol and exchange as a pair — or neither, which flattens the whole account — and `RithmicLinkOrders` at least two non-empty basket ids. Validation stays the consumer's call: `build()` runs it, and the handles send what they are given. An embedded `TrailingStop` or `RithmicIfTouchedTrigger` is likewise not re-validated — their `build()` is the opt-in strict path, since the field evidence on what Rithmic accepts is contradictory.
- **`RithmicExitPosition` can flatten the whole account.** Its `symbol` and `exchange` are `Option<String>` and come as a pair: both set exits one instrument, neither set sends the fields absent, which template 3504 reads as "exit every position on the account". Previously the crate always sent the fields, so the account-wide form was inexpressible.
- **Reconnect backoff is jittered.** Each retry delay is scaled by a random factor in [0.5, 1.5), so plants that lost the same connection no longer retry in lockstep against a recovering server. The schedule is otherwise unchanged — 500 ms more per attempt, capped at 60 seconds — with the jitter applied after the cap, so a long outage keeps its spread (delays range 30–90 s at the cap).
- **`RithmicConfigBuilder::from_env(env)`** — a construction path for a type that lost struct-literal syntax. It pre-fills the builder from the same environment variables `RithmicConfig::from_env` reads, so a single field can be overridden.
- **Crate-owned `ManualOrAutoEntry`, `OrderCondition` and `OrderPriceField`** at the crate root, joining `OrderSide`, `OrderType` and `TimeInForce`, which already existed at 2.0.0. They stand in for the removed generated re-exports (see the table under Breaking Changes). `ManualOrAutoEntry` is new outright — origination was never exposed before. `OrderCondition` defaults to `GreaterThanEqualTo` and `OrderPriceField` to `TradePrice`, the values `RithmicIfTouchedTrigger::new()` starts from.
- **`Clone` on `RithmicOrderPlantHandle` and `RithmicPnlPlantHandle`** — the ticker and history handles already had it. A clone's `subscription_receiver` picks up the stream from the moment of the clone; it does not replay earlier updates.
- **`SubscriptionFilter` is re-exported at the crate root** — it is the type of `subscription_receiver` on the order and PnL handles, previously reachable only as `rithmic_rs::plants::subscription::SubscriptionFilter`. `rithmic_rs::api` now also re-exports `BracketOperationType` and `FillHistoryRange` alongside the other order enums, and the new `VolumeProfileMinuteBarsRequest` is at the crate root with the other request types.
- **Multi-leg OCO orders** — a `RithmicOcoOrder` now carries two or more `RithmicOcoOrderLeg`s instead of a fixed pair, and each leg can carry its own trailing stop via the new `RithmicOcoOrderLeg::trailing_stop`. Fewer than two legs is rejected, since Rithmic has nothing to cancel a lone leg against.
- **Per-order trade routes** via the new `trade_route` field, which overrides the route the plant would pick — including a route the server never published.
- **`RithmicOrderPlantHandle::trade_route_for(exchange)`** and **`record_trade_route(update)`** — the route an order would take right now, and a way to apply a `TradeRoute` update to the cache. Updates are not applied automatically; see `examples/trade_routes.rs`.
- **`RithmicError::NoTradeRoute { exchange, cached }`** — returned when no route was published for the exchange and the order set none itself, so nothing was sent.
- **`RithmicMessage::UnknownTemplate(UnknownTemplateMessage)`** — a frame whose `template_id` has no message definition in this crate, with the body kept as received. `RithmicMessage` is `#[non_exhaustive]`, so the new variant does not break existing matches.
- **`UnknownTemplateMessage::decode_as<M>()`, `payload_hex()` and `from_payload_hex()`** — decode an unmapped template into your own `prost` type, or capture the full payload for replay in a test. `Ok` from `decode_as` is not proof the type was guessed right.
- **`RithmicMessage::RequestHeartbeat(RequestHeartbeat)`** — the server's keep-alive (template 18), which previously arrived as `UnknownTemplate`. The library does not reply to it.
- **`rithmic_rs::prost`** — re-exports the `prost` this crate's types are generated against. It is a public dependency, so a major bump of `prost` remains a breaking change here.
- **`rithmic_rs::DEFAULT_REQUEST_TIMEOUT`**, `RithmicConfigBuilder::request_timeout` and the `RITHMIC_REQUEST_TIMEOUT_SECS` environment variable — control how long a request waits for a response. The default is 30 seconds; the environment variable takes plain digits only.
- **`RithmicBracketLevelAdjustment`** — the command struct for `adjust_target`/`adjust_stop`: basket `id`, new `ticks` distance, and the `level` selecting a leg.
- **`RithmicCancelAllOrders`, `RithmicExitPosition`, `RithmicLinkOrders` and `RithmicModifyOrderReferenceData`** — command structs for the four order calls that previously took loose arguments. Every order call now takes a command struct.
- **`PartialEq` on `RithmicOrder`, `RithmicBracketOrder`, `RithmicOcoOrderLeg`, `RithmicModifyOrder`, `RithmicCancelOrder`, `RithmicIfTouchedTrigger` and `TrailingStop`** (and on the six new command structs above), so a built command can be compared in a test. `Eq` is not derived: most of these carry `f64` price fields, and the rest stay `PartialEq`-only so a future proto field with an `f64` cannot take an already-promised `Eq` away.
- **`PartialEq` on `RithmicMessage`, `RithmicResponse`, `LoginConfig` and `ConfigError`** (with `Eq` on `ConfigError`), so a whole response can be `assert_eq!`'d in a downstream test instead of being matched field by field.
- **`#[must_use]` on the command, trigger and request types and on `RithmicConfigBuilder`.** The setters consume and return the value, so `order.symbol("ESH6");` on its own line compiles while silently discarding the order; the attribute makes that a warning.
- **RMS auto-liquidation streaming** — `subscribe_account_rms_updates` takes a `Vec` of selectors; pass the new `rithmic_rs::RmsUpdateBits::AutoLiqThresholdCurrentValue` to receive auto-liquidation threshold updates. An empty `Vec` omits `update_bits` rather than sending `0`.
- **`serde` derives on every order command type** behind the `serde` feature — `RithmicOrder`, `RithmicBracketOrder`, `RithmicOcoOrder` and its legs, `RithmicModifyOrder`, the cancel/exit/link/retag/adjustment commands, `TrailingStop`, `RithmicIfTouchedTrigger` and `VolumeProfileMinuteBarsRequest` — so a strategy can persist and replay any command. The feature-flag table in the crate docs had claimed `RithmicOrder` and `TrailingStop` since 1.0.0 without the impls existing; the impls now exist, and the table lists the feature's actual coverage.
- **`RithmicModifyOrder::trigger_price`** — a modify can now set a trigger distinct from the limit price. When `None`, the four triggering price types send `price` in its place.
- **`window_name` on `RithmicBracketOrder`, `RithmicModifyOrder`, `RithmicCancelOrder`, `RithmicExitPosition` and `RithmicOcoOrderLeg`** — the originating window Rithmic records; `RithmicOrder` already had it. On the OCO leg it is per-leg, since `RequestOCOOrder` declares the field `repeated`: like `user_tag`, every leg gets a slot once any leg sets one, and the field is omitted when none does.
- **`RithmicOcoOrder::cancel_at_ssboe`/`cancel_at_usecs`/`cancel_after_secs`** — group-level cancel timing, with `.cancel_at(ssboe, usecs)` setting both halves, matching `RithmicBracketOrder`.
- **`RithmicExitPosition::trading_algorithm`** — the algorithm name credited with the exit.
- **`RithmicModifyOrder::trail_by_ticks` and `if_touched`** — the last two `RequestModifyOrder` fields that had no way to be set. `RequestModifyOrder` declares no `trail_by_price_id`, so the distance is a plain tick count rather than a `TrailingStop`, and the message's `trailing_stop` flag is derived from it.

  With these, every field of the eleven order request messages is reachable from a command type. The request builders set each field explicitly instead of filling the rest from `Default`, so a field added by a future proto refresh fails to compile instead of being silently dropped.

### Changed

- **`generate_protos` names four protos the 0.89.0.0 pool stopped importing.** `request`/`response_accept_agreement` and `request`/`response_set_rithmic_mrkt_data_self_cert_status` still ship as files and templates 504/505 and 508/509 still answer, so the generator compiles them alongside the pool and the crate keeps its agreement and self-certification calls.
- **An unrecognized `template_id` is no longer treated as a decode failure.** It used to produce `Err` with `ProtocolError("Unknown message type…")` and discard the payload; it now returns `Ok` with `RithmicMessage::UnknownTemplate` and logs a `warn`. Code matching `RithmicMessage::Unknown` still compiles but no longer sees these frames — `Unknown` now means only that a frame failed to decode.
- **An unsolicited `Reject` (one that echoes no `user_msg`) is logged at `warn` and dropped,** instead of reaching the request handler, matching nothing, and being dumped at `error`.
- **A `Reject` is always surfaced as a rejection,** with `rp_code` passed through element for element. The success rules for `Response*` messages no longer apply to it, so codes like `["0"]` no longer make a rejected request look like a success.
- **`RithmicError::RequestRejected` renders as `request rejected`** when the rejection carried no code and no message, instead of leaving a dangling separator.
- **The crate-level "Error Handling" docs now cover every way an error surfaces** and what to do about each. Bad data never stops a plant; only transport failure does. `examples/error_handling.rs` covers the same ground as runnable code.
- **Documentation pass across the crate.** Comments and rustdoc now describe what the code does, without describing server behaviour or assuming familiarity with the wire protocol. No API changed.
- **Rustdoc examples on the command types now compile,** instead of being fenced as `ignore` blocks. The old fences hid code that would not build.
- **docs.rs now builds the crate with all features enabled,** so the `serde` impls show up in the rendered docs.
- **The README samples now show correct API usage.** `cancel_order` takes a `RithmicCancelOrder` rather than a bare id, `place_bracket_order` had a literal `...` for its argument, and the history plant block passed `&str` where owned `String`s are required and used an unimported `BarType`.

### Fixed

- **Market orders were sent with `price = 0.0`.** The `price` field was sent unconditionally and defaults to zero, so an order with no meaningful price was priced at zero on the wire. It is now omitted when the caller supplies none.
- **Multi-leg OCO orders zero-filled `price` and `trigger_price`.** Both now follow the all-or-none rule the trailing-stop fields already used: absent when no leg carries one, index-aligned once any leg does.
- **Empty `user_tag` and `localid` were sent as `""`** instead of being omitted. `request_modify_order_reference_data` still sends whatever it is given — an empty tag there is how a tag is cleared.
- **Order origination was attributed inconsistently.** Orders, modifies, cancels and exits sent the bare literal `2` while cancel-all sent `Manual`, so one session reported two different originators. All commands now use the typed enum of their own request module and default to `Auto`.
- **Every order went out on `"globex"` (live) or `"simulator"` regardless of exchange,** ignoring the routes the server publishes. The plant now uses the published route for the order's exchange, preferring one marked default. `login()` reads the routes once, and orders route off that snapshot for the life of the connection.
- **Requests were sent with a hardcoded `Trader` user type,** and the account list request carried no `fcm_id`/`ib_id`, so FCM and IB logins saw no accounts. `login()` now retrieves the login info once and scopes requests with it. As a result, **`get_account_list` and `get_account_rms_info` may return fewer accounts than before**, including for `Trader` logins.
- **Requests could wait forever.** A request whose response never arrives now fails after 30 seconds with the new `RithmicError::RequestTimeout` instead of blocking its caller indefinitely. Reconcile a timed-out order rather than re-sending it.
- **A frame that fails to decode is no longer discarded.** The echoed `user_msg` is now read off the wire even when the body will not decode, so the failure resolves the matching request with `ProtocolError` instead of leaving its caller waiting. Where no id is recoverable, the per-request timeout covers it.
- **A response that ended a request without being marked `multi_response` leaked the parts accumulated under its id.** They were kept for the life of the connection and prepended to a later request that reused the id.
- **The order and PnL plants now reject queued commands once a disconnect is in flight.** Previously, an order submitted from a cloned handle at the same time as `disconnect()` could still be sent to Rithmic while its caller saw `ConnectionClosed` — a failure on record for an order that was live at the exchange.
- **`disconnect()` now sends `Close` even when the logout fails.** It used to return early, leaving the actor with `close_requested` set: no heartbeats, every later command dropped, pending requests never drained.
- **A server `ForcedLogout` (template 77) now stops the plant actor,** failing pending requests with `ConnectionClosed`. Previously the plant kept heartbeating a session the server had already ended, while callers waited on responses that would never come.
- **The `heartbeat_interval` from the login response is now used as the heartbeat period.** Plants used `hb.max(60)`, so a server asking for a heartbeat every 30 seconds got one every 60 and dropped the connection as idle.
- **A `MarketIfTouched` or `LimitIfTouched` modify omitted `trigger_price`.** The fallback to the order's own price covered only the two stop types, so an if-touched modify with no separate trigger level went out with the field unset. The fallback now covers the same four types `validate()` requires a trigger for.
- **Single-order trailing stops now populate `trail_by_price_id`.** It was omitted before, and Rithmic rejected the trailing stop with rp_code 1112.
- **Bracket target/stop adjustments omitted the `level` field,** so on a multi-leg bracket every adjustment landed on the server's default leg and the other legs were unreachable.
- **`request_account_rms_updates` sent `update_bits: None`,** so `auto_liq_threshold_current_value` never streamed even when subscribed.
- **`examples/bracket_order.rs` no longer exits its listener on a recoverable error.** It broke out of the loop on any populated `update.error` — which a per-message decode failure also sets — so a single undecodable frame silently stopped order updates on a live bracket.
- **Samples handled a rejected `subscribe` in an `Err` arm that can never match,** so a rejected subscription was reported as a success. They now check `resp.error`.

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

[Unreleased]: https://github.com/pbeets/rithmic-rs/compare/v2.0.0...HEAD
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
