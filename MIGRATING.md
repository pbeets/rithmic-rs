# Migrating to Protocol API 0.90 (unreleased)

Coming from 2.x, work through the 3.0.0 guide below first, then this list.

- `RithmicOrder`, `RithmicModifyOrder`, `RithmicBracketOrder` and
  `RithmicOcoOrderLeg` quantities and their setters now take `u64`.
- Bracket `target_quantity` and `stop_quantity` are `Vec<u64>`. The `targets()`
  and `stops()` setters take `(u64, i32)` pairs. Tick distances stay `i32`.
  Integer literals still work. Convert signed variables with `u64::try_from()`
  so negative quantities are rejected.
- Requests send only `quantity_64`, `target_quantity_64` and
  `stop_quantity_64`. Read sizes from the `*_64` fields, such as
  `trade_size_64`, `bid_size_64` and `fill_size_64`. The 32-bit fields Rithmic
  deprecated are no longer generated.
- `ResponseListExchangePermissions::entitlement_flag` is no longer generated.
  Read `level_1_market_data` and `level_2_market_data` instead.
- `show_fill_history()` accepts `None` or a cap in `1..=10_000`; `Some(0)` is now
  rejected locally.
- Match template 358 as `RithmicMessage::AccountAndUserUpdate`.
  `UserAccountUpdate` now matches only template 76.
- Remove code that uses `DepthByOrderEndEvent` or its `RithmicMessage`
  variant. Template 161 no longer exists.
- Rename `rti::last_trade::PresenceBits::PrecentChange` to `PercentChange`.
  `as_str_name()` now returns the corrected `PERCENT_CHANGE` spelling; the
  numeric value remains 4.
- `BracketOperationType::Oca` is removed. Pick one of the remaining variants,
  or leave `operation_type` unset for the server default.
- `get_order_session_config(Some(defer))` is deprecated. Call
  `set_session_config(SessionConfig::new().should_defer_request(defer))`.
- The request-timeout items are gone: `DEFAULT_REQUEST_TIMEOUT`,
  `RithmicError::RequestTimeout`, the `request_timeout` field and the
  `request_timeout()` setter. They had no effect, so delete them. For
  a deadline, wrap the call in `tokio::time::timeout` (see
  `examples/request_timeout.rs`). Nothing reads `RITHMIC_REQUEST_TIMEOUT_SECS`.
- Replace `retry_timeout()` with `connect_total_timeout()`, and drop any use of
  the `retry_timeout` field.
- Delete calls to the history plant's `resume_bars()`. The `load_*` methods
  already resume replays the server cuts short.
- Replace `load_ticks(symbol, exchange, start, end)` with `load_ticks_all()`,
  which takes the same arguments and has no 10,000-trade cap. To keep the cap,
  call `load_tick_bars(symbol, exchange, 1, start, end)`.
- The ticker handle picks feeds with `MarketData`. Pass the feeds you want to
  `subscribe` or `unsubscribe`, combined with `|`. Each old method maps to:

  | Old method | `MarketData` feeds |
  |---|---|
  | `subscribe` | `LAST_TRADE \| BBO` |
  | `subscribe_instrument_status` | `MARKET_MODE` |
  | `subscribe_order_book_summary` | `ORDER_BOOK` |
  | `subscribe_session_prices` | `OPEN \| HIGH_LOW` |
  | `subscribe_quote_statistics` | `HIGH_BID_LOW_ASK` |
  | `subscribe_indicator_prices` | `OPENING_INDICATOR \| CLOSING_INDICATOR` |
  | `subscribe_open_interest` | `OPEN_INTEREST` |
  | `subscribe_end_of_day_prices` | `CLOSE \| SETTLEMENT \| PROJECTED_SETTLEMENT \| ADJUSTED_CLOSE` |
  | `subscribe_order_price_limits` | `HIGH_PRICE_LIMIT \| LOW_PRICE_LIMIT` |
  | `subscribe_symbol_margin_rate` | `MARGIN_RATE` |

  Each `unsubscribe_*` method maps the same way onto `unsubscribe`. The
  depth-by-order methods are unchanged.
- `subscribe_by_underlying(.., fields, request_type)` becomes
  `subscribe_by_underlying(.., data)` or `unsubscribe_by_underlying(.., data)`.
  Template 105 does not list `ADJUSTED_CLOSE`, so that bit is sent as-is and
  the server decides what to do with it.
- Calls that started or stopped a feed by argument are now a `subscribe_*` and
  `unsubscribe_*` pair. Each unsubscribe takes the same arguments as its
  subscribe:

  | Old call | New call |
  |---|---|
  | `get_easy_to_borrow_list(EasyToBorrowRequest::Subscribe)` | `subscribe_easy_to_borrow_list()` |
  | `get_easy_to_borrow_list(EasyToBorrowRequest::Unsubscribe)` | `unsubscribe_easy_to_borrow_list()` |
  | `subscribe_account_rms_updates(true, bits)` | `subscribe_account_rms_updates(data)` |
  | `subscribe_account_rms_updates(false, bits)` | `unsubscribe_account_rms_updates(data)` |
  | `subscribe_time_bar_updates(.., period, Request::Subscribe)` | `subscribe_time_bar_updates(.., period)` |
  | `subscribe_time_bar_updates(.., period, Request::Unsubscribe)` | `unsubscribe_time_bar_updates(.., period)` |
  | `subscribe_tick_bar_updates(.., specifier, Request::Subscribe)` | `subscribe_tick_bar_updates(.., specifier)` |
  | `subscribe_tick_bar_updates(.., specifier, Request::Unsubscribe)` | `unsubscribe_tick_bar_updates(.., specifier)` |
  | `get_front_month_contract(symbol, exchange, false)` | `get_front_month_contract(symbol, exchange)` |
  | `get_front_month_contract(symbol, exchange, true)` | `subscribe_front_month_contract(symbol, exchange)` |
  | `get_trade_routes(subscribe_for_updates)` | `get_trade_routes()` |

  `subscribe_easy_to_borrow_list` returns every reply as a `Vec`, as before.
  The unsubscribe returns one `RithmicResponse`.
- The RMS `data` is an `Option<RmsUpdates>`: `vec![]` becomes `None`, and
  `vec![RmsUpdateBits::AutoLiqThresholdCurrentValue]` becomes
  `Some(RmsUpdates::AUTO_LIQ_THRESHOLD_CURRENT_VALUE)`. Combine fields with
  `|`. `RmsUpdates` has no serde support.
- `EasyToBorrowRequest` and `RmsUpdateBits` are removed.
- `subscribe_time_bar_updates` and `unsubscribe_time_bar_updates` take
  `TimeBarType`, the same type as the time bar loaders. Replace
  `rti::request_time_bar_update::BarType::MinuteBar` with
  `TimeBarType::MinuteBar`. Tick bar updates keep the `rti` types.
- `get_trade_routes()` always asks for route updates, as login does. Front
  month and trade route updates have no unsubscribe.

# Migrating to 3.0.0

Work through the sections in order. The compiler finds most of the work, and
section 1 fixes most of the errors.

Section 10 lists six changes the compiler will *not* find. They compile
untouched and change what reaches the exchange. Read it even if everything
else builds.

## 1. Build commands with `new()` and setters

The order command types are `#[non_exhaustive]`, so `..Default::default()` does
not compile outside this crate. There is no separate builder type: `new()`
takes no arguments and returns the command with its defaults filled in, every
field has a setter of the same name, and `build()` runs `validate()` and returns
`Result<Self, RithmicError>`.

```rust
// Before
let order = RithmicOrder {
    symbol: "ESZ6".into(),
    exchange: "CME".into(),
    quantity: 1,
    ..Default::default()
};

// After
let order = RithmicOrder::new()
    .symbol("ESZ6")
    .exchange("CME")
    .quantity(1)
    .transaction_type(OrderSide::Buy)
    .price_type(OrderType::Limit)
    .price(5000.0)
    .build()?;
```

The fields are still public, so direct assignment works when you need it:

```rust
let mut order = RithmicOrder::new();
order.symbol = "ESZ6".into();
```

`build()` is the opt-in strict path. Commands carrying an instrument need a symbol,
an exchange and a positive quantity; commands naming an existing order need its
basket id; and every price type needs the prices it uses. The handles send what
they are given, so a command assembled by field access skips those checks. Call
`validate()` yourself if you want them.

`TrailingStop` and `RithmicIfTouchedTrigger` are built the same way, though they
have no separate `validate()`, and neither implements `Default`. `TrailingStop`
now requires `trail_by_price_id`, and `build()` refuses a zero id.

```rust
let stop = TrailingStop::new().trail_by_ticks(15).trail_by_price_id(7).build()?;
```

### Other types that lost struct-literal syntax

`RithmicConfig`, `RithmicAccount`, `LoginConfig`, `InstrumentInfo`,
`InstrumentInfoError`, `RithmicEnv` and every generated protobuf type are
`#[non_exhaustive]`.

- Config: use `RithmicConfig::builder(env)` or `RithmicConfigBuilder::from_env(env)`.
- Generated types: `Default::default()` followed by field assignment.
- Matches on generated enums need a `_` arm.

`ConfigError::InvalidValue`, `RithmicError::NoTradeRoute`, `FillHistoryRange::Ssboe`
and `FillHistoryRange::TradeDate` carry the attribute too, so their fields cannot
be matched exhaustively or built as literals downstream.

## 2. Replace the generated enums

Fourteen generated enum re-exports are gone. Twelve are replaced outright:

| removed | use instead |
|---|---|
| `BracketTransactionType`, `OcoTransactionType`, `NewOrderTransactionType` | `OrderSide` |
| `BracketPriceType`, `OcoPriceType`, `NewOrderPriceType`, `ModifyPriceType` | `OrderType` |
| `BracketDuration`, `OcoDuration`, `NewOrderDuration` | `TimeInForce` |
| `BracketCondition` | `OrderCondition` |
| `BracketPriceField` | `OrderPriceField` |

Two keep their names but now resolve to crate-owned enums: `BracketType`
(was `rti::request_bracket_order::BracketType`) and `EasyToBorrowRequest`
(was `rti::request_easy_to_borrow_list::Request`).

Variant names are unchanged, so match arms port over as written. Also:

- **The crate-owned enums are `#[non_exhaustive]`.** Add a `_` arm to every
  exhaustive `match`.
- **`OrderType` has six variants where `OcoPriceType` had four**, adding
  `MarketIfTouched` and `LimitIfTouched`.
- **The prost methods are gone:** `as i32`, `try_from`, `from_str_name` and the
  derived `Ord`/`PartialOrd`. `as_str_name` still works.

## 3. Rename handle methods

Seven names change, covering ten methods (`list_system_info` exists on all four
plants). Signatures are unchanged except `adjust_target`, which also takes a
command struct (see section 6).

| old | new | handle |
|---|---|---|
| `list_system_info` | `get_system_info` | all four plants |
| `list_exchanges` | `list_exchange_permissions` | ticker |
| `request_depth_by_order_snapshot` | `get_depth_by_order_snapshot` | ticker |
| `subscribe_order_book` | `subscribe_depth_by_order_update` | ticker |
| `unsubscribe_order_book` | `unsubscribe_depth_by_order_update` | ticker |
| `pnl_position_snapshots` | `get_pnl_position_snapshot` | pnl |
| `adjust_profit` | `adjust_target` | order |

`RithmicSenderApi::request_depth_by_order_snapshot` keeps its name.

## 4. Prices are `Option<f64>`

`price` on `RithmicOrder`, `RithmicOcoOrderLeg` and `RithmicModifyOrder` is now
`Option<f64>`. Wrap existing values in `Some(..)`, and pass `None` for market
orders. An unset price is left out of the request.

A modify restates the whole order, so set `price` to the order's current
price when only the quantity is changing.

## 5. `RithmicAdvancedBracketOrder` is gone

`RithmicBracketOrder` now carries every venue-native field the advanced type did:
trailing stops, break-even, timed release and cancel, if-touched entry. Call
`place_bracket_order` instead of the removed `place_advanced_bracket_order`.

What to rename depends on which type you used.

**Coming from the plain `RithmicBracketOrder`:** the tick fields change name *and*
type. `profit_ticks: i32` becomes `target_ticks: Vec<i32>`, and `stop_ticks: i32`
becomes `stop_ticks: Vec<i32>`: same name, new type. Reads need an index:
`bracket.stop_ticks[0]` where you had `bracket.stop_ticks`.

**Coming from `RithmicAdvancedBracketOrder`:** `target_quantity`, `target_ticks`,
`stop_quantity` and `stop_ticks` keep both their names and their `Vec<i32>` types.
Field access ports over unchanged.

Either way:

- The single-value **setters** are renamed: `.profit_ticks(n)` is now `.target(n)`,
  `.stop_ticks(n)` is now `.stop(n)`.
- `.target(n)`/`.stop(n)` size their leg from the entry quantity at the moment
  they are called, so call `.quantity()` first, or `build()` rejects the
  zero-sized leg. The plural `.targets(..)`/`.stops(..)` take explicit
  `(quantity, ticks)` pairs and can be called in any order.
- `bracket_type` is now `Option<BracketType>`. Left unset, `build()` derives it
  from the exit legs present and omits it when there are none.

A target-only bracket now sends `TARGET_ONLY_STATIC`, not `TargetAndStopStatic`.
Set `.bracket_type(..)` to override; `validate()` rejects a value that does not
match the legs supplied.

## 6. Order calls take command structs

`cancel_all_orders()` now takes a `RithmicCancelAllOrders`, and
`exit_position(symbol, exchange)` now takes a `RithmicExitPosition`.

Origination is a field on the command rather than a handle argument, set with
`.manual_or_auto(..)`. It defaults to `ManualOrAutoEntry::Auto` on every command
type. Set it explicitly on all of them or none, or one session will report two
different originators.

`adjust_target`/`adjust_stop` take a `RithmicBracketLevelAdjustment` instead of
`(id, ticks)`. Its `level` field selects which bracket leg to adjust, and
`level: None` keeps the old behavior.

`load_volume_profile_minute_bars` takes a `VolumeProfileMinuteBarsRequest` instead
of seven positional arguments:

```rust
let request = VolumeProfileMinuteBarsRequest::new()
    .symbol("ESZ6")
    .exchange("CME")
    .bar_type_period(5)
    .start_time_sec(start_ssboe)
    .end_time_sec(end_ssboe)
    .build()?;

let bars = handle.load_volume_profile_minute_bars(request).await?;
```

`build()` requires a symbol, an exchange, a bar period of at least one minute, and
a window whose end does not precede its start. The handle runs the same checks, so
a request that skipped `build()` fails with `RithmicError::InvalidArgument` rather
than reaching the server incomplete.

`RithmicSenderApi::request_tick_bar_replay` and `request_time_bar_replay` take a
`TickBarReplayRequest` and a `TimeBarReplayRequest`, built the same way.

`TimeBarType` is a new alias for `rti::request_time_bar_replay::BarType`, exported
at the crate root. The old path still works.

`subscribe_account_rms_updates` gains a required `update_bits` parameter. Pass
`vec![]` for the old behavior.

## 7. Switch replays to the `_all` methods

Rithmic caps a replay at 10,000 records, and a capped reply looks the same as a
complete one. `load_tick_bars` and `load_time_bars` keep that cap.
`load_tick_bars_all` and `load_time_bars_all` lift it and take the same
arguments, so the switch is the method name. For ticks, use `load_ticks_all`.

The server can also cut a large reply short. The plant usually continues it
automatically, but for large time bar windows, check the last bar reaches the
end of the window; the README's History Plant section shows how.

```rust
// Before: first 10,000 bars, silently
let bars = handle
    .load_time_bars(symbol, exchange, BarType::MinuteBar, 5, start, end)
    .await?;

// After: the whole window
let bars = handle
    .load_time_bars_all(symbol, exchange, TimeBarType::MinuteBar, 5, start, end)
    .await?;
```

The whole window is buffered before the call returns, so a full 23-hour ES
session runs to hundreds of thousands of records in memory. The capped methods
remain for when that is what you want.

The `load_*` methods also validate now. An empty symbol or exchange, a
`bar_length` or `bar_type_period` below 1, a non-positive timestamp, or an
`end_time_sec` before `start_time_sec` returns `RithmicError::InvalidArgument`
without a round trip. A call that used to come back empty may now return this
error.

## 8. Build `RithmicConfig` through the builder

Use `RithmicConfig::builder(env)` or `RithmicConfigBuilder::from_env(env)`, which
pre-fills from the same environment variables `RithmicConfig::from_env` reads so a
single field can be overridden.

## 9. Field and module changes

| change | what to do |
|---|---|
| `RithmicModifyOrder::qty` renamed to `quantity` | rename field and setter |
| `RithmicOrder::duration` is `TimeInForce`, not `Option<TimeInForce>` | unwrap; `None` sent `Day`, so the wire is unchanged |
| `ws` module is private | spell `rithmic_rs::ws::ConnectStrategy` as `rithmic_rs::ConnectStrategy` |
| new `Option` fields on the command types | leave `None` to keep old behavior |

The new fields are `trade_route` on `RithmicOrder`/`RithmicBracketOrder`/`RithmicOcoOrderLeg`;
`trailing_stop` on `RithmicOcoOrderLeg`; `trigger_price`, `trail_by_ticks` and
`if_touched` on `RithmicModifyOrder`; `window_name`, `release_at_*`, `cancel_at_*`,
`cancel_after_secs` and `if_touched` on `RithmicOrder`; and `manual_or_auto` on
every order command.

## 10. Behavior changes that need no code change

These compile as-is but change what goes on the wire or what the server records.

- **Orders route off the exchange's published trade route**, not
  `"globex"`/`"simulator"`. `login()` reads the routes once and orders route off
  that snapshot. With no route published and none set on the command, the order is
  not sent and you get `RithmicError::NoTradeRoute`.
- **`get_account_list` and `get_account_rms_info` may return fewer accounts.**
  `login()` now scopes requests with the real login info, which also lets FCM and
  IB logins see their accounts.
- **`cancel_all_orders` is attributed as `Auto`, not `Manual`.** Set
  `.manual_or_auto(ManualOrAutoEntry::Manual)` on the command to keep the old
  attribution.
- **An order placed with an empty `user_tag` echoes back as `None`, not `Some("")`,**
  because the field is no longer sent as `""`.
- **An unrecognized `template_id` is no longer a decode failure.** It arrives as
  `RithmicMessage::UnknownTemplate` with the body intact. Code matching
  `RithmicMessage::Unknown` still compiles but no longer sees these frames.
  `Unknown` now means only that a frame failed to decode.
- **Reconnect delays are jittered** by a factor in `[0.5, 1.5)`, applied after the
  cap. The schedule is otherwise unchanged.

## 11. Proto changes

`RithmicMessage::AccountListUpdates` (template 354) is removed. Subscribe with
`subscribe_account_rms_updates` and match `RithmicMessage::AccountRmsUpdates`
instead.

Two `UserAccountUpdate` fields became strings: `update_type` and `access_type` are
now `Option<String>` (values like `add_account`, `modify_account`), and the
generated `UpdateType`/`AccessType` enums no longer exist.

## Checklist

- [ ] Replace struct literals and `..Default::default()` with `new()` + setters
- [ ] Swap the generated enums for the crate-owned ones (section 2)
- [ ] Rename the handle methods (section 3)
- [ ] Wrap prices in `Some(..)`; pass `None` for market orders
- [ ] Move off `RithmicAdvancedBracketOrder`
- [ ] Convert loose-argument order calls to command structs
- [ ] Pass a request struct to the `RithmicSenderApi::request_*_replay` methods
- [ ] Switch replays to `load_ticks_all` / `load_tick_bars_all` / `load_time_bars_all` (section 7)
- [ ] Build `RithmicConfig` through the builder
- [ ] Add a `_` arm to matches on generated enums
- [ ] Handle `RithmicError::NoTradeRoute`
- [ ] Re-check account lists if you rely on `get_account_list` returning everything
- [ ] Decide whether `cancel_all_orders` should stay `Manual` (it is now `Auto`)
- [ ] Check target-only brackets: they now send `TARGET_ONLY_STATIC`
- [ ] Confirm a trade route exists for every exchange you trade
