# Migrating to 3.0.0

3.0.0 reshapes the order surface. Commands are built with `new()` and chained
setters instead of struct literals, the generated protobuf enums are replaced by
crate-owned ones, and every order call takes a command struct.

Most of the work is mechanical and the compiler finds it. Work through the
sections in order — section 1 resolves the majority of the errors.

Section 10 lists what the compiler will *not* find: six changes that compile
untouched and alter what reaches the exchange. Read that one even if everything
else builds.

## 1. Build commands with `new()` and setters

The order command types are `#[non_exhaustive]`, so `..Default::default()` no
longer compiles outside this crate. There is no separate builder type: `new()`
takes no arguments and returns the command with its defaults filled in, every
field has a setter of the same name, and `build()` runs `validate()` and returns
`Result<Self, RithmicError>`.

```rust
// Before
let order = RithmicOrder {
    symbol: "ESM6".into(),
    exchange: "CME".into(),
    quantity: 1,
    ..Default::default()
};

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

The fields are still public, so direct assignment works when you need it:

```rust
let mut order = RithmicOrder::new();
order.symbol = "ESH6".into();
```

`build()` is the opt-in strict path. Commands carrying an instrument need a symbol,
an exchange and a positive quantity; commands naming an existing order need its
basket id; and every price type needs the prices it uses. The handles send what
they are given, so a command assembled by field access skips those checks — call
`validate()` yourself if you want them.

`TrailingStop` and `RithmicIfTouchedTrigger` are built the same way, though they
have no separate `validate()`, and neither implements `Default`. `TrailingStop`
now requires `trail_by_price_id`, and `build()` refuses a zero id.

```rust
let stop = TrailingStop::new().trail_by_ticks(15).trail_by_price_id(7).build()?;
```

### Other types that lost struct-literal syntax

`RithmicConfig`, `RithmicAccount`, `LoginConfig`, `InstrumentInfo`,
`InstrumentInfoError`, `RithmicEnv` and all 246 generated protobuf types are
`#[non_exhaustive]`. Rithmic added fields to existing messages in 24 of the 35
template releases in its change log; without the attribute, every proto refresh
would be a major version bump here.

- Config: use `RithmicConfig::builder(env)` or `RithmicConfigBuilder::from_env(env)`.
- Generated types: `Default::default()` followed by field assignment.
- Matches on generated enums need a `_` arm.

`ConfigError::InvalidValue`, `RithmicError::NoTradeRoute`, `FillHistoryRange::Ssboe`
and `FillHistoryRange::TradeDate` carry the attribute too, so their fields cannot
be matched exhaustively or built as literals downstream.

## 2. Replace the generated enums

Fourteen generated enum re-exports leave the curated surface. The generated
per-request enums named the same concepts but were incompatible with each other,
so the same order could not be expressed against two request types.

Twelve are replaced outright:

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

Variant names are unchanged throughout, so the arms port over as written. Two
things do not:

- **The crate-owned enums are `#[non_exhaustive]`.** The generated enums were not,
  so an exhaustive `match` that compiled at 2.0.0 now needs a `_` arm.
- **`OrderType` has six variants where `OcoPriceType` had four**, adding
  `MarketIfTouched` and `LimitIfTouched`.

The prost-specific surface does not carry over either — `as i32`, `try_from`,
`from_str_name` and the derived `Ord`/`PartialOrd`. `as_str_name` is still
available.

## 3. Rename handle methods

Seven names change, covering ten methods (`list_system_info` exists on all four
plants). Each method is now named after the request it sends. Signatures are
unchanged except `adjust_target`, which also swaps `(id, ticks)` for a command
struct — see section 6.

| old | new | handle |
|---|---|---|
| `list_system_info` | `get_system_info` | all four plants |
| `list_exchanges` | `list_exchange_permissions` | ticker |
| `request_depth_by_order_snapshot` | `get_depth_by_order_snapshot` | ticker |
| `subscribe_order_book` | `subscribe_depth_by_order_update` | ticker |
| `unsubscribe_order_book` | `unsubscribe_depth_by_order_update` | ticker |
| `pnl_position_snapshots` | `get_pnl_position_snapshot` | pnl |
| `adjust_profit` | `adjust_target` | order |

`list_exchange_permissions` already existed on the order plant; the ticker method
was renamed to match, and both remain — two plants' handles onto the same request.
`subscribe_order_book_summary`/`unsubscribe_order_book_summary` is a different
request pair and is unchanged. `RithmicSenderApi::request_depth_by_order_snapshot`
keeps its name, since every sender-api method is `request_*`.

## 4. Prices are `Option<f64>`

`price` on `RithmicOrder`, `RithmicOcoOrderLeg` and `RithmicModifyOrder` is now
`Option<f64>`. Wrap existing values in `Some(..)`, and pass `None` for market
orders — those previously went out priced at `0.0`.

An unset price is now left out of the request entirely. It can no longer reach the
wire as `0.0` or stand in as a `0.0` trigger.

A modify still restates the whole order, so set `price` to the order's current
price when only the quantity is changing.

## 5. `RithmicAdvancedBracketOrder` is gone

`RithmicBracketOrder` now carries every venue-native field the advanced type did —
trailing stops, break-even, timed release and cancel, if-touched entry. Call
`place_bracket_order` instead of the removed `place_advanced_bracket_order`.

Which fields you rename depends on which type you were using — 2.0.0 had both, and
they did not agree.

**Coming from the plain `RithmicBracketOrder`:** the tick fields change name *and*
type. `profit_ticks: i32` becomes `target_ticks: Vec<i32>`, and `stop_ticks: i32`
becomes `stop_ticks: Vec<i32>` — same name, new type. Reads need an index:
`bracket.stop_ticks[0]` where you had `bracket.stop_ticks`.

**Coming from `RithmicAdvancedBracketOrder`:** `target_quantity`, `target_ticks`,
`stop_quantity` and `stop_ticks` keep both their names and their `Vec<i32>` types.
Field access ports over unchanged.

Either way:

- The single-value **setters** are renamed: `.profit_ticks(n)` is now `.target(n)`,
  `.stop_ticks(n)` is now `.stop(n)`.
- `.target(n)`/`.stop(n)` size their leg from the entry quantity at the moment
  they are called, so call `.quantity()` first — otherwise `build()` rejects the
  zero-sized leg. The plural `.targets(..)`/`.stops(..)` take explicit
  `(quantity, ticks)` pairs and can be called in any order.
- `bracket_type` is now `Option<BracketType>`. Left unset, `build()` derives it
  from the exit legs present and omits it when there are none.

That last point changes the wire for a target-only bracket: it now sends
`TARGET_ONLY_STATIC`. At 2.0.0 the plain `RithmicBracketOrder` always sent
`TargetAndStopStatic`, whatever legs you gave it, because its conversion into the
advanced type hardcoded that value. Set `.bracket_type(..)` to override;
`validate()` rejects a value that does not match the legs supplied.

## 6. Order calls take command structs

`cancel_all_orders()` now takes a `RithmicCancelAllOrders`, and
`exit_position(symbol, exchange)` now takes a `RithmicExitPosition`, matching every
other order call.

Origination is a field on the command rather than a handle argument, set with
`.manual_or_auto(..)`. It defaults to `ManualOrAutoEntry::Auto` on every command
type — set it explicitly on all of them or none, or one session will report two
different originators.

`adjust_target`/`adjust_stop` take a `RithmicBracketLevelAdjustment` instead of
`(id, ticks)`. Its `level` field selects which bracket leg to adjust, and
`level: None` keeps the old behavior.

`load_volume_profile_minute_bars` takes a `VolumeProfileMinuteBarsRequest` instead
of seven positional arguments, so fields Rithmic adds later land as setters rather
than signature changes:

```rust
let request = VolumeProfileMinuteBarsRequest::new()
    .symbol("ESM6")
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

`TimeBarType` is a new alias for `rti::request_time_bar_replay::BarType`, exported
at the crate root. The old path still works.

`subscribe_account_rms_updates` gains a required `update_bits` parameter. Pass
`vec![]` for the old behavior.

## 7. Your replays were probably truncated

Nothing here breaks, but it is the change most likely to have been quietly
costing you data.

Rithmic caps a replay at 10,000 records and gives no sign that it did: the
closing response of a truncated replay is byte-identical to a complete one's.
2.x never asked for the cap to be lifted, so `load_ticks`, `load_tick_bars` and
`load_time_bars` returned the first 10,000 records of the window and looked like
they had returned all of it. An hour of a liquid contract runs well past that,
and one-second bars pass it in under three hours.

`load_ticks_all`, `load_tick_bars_all` and `load_time_bars_all` set Rithmic's
`resume_bars` flag, which lifts the cap — one request, whole window, no paging.
Same signatures otherwise, so the switch is the method name.

```rust
// Before — first 10,000 bars, silently
let bars = handle.load_time_bars(symbol, exchange, BarType::MinuteBar, 5, start, end).await?;

// After — the whole window
let bars = handle.load_time_bars_all(symbol, exchange, TimeBarType::MinuteBar, 5, start, end).await?;
```

The whole window is buffered before the call returns, so a full 23-hour ES
session runs to hundreds of thousands of records in memory. The capped methods remain for when
that is what you want.

The `load_*` methods also validate now. An empty symbol or exchange, a
`bar_length` or `bar_type_period` below 1, a non-positive timestamp, or an
`end_time_sec` before `start_time_sec` returns `RithmicError::InvalidArgument`
without a round trip. Only a zero `bar_length` was caught before, so a call that
appeared to work and came back empty may now surface as an error — which is the
point.

## 8. Build `RithmicConfig` through the builder

Use `RithmicConfig::builder(env)` or `RithmicConfigBuilder::from_env(env)`, which
pre-fills from the same environment variables `RithmicConfig::from_env` reads so a
single field can be overridden.

## 9. Field and module changes

| change | what to do |
|---|---|
| `RithmicModifyOrder::qty` renamed to `quantity` | rename field and setter |
| `RithmicOrder::duration` is `TimeInForce`, not `Option<TimeInForce>` | unwrap; `None` already sent `Day`, so the wire is unchanged |
| `ws` module is private | spell `rithmic_rs::ws::ConnectStrategy` as `rithmic_rs::ConnectStrategy` |
| new `Option` fields on the command types | leave `None` to keep old behavior |

The new fields are `trade_route` on `RithmicOrder`/`RithmicBracketOrder`/`RithmicOcoOrderLeg`;
`trailing_stop` on `RithmicOcoOrderLeg`; `trigger_price`, `trail_by_ticks` and
`if_touched` on `RithmicModifyOrder`; `window_name`, `release_at_*`, `cancel_at_*`,
`cancel_after_secs` and `if_touched` on `RithmicOrder`; and `manual_or_auto` on
every order command.

## 10. Behavior changes that need no code change

These compile as-is but change what goes on the wire or what the server records.

- **Orders route off the exchange's published trade route** instead of a hardcoded
  `"globex"`/`"simulator"`. `login()` reads the routes once and orders route off
  that snapshot. With no route published and none set on the command, the order is
  not sent and you get `RithmicError::NoTradeRoute`.
- **`get_account_list` and `get_account_rms_info` may return fewer accounts.**
  Requests used to carry a hardcoded `Trader` user type and no `fcm_id`/`ib_id`.
  `login()` now scopes requests with the real login info, which also fixes FCM and
  IB logins seeing no accounts at all.
- **`cancel_all_orders` is attributed as `Auto`, not `Manual`.** Set
  `.manual_or_auto(ManualOrAutoEntry::Manual)` on the command to keep the old
  attribution.
- **An order placed with an empty `user_tag` echoes back as `None`, not `Some("")`,**
  because the field is no longer sent as `""`.
- **An unrecognized `template_id` is no longer a decode failure.** It arrives as
  `RithmicMessage::UnknownTemplate` with the body intact. Code matching
  `RithmicMessage::Unknown` still compiles but no longer sees these frames —
  `Unknown` now means only that a frame failed to decode.
- **Reconnect delays are jittered** by a clock-derived factor in `[0.5, 1.5)`, applied
  after the cap, so plants that lost the same connection no longer retry in
  lockstep. The schedule is otherwise unchanged.

## 11. Protos move to 0.89.0.0

The bundled protos track R | Protocol API 0.89.0.0 (template version 5.42).

`RithmicMessage::AccountListUpdates` is removed — Rithmic dropped
`account_list_updates.proto` (template 354) from the pool and its release notes
point to the account RMS updates stream instead. Subscribe with
`subscribe_account_rms_updates` and match `RithmicMessage::AccountRmsUpdates`.

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
- [ ] Decide whether `cancel_all_orders` should stay `Manual` — it is now `Auto`
- [ ] Check target-only brackets: they now send `TARGET_ONLY_STATIC`
- [ ] Confirm a trade route exists for every exchange you trade
