# Rithmic server behavior

> What the Rithmic servers actually do, as seen through this crate, so you
> don't have to find it out again.

## Who this is for

You are building on top of `rithmic-rs` and need to know what the server
sends about your orders, fills and positions. The crate docs say what each
call does. This guide says what the server sends back, in what order, and
where it surprised us.

**Seen** marks something we recorded and decoded. **Read** marks something
taken only from the protocol definitions. Unmarked items come from everyday
use of the crate. Nothing here is a Rithmic guarantee.

- **Protocol:** R | Protocol 0.89.0.0 (template 5.42), as bundled here.
- **Crate:** 3.2.0 and `main`, October 2026.
- **Environment:** a Rithmic demo (paper trading) system.
- **Instruments:** CME equity index micro futures.
- **Evidence:** order and PnL plant messages, recorded in receipt order.

> [!NOTE]
> These are a few short sessions on one environment. Timings are what we saw,
> not bounds, and live systems may differ.

## At a glance

| Plant | Carries |
| --- | --- |
| Ticker | Trades, quotes, depth, search, reference data, front months |
| History | Bar and tick replays, live bar subscriptions |
| Order | Orders, fills, order and fill replays, brackets, RMS |
| PnL | Positions, balances and day figures |
| Repository | First-use agreements only |

Fills arrive only on the order plant. The PnL plant states levels (net
position, balance) and never sends a fill.

## Connection

### One session per user

Each plant is its own WebSocket and its own login, and one user can have all
four trading plants connected at once. Do not plan on a second, parallel
session for the same user, for example a separate read-only login for
checks. We were told the system does not allow it, so we never tried. Share
the session you have.

### Plants fail on their own

**Seen.** We cut only the order plant's connection. The PnL plant kept
streaming the whole time, and stated the position going flat while the order
plant was down. So your view can be half current: positions moved on, fills
did not. Track each plant's health separately.

### When the connection drops

**Seen.** We sent `exit_position` and cut the order plant's socket half a
millisecond later. The call returned `ConnectionClosed`, but the server had
already received it and filled the exit.

1. A call in flight when the socket dies fails with `ConnectionClosed`
   whether or not the server acted on it. Never resend on that error alone;
   reconcile first.
2. Messages pushed while you were disconnected are not sent again live. The
   exit's fill never arrived as a live notification.
3. A new plant and login to the same gateways worked. Subscriptions do not
   carry over: subscribe again.
4. What you missed comes back only through replays: the fill was in the
   `show_orders` replay, as a snapshot row, and in `show_fill_history`.

Each connection attempt times out after 5 seconds by default. See
[Connection strategies](README.md#connection-strategies), and the
[crate docs](https://docs.rs/rithmic-rs/latest/rithmic_rs/#error-handling)
for which messages mean "reconnect".

## Data

### Finding a contract

| Need | Ask | Note |
| --- | --- | --- |
| Contracts matching text | `search_symbols` | No tick size or point value |
| Tick size, point value | Reference data | One request per contract |
| Product families | Product codes | Families, not contracts |
| Front month | `get_front_month_contract` | Many products have none |

A search can return product alias rows beside the contracts. An alias row
can name an old contract, so trust one only when the same search also lists
the contract it names. A product with no designated front month is answered
with the no-data code `7`.

**Seen.** Rithmic spells a dated CME contract with a one-digit year, for
example `MNQZ6`.

### Bars and ticks

- Live bar subscriptions are on the history plant, not the ticker plant.
- Intraday bars are stamped at their close. Daily and weekly bars use
  `YYYYMMDD` trade dates.
- In a tick-bar replay, the first row's first timestamp is the request's
  start second, not a trade time.
- Only a successful final frame means a replay is complete. The crate
  resumes replays the server cuts short; see
  [History Plant](README.md#history-plant).
- **Read.** `LastTrade` has no trade id, and its exchange order ids are
  optional.
- **Read.** Depth-by-order `sequence_number` fields are optional, so be
  ready to order updates by arrival alone.

## Execution

### Replies and notifications

A call's reply only says the request was taken. What happened to the order
arrives on `subscription_receiver`, a separate channel from the reply, so
the two can reach you in either order. A market order can fill, and report
`complete`, before `place_order` returns. Match notifications to your order
by `user_tag` and `basket_id`, never by arrival order.

**Seen** (demo, 2026-10-16). A new-order, modify and cancel request are each
answered by two frames of their reply template, sharing the request id. The
first frame names the basket in `basket_id`, carries handler code `["0"]`, an
empty response code and the venue time; the new-order one also echoes your
`user_tag`. The second, final frame carries response code `["0"]` and no basket,
handler code or time. The SDK marks the first frame as having more to follow.
So only the first frame names the basket, and the response code is empty on
every frame but the last; a check that wants every frame to name the basket and
carry `0` rejects a cancel that worked. The cancel shape was seen on five
cancels, the new-order shape on three and the modify shape on one. The reply
templates have no window-name field. `user_msg` comes back as sent on every
reply frame and never on a notification; the crate fills it with its own
request id, so you cannot use it as your own identity. A cancel reply is not
the order's end: the order notifications report the cancellation separately.

### Order labels: `user_tag` and `window_name`

**Seen** (demo, 2026-10-16, one MNQ limit placed, modified once and cancelled).

- **`user_tag`** is sent on the new order only; modify and cancel have no such
  field. It came back in full (101 ASCII characters, not cut short) on the
  new-order reply's first frame and on every `RithmicOrderNotification` and
  `ExchangeOrderNotification` for the order except `CANCEL_RCVD_FROM_CLNT`,
  and on the reconnect replay.
- **`window_name`** is sent on the new order, modify and cancel. It came back
  only on `ORDER_RCVD_FROM_CLNT`, `MODIFY_RCVD_FROM_CLNT` and
  `CANCEL_RCVD_FROM_CLNT`.
- **`originator_window_name`** carried the new order's `window_name`,
  unchanged through the modify and cancel, on every other notification. A
  replayed (`is_snapshot`) row carried both window fields.

`window_name` is free text naming the front end. An order placed from Rithmic's
own website shows `Rithmic Web` there (reported by the account holder, not
recorded). Each request produced six order-plant rows: received from client,
pending, received by the exchange gateway, sent to the exchange, the exchange's
own row, then the result (`open`, `modified` or `complete`). The cancel's reply
frames arrived after its six rows.

**Seen.** For a new order, the first live `ExchangeOrderNotification` (`STATUS`)
has no `status` text and no quantity. The `RithmicOrderNotification` `OPEN`
that follows (or `TRIGGER_PENDING` for stop and touch orders) has the status
text and the full terms, so read an order as working from that row. The
exchange's `MODIFY` and `CANCEL` rows have no status text either.

**Seen.** A market order goes through these `status` values, each on its own
`RithmicOrderNotification`:

| `status` | Meaning |
| --- | --- |
| `Order received from client` | Note the capital O |
| `open pending` | With Rithmic |
| `order received by exch gateway` | |
| `order sent to exch` | |
| `open` | Working at the exchange |
| `complete` | Done; `completion_reason` says why |

A stop rests as `trigger pending`. A cancel goes through `Cancel received
from client`, `cancel pending`, `cancel received by exch gateway` and
`cancel sent to exch` before `complete`. The fill itself arrives on an
`ExchangeOrderNotification` with `report_type` `fill`.

**Seen** (demo, 2026-10-16). An order that fills in parts also gets progress
rows: a `RithmicOrderNotification` with notify type `GENERIC` and status
`partial`, arriving between the exchange's fill rows. The fills are on the fill
rows, not on the `partial` one.

| `completion_reason` | Seen on |
| --- | --- |
| `F` | A filled order |
| `C` | A cancelled order |
| `FA` | An order the account was not allowed to place |

> [!WARNING]
> **Seen.** After an account was auto-liquidated, new orders still got a
> basket id in the reply, then came back `complete` with reason `FA` and the
> text "Order can be placed by administrators only". There was no exchange
> reject. Watch for `complete` without a fill, not only for rejects.

**Seen,** and easy to misread:

- **Placeholder rows.** Gateway status rows can carry a quantity of zero, and
  status rows can carry `fill_size` 0 with no `fill_id`. Neither is a size or
  a fill.
- **Modify rows.** An exchange modify notification can leave out `quantity`.
  Read the size from `modified_size`, or from `total_fill_size` plus
  `total_unfilled_size`.
- **`manual_or_auto`** belongs to each message, not to the order. An entry
  sent as manual reports `1`; the bracket children the server creates report
  `2` (auto); a manual cancel of a child reports `1`. Fill history spells it
  `M` instead of a number.
- **Touch orders.** **Seen** (demo, 2026-10-16). Working market-if-touched
  and limit-if-touched orders arrived, live and in replays, as exchange
  order notifications with `quantity` 0, `total_fill_size` 0 and
  `total_unfilled_size` 1. They carried the trigger price, and the limit
  price for limit-if-touched, but no `price_type`. A reader that needs
  `price_type` or a non-zero `quantity` drops them. Read the size from
  `total_fill_size` plus `total_unfilled_size`.

### Replays and snapshots

`show_orders`, `show_brackets` and `show_bracket_stops` make the server push
rows with `is_snapshot` set. **Seen:**

- `show_orders` replays every order of the trading day, finished ones
  included, from earlier sessions too.
- The call's reply can arrive in the middle of the replayed rows. It is not
  an end-of-list marker, and the crate has none.
- Pushed rows carry an empty request id, so you cannot tell which request
  caused them. Each replay request pushes the same rows again.
- Snapshot timestamps are rounded to the millisecond (`usecs` ends in
  `000`). Live rows normally carry full microseconds.
- **Seen** (demo, 2026-10-16). An order that had been modified was replayed
  after a reconnect as an exchange notification with notify type modify and
  `status` `modified`. That row is the order's current working state. Skip
  it and the order is missing from your snapshot.
- **Seen** (demo, 2026-10-16). Live pushes can arrive while a snapshot read
  (`show_orders` and the PnL position snapshot) is still running. Replayed
  rows carry `is_snapshot` true and live pushes do not, so sort them by that
  flag.

A replayed fill is old news when you saw it live, and may be the only copy
when you did not. See [When the connection drops](#when-the-connection-drops).

### Fills

- **`fill_id` belongs to an account.** **Seen:** two accounts reported the
  same `fill_id` for different fills. Key fills by account and `fill_id`.
- **`fill_id` is not in time order.** **Seen:** later fills had lower ids.
  Do not use it as a cursor.
- **`cor_sequence_number` is set on normal fills.** **Seen:** it equals
  `orig_sequence_number`. Only a different value marks a correction, and we
  never saw one.
- **A replayed order sums its partial fills.** **Seen** (demo, 2026-10-16).
  `show_fill_history` lists each partial fill as its own row, with its own
  `fill_id`, quantity and price. The order's terminal replayed snapshot (an
  `ExchangeOrderNotification` with `is_snapshot` set and status `complete`)
  names only the last fill's `fill_id` and price, but states the order's total
  in `fill_size` and `total_fill_size`, and its average in `avg_fill_price`.
  To check a replayed fill against fill history, sum the basket's partial
  fills first; one history row alone will not match.
- **No fill states a fee or a realized PnL.** **Seen** live and in history.
- **Commission is only a running total** for the account:
  `rms_account_commission` on the account PnL row.
- **Fill history** takes up to 10,000 records per request. With
  `max_record_count` set to `None` the server picks the cap. **Seen:** it
  spells sides `B` and `S`, where notifications use numbers.

### Positions and PnL

**Seen.** Position statements trail the fill that changes them. After each
fill the PnL plant restated the old net two or three times, then sent the new
one, sometimes twice:

| Statement | Server time | Received after the fill |
| --- | --- | --- |
| Old net | 0.5 to 1.0 ms before the fill | 1.0 to 9.5 ms |
| New net | 0.6 to 0.9 ms after the fill | 2.8 to 11.5 ms |

So the newest statement can still describe the position before your latest
fill. While the order plant was cut, the PnL plant still stated the new net
0.7 ms after a fill the order plant never delivered live.

Every instrument PnL row carries `fill_buy_qty` and `fill_sell_qty`, running
counts of the contracts bought and sold. They show which fills a statement
already includes.

- **Seen.** The counts do not reset when the position goes flat. A flat
  account read `4/4`, then `5/4` after one buy.
- **Seen.** The counts carry across sessions within one trading day. A
  session's first read stated `4/4`: every fill the `show_orders` replay listed
  for that day, an earlier session's three buys and one sell of three
  included.
- **Seen.** A row can restate an unchanged position under a new timestamp,
  beside a snapshot read or an order change. Seven rows restated a flat `5/5`
  after the first. Compare the counts, net and average, not the timestamp, to
  find a change.
- **Seen.** A PnL snapshot row is stamped near the read, not at the position's
  last change: a row read 3.5 s after the position went flat was stamped 3.5 s
  after the flat row. Every snapshot row seen was stamped to the millisecond,
  and so were a few live rows.
- **Seen.** A contract not traded that day can have no instrument row at
  all. A missing row is not a stated zero. A contract traded and closed does
  get an explicit zero row.
- **Read.** `net_quantity` is optional. We read `buy_qty` and `sell_qty` as
  signed views of the same net (`buy_qty` 3 and `sell_qty` -3 both mean long
  3), not as two legs to add.
- **Seen.** Live and snapshot account rows can state different day figures
  for the same moment, and the snapshot row left out commission.

### Native brackets

`place_bracket_order` makes one entry. As it fills, the server creates the
target and stop as separate orders, each with its own `basket_id`. **Seen:**

- Children carry no `user_tag`. Link them by `original_basket_id`, the
  parent's basket id.
- **One pair per fill.** **Seen** (demo, 2026-10-16). An entry that fills in
  parts gets one target and one stop for each fill. A BUY 2 that filled 1 + 1
  produced two target and stop pairs, each priced from its own fill and each
  linked to the entry by `original_basket_id`.
- Each pair is one-cancels-other. When we cancelled the target, the server
  cancelled the stop itself. The stop's rows started at `cancel pending`, with
  no `Cancel received from client` row, and reported auto mode `2`.
- **Seen** (demo, 2026-10-16). With two pairs, cancelling one leg ended only
  its own sibling, which the venue cancelled itself. The other fill's pair
  stayed working. A later cancel of the already ended sibling was refused.
- Bracket updates can report a tier's quantity before the children's own
  notifications arrive.
- `adjust_target` and `adjust_stop` need `level`. Leaving it out, even for a
  one-tier bracket, was refused with code `1072`, "level field is not
  received."

> [!CAUTION]
> **Seen twice.** The brackets are tied to the order, not to the position.
> After `exit_position` closed the position, both children stayed working
> on the flat account until something cancelled them. Once, the stop later
> triggered and opened a new short. Cancel a bracket's children yourself
> when you exit outside the bracket.

### Native exit

`exit_position` asks the server to flatten one instrument, or the whole
account.

- The reply only says the request was taken. It names no order or fill, and
  it does not mean you are flat.
- **Seen** (demo, 2026-10-16). Sent for the whole account (no symbol), the
  reply is two `ResponseExitPosition` frames sharing the request id. The first
  has an empty response code and more to follow; the last has `["0"]`. That is
  the same shape as new-order and cancel, and it held when the account was
  already flat on the server, so a final `0` does not mean anything was exited.
  The crate returns every frame for this call.
- No reply says the position was not there. **Read.** `ResponseExitPosition`
  carries only its codes, symbol and exchange, and the reference guide names
  no code for a missing position. **Seen.** The account-wide exit sent while
  the server already held the account flat placed no order and was answered
  `0`, like an exit of a real position. Learn that you were already flat from
  the PnL plant, not from the reply.
- The exit's order and fill carry no `user_tag`, because the request has no
  field for one.
- **Seen.** The request's `window_name` came back on the exit order's first
  row. Later rows and the live fill carried it as `originator_window_name`.
  Replayed snapshot rows carried both. Put your own id in `window_name` to
  find the exit's fill.

### Native cancel-all

`cancel_all_orders` asks the server to cancel every working order on the
account.

**Seen** (demo, 2026-10-16). The reply is a single `ResponseCancelAllOrders`
frame with response code `["0"]`, not the two frames of new-order, cancel and
exit. The cancellations arrive as order notifications. The crate returns only
the first frame for this call, so it loses nothing here. As with any reply, the
`0` says the request was taken, not which orders were cancelled.

## Wrapping the SDK

What an integration has to handle, in the order it tends to bite:

1. **Correlate, don't sequence.** Match replies and notifications by
   `user_tag`, `basket_id` and `original_basket_id`. A fill may come before
   its reply.
2. **Deduplicate fills by account and `fill_id`.** Replays repeat fills you
   already have. A replayed order names only its last fill, so compare it with
   fill history by summing the basket's fills.
3. **Treat `ConnectionClosed` on a mutation as unknown.** Reconnect, read
   `show_orders` and `show_fill_history`, then decide. Never resend blindly.
4. **Reconcile after any gap.** A `Lagged` receiver or a dropped plant means
   fills may be missing. Only replays and fill history bring them back.
5. **Read orders from the fields present.** Size touch orders from filled
   plus unfilled, keep a replayed modify row as the order's working state,
   and split snapshot rows from live pushes by `is_snapshot`.
6. **Expect PnL statements to trail fills.** A statement received after a
   fill can still state the old position.
7. **Keep missing apart from zero.** A missing row or field is unknown, not
   flat.
8. **Clean up bracket children** after any exit that did not come from the
   bracket itself. An entry that filled in parts has a pair per fill.
9. **Tag native exits** through `window_name`.
10. **Read `completion_reason`.** A refusal can arrive as `complete` with no
    fill.
11. **Hold one session.** Share it rather than logging in again.

## Known gaps and unproven

- **Day reset.** Whether the fill counts reset when a trading day starts.
  They do not reset when flat, and they carry across sessions within one day.
- **Snapshot cut.** Whether a PnL snapshot can leave out a fill stamped before
  the snapshot row's own timestamp.
- **Fill history size.** How many fills come back with no cap set.
- **Replay depth.** Whether `show_orders` replays more than the current
  trading day.
- **Corrections.** What a correction fill looks like. Every
  `cor_sequence_number` we saw matched `orig_sequence_number`.
- **Statement delay.** How late a statement can be. We have only the
  timings above, from short sessions.
- **Exit echo.** Whether every native exit echoes `window_name`. It did each
  time we tried, but it is not documented.
- **`user_tag` length.** The longest tag we sent, 101 characters, came back
  whole; the limit was not searched for.
- **Second login.** What a second login of the same user does to the first.
  We were told not to and did not try it.
- **Trailing brackets.** The 0.89 bracket request has no field for a
  trailing stop's distance, and nothing states what
  `trailing_stop_by_last_trade_price = false` selects.
- **Trade matching.** Whether a depth-by-order row can be matched to a
  `LastTrade`.
