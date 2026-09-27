# Examples

Runnable programs that connect to Rithmic. Each one runs against a Demo (paper
trading) account.

## Setup

You need a Rithmic account and the connection details Rithmic gives you: an app
name and version, a username and password, and the gateway URLs.

1. Copy the template to `.env` in the repo root. It is gitignored.

   ```sh
   cp examples/.env.blank .env
   ```

2. Fill in the shared app fields and the `RITHMIC_DEMO_*` block. You can leave the
   Test and Live blocks empty.

   ```sh
   RITHMIC_APP_NAME=your_app_name
   RITHMIC_APP_VERSION=1

   RITHMIC_DEMO_USER=your_username
   RITHMIC_DEMO_PW=your_password
   RITHMIC_DEMO_URL=<provided_by_rithmic>
   RITHMIC_DEMO_ALT_URL=<provided_by_rithmic>

   # Only the order and PnL examples need these
   RITHMIC_DEMO_ACCOUNT_ID=your_account_id
   RITHMIC_DEMO_FCM_ID=your_fcm_id
   RITHMIC_DEMO_IB_ID=your_ib_id
   ```

3. Check the connection:

   ```sh
   cargo run --example connect
   ```

   It logs in, logs the login reply and disconnects. If this works, the rest will too.

Every example loads `.env` from the directory you run `cargo` in, so run them
from the repo root. Variables already set in your shell take precedence.

## The examples

| Example | Shows | Needs account IDs |
|---|---|---|
| [`connect.rs`](connect.rs) | Connect, log in, disconnect | |
| [`ticker.rs`](ticker.rs) | Streaming quotes and trades for the front month | |
| [`load_historical_bars.rs`](load_historical_bars.rs) | Time bar replay | |
| [`load_historical_ticks.rs`](load_historical_ticks.rs) | Tick replay | |
| [`backfill.rs`](backfill.rs) | Backfilling large windows and checking you got all of them | |
| [`error_handling.rs`](error_handling.rs) | Every error the crate can hand you, in one file | optional, skips orders without them |
| [`reconnect.rs`](reconnect.rs) | A reconnection loop that restores subscriptions | |
| [`pnl.rs`](pnl.rs) | Position and P&L updates | yes |
| [`bracket_order.rs`](bracket_order.rs) | Placing a bracket and reading order notifications | yes, **places orders** |
| [`trade_routes.rs`](trade_routes.rs) | Inspecting the routes orders will take | yes, **places orders** |
| [`request_timeout.rs`](request_timeout.rs) | Timing out requests and orders, and finding out what a timed-out order did | yes, **places orders** |

The three marked **places orders** send limit orders to your Demo account, priced to rest
below the market, and cancel them before they exit.

[`generate_protos.rs`](generate_protos.rs) is a maintainer tool, not an example. It
regenerates `src/rti.rs` from the `.proto` files and never connects.

## Options

Every example except `connect` and `pnl` picks its contract from these. Set them in
`.env` or on the command line. The shared code is in [`shared/common.rs`](shared/common.rs).

| Variable | Used by | Default |
|---|---|---|
| `SYMBOL` | all but `connect`, `pnl` | `ticker`, `error_handling`: the front month of `PRODUCT`. The rest: `ESZ6` |
| `PRODUCT` | `ticker`, `error_handling` | `ES` (ignored when `SYMBOL` is set) |
| `EXCHANGE` | all but `connect`, `pnl` | `CME` |
| `START_TIME` | `load_historical_*`, `error_handling` | Midnight UTC on the last weekday before today |

Only the ticker plant can look up a front month, so examples that don't use it
fall back to a fixed contract. Set `SYMBOL` once it rolls.

```sh
SYMBOL=NQZ6 cargo run --example ticker                         # another contract
START_TIME=1790294400 cargo run --example load_historical_ticks  # Fri 2026-09-25
SYMBOL=NQZ6 cargo run --release --example backfill
```

The order examples price their limit orders for ES. Change the price before
pointing them at another product.

## Test and Live

Each example sets `const ENV: RithmicEnv = RithmicEnv::Demo` near the top. To run
one elsewhere, fill in the `RITHMIC_TEST_*` or `RITHMIC_LIVE_*` block and change
that line. On Live, set `RITHMIC_LIVE_SYSTEM_NAME` if your provider is not
`Rithmic 01`.
