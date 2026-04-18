# Code Formatting Guide

Rules that go beyond what `cargo fmt` enforces. These are applied consistently across all `.rs` files in this project.

## 1. Return Line Linebreaks

Always insert a blank line above a `return` expression (including `return Err(...)`, bare `break`, bare `continue`, and expression-returns like `Ok(value)` that exit a block) **unless** it is the only statement in its block.

```rust
// GOOD — not the only line, so blank line above return
let result = compute();
return Ok(result);

// BAD — no blank line above return
let result = compute();
return Ok(result);

// GOOD — only line in block, no extra blank line needed
if cond { return None; }

// GOOD — blank line above break/continue
connection_lost = true;

break;
```

## 2. Import Grouping & Ordering

Organize imports in three groups separated by blank lines, in this order:

1. **Standard library** (`std::*`)
2. **External crates** (`tokio`, `tracing`, `futures_util`, etc.)
3. **Crate-local** (`crate::*`, `rithmic_rs::*`)

Within each group:

- **Single-line imports** go first, sorted alphabetically. If multiple single-line imports share the same crate path, combine them: `use tokio::{sync::broadcast, time::sleep};` instead of two separate `use tokio::…` lines.
- After all single-line imports for a group, **multi-line imports** follow with a blank line above the first multi-line import and below the last one in that group.
- After running `cargo fmt`, if a combined single-line import becomes multi-line, move it down to the multi-line section.

```rust
// GOOD
use futures_util::StreamExt;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

use crate::{
    ConnectStrategy, RithmicConfig, RithmicError,
    rti::messages::RithmicMessage,
};

use tokio::{
    sync::{broadcast, mpsc, oneshot},
    time::sleep_until,
};

// BAD — single-line imports scattered among multi-line blocks
use crate::{...};

use futures_util::StreamExt;

use tokio::{...};
```

## 3. Logging Macro Linebreaks

Add blank lines **above and below** `debug!()`, `info!()`, `warn!()`, and `error!()` macro calls **when there are other code lines in the same block**. If the macro is at the very start or end of a block, only add the side that has neighboring code.

```rust
// GOOD
Err(e) => {
    error!("Connect failed: {e}");
    sleep_with_backoff(&mut backoff, &mut rng_state).await;

    continue;
}

// BAD
Err(e) => {
    error!("Connect failed: {e}");
    sleep_with_backoff(&mut backoff, &mut rng_state).await;
    continue;
}
```

## 4. Let-Statement Linebreaks

Single-line `let` statements that are sequential should be **grouped together without line breaks between them**. Add a single blank line after the last `let` when non-`let` code follows. This separates "setup" (bindings) from "action" (function calls, mutations, returns).

```rust
// GOOD — single-line lets grouped, then blank line before action
let (tx, rx) = oneshot::channel();
let mut config = config;

config.aggregated_quotes = None;

// BAD — line breaks between single-line lets
let (tx, rx) = oneshot::channel();

let mut config = config;
config.aggregated_quotes = None;

// GOOD — multi-line lets are fine on their own line, blank line before action
let offset_ns = if jitter_ns > 0 {
    (r.rem_euclid(2 * jitter_ns + 1)) - jitter_ns
} else {
    0
};

let sleep_for = Duration::from_nanos(...);

info!("Reconnecting in {:?}", sleep_for);

// BAD — no separation between lets and action
let (tx, rx) = oneshot::channel();
let mut config = config;
config.aggregated_quotes = None;
```

## 5. No Blank Lines Between Match Arms

Do not add blank lines between `match` arms. Match arms should be densely grouped — the comma at the end of each arm is sufficient visual separation.

```rust
// GOOD
match result {
    SelectResult::HeartbeatFired => self.core.send_heartbeat().await,
    SelectResult::PingFired => self.core.send_ping().await,
    SelectResult::PingTimeout => {
        if self.core.ping_manager.check_timeout() {
            true
        } else {
            false
        }
    }
    SelectResult::Command(cmd) => {
        self.handle_command(cmd).await;
        false
    }
    SelectResult::RithmicMessage(msg) => self.core.handle_rithmic_message(msg).await,
    SelectResult::StreamClosed => self.core.handle_stream_closed(),
}

// BAD — blank lines between match arms
match result {
    SelectResult::HeartbeatFired => self.core.send_heartbeat().await,

    SelectResult::PingFired => self.core.send_ping().await,

    SelectResult::PingTimeout => { ... }
}
```

Note: This applies to the outer match arms only. Blank lines **within** an arm's block body are still governed by the other rules (return linebreaks, logging macros, let groups, etc.).

## 6. If-Block Linebreaks

`if` blocks should always have a blank line **before and after** them, with two exceptions:

- **No blank line before** an `if` when the line above is a block opener (e.g., `{` after a function signature, `loop {`, `match {`, `if {`).
- **No blank line after** an `if` when the line below is a block closer (e.g., `}` at the end of a function, `}` closing a match arm).

When the `if` has an `else if` or `else`, there is no blank line between them — the blank line goes before the first `if` and after the last `else` block.

```rust
// GOOD — blank line before if (preceding line is not a block opener)
let response = rx.await??;

if let Some(err) = response.request_error() {
    error!("login failed: {err}");
    return Err(err);
}

let _ = self.sender.send(Command::SetLogin).await;

// BAD — no blank lines around if
let response = rx.await??;
if let Some(err) = response.request_error() {
    error!("login failed: {err}");
    return Err(err);
}
let _ = self.sender.send(Command::SetLogin).await;

// GOOD — no blank line before if when line above is a block opener
loop {
    if some_condition {
        continue;
    }

    do_work();
}

// GOOD — no blank line after if when line below is a block closer
if let Some(err) = response.request_error() {
    return Err(err);
}
}
```

## 7. Always Run `cargo fmt`

After applying any of the above rules, always run `cargo fmt` to ensure baseline formatting is consistent. If `cargo fmt` reformats a combined import into multi-line, move it to the multi-line section per Rule 2.