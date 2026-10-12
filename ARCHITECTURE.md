# Architecture notes

The README has the overview. This file lists the races the code is built around, and where each one is handled.

## Data model

| Table | What it holds |
|---|---|
| `orders` | one row per order: status, money, drop-off, every step's timestamp, quoted and live ETA. `UNIQUE (customer_id, idempotency_key)` and the partial unique index `one_active_order_per_courier` |
| `order_items` | price copied from the menu at order time |
| `order_steps` | the saga log, one row per step with its detail (who accepted, courier distance, compensation) |
| `payments` | the fake gateway; `UNIQUE (order_id)` is its idempotency key |
| `outbox_events` | events waiting for (or already sent to) Kafka; `bigserial` id = publish order |
| `restaurants`, `menu_items`, `users` | seed data; `users.is_sim` marks the 40 simulated couriers |
| `simulation_runs` | load-simulator reports |

Redis holds only what can be rebuilt: courier positions, the free set, each courier's current order, and prep-time averages. The one exception is each order's courier trail (`order:trail:ID`), which is only drawn on the map; losing it loses a line, not an order. The courier's planned ride (`order:plan:ID`) is remade from its current position if it is missing.

## Races and where they are handled

| Race | What would go wrong | Handled by |
|---|---|---|
| Double click on "Place order" | two orders, two charges | `UNIQUE (customer_id, idempotency_key)`: the second insert waits for the first transaction, gets nothing back, and returns the first order (`replayed: true`). Same key with a different body returns 422 (`request_hash`). |
| Crash between insert and payment | order stuck, or charged twice on retry | the saga loop retries `PLACED` orders older than 5 s; the gateway's `UNIQUE (order_id)` returns the first result |
| Restaurant clicks "Accept" as the 60 s timeout fires | accepted *and* rejected | both are conditional steps `WHERE status='PAYMENT_AUTHORIZED'`: the first commit wins, the other gets 409 |
| Customer cancels as the kitchen accepts | cancelled order with captured money | same: cancel is a step from `PAYMENT_AUTHORIZED` only |
| Two orders claim the same courier | one courier, two orders | the claim is one Lua script (search + remove from free set + set order key) |
| Position ping races a claim | courier put back into the free set while carrying an order | the ping is a Lua script that re-adds to the free set only when no order key exists |
| Redis wrong anyway (flush, bug) | courier double-booked | `one_active_order_per_courier` fails the step; the claim is released |
| Order cancelled while a claim is in flight | courier stuck as busy | the step `WHERE status='RESTAURANT_ACCEPTED'` updates nothing, so the claim is released |
| Delivery commits, Redis release fails | courier never free again | startup / admin reconcile rebuilds Redis from the active orders in PostgreSQL |
| Kafka down | orders blocked | events wait in the outbox; the publisher reconnects |
| Event order across partitions | consumers see `DELIVERED` before `PICKED_UP` | every event is keyed by order id, so one order's events land on one partition in commit order |

## Timeouts (real seconds, `TIME_SCALE=20`)

| Setting | Default | Meaning |
|---|---|---|
| `RESTAURANT_TIMEOUT_S` | 60 | a hand-accept kitchen that does not answer is rejected (void) |
| `DISPATCH_TIMEOUT_S` | 90 | an accepted order with no courier is cancelled (refund) |
| auto-accept delay | 1.5 s | auto kitchens "read" the ticket |
| at-place radius | 150 m | pick-up / delivery allowed only this close |
