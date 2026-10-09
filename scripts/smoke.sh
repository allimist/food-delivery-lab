#!/usr/bin/env bash
# End-to-end smoke test against a running stack (podman compose / docker compose up -d).
# Drives every saga path: delivered, card declined, restaurant rejects, customer cancels, no courier, plus idempotency.
#   API=http://localhost:3030/api ./scripts/smoke.sh
set -euo pipefail
API=${API:-http://localhost:3030/api}
step() { echo "▶ $*"; }
ok() { echo "  ✓ $*"; }
fail() { echo "  ✕ $*" >&2; exit 1; }
call() { # method path token [json] [idempotency-key] -> sets BODY and STATUS
  local out args=(-s -w '\n%{http_code}' -X "$1" "$API$2" -H "authorization: Bearer ${3:-}")
  [ -n "${4:-}" ] && args+=(-H 'content-type: application/json' -d "$4")
  [ -n "${5:-}" ] && args+=(-H "idempotency-key: $5")
  out=$(curl "${args[@]}"); STATUS=${out##*$'\n'}; BODY=${out%$'\n'*}
}
expect() { [ "$STATUS" = "$1" ] || fail "expected HTTP $1, got $STATUS: $BODY"; }
login() { call POST /auth/login "" "{\"email\":\"$1\",\"password\":\"$2\"}"; expect 200; jq -r .token <<<"$BODY"; }
uuid() { cat /proc/sys/kernel/random/uuid 2>/dev/null || uuidgen; }
order_body() { # restaurant-name [card]
  local r; r=$(jq -c --arg n "$1" '.[] | select(.name==$n)' <<<"$RESTS")
  call GET "/restaurants/$(jq -r .id <<<"$r")" ""; local item; item=$(jq -r '.menu[0].id' <<<"$BODY")
  echo "{\"restaurantId\":\"$(jq -r .id <<<"$r")\",\"items\":[{\"menuItemId\":\"$item\",\"qty\":2}],\"addressId\":\"lumpini\",\"card\":\"${2:-ok}\"}"
}
wait_status() { # order-id token wanted-status seconds
  local s=""
  for _ in $(seq 1 "$4"); do
    call GET "/orders/$1" "$2"; s=$(jq -r .status <<<"$BODY")
    [ "$s" = "$3" ] && return 0
    sleep 1
  done
  fail "order $1 is $s after $4s, expected $3. Steps: $(jq -c '[.steps[].status]' <<<"$BODY")"
}

step "wait for the API"
for i in $(seq 1 60); do curl -sf "$API/health" >/dev/null && break; [ "$i" = 60 ] && fail "API not healthy"; sleep 3; done
ADMIN=$(login admin@example.com admin123); CUST=$(login customer@example.com customer123)
REST=$(login restaurant@example.com restaurant123)
call GET /restaurants ""; RESTS=$BODY
ok "$(jq length <<<"$RESTS") restaurants"

step "couriers are online"
call PUT /admin/sim-settings "$ADMIN" '{"onlineCouriers":20}'; expect 200
for i in $(seq 1 30); do call GET /admin/stats "$ADMIN"; [ "$(jq .couriers.online <<<"$BODY")" -ge 5 ] && break; [ "$i" = 30 ] && fail "no courier came online"; sleep 2; done
ok "$(jq .couriers.online <<<"$BODY") online"

step "idempotency: the same request twice at once makes one order"
KEY=$(uuid); B=$(order_body "Pad Thai Yaowarat")
R1=$(mktemp); R2=$(mktemp)
curl -s -X POST "$API/orders" -H "authorization: Bearer $CUST" -H 'content-type: application/json' -H "idempotency-key: $KEY" -d "$B" >"$R1" &
curl -s -X POST "$API/orders" -H "authorization: Bearer $CUST" -H 'content-type: application/json' -H "idempotency-key: $KEY" -d "$B" >"$R2" &
wait
[ "$(jq -r .orderId "$R1")" = "$(jq -r .orderId "$R2")" ] || fail "two orders: $(cat "$R1") / $(cat "$R2")"
[ "$(jq -s '[.[].replayed] | sort' "$R1" "$R2" | jq -c .)" = '[false,true]' ] || fail "expected one replay: $(cat "$R1") / $(cat "$R2")"
HAPPY=$(jq -r .orderId "$R1")
ok "order ${HAPPY:0:8}, second answer replayed"
call POST /orders "$CUST" "$(order_body "Siam Sushi Bar")" "$KEY"; expect 422
ok "same key, different body: $(jq -r .error <<<"$BODY")"

step "card declined"
call POST /orders "$CUST" "$(order_body "Pad Thai Yaowarat" decline)" "$(uuid)"; expect 200
[ "$(jq -r .status <<<"$BODY")" = PAYMENT_FAILED ] || fail "expected PAYMENT_FAILED: $BODY"
ok "PAYMENT_FAILED"

step "restaurant rejects -> payment voided"
call POST /orders "$CUST" "$(order_body "Somtam Corner")" "$(uuid)"; expect 200; REJ=$(jq -r .orderId <<<"$BODY")
call POST "/restaurant/orders/$REJ/reject" "$REST" '{"reason":"Out of papaya"}'; expect 200
call GET "/orders/$REJ" "$CUST"
[ "$(jq -r .status <<<"$BODY")/$(jq -r .payment.status <<<"$BODY")" = REJECTED/VOIDED ] || fail "got $(jq -c '{status,payment}' <<<"$BODY")"
ok "REJECTED, payment VOIDED"

step "customer cancels before the restaurant answers -> payment voided"
call POST /orders "$CUST" "$(order_body "Somtam Corner")" "$(uuid)"; expect 200; CAN=$(jq -r .orderId <<<"$BODY")
call POST "/orders/$CAN/cancel" "$CUST" '{}'; expect 200
call GET "/orders/$CAN" "$CUST"
[ "$(jq -r .status <<<"$BODY")/$(jq -r .payment.status <<<"$BODY")" = CANCELLED/VOIDED ] || fail "got $(jq -c '{status,payment}' <<<"$BODY")"
ok "CANCELLED, payment VOIDED"

step "restaurant accepts and marks ready by hand"
call POST /orders "$CUST" "$(order_body "Somtam Corner")" "$(uuid)"; expect 200; MAN=$(jq -r .orderId <<<"$BODY")
call POST "/restaurant/orders/$MAN/accept" "$REST" '{}'; expect 200
call POST "/restaurant/orders/$MAN/ready" "$REST" '{}'; expect 200
ok "accepted and ready"

step "happy path: delivered by a simulated courier"
wait_status "$HAPPY" "$CUST" DELIVERED 240
[ "$(jq -r .payment.status <<<"$BODY")" = CAPTURED ] || fail "payment $(jq -r .payment.status <<<"$BODY")"
ok "$(jq -c '[.steps[].status]' <<<"$BODY")"
wait_status "$MAN" "$CUST" DELIVERED 240
ok "hand-accepted order delivered by $(jq -r .courier <<<"$BODY")"

step "no courier online -> cancelled and refunded"
call PUT /admin/sim-settings "$ADMIN" '{"onlineCouriers":0}'; expect 200
for i in $(seq 1 90); do call GET /admin/stats "$ADMIN"; [ "$(jq .couriers.free <<<"$BODY")" = 0 ] && break; [ "$i" = 90 ] && fail "couriers still free"; sleep 1; done
call POST /orders "$CUST" "$(order_body "Ekkamai Pizza Lab")" "$(uuid)"; expect 200; NOC=$(jq -r .orderId <<<"$BODY")
call GET /config ""; T=$(jq .dispatchTimeoutS <<<"$BODY")
wait_status "$NOC" "$CUST" CANCELLED_NO_COURIER $((T + 30))
[ "$(jq -r .payment.status <<<"$BODY")" = REFUNDED ] || fail "payment $(jq -r .payment.status <<<"$BODY")"
ok "CANCELLED_NO_COURIER, payment REFUNDED"
call PUT /admin/sim-settings "$ADMIN" '{"onlineCouriers":20}'; expect 200

step "outbox drained into Kafka"
for i in $(seq 1 20); do call GET /admin/stats "$ADMIN"; [ "$(jq .outbox.pending <<<"$BODY")" = 0 ] && break; [ "$i" = 20 ] && fail "outbox: $(jq -c .outbox <<<"$BODY")"; sleep 1; done
ok "$(jq -c .outbox.byTopic <<<"$BODY")"

step "Redis agrees with PostgreSQL"
call GET /admin/consistency "$ADMIN"; [ "$(jq .ok <<<"$BODY")" = true ] || fail "$(jq -c .problems <<<"$BODY")"
ok "$(jq .checkedCouriers <<<"$BODY") couriers checked"
echo "✓ smoke test passed"
