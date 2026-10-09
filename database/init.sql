-- Food Delivery Lab schema. PostgreSQL is the source of truth; Redis only holds courier positions and the
-- "who is free right now" set used for dispatch, and can be rebuilt from this database at any time.

CREATE TABLE IF NOT EXISTS users (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,             -- plain text on purpose: the login page shows every demo password
  name TEXT NOT NULL,
  role TEXT NOT NULL CHECK (role IN ('CUSTOMER','RESTAURANT','COURIER','ADMIN')),
  is_sim BOOLEAN NOT NULL DEFAULT FALSE,   -- couriers driven by the courier-sim worker
  is_load_test BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit_logs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  actor_user_id UUID REFERENCES users(id),
  action TEXT NOT NULL,
  target_user_id UUID,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Transactional outbox. A bigserial keeps the publish order equal to the commit order of the rows.
CREATE TABLE IF NOT EXISTS outbox_events (
  id BIGSERIAL PRIMARY KEY,
  topic TEXT NOT NULL,
  event_key TEXT NOT NULL,                 -- the order id: Kafka keeps every event of one order on one partition, in order
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS outbox_unpublished ON outbox_events(id) WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS simulation_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  status TEXT NOT NULL,
  params JSONB NOT NULL DEFAULT '{}'::jsonb,
  report JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_by UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS restaurants (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  owner_user_id UUID REFERENCES users(id),
  name TEXT NOT NULL UNIQUE,
  cuisine TEXT NOT NULL,
  area TEXT NOT NULL,
  lat DOUBLE PRECISION NOT NULL,
  lon DOUBLE PRECISION NOT NULL,
  base_prep_min NUMERIC(5,1) NOT NULL,     -- starting value of the prep-time average (real-world minutes)
  auto_accept BOOLEAN NOT NULL DEFAULT TRUE -- FALSE: a person must accept in the restaurant screen
);

CREATE TABLE IF NOT EXISTS menu_items (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  restaurant_id UUID NOT NULL REFERENCES restaurants(id),
  name TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  price NUMERIC(10,2) NOT NULL CHECK (price > 0),
  UNIQUE (restaurant_id, name)
);

CREATE TABLE IF NOT EXISTS orders (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id UUID NOT NULL REFERENCES users(id),
  restaurant_id UUID NOT NULL REFERENCES restaurants(id),
  courier_id UUID REFERENCES users(id),
  status TEXT NOT NULL CHECK (status IN ('PLACED','PAYMENT_AUTHORIZED','RESTAURANT_ACCEPTED','COURIER_ASSIGNED','PICKED_UP','DELIVERED',
                                         'PAYMENT_FAILED','REJECTED','CANCELLED','CANCELLED_NO_COURIER')),
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,              -- same key + different body = 422, never a second order
  address TEXT NOT NULL,
  drop_lat DOUBLE PRECISION NOT NULL,
  drop_lon DOUBLE PRECISION NOT NULL,
  subtotal NUMERIC(10,2) NOT NULL,
  delivery_fee NUMERIC(10,2) NOT NULL,
  total NUMERIC(10,2) NOT NULL,
  card TEXT NOT NULL DEFAULT 'ok',
  cancel_reason TEXT,
  eta_quoted_at TIMESTAMPTZ NOT NULL,      -- the promise shown at checkout, kept to measure ETA error
  eta_at TIMESTAMPTZ,                      -- recalculated at every saga step
  prep_due_at TIMESTAMPTZ,                 -- auto-accept kitchens mark the food ready at this time
  accepted_at TIMESTAMPTZ,
  ready_at TIMESTAMPTZ,
  assigned_at TIMESTAMPTZ,
  picked_up_at TIMESTAMPTZ,
  delivered_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  is_load_test BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (customer_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS orders_open ON orders(status, created_at)
  WHERE status IN ('PLACED','PAYMENT_AUTHORIZED','RESTAURANT_ACCEPTED','COURIER_ASSIGNED','PICKED_UP');
CREATE INDEX IF NOT EXISTS orders_customer ON orders(customer_id, created_at DESC);
CREATE INDEX IF NOT EXISTS orders_restaurant ON orders(restaurant_id, created_at DESC);
-- The last line of defence for dispatch: a courier can carry one order at a time. If Redis ever handed the same courier
-- to two orders, the second UPDATE fails here and the claim is rolled back.
CREATE UNIQUE INDEX IF NOT EXISTS one_active_order_per_courier ON orders(courier_id)
  WHERE status IN ('COURIER_ASSIGNED','PICKED_UP');

CREATE TABLE IF NOT EXISTS order_items (
  order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  menu_item_id UUID NOT NULL REFERENCES menu_items(id),
  name TEXT NOT NULL,
  unit_price NUMERIC(10,2) NOT NULL,
  qty INT NOT NULL CHECK (qty BETWEEN 1 AND 20),
  PRIMARY KEY (order_id, menu_item_id)
);

-- The saga log: one row per step, so the tracker and the admin can replay what happened and why.
CREATE TABLE IF NOT EXISTS order_steps (
  id BIGSERIAL PRIMARY KEY,
  order_id UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
  status TEXT NOT NULL,
  detail JSONB NOT NULL DEFAULT '{}'::jsonb,
  at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS order_steps_order ON order_steps(order_id, id);

-- The fake payment gateway. UNIQUE(order_id) is its idempotency key: authorizing the same order twice returns the
-- first result instead of holding the money twice.
CREATE TABLE IF NOT EXISTS payments (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id UUID NOT NULL UNIQUE REFERENCES orders(id) ON DELETE CASCADE,
  amount NUMERIC(10,2) NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('AUTHORIZED','DECLINED','CAPTURED','VOIDED','REFUNDED')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ---- Seed data: Bangkok -------------------------------------------------------------------------------------------
INSERT INTO users(email,password_hash,name,role) VALUES
  ('admin@example.com','admin123','Ada Admin','ADMIN'),
  ('customer@example.com','customer123','Chai Customer','CUSTOMER'),
  ('customer2@example.com','customer123','Nok Customer','CUSTOMER'),
  ('restaurant@example.com','restaurant123','Somsri (Somtam Corner)','RESTAURANT'),
  ('restaurant2@example.com','restaurant123','Lek (Pad Thai Yaowarat)','RESTAURANT'),
  ('courier@example.com','courier123','Kit Courier','COURIER')
ON CONFLICT (email) DO NOTHING;

INSERT INTO users(email,password_hash,name,role)
SELECT 'restaurant'||n||'@example.com','restaurant123','Owner '||n,'RESTAURANT' FROM generate_series(3,8) n
ON CONFLICT (email) DO NOTHING;

-- 40 couriers for the simulator. The admin chooses how many of them are online.
INSERT INTO users(email,password_hash,name,role,is_sim)
SELECT 'courier'||n||'@example.com','courier123','Sim courier '||n,'COURIER',TRUE FROM generate_series(1,40) n
ON CONFLICT (email) DO NOTHING;

INSERT INTO restaurants(owner_user_id,name,cuisine,area,lat,lon,base_prep_min,auto_accept)
SELECT u.id, r.name, r.cuisine, r.area, r.lat, r.lon, r.prep, r.auto
FROM (VALUES
  ('restaurant@example.com', 'Somtam Corner',        'Isaan',    'Ari',        13.7797, 100.5446, 10, FALSE),
  ('restaurant2@example.com','Pad Thai Yaowarat',    'Thai',     'Chinatown',  13.7398, 100.5090, 12, TRUE),
  ('restaurant3@example.com','Silom Noodle House',   'Noodles',  'Silom',      13.7286, 100.5340,  8, TRUE),
  ('restaurant4@example.com','Thonglor Burger Co',   'Burgers',  'Thonglor',   13.7326, 100.5800, 15, TRUE),
  ('restaurant5@example.com','Siam Sushi Bar',       'Japanese', 'Siam',       13.7460, 100.5340, 18, TRUE),
  ('restaurant6@example.com','Sathorn Curry Kitchen','Indian',   'Sathorn',    13.7210, 100.5290, 14, TRUE),
  ('restaurant7@example.com','Ratchada Night Grill', 'Grill',    'Ratchada',   13.7650, 100.5740, 20, TRUE),
  ('restaurant8@example.com','Ekkamai Pizza Lab',    'Pizza',    'Ekkamai',    13.7200, 100.5850, 16, TRUE)
) AS r(email,name,cuisine,area,lat,lon,prep,auto)
JOIN users u ON u.email = r.email
ON CONFLICT (name) DO NOTHING;

INSERT INTO menu_items(restaurant_id,name,description,price)
SELECT r.id, m.name, m.descr, m.price
FROM (VALUES
  ('Somtam Corner','Som tam Thai','Green papaya salad, peanuts, lime',60),
  ('Somtam Corner','Gai yang','Grilled chicken with tamarind dip',120),
  ('Somtam Corner','Larb moo','Spicy minced pork salad',90),
  ('Somtam Corner','Sticky rice','',20),
  ('Pad Thai Yaowarat','Pad Thai goong','Rice noodles with river prawns',150),
  ('Pad Thai Yaowarat','Pad see ew','Wide noodles, kale, soy',90),
  ('Pad Thai Yaowarat','Mango sticky rice','',80),
  ('Pad Thai Yaowarat','Thai iced tea','',45),
  ('Silom Noodle House','Boat noodles','Rich pork broth',60),
  ('Silom Noodle House','Khao soi','Northern curry noodles',95),
  ('Silom Noodle House','Wonton soup','',70),
  ('Thonglor Burger Co','Classic smash burger','Double patty, cheese',260),
  ('Thonglor Burger Co','Fries','',90),
  ('Thonglor Burger Co','Chocolate shake','',140),
  ('Siam Sushi Bar','Salmon set','12 pieces',420),
  ('Siam Sushi Bar','California roll','',220),
  ('Siam Sushi Bar','Miso soup','',60),
  ('Sathorn Curry Kitchen','Butter chicken','With basmati rice',240),
  ('Sathorn Curry Kitchen','Garlic naan','',60),
  ('Sathorn Curry Kitchen','Mango lassi','',85),
  ('Ratchada Night Grill','Moo ping skewers','5 skewers',100),
  ('Ratchada Night Grill','Crying tiger','Grilled beef, jaew sauce',220),
  ('Ratchada Night Grill','Grilled squid','',180),
  ('Ekkamai Pizza Lab','Margherita','12 inch',290),
  ('Ekkamai Pizza Lab','Tom yum pizza','12 inch, prawns',360),
  ('Ekkamai Pizza Lab','Garlic bread','',110)
) AS m(rname,name,descr,price)
JOIN restaurants r ON r.name = m.rname
ON CONFLICT (restaurant_id, name) DO NOTHING;
