-- E-commerce Platform Seed Data (MySQL)
--
-- Mirrors docker/db/init/001_ecommerce_seed.sql (the Postgres playground
-- seed)'s table shape and data, in MySQL types: AUTO_INCREMENT not SERIAL,
-- JSON not JSONB, DATETIME not TIMESTAMP (specifically to exercise
-- data_types.clj's "datetime" branch), explicit FOREIGN KEY, InnoDB (the
-- default engine, set explicitly - MyISAM would silently drop the
-- constraints). The Postgres-only audit schema isn't ported: MySQL has no
-- array type or INET, which that schema depends on, and it isn't needed to
-- exercise the MySQL dialect work.

CREATE TABLE IF NOT EXISTS categories (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  description TEXT,
  parent_id INT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  is_active BOOLEAN DEFAULT true,
  FOREIGN KEY (parent_id) REFERENCES categories(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS customers (
  id INT AUTO_INCREMENT PRIMARY KEY,
  email VARCHAR(255) UNIQUE NOT NULL,
  first_name VARCHAR(100) NOT NULL,
  last_name VARCHAR(100) NOT NULL,
  phone VARCHAR(20),
  date_of_birth DATE,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  is_active BOOLEAN DEFAULT true,
  loyalty_points INT DEFAULT 0,
  data JSON  -- For testing schema-based type conversion
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS customer_addresses (
  id INT AUTO_INCREMENT PRIMARY KEY,
  customer_id INT NOT NULL,
  address_type VARCHAR(20) DEFAULT 'shipping', -- 'shipping', 'billing'
  street_address VARCHAR(255) NOT NULL,
  city VARCHAR(100) NOT NULL,
  state VARCHAR(100),
  postal_code VARCHAR(20),
  country VARCHAR(100) NOT NULL,
  is_default BOOLEAN DEFAULT false,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (customer_id) REFERENCES customers(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS products (
  id INT AUTO_INCREMENT PRIMARY KEY,
  sku VARCHAR(50) UNIQUE NOT NULL,
  name VARCHAR(255) NOT NULL,
  description TEXT,
  category_id INT,
  price DECIMAL(10,2) NOT NULL,
  cost_price DECIMAL(10,2),
  weight_grams INT,
  dimensions_json JSON, -- {"length": 10, "width": 5, "height": 2}
  is_digital BOOLEAN DEFAULT false,
  requires_shipping BOOLEAN DEFAULT true,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  is_active BOOLEAN DEFAULT true,
  FOREIGN KEY (category_id) REFERENCES categories(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS product_inventory (
  id INT AUTO_INCREMENT PRIMARY KEY,
  product_id INT NOT NULL,
  quantity_available INT NOT NULL DEFAULT 0,
  quantity_reserved INT NOT NULL DEFAULT 0,
  reorder_level INT DEFAULT 10,
  last_restocked_at DATETIME,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (product_id) REFERENCES products(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS orders (
  id INT AUTO_INCREMENT PRIMARY KEY,
  order_number VARCHAR(20) UNIQUE NOT NULL,
  customer_id INT NOT NULL,
  status VARCHAR(20) DEFAULT 'pending', -- 'pending', 'confirmed', 'shipped', 'delivered', 'cancelled'
  subtotal DECIMAL(10,2) NOT NULL,
  tax_amount DECIMAL(10,2) DEFAULT 0,
  shipping_amount DECIMAL(10,2) DEFAULT 0,
  discount_amount DECIMAL(10,2) DEFAULT 0,
  total_amount DECIMAL(10,2) NOT NULL,
  currency VARCHAR(3) DEFAULT 'USD',
  shipping_address_id INT,
  billing_address_id INT,
  payment_method VARCHAR(50),
  payment_status VARCHAR(20) DEFAULT 'pending', -- 'pending', 'paid', 'failed', 'refunded'
  notes TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  shipped_at DATETIME,
  delivered_at DATETIME,
  FOREIGN KEY (customer_id) REFERENCES customers(id),
  FOREIGN KEY (shipping_address_id) REFERENCES customer_addresses(id),
  FOREIGN KEY (billing_address_id) REFERENCES customer_addresses(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS order_items (
  id INT AUTO_INCREMENT PRIMARY KEY,
  order_id INT NOT NULL,
  product_id INT NOT NULL,
  quantity INT NOT NULL,
  unit_price DECIMAL(10,2) NOT NULL,
  total_price DECIMAL(10,2) NOT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (order_id) REFERENCES orders(id),
  FOREIGN KEY (product_id) REFERENCES products(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS product_reviews (
  id INT AUTO_INCREMENT PRIMARY KEY,
  product_id INT NOT NULL,
  customer_id INT NOT NULL,
  order_id INT,
  rating INT NOT NULL CHECK (rating >= 1 AND rating <= 5),
  title VARCHAR(255),
  review_text TEXT,
  is_verified_purchase BOOLEAN DEFAULT false,
  helpful_count INT DEFAULT 0,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (product_id) REFERENCES products(id),
  FOREIGN KEY (customer_id) REFERENCES customers(id),
  FOREIGN KEY (order_id) REFERENCES orders(id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS coupons (
  id INT AUTO_INCREMENT PRIMARY KEY,
  code VARCHAR(50) UNIQUE NOT NULL,
  name VARCHAR(255) NOT NULL,
  discount_type VARCHAR(20) NOT NULL, -- 'percentage', 'fixed_amount'
  discount_value DECIMAL(10,2) NOT NULL,
  minimum_order_amount DECIMAL(10,2),
  maximum_discount_amount DECIMAL(10,2),
  usage_limit INT,
  used_count INT DEFAULT 0,
  valid_from DATETIME NOT NULL,
  valid_until DATETIME NOT NULL,
  is_active BOOLEAN DEFAULT true,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- Categories
INSERT INTO categories (name, description, parent_id) VALUES
('Electronics', 'Electronic devices and accessories', NULL),
('Computers', 'Laptops, desktops, and computer accessories', 1),
('Smartphones', 'Mobile phones and accessories', 1),
('Audio', 'Headphones, speakers, and audio equipment', 1),
('Home & Garden', 'Home improvement and garden supplies', NULL),
('Furniture', 'Home and office furniture', 5),
('Tools', 'Hand tools and power tools', 5),
('Clothing', 'Apparel for men, women, and children', NULL),
('Men''s Clothing', 'Clothing for men', 8),
('Women''s Clothing', 'Clothing for women', 8),
('Books', 'Physical and digital books', NULL),
('Fiction', 'Fiction books and novels', 11),
('Non-Fiction', 'Educational and informational books', 11);

-- Customers
INSERT INTO customers (email, first_name, last_name, phone, date_of_birth, loyalty_points, data) VALUES
('john.doe@email.com', 'John', 'Doe', '+1-555-0101', '1985-03-15', 150, '{"vip": true}'),
('jane.smith@email.com', 'Jane', 'Smith', '+1-555-0102', '1990-07-22', 320, NULL),
('bob.johnson@email.com', 'Bob', 'Johnson', '+1-555-0103', '1978-11-08', 75, NULL),
('alice.brown@email.com', 'Alice', 'Brown', '+1-555-0104', '1992-05-30', 0, NULL),
('charlie.wilson@email.com', 'Charlie', 'Wilson', '+1-555-0105', '1987-09-12', 500, '{"referred_by": "jane.smith@email.com"}'),
('diana.garcia@email.com', 'Diana', 'Garcia', '+1-555-0106', '1995-01-18', 220, NULL),
('frank.miller@email.com', 'Frank', 'Miller', '+1-555-0107', '1982-12-03', 180, NULL),
('grace.davis@email.com', 'Grace', 'Davis', '+1-555-0108', '1988-04-25', 90, NULL);

-- Customer addresses
INSERT INTO customer_addresses (customer_id, address_type, street_address, city, state, postal_code, country, is_default) VALUES
(1, 'shipping', '123 Main St', 'New York', 'NY', '10001', 'USA', true),
(1, 'billing', '123 Main St', 'New York', 'NY', '10001', 'USA', true),
(2, 'shipping', '456 Oak Ave', 'Los Angeles', 'CA', '90210', 'USA', true),
(3, 'shipping', '789 Pine Rd', 'Chicago', 'IL', '60601', 'USA', true),
(4, 'shipping', '321 Elm St', 'Houston', 'TX', '77001', 'USA', true),
(5, 'shipping', '654 Cedar Blvd', 'Phoenix', 'AZ', '85001', 'USA', true),
(6, 'shipping', '987 Birch Lane', 'Philadelphia', 'PA', '19101', 'USA', true),
(7, 'shipping', '147 Maple Dr', 'San Antonio', 'TX', '78201', 'USA', true),
(8, 'shipping', '258 Walnut Ave', 'San Diego', 'CA', '92101', 'USA', true);

-- Products
INSERT INTO products (sku, name, description, category_id, price, cost_price, weight_grams, dimensions_json, is_digital) VALUES
('LAPTOP-001', 'MacBook Pro 16"', 'Apple MacBook Pro with M2 chip, 16GB RAM, 512GB SSD', 2, 2499.00, 1800.00, 2100, '{"length": 35.57, "width": 24.59, "height": 1.68}', false),
('LAPTOP-002', 'Dell XPS 13', 'Dell XPS 13 with Intel i7, 16GB RAM, 256GB SSD', 2, 1299.00, 950.00, 1200, '{"length": 29.6, "width": 19.9, "height": 1.17}', false),
('PHONE-001', 'iPhone 14 Pro', 'Apple iPhone 14 Pro 128GB in Space Black', 3, 999.00, 720.00, 206, '{"length": 14.79, "width": 7.15, "height": 0.79}', false),
('PHONE-002', 'Samsung Galaxy S23', 'Samsung Galaxy S23 256GB in Phantom Black', 3, 799.00, 580.00, 168, '{"length": 14.62, "width": 7.08, "height": 0.76}', false),
('AUDIO-001', 'Sony WH-1000XM4', 'Sony Wireless Noise Canceling Headphones', 4, 349.00, 200.00, 254, '{"length": 25.4, "width": 20.3, "height": 7.6}', false),
('AUDIO-002', 'AirPods Pro', 'Apple AirPods Pro with Active Noise Cancellation', 4, 249.00, 150.00, 56, '{"length": 4.5, "width": 6.1, "height": 2.4}', false),
('CHAIR-001', 'Ergonomic Office Chair', 'Comfortable ergonomic office chair with lumbar support', 6, 299.00, 180.00, 15000, '{"length": 66, "width": 66, "height": 107}', false),
('DESK-001', 'Standing Desk', 'Adjustable height standing desk 48" x 30"', 6, 599.00, 350.00, 35000, '{"length": 122, "width": 76, "height": 73}', false),
('SHIRT-001', 'Cotton T-Shirt', 'Premium cotton t-shirt available in multiple colors', 9, 24.99, 8.00, 200, '{"length": 71, "width": 51, "height": 1}', false),
('JEANS-001', 'Classic Blue Jeans', 'Classic fit blue jeans, 100% cotton denim', 9, 79.99, 35.00, 600, '{"length": 107, "width": 43, "height": 2}', false),
('BOOK-001', 'The Great Gatsby', 'Classic American novel by F. Scott Fitzgerald', 12, 12.99, 5.00, 300, '{"length": 19.8, "width": 12.9, "height": 1.5}', false),
('EBOOK-001', 'Digital Marketing Guide', 'Complete guide to digital marketing strategies', 13, 29.99, 0.00, 0, '{}', true);

-- Product inventory
INSERT INTO product_inventory (product_id, quantity_available, quantity_reserved, reorder_level, last_restocked_at) VALUES
(1, 25, 2, 5, '2024-01-15 10:00:00'),
(2, 18, 1, 5, '2024-01-10 14:30:00'),
(3, 50, 5, 10, '2024-01-20 09:15:00'),
(4, 35, 3, 8, '2024-01-18 11:45:00'),
(5, 40, 2, 10, '2024-01-12 16:20:00'),
(6, 60, 4, 15, '2024-01-22 13:10:00'),
(7, 12, 1, 3, '2024-01-08 08:30:00'),
(8, 8, 0, 2, '2024-01-05 15:45:00'),
(9, 100, 8, 20, '2024-01-25 12:00:00'),
(10, 75, 6, 15, '2024-01-23 10:30:00'),
(11, 200, 15, 50, '2024-01-20 14:00:00'),
(12, 999999, 0, 0, NULL); -- Digital product

-- Orders
INSERT INTO orders (order_number, customer_id, status, subtotal, tax_amount, shipping_amount, discount_amount, total_amount, shipping_address_id, billing_address_id, payment_method, payment_status, created_at, shipped_at, delivered_at) VALUES
('ORD-2024-001', 1, 'delivered', 2499.00, 199.92, 0.00, 0.00, 2698.92, 1, 2, 'credit_card', 'paid', '2024-01-15 10:30:00', '2024-01-16 09:00:00', '2024-01-18 14:30:00'),
('ORD-2024-002', 2, 'delivered', 598.00, 47.84, 15.99, 50.00, 611.83, 3, 3, 'paypal', 'paid', '2024-01-18 14:15:00', '2024-01-19 11:30:00', '2024-01-22 16:45:00'),
('ORD-2024-003', 3, 'shipped', 1299.00, 103.92, 0.00, 0.00, 1402.92, 4, 4, 'credit_card', 'paid', '2024-01-20 09:45:00', '2024-01-21 08:15:00', NULL),
('ORD-2024-004', 4, 'confirmed', 104.98, 8.40, 12.99, 0.00, 126.37, 5, 5, 'credit_card', 'paid', '2024-01-22 16:20:00', NULL, NULL),
('ORD-2024-005', 5, 'pending', 999.00, 79.92, 0.00, 0.00, 1078.92, 6, 6, 'bank_transfer', 'pending', '2024-01-25 11:10:00', NULL, NULL),
('ORD-2024-006', 1, 'delivered', 349.00, 27.92, 9.99, 0.00, 386.91, 1, 2, 'credit_card', 'paid', '2024-01-12 13:45:00', '2024-01-13 10:20:00', '2024-01-15 12:30:00'),
('ORD-2024-007', 6, 'cancelled', 79.99, 6.40, 8.99, 0.00, 95.38, 7, 7, 'credit_card', 'refunded', '2024-01-10 15:30:00', NULL, NULL),
('ORD-2024-008', 7, 'delivered', 29.99, 2.40, 0.00, 0.00, 32.39, 8, 8, 'credit_card', 'paid', '2024-01-08 09:15:00', NULL, '2024-01-08 09:16:00'); -- Digital product

-- Order items
INSERT INTO order_items (order_id, product_id, quantity, unit_price, total_price) VALUES
(1, 1, 1, 2499.00, 2499.00),
(2, 5, 1, 349.00, 349.00),
(2, 6, 1, 249.00, 249.00),
(3, 2, 1, 1299.00, 1299.00),
(4, 9, 1, 24.99, 24.99),
(4, 10, 1, 79.99, 79.99),
(5, 3, 1, 999.00, 999.00),
(6, 5, 1, 349.00, 349.00),
(7, 10, 1, 79.99, 79.99),
(8, 12, 1, 29.99, 29.99);

-- Product reviews
INSERT INTO product_reviews (product_id, customer_id, order_id, rating, title, review_text, is_verified_purchase, helpful_count, created_at) VALUES
(1, 1, 1, 5, 'Excellent laptop!', 'This MacBook Pro is amazing. Fast, reliable, and the screen quality is outstanding. Perfect for development work.', true, 12, '2024-01-20 15:30:00'),
(5, 2, 2, 4, 'Great noise cancellation', 'These headphones have excellent noise cancellation. Sound quality is very good, but they can be a bit heavy for long sessions.', true, 8, '2024-01-25 10:15:00'),
(6, 2, 2, 5, 'Perfect for workouts', 'Love these AirPods Pro! They stay in place during workouts and the noise cancellation is perfect for the gym.', true, 15, '2024-01-25 10:20:00'),
(2, 3, 3, 4, 'Solid laptop', 'Good performance and build quality. Battery life could be better, but overall very satisfied with this purchase.', true, 6, '2024-01-28 14:45:00'),
(5, 1, 6, 5, 'Second pair - still great!', 'Bought another pair of these headphones. They consistently deliver excellent quality and comfort.', true, 4, '2024-01-18 09:20:00'),
(9, 4, 4, 3, 'Average quality', 'The shirt is okay for the price. Fabric feels a bit thin and sizing runs small. Would recommend ordering a size up.', true, 2, '2024-01-26 16:30:00'),
(12, 7, 8, 5, 'Very informative', 'This digital marketing guide is comprehensive and up-to-date. Great value for money and immediately applicable tips.', true, 9, '2024-01-10 11:45:00');

-- Coupons
INSERT INTO coupons (code, name, discount_type, discount_value, minimum_order_amount, maximum_discount_amount, usage_limit, used_count, valid_from, valid_until) VALUES
('WELCOME10', 'Welcome 10% Off', 'percentage', 10.00, 50.00, 100.00, 1000, 45, '2024-01-01 00:00:00', '2024-12-31 23:59:59'),
('SAVE50', 'Save $50 on Orders Over $500', 'fixed_amount', 50.00, 500.00, 50.00, 500, 23, '2024-01-01 00:00:00', '2024-06-30 23:59:59'),
('TECH20', '20% Off Electronics', 'percentage', 20.00, 100.00, 200.00, 200, 67, '2024-01-15 00:00:00', '2024-03-15 23:59:59'),
('FREESHIP', 'Free Shipping', 'fixed_amount', 15.99, 75.00, 15.99, NULL, 156, '2024-01-01 00:00:00', '2024-12-31 23:59:59'),
('LOYALTY25', 'Loyalty Member 25% Off', 'percentage', 25.00, 200.00, 150.00, 100, 8, '2024-02-01 00:00:00', '2024-02-29 23:59:59');

-- Indexes
CREATE INDEX idx_products_category ON products(category_id);
CREATE INDEX idx_products_sku ON products(sku);
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_orders_created_at ON orders(created_at);
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);
CREATE INDEX idx_reviews_product ON product_reviews(product_id);
CREATE INDEX idx_reviews_customer ON product_reviews(customer_id);
CREATE INDEX idx_addresses_customer ON customer_addresses(customer_id);

-- Order summary view (STRING_AGG -> GROUP_CONCAT, || -> CONCAT)
CREATE OR REPLACE VIEW order_summary AS
SELECT
    o.id,
    o.order_number,
    CONCAT(c.first_name, ' ', c.last_name) as customer_name,
    c.email as customer_email,
    o.status,
    o.total_amount,
    o.created_at,
    COUNT(oi.id) as item_count,
    GROUP_CONCAT(p.name SEPARATOR ', ') as product_names
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN order_items oi ON o.id = oi.order_id
JOIN products p ON oi.product_id = p.id
GROUP BY o.id, c.first_name, c.last_name, c.email, o.order_number, o.status, o.total_amount, o.created_at
ORDER BY o.created_at DESC;
