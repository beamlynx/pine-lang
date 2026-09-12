-- Two cases the Postgres seed can't exercise on its own:
--
-- 1. A *_id-style column with no FK constraint, so the heuristic relation
--    detector (naming-convention guess, not a real foreign key) has
--    something to find.
-- 2. A second database, with SELECT granted to the `pine` user, to check
--    that MySQL's DATABASE()-scoped introspection actually excludes it -
--    unlike Postgres, MySQL's information_schema is server-global, not
--    connection-scoped.

CREATE TABLE IF NOT EXISTS warehouses (
  id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  city VARCHAR(100) NOT NULL
) ENGINE=InnoDB;

-- warehouse_id deliberately has NO foreign key constraint to warehouses -
-- only the naming convention ties them together.
CREATE TABLE IF NOT EXISTS warehouse_staff (
  id INT AUTO_INCREMENT PRIMARY KEY,
  warehouse_id INT NOT NULL,
  name VARCHAR(100) NOT NULL,
  role VARCHAR(50)
) ENGINE=InnoDB;

INSERT INTO warehouses (name, city) VALUES
('Main Warehouse', 'Newark'),
('West Coast Hub', 'Reno');

INSERT INTO warehouse_staff (warehouse_id, name, role) VALUES
(1, 'Sam Rivera', 'manager'),
(1, 'Priya Nair', 'picker'),
(2, 'Tom Baker', 'manager');

CREATE DATABASE IF NOT EXISTS pine_other;

USE pine_other;

CREATE TABLE IF NOT EXISTS secret_table (
  id INT AUTO_INCREMENT PRIMARY KEY,
  note VARCHAR(255) NOT NULL
) ENGINE=InnoDB;

INSERT INTO secret_table (note) VALUES
('If this shows up in the pine schema for the `pine` database connection, DATABASE()-scoping is broken.');

GRANT SELECT ON pine_other.* TO 'pine'@'%';
FLUSH PRIVILEGES;
