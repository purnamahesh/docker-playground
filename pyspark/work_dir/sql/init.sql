CREATE SCHEMA staging_text;

CREATE SCHEMA staging;

CREATE TABLE staging.aisle (
    aisle_id SMALLINT PRIMARY KEY,
    aisle VARCHAR(200) NOT NULL
);

CREATE TABLE staging.department (
    department_id SMALLINT PRIMARY KEY,
    department VARCHAR(200) NOT NULL
);

CREATE TABLE staging.products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    aisle_id SMALLINT REFERENCES staging.aisle(aisle_id),
    department_id SMALLINT REFERENCES staging.department(department_id)
);

CREATE TABLE staging.orders (
    order_id INTEGER PRIMARY KEY,
    user_id INTEGER,
    eval_set VARCHAR(50),
    order_number INTEGER,
    order_dow SMALLINT,
    order_hour_of_day SMALLINT,
    days_since_prior_order SMALLINT
);

CREATE TABLE staging.order_products (
    product_id INTEGER PRIMARY KEY,
    order_id INTEGER REFERENCES staging.orders(order_id),
    add_to_cart_order INTEGER,
    reordered SMALLINT  
);

-- COPY aisle(id, aisle)
-- FROM '/tmp/InstaCart/aisles.csv'
-- DELIMITER ','
-- CSV HEADER;

-- COPY department(department_id, department) 
-- FROM '/tmp/departments.csv'
-- DELIMITER ','
-- CSV HEADER;