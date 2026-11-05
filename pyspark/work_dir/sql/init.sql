CREATE SCHEMA staging_text;

CREATE SCHEMA staging;

CREATE TABLE staging.aisle (
    id SMALLINT PRIMARY KEY,
    aisle VARCHAR2(200) NOT NULL
);

CREATE TABLE staging.department (
    department_id SMALLINT PRIMARY KEY,
    department VARCHAR2(200) NOT NULL
);

CREATE TABLE staging.order_products (
    product_id INTEGER PRIMARY KEY,
    order_id INTEGER FOREIGN KEY,
    add_to_cart_order INTEGER,
    reordered SMALLINT  
)

CREATE TABLE staging.products (
    product_id INTEGER PRIMARY KEY,
    product_name VARCHAR2(200) NOT NULL,
    aisle_id SMALLINT FOREIGN KEY,
    department_id SMALLINT FOREIGN KEY,
)

CREATE TABLE staging.orders (
    order_id INTEGER PRIMARY KEY,
    user_id INTEGER,
    eval_set VARCHAR2(50),
    order_number INTEGER,
    order_dow SMALLINT,
    order_hour_of_day SMALLINT,
    days_since_prior_order SMALLINT
)

-- COPY aisle(id, aisle)
-- FROM '/tmp/InstaCart/aisles.csv'
-- DELIMITER ','
-- CSV HEADER;

-- COPY department(department_id, department) 
-- FROM '/tmp/departments.csv'
-- DELIMITER ','
-- CSV HEADER;