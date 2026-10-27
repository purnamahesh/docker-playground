CREATE TABLE aisle (
    id INTEGER PRIMARY KEY,
    aisle VARCHAR(100) NOT NULL
);

CREATE TABLE department (
    department_id INTEGER PRIMARY KEY,
    department VARCHAR(100) NOT NULL
);

COPY aisle(id, aisle)
FROM '/tmp/InstaCart/aisles.csv'
DELIMITER ','
CSV HEADER;

COPY department(department_id, department) 
FROM '/tmp/departments.csv'
DELIMITER ','
CSV HEADER;