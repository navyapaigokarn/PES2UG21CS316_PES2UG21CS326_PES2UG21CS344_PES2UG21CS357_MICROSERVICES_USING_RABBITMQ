create database ims;
use ims;
CREATE TABLE items (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    quantity INTEGER NOT NULL
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    item_id INTEGER REFERENCES items(id),
    quantity INTEGER NOT NULL,
    customer_name VARCHAR(20) NOT NULL,
    shipping_address VARCHAR(100) NOT NULL
);

CREATE TABLE health_checks (
    id SERIAL PRIMARY KEY,
    service_name VARCHAR(255) NOT NULL,
    status VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP NOT NULL
);
