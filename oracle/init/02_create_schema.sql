-- 02_create_schema.sql
-- Create Application User
CREATE USER myuser IDENTIFIED BY mypassword;
GRANT CONNECT, RESOURCE, DBA TO myuser;
ALTER USER myuser QUOTA UNLIMITED ON USERS;

-- Connect as myuser to create tables
ALTER SESSION SET CURRENT_SCHEMA = myuser;

-- 1. CUSTOMER
CREATE TABLE CUSTOMER (
    id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name VARCHAR2(100),
    email VARCHAR2(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Enable Supplemental Logging for Table (Debezium Requirement)
ALTER TABLE CUSTOMER ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- 2. ORDERS
CREATE TABLE ORDERS (
    id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id NUMBER,
    amount NUMBER(10, 2),
    status VARCHAR2(20), -- 'PENDING', 'PAID', 'SHIPPED'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_customer FOREIGN KEY (customer_id) REFERENCES CUSTOMER(id)
);
ALTER TABLE ORDERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- 3. PAYMENT
CREATE TABLE PAYMENT (
    id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id NUMBER,
    method VARCHAR2(20), -- 'CREDIT_CARD', 'PAYPAL'
    paid_amount NUMBER(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES ORDERS(id)
);
ALTER TABLE PAYMENT ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- 4. REVENUE_REPORT (Output for Analytics)
CREATE TABLE REVENUE_REPORT (
    id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    report_date VARCHAR2(20), -- 'YYYY-MM-DD'
    total_revenue NUMBER(15, 2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Seed Data
INSERT INTO CUSTOMER (name, email) VALUES ('Alice Smith', 'alice@example.com');
INSERT INTO CUSTOMER (name, email) VALUES ('Bob Jones', 'bob@example.com');
INSERT INTO CUSTOMER (name, email) VALUES ('Charlie Brown', 'charlie@example.com');

INSERT INTO ORDERS (customer_id, amount, status) VALUES (1, 100.00, 'PENDING');
INSERT INTO ORDERS (customer_id, amount, status) VALUES (1, 50.50, 'PAID');
INSERT INTO ORDERS (customer_id, amount, status) VALUES (2, 200.00, 'PENDING');
INSERT INTO ORDERS (customer_id, amount, status) VALUES (3, 300.00, 'SHIPPED');

INSERT INTO PAYMENT (order_id, method, paid_amount) VALUES (2, 'CREDIT_CARD', 50.50);

COMMIT;
