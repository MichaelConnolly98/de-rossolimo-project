from test_warehouse_data.connection_m import db

def seed():
    '''Seeds database'''
    db.run("DROP TABLE IF EXISTS dim_currency CASCADE;")
    db.run("DROP TABLE IF EXISTS dim_design CASCADE;")
    db.run("DROP TABLE IF EXISTS dim_date CASCADE;")
    db.run("DROP TABLE IF EXISTS dim_staff CASCADE;")
    db.run("DROP TABLE IF EXISTS dim_location CASCADE;")
    db.run("DROP TABLE IF EXISTS dim_counterparty CASCADE;")
    db.run("DROP TABLE IF EXISTS dim_payment_type CASCADE;")
    db.run("DROP TABLE IF EXISTS dim_transaction CASCADE;")
    db.run("DROP TABLE IF EXISTS fact_purchase_order CASCADE;")
    db.run("DROP TABLE IF EXISTS fact_payment CASCADE;")
    db.run("DROP TABLE IF EXISTS fact_sales_order CASCADE;")


    create_dim_currency()
    create_dim_design()
    create_dim_date()
    create_dim_staff()
    create_dim_location()
    create_dim_counterparty()
    create_dim_payment_type()
    create_dim_transaction()
    create_fact_purchase_order()
    create_fact_payment()
    create_fact_sales_order()

def create_dim_currency():
    create_dim_currency_sql = '''CREATE TABLE dim_currency (
                            currency_id INT PRIMARY KEY,
                            currency_code VARCHAR(50) NOT NULL,
                            currency_name VARCHAR(50) NOT NULL);'''
    return db.run(create_dim_currency_sql)

def create_dim_design():
    create_dim_design_sql = '''CREATE TABLE dim_design (
                            design_id INT PRIMARY KEY,
                            design_name VARCHAR(50) NOT NULL,
                            file_location VARCHAR(50) NOT NULL,
                            file_name VARCHAR(50) NOT NULL);'''
    return db.run(create_dim_design_sql)

def create_dim_date():
    create_dim_date_sql = '''CREATE TABLE dim_date (
                        date_id DATE PRIMARY KEY,
                        year INTEGER NOT NULL,
                        month INTEGER NOT NULL,
                        day INTEGER NOT NULL,
                        day_of_week INTEGER NOT NULL,
                        day_name VARCHAR(50) NOT NULL,
                        month_name VARCHAR(50) NOT NULL,
                        quarter INTEGER NOT NULL);'''
    return db.run(create_dim_date_sql)

def create_dim_staff():
    create_dim_staff_sql = '''CREATE TABLE dim_staff (
                        staff_id INT PRIMARY KEY,
                        first_name VARCHAR(50) NOT NULL,
                        last_name VARCHAR(50) NOT NULL,
                        department_name VARCHAR(50) NOT NULL,
                        location VARCHAR(50) NOT NULL,
                        email_address VARCHAR(50) NOT NULL);'''
    return db.run(create_dim_staff_sql)

def create_dim_location():
    create_dim_location_sql = '''CREATE TABLE dim_location (
                        location_id INT PRIMARY KEY,
                        address_line_1 VARCHAR(50) NOT NULL,
                        address_line_2 VARCHAR(50),
                        district VARCHAR(50),
                        city VARCHAR(50) NOT NULL,
                        postal_code VARCHAR(50) NOT NULL,
                        country VARCHAR(50) NOT NULL,
                        phone VARCHAR(50) NOT NULL);'''
    return db.run(create_dim_location_sql)

def create_dim_counterparty():
    create_dim_counterparty_sql = '''CREATE TABLE dim_counterparty (
                        counterparty_id INT PRIMARY KEY,
                        counterparty_legal_name VARCHAR(50) NOT NULL,
                        counterparty_legal_address_line_1 VARCHAR(50) NOT NULL,
                        counterparty_legal_address_line2 VARCHAR(50),
                        counterparty_legal_district VARCHAR(50),
                        counterparty_legal_city VARCHAR(50) NOT NULL,
                        counterparty_legal_postal_code VARCHAR(50) NOT NULL,
                        counterparty_legal_country VARCHAR(50) NOT NULL,
                        counterparty_legal_phone_number VARCHAR(50) NOT NULL);'''
    return db.run(create_dim_counterparty_sql)

def create_dim_payment_type():
    create_dim_payment_type_sql = '''CREATE TABLE dim_payment_type (
                        payment_type_id INT PRIMARY KEY,
                        payment_type_name VARCHAR(50) NOT NULL);'''
    return db.run(create_dim_payment_type_sql)

def create_dim_transaction():
    create_dim_transaction_sql = '''CREATE TABLE dim_transaction (
                        transaction_id INT PRIMARY KEY,
                        transaction_type VARCHAR(50) NOT NULL,
                        sales_order_id INTEGER,
                        purchase_order_id INTEGER);'''
    return db.run(create_dim_transaction_sql)

def create_fact_purchase_order():
    create_fact_purchase_order_sql = '''CREATE TABLE fact_purchase_order (
                        purchase_record_id SERIAL PRIMARY KEY,
                        purchase_order_id INT NOT NULL,
                        created_date DATE NOT NULL REFERENCES dim_date (date_id),
                        created_time TIME NOT NULL,
                        last_updated_date DATE NOT NULL REFERENCES dim_date (date_id),
                        last_updated_time TIME NOT NULL,
                        staff_id INTEGER NOT NULL REFERENCES dim_staff (staff_id),
                        counterparty_id INT NOT NULL REFERENCES dim_counterparty (counterparty_id),
                        item_code VARCHAR(50) NOT NULL,
                        item_quanitity INT NOT NULL,
                        item_unit_price NUMERIC NOT NULL,
                        currency_id INT NOT NULL REFERENCES dim_currency (currency_id),
                        agreed_delivery_date DATE NOT NULL REFERENCES dim_date (date_id),
                        agreed_payment_date DATE NOT NULL REFERENCES dim_date (date_id),
                        agreed_delivery_location_id INTEGER NOT NULL REFERENCES dim_location (location_id));'''
    return db.run(create_fact_purchase_order_sql)

def create_fact_payment():
    create_fact_payment_sql = '''CREATE TABLE fact_payment (
                        payment_record_id SERIAL PRIMARY KEY,
                        payment_id INT NOT NULL,
                        created_date DATE NOT NULL REFERENCES dim_date (date_id),
                        created_time TIME NOT NULL,
                        last_updated_date DATE NOT NULL REFERENCES dim_date (date_id),
                        last_updated_time TIME NOT NULL,
                        transaction_id INTEGER NOT NULL REFERENCES dim_transaction (transaction_id),
                        counterparty_id INTEGER NOT NULL REFERENCES dim_counterparty (counterparty_id),
                        payment_amount NUMERIC NOT NULL,
                        currency_id INTEGER NOT NULL REFERENCES dim_currency (currency_id),
                        payment_type_id INTEGER NOT NULL REFERENCES dim_payment_type (payment_type_id),
                        paid BOOLEAN NOT NULL,
                        payment_date DATE NOT NULL REFERENCES dim_date (date_id));'''
    return db.run(create_fact_payment_sql)

def create_fact_sales_order():
    create_fact_sales_order_sql = '''CREATE TABLE fact_sales_order (
                        sales_record_id SERIAL PRIMARY KEY,
                        sales_order_id INT NOT NULL,
                        created_date DATE NOT NULL REFERENCES dim_date (date_id),
                        created_time TIME NOT NULL,
                        last_updated_date DATE NOT NULL REFERENCES dim_date (date_id),
                        last_updated_time TIME NOT NULL,
                        sales_staff_id INTEGER NOT NULL REFERENCES dim_staff (staff_id),
                        counterparty_id INTEGER NOT NULL,
                        units_sold INTEGER NOT NULL,
                        unit_price NUMERIC(10, 2) NOT NULL,
                        currency_id INTEGER NOT NULL REFERENCES dim_currency (currency_id),
                        design_id INTEGER NOT NULL REFERENCES dim_design (design_id),
                        agreed_payment_date DATE NOT NULL REFERENCES dim_date (date_id),
                        agreed_delivery_date DATE NOT NULL REFERENCES dim_date (date_id),
                        agreed_delivery_location_id INTEGER NOT NULL REFERENCES dim_location (location_id));'''
    return db.run(create_fact_sales_order_sql)