-- Create warehouse database
-- CREATE DATABASE warehouse_db;

-- Create schemas
CREATE SCHEMA staging;
CREATE SCHEMA warehouse;

-- Staging tables (staging schema)
CREATE TABLE staging.acquisition (
    acquisition_id integer NOT NULL,
    acquiring_object_id character varying(255),
    acquired_object_id character varying(255),
    term_code character varying(255),
    price_amount numeric(15,2),
    price_currency_code character varying(3),
    acquired_at timestamp without time zone,
    source_url text,
    source_description text,
    created_at timestamp without time zone,
    updated_at timestamp without time zone
);

CREATE TABLE staging.company (
    office_id integer NOT NULL,
    object_id character varying(255),
    description text,
    region character varying(255),
    address1 text,
    address2 text,
    city character varying(255),
    zip_code character varying(200),
    state_code character varying(255),
    country_code character varying(255),
    latitude numeric(9,6),
    longitude numeric(9,6),
    created_at timestamp without time zone,
    updated_at timestamp without time zone
);

CREATE TABLE staging.funding_rounds (
    funding_round_id integer NOT NULL,
    object_id character varying(255),
    funded_at date,
    funding_round_type character varying(255),
    funding_round_code character varying(255),
    raised_amount_usd numeric(15,2),
    raised_amount numeric(15,2),
    raised_currency_code character varying(255),
    pre_money_valuation_usd numeric(15,2),
    pre_money_valuation numeric(15,2),
    pre_money_currency_code character varying(255),
    post_money_valuation_usd numeric(15,2),
    post_money_valuation numeric(15,2),
    post_money_currency_code character varying(255),
    participants text,
    is_first_round boolean,
    is_last_round boolean,
    source_url text,
    source_description text,
    created_by character varying(255),
    created_at timestamp without time zone,
    updated_at timestamp without time zone
);

CREATE TABLE staging.funds (
    fund_id character varying(255) NOT NULL,
    object_id character varying(255),
    name character varying(255),
    funded_at date,
    raised_amount numeric(15,2),
    raised_currency_code character varying(3),
    source_url text,
    source_description text,
    created_at timestamp without time zone,
    updated_at timestamp without time zone
);

CREATE TABLE staging.investments (
    investment_id integer NOT NULL,
    funding_round_id integer,
    funded_object_id character varying,
    investor_object_id character varying,
    created_at timestamp without time zone,
    updated_at timestamp without time zone
);

CREATE TABLE staging.ipos (
    ipo_id character varying(255) NOT NULL,
    object_id character varying(255),
    valuation_amount numeric(15,2),
    valuation_currency_code character varying(3),
    raised_amount numeric(15,2),
    raised_currency_code character varying(3),
    public_at timestamp without time zone,
    stock_symbol character varying(255),
    source_url text,
    source_description text,
    created_at timestamp without time zone,
    updated_at timestamp without time zone
);

CREATE TABLE staging.people (
    people_id character varying(255) NOT NULL,
    object_id character varying(255),
    first_name character varying(255),
    last_name character varying(255),
    birthplace text,
    affiliation_name text,
    created_at timestamp without time zone,
    updated_at timestamp without time zone
);

CREATE TABLE staging.relationships (
    relationship_id character varying(255) NOT NULL,
    person_object_id character varying(255),
    relationship_object_id character varying(255),
    start_at character varying(255),
    end_at character varying(255),
    is_past character varying(255),
    sequence character varying(255),
    title character varying(255),
    created_at character varying(255),
    updated_at character varying(255)
);

CREATE TABLE staging.milestones (
    milestone_id INT PRIMARY KEY,
    created_at TIMESTAMP NOT NULL,
    description TEXT,
    milestone_at DATE NOT NULL,
    milestone_code VARCHAR(50),
    object_id VARCHAR(50) NOT NULL,
    source_description TEXT,
    source_url VARCHAR(255),
    updated_at TIMESTAMP NOT NULL
);

-- Warehouse tables (warehouse schema)
CREATE TABLE warehouse.fact_company_growth (
    company_growth_id SERIAL PRIMARY KEY,
    company_nk VARCHAR(255),
    date_id INT,
    acquisition_count INT,
    total_funding_usd NUMERIC(15,2),
    ipo_valuation_usd NUMERIC(15,2),
    ipo_raised_amount_usd NUMERIC(15,2)
);

CREATE TABLE warehouse.fact_investments (
    investment_id SERIAL PRIMARY KEY,
    investment_nk VARCHAR(255),
    investor_id INT,
    funding_round_id INT,
    company_id INT,
    date_id INT,
    investment_amount_usd NUMERIC(15,2)
);

CREATE TABLE warehouse.dim_company (
    company_id SERIAL PRIMARY KEY,
    company_nk VARCHAR(255),
    region VARCHAR(255),
    city VARCHAR(255),
    country_code VARCHAR(10),
    latitude NUMERIC(9,6),
    longitude NUMERIC(9,6)
);

CREATE TABLE warehouse.dim_investor (
    investor_id SERIAL PRIMARY KEY,
    investor_nk VARCHAR(255),
    investor_name VARCHAR(255)
);

CREATE TABLE warehouse.dim_funding_round (
    funding_round_id SERIAL PRIMARY KEY,
    funding_round_nk VARCHAR(255),
    funding_type VARCHAR(255),
    funding_code VARCHAR(255),
    is_first_round BOOLEAN,
    is_last_round BOOLEAN
);

CREATE TABLE warehouse.dim_people (
    people_nk VARCHAR PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    birthplace VARCHAR
);

CREATE TABLE warehouse.dim_relationship (
    relationship_nk VARCHAR PRIMARY KEY,
    people_nk VARCHAR,
    related_company_nk VARCHAR,
    start_date DATE,
    end_date DATE,
    is_past BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

CREATE TABLE warehouse.dim_date (
    date_id INT PRIMARY KEY,
    date_actual DATE,
    day_suffix VARCHAR(4),
    day_name VARCHAR(10),
    day_of_year INT,
    week_of_month INT,
    week_of_year INT,
    week_of_year_iso VARCHAR(10),
    month_actual INT,
    month_name VARCHAR(15),
    month_name_abbreviated VARCHAR(5),
    quarter_actual INT,
    quarter_name VARCHAR(10),
    year_actual INT,
    first_day_of_week DATE,
    last_day_of_week DATE,
    first_day_of_month DATE,
    last_day_of_month DATE,
    first_day_of_quarter DATE,
    last_day_of_quarter DATE,
    first_day_of_year DATE,
    last_day_of_year DATE,
    mmyyyy VARCHAR(10),
    mmddyyyy VARCHAR(20),
    weekend_indr VARCHAR(10)
);
