-- =============================
-- Firmable Data Pipeline Schema
-- =============================
-- Author: Devya Mehrotra
-- Date: 2025-07-02
--
-- This schema is designed for integrating Australian company data from Common Crawl and ABR.
-- It supports raw ingestion, entity matching, and analytics for data quality.

-- =============
-- SCHEMA SETUP
-- =============
CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS processed_data;
CREATE SCHEMA IF NOT EXISTS analytics;

-- =============================
-- RAW DATA TABLES
-- =============================

-- Common Crawl raw companies
CREATE TABLE IF NOT EXISTS raw_data.common_crawl_companies (
    id SERIAL PRIMARY KEY,
    website_url VARCHAR(500) NOT NULL,
    company_name VARCHAR(255),
    industry VARCHAR(500),
    extracted_text TEXT,
    crawl_date TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ABR raw companies
CREATE TABLE IF NOT EXISTS raw_data.abr_companies (
    id SERIAL PRIMARY KEY,
    abn VARCHAR(11) UNIQUE NOT NULL,
    entity_name VARCHAR(255) NOT NULL,
    entity_type VARCHAR(500),
    entity_status VARCHAR(20),
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    suburb VARCHAR(100),
    state VARCHAR(3),
    postcode VARCHAR(4),
    start_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================
-- PROCESSED/UNIFIED COMPANIES
-- =============================
CREATE TABLE IF NOT EXISTS processed_data.companies (
    id SERIAL PRIMARY KEY,
    unified_name VARCHAR(255) NOT NULL,
    primary_abn VARCHAR(11),
    website_url VARCHAR(100000),
    industry VARCHAR(500),
    entity_type VARCHAR(100),
    entity_status VARCHAR(20),
    address JSONB,
    confidence_score DECIMAL(4,3),
    data_sources VARCHAR(50)[],
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================
-- ENTITY MATCHES
-- =============================
CREATE TABLE IF NOT EXISTS processed_data.entity_matches (
    id SERIAL PRIMARY KEY,
    cc_company_id INTEGER REFERENCES raw_data.common_crawl_companies(id),
    abr_company_id INTEGER REFERENCES raw_data.abr_companies(id),
    unified_company_id INTEGER REFERENCES processed_data.companies(id),
    match_type VARCHAR(20), -- e.g. exact, fuzzy, llm
    confidence_score DECIMAL(4,3),
    matching_criteria JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================
-- ANALYTICS & DATA QUALITY
-- =============================
CREATE TABLE IF NOT EXISTS analytics.data_quality_metrics (
    id SERIAL PRIMARY KEY,
    pipeline_run_id VARCHAR(50),
    metric_name VARCHAR(100),
    metric_value DECIMAL(10,2),
    threshold_value DECIMAL(10,2),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================
-- INDEXES
-- =============================
CREATE INDEX IF NOT EXISTS idx_cc_url ON raw_data.common_crawl_companies(website_url);
CREATE INDEX IF NOT EXISTS idx_abr_abn ON raw_data.abr_companies(abn);
CREATE INDEX IF NOT EXISTS idx_company_name ON processed_data.companies(unified_name);
CREATE INDEX IF NOT EXISTS idx_match_confidence ON processed_data.entity_matches(confidence_score);

-- =============================
-- ROLES & PERMISSIONS
-- =============================
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'etl_reader') THEN
        CREATE ROLE etl_reader;
    END IF;
END
$$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'etl_writer') THEN
        CREATE ROLE etl_writer;
    END IF;
END
$$;
GRANT USAGE ON SCHEMA raw_data, processed_data, analytics TO etl_reader, etl_writer;
GRANT SELECT ON ALL TABLES IN SCHEMA raw_data, processed_data, analytics TO etl_reader;
GRANT INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA raw_data, processed_data, analytics TO etl_writer;
-- (Add user-specific grants as needed)

-- =============================
-- END OF SCHEMA
-- ============================= 