-- SQL schema matching the Gold CSV outputs; compatible with SQLite and PostgreSQL.
-- The Prefect flow still writes CSVs; use gold.load_sqlite to materialize a local warehouse.

CREATE TABLE IF NOT EXISTS gold_customer_features (
    customer_id BIGINT PRIMARY KEY,
    unique_products BIGINT NOT NULL CHECK (unique_products >= 0),
    purchase_line_count BIGINT NOT NULL CHECK (purchase_line_count > 0),
    total_units_purchased BIGINT NOT NULL CHECK (total_units_purchased > 0),
    total_spend NUMERIC(18, 2) NOT NULL CHECK (total_spend >= 0),
    first_purchase_at TIMESTAMP NOT NULL,
    last_purchase_at TIMESTAMP NOT NULL,
    CHECK (first_purchase_at <= last_purchase_at)
);

CREATE TABLE IF NOT EXISTS gold_product_features (
    stock_code VARCHAR(64) PRIMARY KEY,
    description TEXT NOT NULL DEFAULT '',
    catalog_record_count INTEGER NOT NULL CHECK (catalog_record_count >= 0),
    is_duplicate_catalog_code BOOLEAN NOT NULL,
    purchase_line_count BIGINT NOT NULL DEFAULT 0 CHECK (purchase_line_count >= 0),
    unique_customers BIGINT NOT NULL DEFAULT 0 CHECK (unique_customers >= 0),
    units_sold BIGINT NOT NULL DEFAULT 0 CHECK (units_sold >= 0),
    gross_sales NUMERIC(18, 2) NOT NULL DEFAULT 0 CHECK (gross_sales >= 0),
    first_sold_at TIMESTAMP,
    last_sold_at TIMESTAMP,
    popularity_rank BIGINT CHECK (popularity_rank IS NULL OR popularity_rank > 0),
    CHECK (
        (first_sold_at IS NULL AND last_sold_at IS NULL)
        OR
        (first_sold_at IS NOT NULL AND last_sold_at IS NOT NULL
         AND first_sold_at <= last_sold_at)
    )
);

CREATE TABLE IF NOT EXISTS gold_customer_product_interactions (
    customer_id BIGINT NOT NULL
        REFERENCES gold_customer_features (customer_id),
    stock_code VARCHAR(64) NOT NULL
        REFERENCES gold_product_features (stock_code),
    purchase_line_count BIGINT NOT NULL CHECK (purchase_line_count > 0),
    quantity_purchased BIGINT NOT NULL CHECK (quantity_purchased > 0),
    spend NUMERIC(18, 2) NOT NULL CHECK (spend >= 0),
    first_purchase_at TIMESTAMP NOT NULL,
    last_purchase_at TIMESTAMP NOT NULL,
    PRIMARY KEY (customer_id, stock_code),
    CHECK (first_purchase_at <= last_purchase_at)
);

CREATE INDEX IF NOT EXISTS idx_gold_product_features_popularity_rank
    ON gold_product_features (popularity_rank);

CREATE INDEX IF NOT EXISTS idx_gold_interactions_stock_code
    ON gold_customer_product_interactions (stock_code);
