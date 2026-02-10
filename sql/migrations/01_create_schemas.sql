CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

COMMENT ON SCHEMA raw IS 'Bronze layer: raw data ingested from source APIs.';
COMMENT ON SCHEMA silver IS 'Silver layer: cleaned and normalized data.';
COMMENT ON SCHEMA gold IS 'Gold layer: dimensional model for analytics.';
