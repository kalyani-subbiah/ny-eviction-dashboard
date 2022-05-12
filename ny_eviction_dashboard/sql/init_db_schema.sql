DROP SCHEMA IF EXISTS raw CASCADE;
DROP SCHEMA IF EXISTS staging CASCADE;
DROP SCHEMA IF EXISTS prod CASCADE;

CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA prod;

-- Raw
CREATE UNLOGGED TABLE raw.soda_evictions (
  court_index_number text,
  docket_number text,
  eviction_address text,
  executed_date timestamp,
  residential_commercial_ind text,
  borough text,
  eviction_zip text,
  ejectment text,
  eviction_possession text,
  latitude text,
  longitude text,
  community_board text,
  council_district text,
  census_tract text,
  bin text,
  bbl text,
  nta text
)

