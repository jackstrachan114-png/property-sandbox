# Manual source links mapped to raw folders

Use this when automatic link discovery is blocked or unreliable.

## Place into `data/raw/price_paid/`
- Price Paid complete file (direct):
  - https://price-paid-data.publicdata.landregistry.gov.uk/pp-complete.txt
- Price Paid discovery page:
  - https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads

## Place into `data/raw/ukhpi/`
Use downloadable CSV files linked from the UKHPI data-download page text you provided (for example **UK HPI full file (CSV)** and relevant attribute CSVs such as **Index** and **Average price**).

Suggested minimum set:
- UK HPI full file (CSV)
- Index (CSV)

(These are linked from the GOV.UK UKHPI download page for the release period.)

## Place into `data/raw/epc/`
If available as property-level files (CSV/ZIP), place them here.

Reference pages you provided:
- https://www.gov.uk/government/statistics/energy-performance-of-building-certificates-in-england-and-wales-october-to-december-2025/energy-performance-of-buildings-certificates-statistical-release-october-to-december-2025-england-and-wales
- https://www.gov.uk/government/statistics/energy-performance-of-building-certificates-in-england-and-wales-july-to-september-2025/energy-performance-of-buildings-certificates-statistical-release-july-to-september-2025-england-and-wales
- https://www.gov.uk/government/statistical-data-sets/live-tables-on-energy-performance-of-buildings-certificates

## Place into `data/raw/ownership/`
If you can export downloadable ownership-level files (CSV/ZIP/JSON), place them here.

Reference pages you provided:
- https://use-land-property-data.service.gov.uk/datasets/ocod
- https://www.gov.uk/government/publications/geospatial-commission-data-catalogue-hm-land-registry

## API discovery output folder
- `data/raw/land_property_api/` is used for API-root discovery JSON from:
  - https://use-land-property-data.service.gov.uk/api/v1/
