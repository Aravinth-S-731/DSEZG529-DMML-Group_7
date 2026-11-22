# Member 3: Silver Layer

Silver validates Bronze inputs, standardizes transaction and catalog fields,
preserves Bronze lineage columns, and writes compressed curated CSVs plus
quality reports. It does not build recommendation features; those belong in
Gold.

## Run

Run from the project folder after both Bronze datasets have been processed:

```powershell
python -m silver.ingest --dataset online_retail
python -m silver.ingest --dataset product_catalog
```

For a test run with a separate output folder:

```powershell
python -m silver.ingest `
  --dataset online_retail `
  --silver-root ".\temp\silver"
```

## Transaction handling

- Verifies the Bronze manifest, file checksum, schema, and row count.
- Renames columns to snake case, normalizes whitespace and ISO timestamps,
  converts quantities to integers, and normalizes prices to three decimal
  places. Three places preserve the dataset's four `0.001` prices as well as
  the two-decimal values affected by Excel floating-point representation.
- Keeps valid transaction lines, including cancellations, negative-quantity
  returns, zero quantities, and rows without a customer ID.
- Adds `line_total`, cancellation/return/adjustment flags,
  `is_recommendation_eligible`, duplicate-content flags, and `quality_flags`.
- Preserves all Bronze lineage fields. Exact repeated record hashes are marked
  but not dropped.
- Sends rows with unparseable required values to a `quarantine` CSV with an
  error reason; it never silently drops them.

Rows without a customer ID remain in Silver for audit, but are not marked as
eligible for personalized recommendations. Gold can use
`is_recommendation_eligible` as its starting filter.

## Product catalog handling

Silver trims descriptions, normalizes product codes to uppercase, flags blank
descriptions and duplicate product codes, and preserves Bronze lineage.
Catalog records are not silently deduplicated.

## Outputs

```text
data/
  silver/
    online_retail/
      transactions_2010-12.csv.gz
      transactions_2011-01.csv.gz
      ...
      quarantine/                 # created only when invalid rows exist
      quality_report.json
      silver_manifest.json
    product_catalog/
      products.csv.gz
      quarantine/                 # created only when invalid rows exist
      quality_report.json
      silver_manifest.json
```

`quality_report.json` summarizes row counts, cancellations, returns, missing
values, duplicate flags, recommendation-eligible rows, and quarantined rows.
`silver_manifest.json` records Bronze and Silver checksums and the input/output
row counts. Data files are gzip-compressed CSVs; Python's `gzip.open()` and
Pandas' compression inference can read them directly.
