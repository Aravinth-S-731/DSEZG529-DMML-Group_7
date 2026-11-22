# Member 4: Gold Layer

Gold combines the monthly Silver transactions into recommendation and
analytical feature tables. It verifies Silver manifests, checksums, schemas,
and row counts before using the data.

## Run

Run after both Silver datasets are available:

```powershell
python -m gold.build
```

Optional alternate roots:

```powershell
python -m gold.build `
  --silver-root ".\data\silver" `
  --gold-root ".\data\gold"
```

## Recommendation policy

- Uses Silver rows marked `is_recommendation_eligible=true`.
- Excludes rows flagged `is_duplicate_content=true` from recommendation
  interactions to avoid counting repeated record content twice.
- Silver cancellations, returns, zero-quantity rows, negative-price rows, and
  rows without customer IDs remain auditable in Silver but do not enter these
  positive-purchase features.
- Aggregates at customer-product level. Purchase-line count, quantity, and
  spend are retained as separate implicit-feedback signals; no single model
  score or train/test split is imposed.
- Product catalog duplicates are represented once per stock code. The most
  frequent non-empty description is selected; ties are resolved
  alphabetically.

## Outputs

```text
data/
  gold/
    customer_product_interactions.csv.gz
    customer_features.csv.gz
    product_features.csv.gz
    quality_report.json
    gold_manifest.json
```

- `customer_product_interactions.csv.gz`: one row per customer-product pair,
  with purchase-line count, units, spend, and first/last purchase timestamps.
- `customer_features.csv.gz`: one row per customer with product diversity,
  purchase activity, total units/spend, and first/last purchase timestamps.
- `product_features.csv.gz`: one row per catalog or sold product, with
  description, customer reach, units, sales, dates, and popularity rank.
  Products without sales remain present with zero sales metrics and no rank.
- `quality_report.json` records eligible/ineligible and duplicate-excluded
  counts, catalog duplicates, and products missing from either side.
- `gold_manifest.json` records the Silver manifest checksums and output file
  checksums, sizes, and row counts.

All data outputs are gzip-compressed CSVs and can be opened with Python's
`gzip` module or Pandas compression inference.
