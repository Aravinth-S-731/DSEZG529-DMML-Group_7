# Member 2: Bronze Layer

Bronze incrementally consumes the processed CSV files in a Member 1 Landing
folder and writes Silver-ready CSVs under `data\bronze\<dataset>\`. It preserves
the business values from Landing; it does not clean, filter, or aggregate them.

## What Bronze adds

- **Schema contract:** checks the Landing manifest and exact CSV columns for
  transaction batches and product catalogs.
- **File integrity:** recomputes each input SHA-256 and compares it with the
  Landing manifest before processing.
- **Row lineage:** adds `_source_file`, `_source_type`, `_source_sha256`,
  `_source_row_number`, `_batch_id`, and `_ingested_at_utc`.
- **Record identity:** adds `_record_id`, derived from dataset, source file
  checksum, filename, and row number. Repeated identical rows remain separate.
- **Record content hash:** adds `_record_hash`, a SHA-256 of the business
  columns. Equal hashes identify equal row contents; Bronze does not deduplicate
  them.
- **Schema version:** adds `_schema_version` so downstream code can identify
  the metadata contract.
- **Incremental processing:** `bronze_manifest.json` tracks each source batch.
  An unchanged file whose Bronze output still matches its recorded hash is
  skipped; changed or missing outputs are rebuilt.
- **Audit summary:** records source/output hashes, row counts, empty-value
  counts, batch IDs, and processing status in the manifest.

The CSV processing is streamed row by row to avoid loading the complete dataset
into memory. A schema, checksum, or row-count mismatch raises an error instead
of silently accepting a partial batch.

## Run

Run from the project folder after Member 1 has created Landing files:

```powershell
python -m bronze.ingest --landing-dir ".\data\landing\online_retail"
python -m bronze.ingest --landing-dir ".\data\landing\product_catalog"
```

An alternate Bronze output root can be supplied for testing:

```powershell
python -m bronze.ingest `
  --landing-dir ".\data\landing\product_catalog" `
  --output-root ".\temp\bronze"
```

## Output

Each input CSV keeps its name, with the Bronze metadata columns appended:

```text
data/
  bronze/
    online_retail/
      transactions_2010-12.csv
      transactions_2011-01.csv
      ...
      bronze_manifest.json
    product_catalog/
      products.csv
      bronze_manifest.json
```

`bronze_manifest.json` is the incremental ledger and the handoff record for
Silver. Original ZIP, XLSX, and JSON files remain under `data\raw`; Landing
manifests and Silver processing are separate from this Bronze step.
