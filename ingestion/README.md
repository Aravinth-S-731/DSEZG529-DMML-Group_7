
# Member 1: Ingestion and Landing Preparation

Member 1 downloads the UCI batch source and fetches the catalog API source.
Unchanged source files are kept under `data\raw`; only processed files intended
for Bronze are written under `data\landing`.

The UCI workbook is converted into one CSV batch per `InvoiceDate` month.
Rows are not filtered or cleaned. The catalog API JSON is retained in `raw`
and converted to a product CSV for `landing`.

## Run process

Run all commands from the project folder:

```powershell
cd D:\BITS-SEM2\Assignments\DMML
```

For the full automated run, use the Member 5 orchestrator:

```powershell
python -m orchestration.run
```

It manages the local catalog API and runs the ingestion, Bronze, Silver, and
Gold stages in order. To run one layer manually or resume downstream work,
see [orchestration/README.md](../orchestration/README.md).

### 1. Download and batch the UCI workbook

```powershell
python .\ingestion\batch_ingest.py uci
```

The script downloads the UCI ZIP, retains it and the extracted workbook under
`data\raw\online_retail\`, and creates monthly CSVs under
`data\landing\online_retail\`. It prints the landing folder when complete.

Expected dataset profile: 541,909 rows across 13 monthly files, from
`transactions_2010-12.csv` through `transactions_2011-12.csv`.

### 2. Start the local catalog API

In the first terminal:

```powershell
python .\catalog_api\server.py
```

The API reads the workbook from `data\raw\online_retail\Online Retail.xlsx`.
Keep this terminal running while you fetch the API data. You can check the
service at `http://127.0.0.1:8000/health`.

### 3. Fetch and prepare the catalog data

Open a second terminal in the project folder and run:

```powershell
python .\ingestion\batch_ingest.py api
```

This saves the original API response and its manifest under
`data\raw\product_catalog\`, then creates `products.csv` under
`data\landing\product_catalog\`. The current API response contains 4,070
products.

When finished, stop the API server with **Ctrl+C** in the first terminal.

### 4. Check the Landing outputs

```powershell
Get-ChildItem .\data\landing\online_retail
Get-ChildItem .\data\landing\product_catalog
Get-Content .\data\landing\online_retail\manifest.json
Get-Content .\data\landing\product_catalog\manifest.json
```

Landing should contain monthly transaction CSVs and `products.csv`, plus
manifests describing the row counts, checksums, and raw-source locations.
It should not contain the source ZIP, workbook, or raw API JSON.

### 5. Handoff to Member 2

Member 2 can promote the processed files to Bronze after both Member 1 steps
finish:

```powershell
python -m bronze.ingest --landing-dir ".\data\landing\online_retail"
python -m bronze.ingest --landing-dir ".\data\landing\product_catalog"
```

## Resulting folder structure

```text
data/
  raw/
    online_retail/
      source.zip
      Online Retail.xlsx
      manifest.json
    product_catalog/
      raw_response.json
      manifest.json
  landing/
    online_retail/
      transactions_2010-12.csv
      transactions_2011-01.csv
      ...
      manifest.json
    product_catalog/
      products.csv
      manifest.json
```

Re-running either ingestion refreshes the latest snapshot. The catalog API is a
demonstration source derived from the same UCI workbook, not an independent
external dataset.
