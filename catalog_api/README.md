# Catalog API and Bronze Layer

The local REST API exposes product codes and their most frequently occurring
non-empty descriptions from `Online Retail.xlsx`. This is a demonstration
catalog derived from the transaction workbook, not a separately sourced catalog.

## Run the API

From the assignment folder:

```powershell
python .\ingestion\batch_ingest.py uci
python .\catalog_api\server.py
```

Endpoints:

- `GET http://127.0.0.1:8000/health`
- `GET http://127.0.0.1:8000/products`
- `GET http://127.0.0.1:8000/products/85123A`

Member 1 fetches the API response into the landing area:

```powershell
python .\ingestion\batch_ingest.py api
```

The API server reads its workbook from `data\raw\online_retail\Online Retail.xlsx`.
The original JSON response is stored under `data\raw\product_catalog\`, while
the processed `products.csv` is stored under `data\landing\product_catalog\`.

## Member 2: Bronze

Member 2 does not fetch from the source API. The Bronze module copies all data
files from one Member 1 landing dataset folder without changing their bytes and
records checksums plus the original landing manifest.

Use the exact landing directory printed by the Member 1 ingestion command:

```powershell
python -m bronze.ingest `
  --landing-dir ".\data\landing\product_catalog"
```

Run the same command with the `online_retail` landing directory to promote the
monthly CSV batches. Raw ZIP, XLSX, and JSON files are kept under `data\raw`
and are not part of the Bronze handoff.

The catalog API path demonstrates REST ingestion, but it does not represent an
independent external data source.
