* [ ] 

# Member 5: Prefect Orchestration

The Prefect flow coordinates ingestion, Bronze, Silver, Gold, feature-store
registration, recommendation model training, and EDA. Prefect tracks flow/task
states and logs; it does not replace the validation and manifests owned by
those layers.

## Install

Use Python 3.11 or newer and install the project dependencies in the active
virtual environment:

```powershell
python -m pip install -r requirements.txt
```

## Run the full pipeline

From the project directory:

```powershell
python -m orchestration.run
```

The tasks run in dependency order:

1. Download the UCI workbook and create monthly transaction batches in Landing.
2. Start the local catalog API, wait for its health endpoint, capture products
   into Landing, keep the API running for the rest of the Prefect run, and stop
   it when the flow completes.
3. Process transaction and product-catalog Landing data into Bronze.
4. Standardize both datasets into Silver.
5. Build Gold customer, product, and customer-product feature tables.
6. Snapshot Gold features into a new immutable feature-store version.
7. Train and evaluate an item-item recommender from validated Silver events;
   save the model and track its metrics and artifacts in the local experiment
   store.
8. Generate EDA with Silver invoice dates and verified Gold popularity,
   customer-diversity, and interaction-matrix features.

After a successful run, open `data\eda\index.html` in a browser. Its interactive
SVG charts and retained PNG exports are regenerated on each run; refresh the
page to see the latest metrics. The dashboard is self-contained and does not
require a separate web server.
The feature registry and immutable snapshots are written under
`data\feature_store\`. Model artifacts and reports are written under
`data\models\item_item_cosine\`; local run metadata is stored under
`data\model_tracking\`.

The catalog API is derived from the UCI workbook, not an independent source.
Ingestion already retries transient downloads. The API-capture task also has a
Prefect retry; data-quality and schema errors fail visibly rather than being
silently skipped.
The catalog API's process log is kept under
`data\orchestration\api_logs\`.

The catalog API is enabled by default for runs that include the
`catalog_api_capture` stage. It listens on `127.0.0.1:8000` and remains running
until the Prefect flow completes. Configure or disable it from the CLI:

```powershell
python -m orchestration.run `
  --catalog-api-host "127.0.0.1" `
  --catalog-api-port 8001

python -m orchestration.run --disable-catalog-api

python -m orchestration.serve `
  --catalog-api-host "127.0.0.1" `
  --catalog-api-port 8001
```

## Resume from a stage

`--from-stage` skips earlier stages and runs the selected stage and all
downstream stages. The required upstream outputs must already exist; their
manifests and checksums are verified by the layer modules.

```powershell
python -m orchestration.run --from-stage silver_online_retail
```

Available starting stages:

- `uci_ingestion`
- `catalog_api_capture`
- `bronze_online_retail`
- `bronze_product_catalog`
- `silver_online_retail`
- `silver_product_catalog`
- `gold`
- `feature_store`
- `model_training`
- `eda`

Other options:

```powershell
python -m orchestration.run `
  --data-root ".\data" `
  --api-startup-timeout 45
```

## Persistent Prefect UI

For a persistent run history and dashboard, start the local Prefect server in
one terminal:

```powershell
prefect server start
```

In another terminal, use a dedicated Prefect profile and point it to the local
server. These are one-time profile settings:

```powershell
prefect profile create dmml
prefect profile use dmml
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
```

Then run the flow as usual. Open `http://127.0.0.1:4200` to view flow runs,
task states, logs, and failures. Without `PREFECT_API_URL`, Prefect uses a
temporary local server for the run, so run history is not kept in this UI.

## Trigger from the Prefect UI

To create an on-demand deployment, keep the Prefect server running and start
the deployment runner in another terminal:

```powershell
python -m orchestration.serve
```

Keep that process running. The `online-retail-on-demand` deployment will appear
in the Prefect UI at `http://127.0.0.1:4200`. Choose **Run** there to execute
the complete pipeline from UCI ingestion through Gold, feature-store
registration, model training, and the EDA dashboard. You do not need to run
the individual layer commands.

## Optional scheduled deployment

The UCI dataset is a static archive, so this project does not choose an
automatic recurring schedule by default. Once a refresh cadence is appropriate,
start the deployment runner with a schedule instead:

```powershell
python -m orchestration.serve `
  --cron "0 2 * * *" `
  --timezone "Asia/Kolkata"
```

This example runs daily at 2:00 AM India time. The serve process must remain
running for UI-triggered or scheduled runs to start; use Windows Task Scheduler
or a service manager if it must start automatically after a reboot. Only one
flow run is allowed at a time to avoid overlapping writes to the data folders.

Scheduling and real-time recommendation serving are separate concerns. The
Prefect flow trains and evaluates a batch recommendation model but does not
deploy a live recommendation API.
