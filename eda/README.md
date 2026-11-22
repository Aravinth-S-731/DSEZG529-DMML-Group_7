# Recommendation Interaction EDA

Generate exploratory analysis from validated Silver Online Retail transactions
and Gold recommendation features. Silver provides invoice dates for monthly
activity; Gold provides canonical customer, product, and interaction aggregates.
The script verifies manifest lineage, checksums, schemas, and row counts before
analyzing either layer.

## Run

From the project root, after the Gold stage has completed:

```powershell
python -m pip install -r requirements.txt
python -m eda.generate
```

The Prefect pipeline runs this step automatically after Gold and feature-store
registration. Once the flow completes, open `data\eda\index.html` in a browser
and refresh the page after the next run to see the updated charts and metrics.

Optional input and output directories:

```powershell
python -m eda.generate `
  --silver-root ".\data\silver" `
  --gold-root ".\data\gold" `
  --output-dir ".\data\eda"
```

## Interaction policy

The analysis follows the Gold recommendation policy for Silver monthly trends:
it includes rows marked `is_recommendation_eligible=true` and excludes rows
marked `is_duplicate_content=true`. It then verifies those analyzed lines
reconcile with the Gold interaction table. The dashboard refuses to combine
Silver and Gold outputs if the Gold manifest refers to a different Silver
manifest.

## Outputs

```text
data/
  eda/
    index.html
    summary.json
    plots/
      interaction_patterns.png
      item_popularity.png
      interaction_sparsity.png
```

- `interaction_patterns.png` shows monthly eligible purchase-line volume
  from Silver and the distribution of distinct products per customer from Gold.
  An asterisk marks a partial final month.
- `item_popularity.png` ranks Gold products by unique purchasing customers and
  shows each product's stock code and catalog description.
- `interaction_sparsity.png` reports the density of the complete active Gold
  customer-by-product matrix and shows a binary heatmap for its most active
  customers and most popular products. The heatmap is a selected subset; the
  reported sparsity is computed over the full active matrix.
- `summary.json` records counts, Silver and Gold manifest checksums, monthly
  counts, the analyzed invoice date window, top items, and matrix
  density/sparsity.
- Gold's recorded Silver lineage is checked against the current Silver
  transaction and product-catalog manifests before the dashboard is written.
- `index.html` presents the metrics and interactive, self-contained SVG charts
  with hover details. The downloadable/static PNG exports remain available
  within the page.

The dashboard opens with a plain-language guide to the dataset and its
recommendation signal, followed by short explanations for each chart. A
collapsible technical section retains the manifest hashes and data-validation
details for readers who want the lineage information.
