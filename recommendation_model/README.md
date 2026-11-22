# Recommendation Model

This module trains an item-item collaborative-filtering recommender from
validated Silver Online Retail events, evaluates it on a chronological
holdout, saves an inference artifact, and tracks parameters, metrics, and
artifact checksums in a local experiment store.

## Run

From the project root after Silver data is available:

```powershell
python -m pip install -r requirements.txt
python -m recommendation_model.train
```

Optional paths and training settings:

```powershell
python -m recommendation_model.train `
  --silver-root ".\data\silver" `
  --model-root ".\data\models\item_item_cosine" `
  --tracking-root ".\data\model_tracking" `
  --train-fraction 0.8 `
  --max-neighbors 100
```

For a saved-model inference example:

```powershell
python -m recommendation_model.inference --customer-id 14911 --top-k 10
```

For the interactive Streamlit showcase:

```powershell
python -m pip install "streamlit>=1.40,<2.0"
python -m streamlit run recommendation_model\app.py
```

Open the local URL printed by Streamlit. Customer profiles are identified by
customer number because the dataset contains no names. Search for optional
recent items by product description, choose how many suggestions to see, and
request recommendations. Results show product descriptions, item codes for
reference, and a plain-language reason for each suggestion. Offline metrics
include explanations and a comparison with the popularity baseline.

The app validates and reads product descriptions from the Gold manifest, and
checks that Gold and the saved model use the same Silver transaction data. Use
**Check for latest model** after a successful Prefect model-training run to
refresh the saved model.

The app loads the latest saved model once and caches it for requests. It does
not train or update the model on each interaction. This workspace has limited
free disk space; if the Streamlit install fails, install it after moving the
project to a drive with more space.

If `--customer-id` is omitted or is not in the saved training history, the
inference interface returns the most popular training items. Personalized
results exclude products already in the customer's saved history.

## Model and evaluation

- The interaction signal is binary: each customer contributes at most one
  observation to a product and product-pair count, regardless of purchase-line
  or quantity totals.
- Similarity is cosine similarity between item vectors over training
  customers. The model stores up to 100 nearest item neighbors per product.
- The split is chronological. All eligible, non-duplicate Silver events at or
  before the 80% timestamp cutoff are training data; later events are holdout.
  Events sharing the cutoff timestamp stay together in training.
- Test relevance is a future product interaction that is new to that customer
  and whose product appeared in the training catalog. Previously-seen
  customer-product pairs and test-only cold-start products are reported
  separately rather than treated as recommendable targets.
- The report includes macro-averaged Precision@K, Recall@K, and NDCG@K for
  K=5, 10, and 20, alongside a training-popularity baseline. Precision uses K
  as its denominator.
- After evaluation, the persisted inference artifact is refit on all eligible
  Silver events. Validation metrics still come only from the earlier
  training/holdout split.

## Outputs and tracking

```text
data/
  models/
    item_item_cosine/
      latest.json
      <experiment-run-id>/
        model.json.gz
        evaluation.json
        evaluation.md
  model_tracking/
    experiments/
      online-retail-recommendations/
        runs/
          <run-id>/
            run.json
```

`latest.json` points to the most recently completed saved model. The evaluation
JSON and Markdown report include the split cutoff, event counts, warm/cold item
coverage, all metrics, and the experiment run ID. The local tracker records
parameters, metrics, run status, and model/report artifact paths, sizes, and
SHA-256 checksums in the `online-retail-recommendations` experiment. It needs
no tracking server or additional dependency.

To list local experiment runs and their P@10, R@10, and NDCG@10:

```powershell
python -m recommendation_model.tracking
```

The Prefect flow runs this module after Gold and feature-store registration.
To rerun only model training and subsequent stages:

```powershell
python -m orchestration.run --from-stage model_training
```
