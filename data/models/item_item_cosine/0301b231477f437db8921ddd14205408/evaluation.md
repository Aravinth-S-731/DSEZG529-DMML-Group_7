# Item-Item Recommendation Model Evaluation

- **Experiment run ID:** `0301b231477f437db8921ddd14205408`
- **Silver Online Retail manifest SHA-256:** `910c5f5afcf31d245efe778dea30c763baa00531cf0dcc929eb27cb104a7dd26`
- **Algorithm:** binary-item-item-cosine
- **Training events:** 314,209
- **Held-out events:** 78,523
- **Temporal cutoff (included in training):** `2011-11-01T15:55:00`
- **Effective training fraction:** 80.01%
- **Evaluation users:** 1,428
- **Warm-item coverage of new-to-user test pairs:** 98.01%

Metrics are macro-averaged over users with at least one future, previously unseen product that appeared in the training catalog. Precision uses K as its denominator; relevance is binary.

| Model | P@5 | R@5 | NDCG@5 | P@10 | R@10 | NDCG@10 | P@20 | R@20 | NDCG@20 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Item-item cosine | 0.0605 | 0.0237 | 0.0668 | 0.0518 | 0.0384 | 0.0636 | 0.0448 | 0.0618 | 0.0658 |
| Popularity baseline | 0.0357 | 0.0117 | 0.0376 | 0.0342 | 0.0200 | 0.0378 | 0.0342 | 0.0381 | 0.0427 |

The item-item model uses binary customer-product histories and cosine similarity from co-purchasing customers. The popularity baseline uses training customer counts and excludes products already in each customer's history.

The saved inference model is refit on all eligible Silver events after temporal evaluation. The reported metrics use only the pre-cutoff training partition and the future holdout.
