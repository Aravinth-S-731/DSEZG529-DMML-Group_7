# Item-Item Recommendation Model Evaluation

- **Experiment run ID:** `c4b941b2c6ef4827b1c28762563874fc`
- **Silver Online Retail manifest SHA-256:** `50eca2be14943e55e34484e7630ec39f0855da136e57575e1ff4069cba52b54d`
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
