import argparse
import heapq
import json
import logging
import math
import os
import tempfile
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterator

from gold.common import (
    TRANSACTION_COLUMNS,
    iter_silver_rows,
    read_silver_manifest,
    sha256_file,
)
from recommendation_model.inference import recommend, save_model
from recommendation_model.tracking import record_run


_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_SILVER_ROOT = _PROJECT_ROOT / "data" / "silver"
_DEFAULT_MODEL_ROOT = _PROJECT_ROOT / "data" / "models" / "item_item_cosine"
_DEFAULT_TRACKING_ROOT = _PROJECT_ROOT / "data" / "model_tracking"
_EXPERIMENT_NAME = "online-retail-recommendations"
_TRAIN_FRACTION = 0.8
_MAX_NEIGHBORS = 100
_EVALUATION_K = (5, 10, 20)
_ALGORITHM = "binary-item-item-cosine"


def _iter_recommendation_events(
    silver_root: Path,
    manifest: dict,
) -> Iterator[tuple[datetime, str, str]]:
    for row in iter_silver_rows(
        silver_root,
        "online_retail",
        manifest,
        TRANSACTION_COLUMNS,
    ):
        if (row.get("is_recommendation_eligible") or "").strip().lower() != "true":
            continue
        if (row.get("is_duplicate_content") or "").strip().lower() == "true":
            continue

        customer_id = (row.get("customer_id") or "").strip()
        stock_code = (row.get("stock_code") or "").strip().upper()
        try:
            purchased_at = datetime.fromisoformat(
                (row.get("invoice_datetime") or "").strip()
            )
            quantity = int(row["quantity"])
            line_total = Decimal(row["line_total"])
        except (KeyError, ValueError, InvalidOperation) as error:
            raise ValueError(f"Invalid eligible Silver transaction: {error}") from error
        if not customer_id or not stock_code:
            raise ValueError(
                "Recommendation-eligible Silver rows require customer and product IDs"
            )
        if quantity <= 0 or not line_total.is_finite() or line_total < 0:
            raise ValueError(
                "Recommendation-eligible rows must have positive quantity and "
                "nonnegative line total"
            )
        yield purchased_at, customer_id, stock_code


def _find_temporal_cutoff(
    silver_root: Path,
    manifest: dict,
    train_fraction: float,
) -> tuple[datetime, int, int, datetime]:
    if not 0 < train_fraction < 1:
        raise ValueError("Training fraction must be strictly between zero and one")
    timestamp_counts: Counter[datetime] = Counter()
    for purchased_at, _, _ in _iter_recommendation_events(silver_root, manifest):
        timestamp_counts[purchased_at] += 1

    total_events = sum(timestamp_counts.values())
    if len(timestamp_counts) < 2:
        raise ValueError("A temporal evaluation requires at least two timestamps")
    target_training_events = math.ceil(total_events * train_fraction)
    cumulative_events = 0
    cutoff = None
    for purchased_at in sorted(timestamp_counts):
        cumulative_events += timestamp_counts[purchased_at]
        if cumulative_events >= target_training_events:
            cutoff = purchased_at
            break
    if cutoff is None or cutoff == max(timestamp_counts):
        raise ValueError("Temporal split leaves no future events for evaluation")
    return cutoff, total_events, cumulative_events, max(timestamp_counts)


def _split_user_items(
    silver_root: Path,
    manifest: dict,
    cutoff: datetime,
) -> tuple[dict[str, set[str]], dict[str, set[str]], int, int]:
    train_user_items: dict[str, set[str]] = defaultdict(set)
    test_user_items: dict[str, set[str]] = defaultdict(set)
    training_events = 0
    test_events = 0
    for purchased_at, customer_id, stock_code in _iter_recommendation_events(
        silver_root,
        manifest,
    ):
        if purchased_at <= cutoff:
            train_user_items[customer_id].add(stock_code)
            training_events += 1
        else:
            test_user_items[customer_id].add(stock_code)
            test_events += 1
    if not training_events or not test_events:
        raise ValueError("Temporal split must contain both training and test events")
    return train_user_items, test_user_items, training_events, test_events


def _offer_neighbor(
    heap: list[tuple[float, int, str]],
    similarity: float,
    neighbor_index: int,
    item_codes: list[str],
    max_neighbors: int,
) -> None:
    entry = (similarity, -neighbor_index, item_codes[neighbor_index])
    if len(heap) < max_neighbors:
        heapq.heappush(heap, entry)
    elif entry > heap[0]:
        heapq.heapreplace(heap, entry)


def _fit_item_item_model(
    user_items: dict[str, set[str]],
    *,
    max_neighbors: int,
    training_event_count: int,
    training_through: str,
    silver_manifest_sha256: str,
) -> dict:
    if (
        isinstance(max_neighbors, bool)
        or not isinstance(max_neighbors, int)
        or max_neighbors <= 0
    ):
        raise ValueError("Maximum neighbors per item must be positive")
    normalized_user_items = {
        customer_id: sorted(items)
        for customer_id, items in user_items.items()
        if items
    }
    item_users: dict[str, list[str]] = defaultdict(list)
    for customer_id, items in normalized_user_items.items():
        for stock_code in items:
            item_users[stock_code].append(customer_id)

    item_codes = sorted(item_users)
    item_indices = {stock_code: index for index, stock_code in enumerate(item_codes)}
    neighbor_heaps: list[list[tuple[float, int, str]]] = [
        [] for _ in item_codes
    ]

    for item_index, stock_code in enumerate(item_codes):
        cooccurrence_counts: Counter[int] = Counter()
        for customer_id in item_users[stock_code]:
            for other_stock_code in normalized_user_items[customer_id]:
                other_index = item_indices[other_stock_code]
                if other_index > item_index:
                    cooccurrence_counts[other_index] += 1

        item_customer_count = len(item_users[stock_code])
        for other_index, shared_customer_count in cooccurrence_counts.items():
            other_stock_code = item_codes[other_index]
            similarity = shared_customer_count / math.sqrt(
                item_customer_count * len(item_users[other_stock_code])
            )
            _offer_neighbor(
                neighbor_heaps[item_index],
                similarity,
                other_index,
                item_codes,
                max_neighbors,
            )
            _offer_neighbor(
                neighbor_heaps[other_index],
                similarity,
                item_index,
                item_codes,
                max_neighbors,
            )

    item_neighbors = {}
    for item_index, stock_code in enumerate(item_codes):
        ranked_neighbors = sorted(
            neighbor_heaps[item_index],
            key=lambda entry: (-entry[0], -entry[1]),
        )
        item_neighbors[stock_code] = [
            [neighbor_code, round(similarity, 10)]
            for similarity, _, neighbor_code in ranked_neighbors
        ]

    item_popularity = {
        stock_code: len(item_users[stock_code])
        for stock_code in item_codes
    }
    popularity_order = sorted(
        item_codes,
        key=lambda item: (-item_popularity[item], item),
    )
    return {
        "schema_version": "1",
        "algorithm": _ALGORITHM,
        "similarity": "cosine similarity over binary customer-item interactions",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "training_through_inclusive": training_through,
        "training_event_count": training_event_count,
        "training_user_count": len(normalized_user_items),
        "training_item_count": len(item_codes),
        "max_neighbors_per_item": max_neighbors,
        "silver_manifest_sha256": silver_manifest_sha256,
        "item_neighbors": item_neighbors,
        "item_popularity": item_popularity,
        "popularity_order": popularity_order,
        "user_histories": normalized_user_items,
    }


def _recommend_item_codes(
    model: dict,
    user_history: set[str] | list[str],
    top_k: int,
) -> list[str]:
    return [
        item["stock_code"]
        for item in recommend(model, history=list(user_history), top_k=top_k)
    ]


def _popularity_recommendations(
    model: dict,
    user_history: set[str] | list[str],
    top_k: int,
) -> list[str]:
    if isinstance(top_k, bool) or not isinstance(top_k, int) or top_k <= 0:
        raise ValueError("top_k must be a positive integer")
    seen_items = {item.strip().upper() for item in user_history if item.strip()}
    return [
        item
        for item in model["popularity_order"]
        if item not in seen_items
    ][:top_k]


def _evaluation_targets(
    train_user_items: dict[str, set[str]],
    test_user_items: dict[str, set[str]],
) -> tuple[dict[str, set[str]], dict[str, int]]:
    training_catalog = {
        item
        for items in train_user_items.values()
        for item in items
    }
    targets: dict[str, set[str]] = {}
    test_pair_count = sum(len(items) for items in test_user_items.values())
    known_user_test_pairs = 0
    previously_seen_test_pairs = 0
    new_to_user_pairs = 0
    warm_item_pairs = 0
    cold_item_pairs = 0
    cold_start_user_pairs = 0
    users_without_history = 0

    for customer_id, heldout_items in test_user_items.items():
        history = train_user_items.get(customer_id, set())
        if not history:
            users_without_history += 1
            cold_start_user_pairs += len(heldout_items)
            continue
        known_user_test_pairs += len(heldout_items)
        previously_seen_test_pairs += len(heldout_items & history)
        new_items = heldout_items - history
        new_to_user_pairs += len(new_items)
        warm_items = new_items & training_catalog
        cold_item_pairs += len(new_items - training_catalog)
        warm_item_pairs += len(warm_items)
        if warm_items:
            targets[customer_id] = warm_items

    return targets, {
        "test_users": len(test_user_items),
        "evaluation_users": len(targets),
        "users_without_training_history": users_without_history,
        "heldout_user_product_pairs": test_pair_count,
        "heldout_pairs_for_known_users": known_user_test_pairs,
        "heldout_pairs_without_training_history": cold_start_user_pairs,
        "previously_seen_user_product_pairs_excluded": previously_seen_test_pairs,
        "new_to_user_test_pairs": new_to_user_pairs,
        "warm_item_test_pairs": warm_item_pairs,
        "cold_item_test_pairs": cold_item_pairs,
    }


def _evaluate_ranker(
    model: dict,
    targets: dict[str, set[str]],
    k_values: tuple[int, ...],
    *,
    popularity_baseline: bool,
) -> dict[str, float | int]:
    if not targets:
        raise ValueError("Temporal evaluation has no warm user-item test targets")
    maximum_k = max(k_values)
    metric_totals: Counter[str] = Counter()
    for customer_id, relevant_items in targets.items():
        history = set(model["user_histories"].get(customer_id, []))
        recommend = (
            _popularity_recommendations
            if popularity_baseline
            else _recommend_item_codes
        )
        ranked_items = recommend(model, history, maximum_k)
        for k in k_values:
            top_items = ranked_items[:k]
            hits = sum(item in relevant_items for item in top_items)
            metric_totals[f"precision_at_{k}"] += hits / k
            metric_totals[f"recall_at_{k}"] += hits / len(relevant_items)
            dcg = sum(
                1 / math.log2(rank + 1)
                for rank, item in enumerate(top_items, start=1)
                if item in relevant_items
            )
            ideal_hits = min(k, len(relevant_items))
            ideal_dcg = sum(
                1 / math.log2(rank + 1)
                for rank in range(1, ideal_hits + 1)
            )
            metric_totals[f"ndcg_at_{k}"] += dcg / ideal_dcg

    evaluated_users = len(targets)
    result: dict[str, float | int] = {
        name: round(value / evaluated_users, 8)
        for name, value in metric_totals.items()
    }
    result["evaluated_users"] = evaluated_users
    return result


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            dir=path.parent,
            prefix=f".{path.stem}-",
            suffix=".tmp",
            delete=False,
            encoding="utf-8",
            newline="\n",
        ) as output:
            temporary_path = Path(output.name)
            output.write(text)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_path, path)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _write_json_atomic(path: Path, payload: dict) -> None:
    _write_text_atomic(path, json.dumps(payload, indent=2) + "\n")


def _build_markdown_report(report: dict) -> str:
    lines = [
        "# Item-Item Recommendation Model Evaluation",
        "",
        f"- **Experiment run ID:** `{report['run_id']}`",
        f"- **Silver Online Retail manifest SHA-256:** "
        f"`{report['source']['silver_manifest_sha256']}`",
        f"- **Algorithm:** {report['model']['algorithm']}",
        f"- **Training events:** {report['split']['training_events']:,}",
        f"- **Held-out events:** {report['split']['test_events']:,}",
        f"- **Temporal cutoff (included in training):** "
        f"`{report['split']['cutoff_timestamp_inclusive']}`",
        f"- **Effective training fraction:** "
        f"{report['split']['effective_training_fraction']:.2%}",
        f"- **Evaluation users:** {report['evaluation']['evaluation_users']:,}",
        f"- **Warm-item coverage of new-to-user test pairs:** "
        f"{report['evaluation']['warm_item_pair_coverage_percent']:.2f}%",
        "",
        "Metrics are macro-averaged over users with at least one future, "
        "previously unseen product that appeared in the training catalog. "
        "Precision uses K as its denominator; relevance is binary.",
        "",
        "| Model | P@5 | R@5 | NDCG@5 | P@10 | R@10 | NDCG@10 | P@20 | R@20 | NDCG@20 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for label, key in (
        ("Item-item cosine", "item_item_cosine"),
        ("Popularity baseline", "popularity_baseline"),
    ):
        metrics = report["metrics"][key]
        values = [
            f"{metrics[f'{name}_at_{k}']:.4f}"
            for k in (5, 10, 20)
            for name in ("precision", "recall", "ndcg")
        ]
        lines.append(f"| {label} | " + " | ".join(values) + " |")
    lines.extend(
        [
            "",
            "The item-item model uses binary customer-product histories and "
            "cosine similarity from co-purchasing customers. The popularity "
            "baseline uses training customer counts and excludes products "
            "already in each customer's history.",
            "",
            "The saved inference model is refit on all eligible Silver events "
            "after temporal evaluation. The reported metrics use only the "
            "pre-cutoff training partition and the future holdout.",
            "",
        ]
    )
    return "\n".join(lines)


def train_and_evaluate(
    silver_root: Path,
    model_root: Path,
    tracking_root: Path,
    *,
    train_fraction: float = _TRAIN_FRACTION,
    max_neighbors: int = _MAX_NEIGHBORS,
    k_values: tuple[int, ...] = _EVALUATION_K,
) -> dict[str, str]:
    if not k_values or any(
        isinstance(k, bool) or not isinstance(k, int) or k <= 0 for k in k_values
    ):
        raise ValueError("Evaluation K values must be positive integers")
    if tuple(sorted(set(k_values))) != k_values:
        raise ValueError("Evaluation K values must be unique and ascending")
    if (
        isinstance(max_neighbors, bool)
        or not isinstance(max_neighbors, int)
        or max_neighbors <= 0
    ):
        raise ValueError("Maximum neighbors per item must be positive")

    manifest, silver_manifest_sha256 = read_silver_manifest(
        silver_root,
        "online_retail",
    )
    cutoff, total_events, _, latest_timestamp = _find_temporal_cutoff(
        silver_root,
        manifest,
        train_fraction,
    )
    train_user_items, test_user_items, training_events, test_events = (
        _split_user_items(silver_root, manifest, cutoff)
    )
    if training_events + test_events != total_events:
        raise ValueError(
            "Silver event count changed between temporal split passes; "
            "check the input manifest and files"
        )
    if len(train_user_items) < 2:
        raise ValueError("Item-item training requires at least two customers")

    started_at = datetime.now(timezone.utc).isoformat()
    targets, evaluation_counts = _evaluation_targets(
        train_user_items,
        test_user_items,
    )
    if not targets:
        raise ValueError("Temporal split has no evaluable warm-item targets")
    training_through = cutoff.isoformat()
    evaluation_model = _fit_item_item_model(
        train_user_items,
        max_neighbors=max_neighbors,
        training_event_count=training_events,
        training_through=training_through,
        silver_manifest_sha256=silver_manifest_sha256,
    )
    item_item_metrics = _evaluate_ranker(
        evaluation_model,
        targets,
        k_values,
        popularity_baseline=False,
    )
    popularity_metrics = _evaluate_ranker(
        evaluation_model,
        targets,
        k_values,
        popularity_baseline=True,
    )

    train_user_count = sum(bool(items) for items in train_user_items.values())
    new_to_user_pairs = evaluation_counts["new_to_user_test_pairs"]
    warm_item_coverage = (
        evaluation_counts["warm_item_test_pairs"] / new_to_user_pairs * 100
        if new_to_user_pairs
        else 0.0
    )
    report_without_run_id = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": {
            "silver_dataset": "online_retail",
            "silver_manifest_sha256": silver_manifest_sha256,
        },
        "model": {
            "algorithm": _ALGORITHM,
            "similarity": "cosine similarity over binary customer-item interactions",
            "max_neighbors_per_item": max_neighbors,
            "evaluation_k_values": list(k_values),
        },
        "split": {
            "method": (
                "global chronological holdout; all events at or before the "
                "cutoff timestamp are training, later events are test"
            ),
            "target_training_fraction": train_fraction,
            "effective_training_fraction": training_events / total_events,
            "cutoff_timestamp_inclusive": training_through,
            "training_events": training_events,
            "test_events": test_events,
            "total_events": total_events,
            "training_users": train_user_count,
            "training_items": len(evaluation_model["item_popularity"]),
        },
        "evaluation": {
            **evaluation_counts,
            "warm_item_pair_coverage_percent": round(warm_item_coverage, 6),
            "recommendation_policy": (
                "Evaluate previously unseen user-item pairs only when the item "
                "was present in the training catalog; report cold-start pairs "
                "separately."
            ),
            "precision_denominator": "K",
            "averaging": "macro average over evaluation users",
        },
        "metrics": {
            "item_item_cosine": item_item_metrics,
            "popularity_baseline": popularity_metrics,
        },
    }

    all_user_items: dict[str, set[str]] = defaultdict(set)
    for customer_id, items in train_user_items.items():
        all_user_items[customer_id].update(items)
    for customer_id, items in test_user_items.items():
        all_user_items[customer_id].update(items)
    all_events_model = _fit_item_item_model(
        all_user_items,
        max_neighbors=max_neighbors,
        training_event_count=total_events,
        training_through=latest_timestamp.isoformat(),
        silver_manifest_sha256=silver_manifest_sha256,
    )

    model_root.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex
    run_dir = model_root / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    model_path = run_dir / "model.json.gz"
    evaluation_path = run_dir / "evaluation.json"
    report_path = run_dir / "evaluation.md"
    save_model(all_events_model, model_path)

    report = {**report_without_run_id, "run_id": run_id}
    _write_json_atomic(evaluation_path, report)
    _write_text_atomic(report_path, _build_markdown_report(report))

    metric_values = {
        f"{model_name}_{metric_name}": metric_value
        for model_name, metrics in report["metrics"].items()
        for metric_name, metric_value in metrics.items()
        if metric_name != "evaluated_users"
    }
    metric_values.update(
        {
            "evaluation_users": evaluation_counts["evaluation_users"],
            "warm_item_pair_coverage_percent": warm_item_coverage,
        }
    )
    run_record = {
        "schema_version": "1",
        "experiment_name": _EXPERIMENT_NAME,
        "run_id": run_id,
        "run_name": "item-item-cosine-temporal-evaluation",
        "status": "FINISHED",
        "started_at_utc": started_at,
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "parameters": {
            "algorithm": _ALGORITHM,
            "similarity": "binary_cosine",
            "target_training_fraction": train_fraction,
            "max_neighbors_per_item": max_neighbors,
            "evaluation_k_values": list(k_values),
            "temporal_cutoff_inclusive": training_through,
            "training_events": training_events,
            "test_events": test_events,
            "training_users": train_user_count,
            "training_items": len(evaluation_model["item_popularity"]),
            "silver_manifest_sha256": silver_manifest_sha256,
        },
        "metrics": metric_values,
        "tags": {
            "task": "recommendation",
            "evaluation": "chronological_holdout",
            "saved_model_training_scope": "all_eligible_silver_events",
        },
        "artifacts": {
            "model": {
                "path": str(model_path.resolve()),
                "sha256": sha256_file(model_path),
                "size_bytes": model_path.stat().st_size,
            },
            "evaluation_json": {
                "path": str(evaluation_path.resolve()),
                "sha256": sha256_file(evaluation_path),
                "size_bytes": evaluation_path.stat().st_size,
            },
            "evaluation_report": {
                "path": str(report_path.resolve()),
                "sha256": sha256_file(report_path),
                "size_bytes": report_path.stat().st_size,
            },
        },
    }
    tracking_record_path = record_run(tracking_root, run_record)

    latest = {
        "schema_version": "1",
        "run_id": run_id,
        "model_path": f"{run_id}/{model_path.name}",
        "evaluation_path": f"{run_id}/{evaluation_path.name}",
        "tracking_record_path": str(tracking_record_path),
        "experiment_name": _EXPERIMENT_NAME,
    }
    _write_json_atomic(model_root / "latest.json", latest)
    logging.info(
        "Recommendation model complete: run_id=%s, P@10=%.4f, R@10=%.4f, "
        "NDCG@10=%.4f; model=%s",
        run_id,
        item_item_metrics["precision_at_10"],
        item_item_metrics["recall_at_10"],
        item_item_metrics["ndcg_at_10"],
        model_path,
    )
    return {
        "run_id": run_id,
        "experiment_name": _EXPERIMENT_NAME,
        "tracking_record_path": str(tracking_record_path),
        "model_path": str(model_path),
        "evaluation_path": str(evaluation_path),
        "report_path": str(report_path),
        "latest_path": str(model_root / "latest.json"),
        "precision_at_10": str(item_item_metrics["precision_at_10"]),
        "recall_at_10": str(item_item_metrics["recall_at_10"]),
        "ndcg_at_10": str(item_item_metrics["ndcg_at_10"]),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Train an item-item recommender, evaluate it on a chronological "
            "holdout, save the full-data model, and track the experiment run."
        )
    )
    parser.add_argument("--silver-root", type=Path, default=_DEFAULT_SILVER_ROOT)
    parser.add_argument("--model-root", type=Path, default=_DEFAULT_MODEL_ROOT)
    parser.add_argument("--tracking-root", type=Path, default=_DEFAULT_TRACKING_ROOT)
    parser.add_argument("--train-fraction", type=float, default=_TRAIN_FRACTION)
    parser.add_argument("--max-neighbors", type=int, default=_MAX_NEIGHBORS)
    arguments = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    result = train_and_evaluate(
        arguments.silver_root,
        arguments.model_root,
        arguments.tracking_root,
        train_fraction=arguments.train_fraction,
        max_neighbors=arguments.max_neighbors,
    )
    logging.info(
        "Evaluation report: %s; experiment run: %s",
        result["evaluation_path"],
        result["run_id"],
    )


if __name__ == "__main__":
    main()
