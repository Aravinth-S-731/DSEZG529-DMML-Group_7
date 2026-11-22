import argparse
import gzip
import json
import os
from pathlib import Path
import tempfile
from typing import TypedDict


_PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MODEL_PATH = (
    _PROJECT_ROOT / "data" / "models" / "item_item_cosine" / "latest.json"
)


class Recommendation(TypedDict):
    rank: int
    stock_code: str
    score: float
    training_customer_count: int


def save_model(model: dict, model_path: Path) -> None:
    model_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=model_path.parent,
            prefix=f".{model_path.stem}-",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
        with gzip.open(temporary_path, "wt", encoding="utf-8", newline="\n") as output:
            json.dump(model, output, separators=(",", ":"), sort_keys=True)
            output.write("\n")
        os.replace(temporary_path, model_path)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def load_model(model_path: Path = DEFAULT_MODEL_PATH) -> dict:
    if model_path.name == "latest.json":
        pointer = json.loads(model_path.read_text(encoding="utf-8"))
        relative_model_path = pointer.get("model_path")
        if (
            pointer.get("schema_version") != "1"
            or not isinstance(relative_model_path, str)
            or Path(relative_model_path).is_absolute()
            or ".." in Path(relative_model_path).parts
        ):
            raise ValueError(f"Invalid latest-model pointer: {model_path}")
        model_path = model_path.parent / relative_model_path
    if not model_path.is_file():
        raise FileNotFoundError(f"Recommendation model not found: {model_path}")
    with gzip.open(model_path, "rt", encoding="utf-8") as source:
        model = json.load(source)
    if (
        not isinstance(model, dict)
        or model.get("schema_version") != "1"
        or model.get("algorithm") != "binary-item-item-cosine"
        or not isinstance(model.get("item_neighbors"), dict)
        or not isinstance(model.get("item_popularity"), dict)
        or not isinstance(model.get("popularity_order"), list)
        or not isinstance(model.get("user_histories"), dict)
    ):
        raise ValueError(f"Invalid recommendation model artifact: {model_path}")
    return model


def recommend(
    model: dict,
    *,
    customer_id: str | None = None,
    history: list[str] | None = None,
    top_k: int = 10,
) -> list[Recommendation]:
    if isinstance(top_k, bool) or not isinstance(top_k, int) or top_k <= 0:
        raise ValueError("top_k must be a positive integer")
    if history is not None and customer_id is not None:
        raise ValueError("Pass either customer_id or history, not both")
    if history is None:
        history = model["user_histories"].get(customer_id or "", [])
    seen_items = {item.strip().upper() for item in history if item.strip()}
    scores: dict[str, float] = {}
    for seed_item in seen_items:
        for neighbor_code, similarity in model["item_neighbors"].get(seed_item, []):
            if neighbor_code not in seen_items:
                scores[neighbor_code] = scores.get(neighbor_code, 0.0) + float(
                    similarity
                )
    popularity = model["item_popularity"]
    candidates = [
        item
        for item in popularity
        if item not in seen_items
    ]
    ranked = sorted(
        candidates,
        key=lambda item: (
            -scores.get(item, 0.0),
            -int(popularity[item]),
            item,
        ),
    )[:top_k]
    return [
        {
            "rank": rank,
            "stock_code": item,
            "score": round(scores.get(item, 0.0), 10),
            "training_customer_count": int(popularity[item]),
        }
        for rank, item in enumerate(ranked, start=1)
    ]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate recommendations from the latest saved model."
    )
    parser.add_argument("--model-path", type=Path, default=DEFAULT_MODEL_PATH)
    parser.add_argument(
        "--customer-id",
        help="Recommend for a customer in the saved training history.",
    )
    parser.add_argument("--top-k", type=int, default=10)
    arguments = parser.parse_args()
    if arguments.top_k <= 0:
        parser.error("--top-k must be positive")

    model = load_model(arguments.model_path)
    recommendations = recommend(
        model,
        customer_id=arguments.customer_id,
        top_k=arguments.top_k,
    )
    print(json.dumps(recommendations, indent=2))


if __name__ == "__main__":
    main()
