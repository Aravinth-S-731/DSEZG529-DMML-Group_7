import csv
import gzip
import json
import time
from pathlib import Path

import streamlit as st

from gold.common import sha256_file
from gold.features import PRODUCT_FEATURE_COLUMNS
from recommendation_model.inference import Recommendation, load_model, recommend


_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_MODEL_ROOT = _PROJECT_ROOT / "data" / "models" / "item_item_cosine"
_LATEST_PATH = _MODEL_ROOT / "latest.json"
_GOLD_ROOT = _PROJECT_ROOT / "data" / "gold"


def _resolve_run_artifact(model_root: Path, relative_path: object, key: str) -> Path:
    if not isinstance(relative_path, str) or not relative_path:
        raise ValueError(f"Latest-model pointer has no valid {key}")
    relative = Path(relative_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError(f"Latest-model pointer contains an unsafe {key}")
    resolved_root = model_root.resolve()
    resolved_path = (resolved_root / relative).resolve()
    try:
        resolved_path.relative_to(resolved_root)
    except ValueError as error:
        raise ValueError(
            f"Latest-model pointer escapes the model directory: {key}"
        ) from error
    if not resolved_path.is_file():
        raise FileNotFoundError(f"Model {key} not found: {resolved_path}")
    return resolved_path


def _read_latest_artifacts() -> tuple[str, Path, Path]:
    if not _LATEST_PATH.is_file():
        raise FileNotFoundError(
            f"No saved recommendation model found at {_LATEST_PATH}. "
            "Run model training through Prefect first."
        )
    pointer = json.loads(_LATEST_PATH.read_text(encoding="utf-8"))
    if (
        not isinstance(pointer, dict)
        or pointer.get("schema_version") != "1"
        or not isinstance(pointer.get("run_id"), str)
        or not pointer["run_id"].isalnum()
    ):
        raise ValueError(f"Invalid latest-model pointer: {_LATEST_PATH}")
    model_path = _resolve_run_artifact(
        _MODEL_ROOT,
        pointer.get("model_path"),
        "model",
    )
    evaluation_path = _resolve_run_artifact(
        _MODEL_ROOT,
        pointer.get("evaluation_path"),
        "evaluation report",
    )
    return pointer["run_id"], model_path, evaluation_path


@st.cache_data(show_spinner="Loading product names...")
def _load_product_descriptions(silver_manifest_sha256: str) -> dict[str, str]:
    manifest_path = _GOLD_ROOT / "gold_manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(
            f"Gold product catalog manifest not found: {manifest_path}"
        )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict) or manifest.get("schema_version") != "1":
        raise ValueError(f"Invalid Gold product catalog manifest: {manifest_path}")
    inputs = manifest.get("inputs")
    if (
        not isinstance(inputs, dict)
        or not isinstance(inputs.get("online_retail"), dict)
        or inputs["online_retail"].get("silver_manifest_sha256")
        != silver_manifest_sha256
    ):
        raise ValueError(
            "Product names were built from different transaction data than the "
            "saved recommendation model. Rerun Gold and model training."
        )

    entries = manifest.get("files")
    entry = (
        next(
            (
                item
                for item in entries
                if isinstance(item, dict)
                and item.get("file") == "product_features.csv.gz"
            ),
            None,
        )
        if isinstance(entries, list)
        else None
    )
    if (
        not isinstance(entry, dict)
        or not isinstance(entry.get("sha256"), str)
        or isinstance(entry.get("row_count"), bool)
        or not isinstance(entry.get("row_count"), int)
        or entry["row_count"] < 0
    ):
        raise ValueError("Gold manifest has invalid product feature metadata")

    product_path = _GOLD_ROOT / "product_features.csv.gz"
    if not product_path.is_file():
        raise FileNotFoundError(f"Gold product features not found: {product_path}")
    actual_sha256 = sha256_file(product_path)
    if actual_sha256 != entry["sha256"]:
        raise ValueError(
            "Gold product feature checksum mismatch: "
            f"expected {entry['sha256']}, received {actual_sha256}"
        )

    descriptions: dict[str, str] = {}
    with gzip.open(product_path, "rt", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames != list(PRODUCT_FEATURE_COLUMNS):
            raise ValueError(
                "Gold product feature schema mismatch: "
                f"expected {list(PRODUCT_FEATURE_COLUMNS)}, received {reader.fieldnames}"
            )
        for row in reader:
            stock_code = (row.get("stock_code") or "").strip().upper()
            if not stock_code or stock_code in descriptions:
                raise ValueError("Gold product features contain an invalid product code")
            descriptions[stock_code] = " ".join(
                (row.get("description") or "").split()
            )
    if len(descriptions) != entry["row_count"]:
        raise ValueError(
            "Gold product feature row-count mismatch: "
            f"expected {entry['row_count']}, received {len(descriptions)}"
        )
    return descriptions


def _format_product(stock_code: str, descriptions: dict[str, str]) -> str:
    description = descriptions.get(stock_code, "") or "Product name unavailable"
    return f"{description} (item code {stock_code})"


def _recommendation_rows(
    recommendations: list[Recommendation],
    descriptions: dict[str, str],
) -> list[dict[str, str | int]]:
    return [
        {
            "Rank": item["rank"],
            "Product": _format_product(item["stock_code"], descriptions),
            "Why we suggest it": (
                "Related to products in this customer's history"
                if item["score"] > 0
                else "Popular with other training customers"
            ),
            "Training customers who bought it": item["training_customer_count"],
        }
        for item in recommendations
    ]


@st.cache_resource(show_spinner="Loading the saved recommendation model...")
def _load_model_bundle(
    run_id: str,
    model_path: str,
    evaluation_path: str,
) -> tuple[dict, dict]:
    model = load_model(Path(model_path))
    evaluation = json.loads(Path(evaluation_path).read_text(encoding="utf-8"))
    if (
        evaluation.get("run_id") != run_id
        or evaluation.get("source", {}).get("silver_manifest_sha256")
        != model.get("silver_manifest_sha256")
        or not isinstance(evaluation.get("metrics"), dict)
    ):
        raise ValueError("Saved model and evaluation report do not match")
    return model, evaluation


def main() -> None:
    st.set_page_config(
        page_title="RecoMart Product Recommendations",
        layout="wide",
    )
    st.title("Find products you may like")
    st.caption(
        "Choose a customer profile to see product ideas based on past shopping "
        "patterns. This demo uses historical purchases, not live browsing data."
    )

    refresh_column, status_column = st.columns([1, 4])
    with refresh_column:
        if st.button("Check for latest model"):
            _load_model_bundle.clear()
            st.rerun()

    try:
        run_id, model_path, evaluation_path = _read_latest_artifacts()
        model, evaluation = _load_model_bundle(
            run_id,
            str(model_path),
            str(evaluation_path),
        )
        descriptions = _load_product_descriptions(model["silver_manifest_sha256"])
    except (OSError, ValueError, json.JSONDecodeError) as error:
        st.error(f"Could not load the saved recommendation model: {error}")
        st.info("Run `python -m orchestration.run --from-stage model_training` first.")
        return

    metrics = evaluation["metrics"]["item_item_cosine"]
    baseline = evaluation["metrics"]["popularity_baseline"]
    with status_column:
        st.success("Recommendations are ready")

    with st.sidebar:
        st.header("About this recommender")
        st.write(
            "It finds products often bought alongside products in a customer's history."
        )
        st.caption(
            "The model is trained in the pipeline. It does not learn from clicks "
            "inside this demo."
        )
        with st.expander("Technical model details"):
            st.write(f"**Method:** Item-to-item collaborative filtering")
            st.write(f"**Training data through:** {model['training_through_inclusive']}")
            st.write(f"**Training customers:** {model['training_user_count']:,}")
            st.write(f"**Training products:** {model['training_item_count']:,}")
            st.write(f"**Model run:** `{run_id}`")
            st.caption(
                'Use "Check for latest model" after a successful Prefect training run.'
            )

    st.divider()
    st.subheader("Choose a customer")
    st.write(
        "Customer names are not available in this dataset, so profiles are shown "
        "by customer number and the number of different products they have bought."
    )
    customer_ids = sorted(model["user_histories"])
    preferred_customer = "14911"
    selected_index = (
        customer_ids.index(preferred_customer)
        if preferred_customer in customer_ids
        else 0
    )
    customer_id = st.selectbox(
        "Customer profile",
        customer_ids,
        index=selected_index,
        format_func=lambda value: (
            f"Customer {value} - "
            f"{len(model['user_histories'][value]):,} products in purchase history"
        ),
    )
    recent_products = st.multiselect(
        "Optional: add products from this session",
        options=sorted(model["item_popularity"]),
        format_func=lambda code: _format_product(code, descriptions),
        help=(
            "Search by product name and select any products the customer recently "
            "viewed or bought. These affect this request only."
        ),
    )
    st.caption(
        "Suggestions use the selected customer's past purchases and any optional "
        "session products. Previously purchased items are left out."
    )
    top_k = st.slider(
        "Number of recommendations",
        min_value=1,
        max_value=20,
        value=10,
    )

    if st.button("Get recommendations", type="primary", key="get-recommendations"):
        history = set(model["user_histories"][customer_id])
        history.update(recent_products)
        started = time.perf_counter()
        recommendations = recommend(
            model,
            history=sorted(history),
            top_k=top_k,
        )
        elapsed_ms = (time.perf_counter() - started) * 1000
        st.subheader("Your product suggestions")
        st.caption(
            "A suggestion is related to items in the purchase history, or popular "
            "with other training customers when no related-item signal is available. "
            "It is not a guarantee or a probability."
        )
        st.metric("Time to prepare these suggestions", f"{elapsed_ms:.2f} ms")
        if recommendations:
            st.dataframe(
                _recommendation_rows(recommendations, descriptions),
                hide_index=True,
                use_container_width=True,
            )
        else:
            st.info("No unseen products are available for this customer.")

    st.divider()
    with st.expander("How was the recommender checked?"):
        st.write(
            "The model was tested on a later period of historical purchases that "
            "was held back during evaluation. This checks whether its suggestions "
            "match products customers bought later; it is not a live user survey."
        )
        metric_columns = st.columns(3)
        metric_columns[0].metric(
            "Precision among top 10",
            f"{metrics['precision_at_10']:.2%}",
            help="Average share of the 10 suggestions that appeared in later purchases.",
        )
        metric_columns[1].metric(
            "Later purchases found",
            f"{metrics['recall_at_10']:.2%}",
            help=(
                "Average share of eligible new products bought later that were "
                "included in the 10 suggestions."
            ),
        )
        metric_columns[2].metric(
            "Ranking quality (NDCG)",
            f"{metrics['ndcg_at_10']:.4f}",
            help="A 0-to-1 score that rewards relevant products appearing nearer the top.",
        )
        st.dataframe(
            [
                {
                    "Tested method": "Product suggestions",
                    "Precision (top 10)": f"{metrics['precision_at_10']:.2%}",
                    "Later purchases found": f"{metrics['recall_at_10']:.2%}",
                    "Ranking quality": f"{metrics['ndcg_at_10']:.4f}",
                },
                {
                    "Tested method": "Popular products baseline",
                    "Precision (top 10)": f"{baseline['precision_at_10']:.2%}",
                    "Later purchases found": f"{baseline['recall_at_10']:.2%}",
                    "Ranking quality": f"{baseline['ndcg_at_10']:.4f}",
                },
            ],
            hide_index=True,
            use_container_width=True,
        )
        st.caption(
            f"Evaluation used {evaluation['evaluation']['evaluation_users']:,} "
            "customers and the top 10 suggestions for each."
        )


if __name__ == "__main__":
    main()
