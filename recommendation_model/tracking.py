import argparse
import json
import os
import tempfile
from pathlib import Path


_PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TRACKING_ROOT = _PROJECT_ROOT / "data" / "model_tracking"


def _write_json_atomic(path: Path, payload: dict) -> None:
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
            json.dump(payload, output, indent=2)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_path, path)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def record_run(tracking_root: Path, run_record: dict) -> Path:
    experiment_name = run_record.get("experiment_name")
    run_id = run_record.get("run_id")
    if (
        not isinstance(experiment_name, str)
        or not experiment_name
        or Path(experiment_name).name != experiment_name
        or not isinstance(run_id, str)
        or not run_id.isalnum()
    ):
        raise ValueError("Experiment name or run ID is invalid")
    record_path = (
        tracking_root
        / "experiments"
        / experiment_name
        / "runs"
        / run_id
        / "run.json"
    )
    if record_path.exists():
        raise FileExistsError(f"Experiment run already exists: {record_path}")
    _write_json_atomic(record_path, run_record)
    return record_path


def read_runs(
    tracking_root: Path = DEFAULT_TRACKING_ROOT,
    experiment_name: str = "online-retail-recommendations",
) -> list[dict]:
    experiment_dir = tracking_root / "experiments" / experiment_name
    if not experiment_dir.is_dir():
        return []
    runs = []
    for record_path in experiment_dir.glob("runs/*/run.json"):
        record = json.loads(record_path.read_text(encoding="utf-8"))
        if (
            not isinstance(record, dict)
            or record.get("schema_version") != "1"
            or record.get("experiment_name") != experiment_name
            or not isinstance(record.get("run_id"), str)
            or record_path.parent.name != record.get("run_id")
        ):
            raise ValueError(f"Invalid experiment run record: {record_path}")
        runs.append(record)
    return sorted(
        runs,
        key=lambda record: record["started_at_utc"],
        reverse=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="List tracked local recommendation experiment runs."
    )
    parser.add_argument("--tracking-root", type=Path, default=DEFAULT_TRACKING_ROOT)
    parser.add_argument(
        "--experiment-name",
        default="online-retail-recommendations",
    )
    arguments = parser.parse_args()
    runs = read_runs(arguments.tracking_root, arguments.experiment_name)
    if not runs:
        print(f"No runs found for experiment {arguments.experiment_name!r}.")
        return
    for run in runs:
        metrics = run.get("metrics", {})
        print(
            f"{run['started_at_utc']}  {run['run_id']}  "
            f"P@10={metrics.get('item_item_cosine_precision_at_10', float('nan')):.4f}  "
            f"R@10={metrics.get('item_item_cosine_recall_at_10', float('nan')):.4f}  "
            f"NDCG@10={metrics.get('item_item_cosine_ndcg_at_10', float('nan')):.4f}"
        )


if __name__ == "__main__":
    main()
