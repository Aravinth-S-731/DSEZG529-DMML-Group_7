import argparse
import json
import logging
import socket
import subprocess
import sys
import time
import uuid
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Generator
from urllib.request import urlopen

from prefect import flow, get_run_logger, task

from bronze.ingest import ingest_landing_dataset
from eda.generate import generate_eda
from feature_store.store import register_gold_features
from gold.features import build_gold
from ingestion.batch_ingest import ingest_catalog_api, ingest_uci_archive
from recommendation_model.train import train_and_evaluate
from silver.ingest import run_silver


_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_DATA_ROOT = _PROJECT_ROOT / "data"
_STAGE_NAMES = (
    "uci_ingestion",
    "catalog_api_capture",
    "bronze_online_retail",
    "bronze_product_catalog",
    "silver_online_retail",
    "silver_product_catalog",
    "gold",
    "feature_store",
    "model_training",
    "eda",
)


def _available_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _wait_for_catalog_api(
    process: subprocess.Popen,
    health_url: str,
    timeout_seconds: int,
    server_log_path: Path,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: OSError | None = None

    while time.monotonic() < deadline:
        return_code = process.poll()
        if return_code is not None:
            raise RuntimeError(
                f"Catalog API exited with code {return_code}; "
                f"see {server_log_path}"
            )
        try:
            with urlopen(health_url, timeout=1) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if not isinstance(payload, dict) or payload.get("status") != "ok":
                raise RuntimeError(f"Catalog API health check returned: {payload!r}")
            return
        except OSError as error:
            last_error = error
            time.sleep(0.25)

    raise TimeoutError(
        f"Catalog API did not become healthy within {timeout_seconds} seconds; "
        f"see {server_log_path}"
    ) from last_error


@contextmanager
def _managed_catalog_api(
    workbook_path: Path,
    server_log_path: Path,
    startup_timeout: int,
    host: str = "127.0.0.1",
    port: int = 8000,
) -> Generator[str, None, None]:
    if not workbook_path.is_file():
        raise FileNotFoundError(
            f"Catalog API workbook not found: {workbook_path}. "
            "Run UCI ingestion first or resume from an existing raw workbook."
        )

    server_log_path.parent.mkdir(parents=True, exist_ok=True)
    if port == 0:
        port = _available_local_port()
    health_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
    base_url = f"http://{health_host}:{port}"
    server_path = _PROJECT_ROOT / "catalog_api" / "server.py"
    process: subprocess.Popen | None = None

    with server_log_path.open("w", encoding="utf-8") as server_log:
        try:
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(server_path),
                    "--workbook",
                    str(workbook_path),
                    "--host",
                    host,
                    "--port",
                    str(port),
                ],
                cwd=_PROJECT_ROOT,
                stdout=server_log,
                stderr=subprocess.STDOUT,
            )
            _wait_for_catalog_api(
                process,
                f"{base_url}/health",
                startup_timeout,
                server_log_path,
            )
            get_run_logger().info("Catalog API is healthy at %s", base_url)
            yield base_url
        finally:
            if process is not None and process.poll() is None:
                try:
                    process.terminate()
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                get_run_logger().info(
                    "Stopped the catalog API process started by this task"
                )


def _check_manifest(path: Path, stage: str) -> dict[str, str]:
    if not path.is_file():
        raise FileNotFoundError(f"{stage} did not create its manifest: {path}")
    return {"manifest": str(path)}


@task(name="UCI ingestion", log_prints=True)
def ingest_uci_task(data_root: str) -> dict[str, str]:
    logger = get_run_logger()
    root = Path(data_root)
    output_dir = ingest_uci_archive(
        raw_root=root / "raw",
        landing_root=root / "landing",
    )
    manifest_path = output_dir / "manifest.json"
    result = _check_manifest(manifest_path, "UCI ingestion")
    result["landing_directory"] = str(output_dir)
    logger.info("UCI batches are ready in %s", output_dir)
    return result


@task(
    name="Catalog API capture",
    retries=1,
    retry_delay_seconds=30,
    log_prints=True,
)
def capture_catalog_api_task(
    data_root: str,
    api_startup_timeout: int,
    base_url: str | None = None,
    persistent_server_log: str | None = None,
) -> dict[str, str]:
    logger = get_run_logger()
    root = Path(data_root)
    workbook_path = root / "raw" / "online_retail" / "Online Retail.xlsx"
    log_dir = root / "orchestration" / "api_logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    server_log_path = (
        Path(persistent_server_log)
        if persistent_server_log
        else log_dir / f"catalog_api_{uuid.uuid4().hex[:12]}.log"
    )

    if base_url is not None:
        output_dir = ingest_catalog_api(
            api_url=f"{base_url}/products",
            raw_root=root / "raw",
            landing_root=root / "landing",
        )
        api_url = f"{base_url}/products"
    else:
        with _managed_catalog_api(
            workbook_path,
            server_log_path,
            api_startup_timeout,
        ) as managed_base_url:
            output_dir = ingest_catalog_api(
                api_url=f"{managed_base_url}/products",
                raw_root=root / "raw",
                landing_root=root / "landing",
            )
            api_url = f"{managed_base_url}/products"

    manifest_path = output_dir / "manifest.json"
    result = _check_manifest(manifest_path, "Catalog API capture")
    result["landing_directory"] = str(output_dir)
    result["api_server_log"] = str(server_log_path)
    result["api_url"] = api_url
    logger.info("Catalog API data are ready in %s", output_dir)
    return result


@task(name="Bronze: Online Retail", log_prints=True)
def bronze_online_retail_task(data_root: str) -> dict[str, str]:
    logger = get_run_logger()
    root = Path(data_root)
    output_dir = ingest_landing_dataset(
        root / "landing" / "online_retail",
        output_root=root / "bronze",
    )
    result = _check_manifest(output_dir / "bronze_manifest.json", "Bronze")
    result["bronze_directory"] = str(output_dir)
    logger.info("Online Retail Bronze data are ready in %s", output_dir)
    return result


@task(name="Bronze: Product catalog", log_prints=True)
def bronze_product_catalog_task(data_root: str) -> dict[str, str]:
    logger = get_run_logger()
    root = Path(data_root)
    output_dir = ingest_landing_dataset(
        root / "landing" / "product_catalog",
        output_root=root / "bronze",
    )
    result = _check_manifest(output_dir / "bronze_manifest.json", "Bronze")
    result["bronze_directory"] = str(output_dir)
    logger.info("Product catalog Bronze data are ready in %s", output_dir)
    return result


@task(name="Silver: Online Retail", log_prints=True)
def silver_online_retail_task(data_root: str) -> dict[str, str]:
    logger = get_run_logger()
    root = Path(data_root)
    output_dir = run_silver(
        "online_retail",
        bronze_root=root / "bronze",
        silver_root=root / "silver",
    )
    result = _check_manifest(output_dir / "silver_manifest.json", "Silver")
    result["silver_directory"] = str(output_dir)
    logger.info("Online Retail Silver data are ready in %s", output_dir)
    return result


@task(name="Silver: Product catalog", log_prints=True)
def silver_product_catalog_task(data_root: str) -> dict[str, str]:
    logger = get_run_logger()
    root = Path(data_root)
    output_dir = run_silver(
        "product_catalog",
        bronze_root=root / "bronze",
        silver_root=root / "silver",
    )
    result = _check_manifest(output_dir / "silver_manifest.json", "Silver")
    result["silver_directory"] = str(output_dir)
    logger.info("Product catalog Silver data are ready in %s", output_dir)
    return result


@task(name="Gold feature engineering", log_prints=True)
def gold_task(data_root: str) -> dict[str, str]:
    logger = get_run_logger()
    root = Path(data_root)
    output_dir = build_gold(
        root / "silver",
        root / "gold",
    )
    result = _check_manifest(output_dir / "gold_manifest.json", "Gold")
    result["gold_directory"] = str(output_dir)
    logger.info("Gold features are ready in %s", output_dir)
    return result


@task(name="Feature store registration", log_prints=True)
def feature_store_task(data_root: str) -> dict[str, str]:
    logger = get_run_logger()
    root = Path(data_root)
    result = register_gold_features(
        root / "gold",
        root / "feature_store",
    )
    logger.info(
        "Registered Gold features as version %s in %s",
        result["version_id"],
        result["registry_path"],
    )
    return {
        "version_id": result["version_id"],
        "registry": result["registry_path"],
        "version_directory": result["version_path"],
    }


@task(name="Recommendation model training", log_prints=True)
def recommendation_model_task(data_root: str) -> dict[str, str]:
    logger = get_run_logger()
    root = Path(data_root)
    result = train_and_evaluate(
        root / "silver",
        root / "models" / "item_item_cosine",
        root / "model_tracking",
    )
    logger.info(
        "Recommendation model experiment run %s: P@10=%.4f, "
        "R@10=%.4f, NDCG@10=%.4f",
        result["run_id"],
        float(result["precision_at_10"]),
        float(result["recall_at_10"]),
        float(result["ndcg_at_10"]),
    )
    return result


@task(name="EDA dashboard", log_prints=True)
def eda_task(data_root: str) -> dict[str, str]:
    logger = get_run_logger()
    root = Path(data_root)
    output_dir = root / "eda"
    summary = generate_eda(root / "silver", root / "gold", output_dir)
    dashboard_path = output_dir / "index.html"
    if not dashboard_path.is_file():
        raise FileNotFoundError(f"EDA did not create its dashboard: {dashboard_path}")

    logger.info(
        "EDA dashboard refreshed with %d purchase lines and %.2f%% matrix sparsity: %s",
        summary["counts"]["eligible_purchase_lines_analyzed"],
        summary["interaction_matrix"]["sparsity_percent"],
        dashboard_path,
    )
    return {
        "dashboard": str(dashboard_path),
        "summary": str(output_dir / "summary.json"),
        "purchase_lines": str(
            summary["counts"]["eligible_purchase_lines_analyzed"]
        ),
    }


@flow(
    name="online-retail-medallion-pipeline",
    description=(
        "Run or resume Online Retail through feature registration, model "
        "evaluation, and EDA."
    ),
    log_prints=True,
)
def retail_pipeline(
    data_root: str = str(_DEFAULT_DATA_ROOT),
    from_stage: str = "uci_ingestion",
    api_startup_timeout: int = 30,
    enable_catalog_api: bool = True,
    catalog_api_host: str = "127.0.0.1",
    catalog_api_port: int = 8000,
) -> dict[str, dict[str, str]]:
    if from_stage not in _STAGE_NAMES:
        raise ValueError(
            f"Unsupported starting stage {from_stage!r}; "
            f"choose from {', '.join(_STAGE_NAMES)}"
        )
    if api_startup_timeout <= 0:
        raise ValueError("API startup timeout must be a positive number of seconds")
    if not catalog_api_host.strip():
        raise ValueError("Catalog API host cannot be empty")
    if catalog_api_port < 0 or catalog_api_port > 65535:
        raise ValueError("Catalog API port must be between 0 and 65535")

    logger = get_run_logger()
    data_root = str(Path(data_root).resolve())
    start_index = _STAGE_NAMES.index(from_stage)
    results: dict[str, dict[str, str]] = {}
    stages = (
        ("uci_ingestion", ingest_uci_task, (data_root,)),
        (
            "catalog_api_capture",
            capture_catalog_api_task,
            (data_root, api_startup_timeout),
        ),
        (
            "bronze_online_retail",
            bronze_online_retail_task,
            (data_root,),
        ),
        (
            "bronze_product_catalog",
            bronze_product_catalog_task,
            (data_root,),
        ),
        (
            "silver_online_retail",
            silver_online_retail_task,
            (data_root,),
        ),
        (
            "silver_product_catalog",
            silver_product_catalog_task,
            (data_root,),
        ),
        ("gold", gold_task, (data_root,)),
        ("feature_store", feature_store_task, (data_root,)),
        ("model_training", recommendation_model_task, (data_root,)),
        ("eda", eda_task, (data_root,)),
    )

    logger.info("Starting pipeline at stage %s", from_stage)
    previous_task = None
    catalog_api_stack = ExitStack()
    persistent_catalog_api_url: str | None = None
    catalog_api_log_path: Path | None = None
    try:
        for index, (stage_name, stage_task, task_arguments) in enumerate(stages):
            if index < start_index:
                logger.info("Skipping stage %s before selected starting stage", stage_name)
                continue

            if (
                stage_name == "catalog_api_capture"
                and enable_catalog_api
                and persistent_catalog_api_url is None
            ):
                root = Path(data_root)
                workbook_path = root / "raw" / "online_retail" / "Online Retail.xlsx"
                log_dir = root / "orchestration" / "api_logs"
                log_dir.mkdir(parents=True, exist_ok=True)
                catalog_api_log_path = (
                    log_dir / f"catalog_api_{uuid.uuid4().hex[:12]}.log"
                )
                persistent_catalog_api_url = catalog_api_stack.enter_context(
                    _managed_catalog_api(
                        workbook_path,
                        catalog_api_log_path,
                        api_startup_timeout,
                        catalog_api_host,
                        catalog_api_port,
                    )
                )
                logger.info(
                    "Catalog API enabled for this Prefect run at %s",
                    persistent_catalog_api_url,
                )

            if stage_name == "catalog_api_capture":
                if not enable_catalog_api:
                    logger.info(
                        "Skipping catalog API capture because catalog API is disabled"
                    )
                    continue
                task_arguments = (
                    data_root,
                    api_startup_timeout,
                    persistent_catalog_api_url,
                    str(catalog_api_log_path) if catalog_api_log_path else None,
                )

            if previous_task is None:
                current_task = stage_task.submit(*task_arguments)
            else:
                current_task = stage_task.submit(
                    *task_arguments,
                    wait_for=[previous_task],
                )
            results[stage_name] = current_task.result()
            previous_task = current_task
        logger.info("Online Retail pipeline completed successfully")
    finally:
        catalog_api_stack.close()
    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run or resume the Online Retail Prefect flow."
    )
    parser.add_argument("--data-root", type=Path, default=_DEFAULT_DATA_ROOT)
    parser.add_argument(
        "--from-stage",
        choices=_STAGE_NAMES,
        default="uci_ingestion",
        help="Resume at this stage and run every subsequent stage.",
    )
    parser.add_argument(
        "--api-startup-timeout",
        type=int,
        default=30,
        help="Seconds to wait for the local catalog API health check.",
    )
    parser.add_argument(
        "--disable-catalog-api",
        action="store_true",
        help="Skip starting and capturing the local catalog API.",
    )
    parser.add_argument("--catalog-api-host", default="127.0.0.1")
    parser.add_argument("--catalog-api-port", type=int, default=8000)
    arguments = parser.parse_args()
    if arguments.api_startup_timeout <= 0:
        parser.error("--api-startup-timeout must be a positive integer")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    retail_pipeline(
        data_root=str(arguments.data_root),
        from_stage=arguments.from_stage,
        api_startup_timeout=arguments.api_startup_timeout,
        enable_catalog_api=not arguments.disable_catalog_api,
        catalog_api_host=arguments.catalog_api_host,
        catalog_api_port=arguments.catalog_api_port,
    )


if __name__ == "__main__":
    main()
