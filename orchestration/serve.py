import argparse
from pathlib import Path

from prefect.client.schemas.schedules import CronSchedule

from orchestration.run import _DEFAULT_DATA_ROOT, retail_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Register and serve an on-demand or scheduled Prefect flow."
    )
    parser.add_argument(
        "--cron",
        help='Optional cron schedule, for example "0 2 * * *". Omit for UI-triggered runs.',
    )
    parser.add_argument(
        "--timezone",
        default="Asia/Kolkata",
        help="IANA timezone for the schedule.",
    )
    parser.add_argument("--data-root", type=Path, default=_DEFAULT_DATA_ROOT)
    parser.add_argument("--api-startup-timeout", type=int, default=30)
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

    schedule = (
        CronSchedule(
            cron=arguments.cron,
            timezone=arguments.timezone,
        )
        if arguments.cron
        else None
    )
    deployment_name = (
        "online-retail-batch" if schedule is not None else "online-retail-on-demand"
    )
    retail_pipeline.serve(
        name=deployment_name,
        schedule=schedule,
        parameters={
            "data_root": str(arguments.data_root.resolve()),
            "from_stage": "uci_ingestion",
            "api_startup_timeout": arguments.api_startup_timeout,
            "enable_catalog_api": not arguments.disable_catalog_api,
            "catalog_api_host": arguments.catalog_api_host,
            "catalog_api_port": arguments.catalog_api_port,
        },
        tags=["dmml", "online-retail"],
        limit=1,
    )


if __name__ == "__main__":
    main()
