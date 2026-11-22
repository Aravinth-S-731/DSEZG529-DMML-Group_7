import argparse
import json
from itertools import islice
from pathlib import Path

from feature_store.store import DEFAULT_STORE_ROOT, iter_feature_rows, resolve_version_id


def _parse_filters(values: list[str]) -> dict[str, str]:
    filters = {}
    for value in values:
        key, separator, entity_id = value.partition("=")
        if not separator or not key or not entity_id or key in filters:
            raise ValueError(f"Invalid or duplicate entity filter: {value!r}")
        filters[key] = entity_id
    return filters


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Retrieve a version-pinned feature view or entity sample."
    )
    parser.add_argument("--view", required=True, help="Name of the feature view.")
    parser.add_argument(
        "--version",
        help="Registered version ID; defaults to the registry's active version.",
    )
    parser.add_argument(
        "--filter",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Exact entity-key filter. Repeat for composite keys.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Maximum rows to print (use a Python API iterator for full training reads).",
    )
    parser.add_argument("--store-root", type=Path, default=DEFAULT_STORE_ROOT)
    arguments = parser.parse_args()
    if arguments.limit <= 0:
        parser.error("--limit must be a positive integer")

    try:
        filters = _parse_filters(arguments.filter)
        version_id = resolve_version_id(arguments.store_root, arguments.version)
        records = list(
            islice(
                iter_feature_rows(
                    arguments.store_root,
                    arguments.view,
                    version_id=version_id,
                    entity_filters=filters,
                ),
                arguments.limit,
            )
        )
    except ValueError as error:
        parser.error(str(error))

    print(
        json.dumps(
            {
                "version_id": version_id,
                "feature_view": arguments.view,
                "entity_filters": filters,
                "records": records,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
