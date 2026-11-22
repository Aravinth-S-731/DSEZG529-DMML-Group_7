import argparse
import json
from pathlib import Path

from feature_store.store import (
    DEFAULT_GOLD_ROOT,
    DEFAULT_STORE_ROOT,
    register_gold_features,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Register Gold feature files as an immutable feature-store version."
    )
    parser.add_argument("--gold-root", type=Path, default=DEFAULT_GOLD_ROOT)
    parser.add_argument("--store-root", type=Path, default=DEFAULT_STORE_ROOT)
    arguments = parser.parse_args()
    result = register_gold_features(arguments.gold_root, arguments.store_root)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
