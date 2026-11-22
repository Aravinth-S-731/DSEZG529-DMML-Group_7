import argparse
import logging
from pathlib import Path

from gold.features import build_gold


_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_SILVER_ROOT = _PROJECT_ROOT / "data" / "silver"
_DEFAULT_GOLD_ROOT = _PROJECT_ROOT / "data" / "gold"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build recommendation and analytical Gold datasets from Silver."
    )
    parser.add_argument("--silver-root", type=Path, default=_DEFAULT_SILVER_ROOT)
    parser.add_argument("--gold-root", type=Path, default=_DEFAULT_GOLD_ROOT)
    arguments = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    output_dir = build_gold(arguments.silver_root, arguments.gold_root)
    print(output_dir)


if __name__ == "__main__":
    main()
