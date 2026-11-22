import argparse
import csv
import hashlib
import json
import logging
import os
import tempfile
import time
import zipfile
from collections import defaultdict
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Iterator
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from xml.etree import ElementTree


_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_RAW_ROOT = _PROJECT_ROOT / "data" / "raw"
_DEFAULT_LANDING_ROOT = _PROJECT_ROOT / "data" / "landing"
_DEFAULT_UCI_URL = "https://archive.ics.uci.edu/static/public/352/online%2Bretail.zip"
_DEFAULT_CATALOG_API_URL = "http://127.0.0.1:8000/products"
_EXCEL_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
_XLSX_REQUIRED_FILES = {
    "xl/workbook.xml",
    "xl/worksheets/sheet1.xml",
}
_TRANSACTION_COLUMNS = (
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
)
_CHUNK_SIZE = 1024 * 1024


def _sha256_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    byte_count = 0
    with path.open("rb") as source:
        while chunk := source.read(_CHUNK_SIZE):
            digest.update(chunk)
            byte_count += len(chunk)
    return byte_count, digest.hexdigest()


def _download(
    url: str,
    destination: Path,
    timeout: int,
    retries: int,
) -> tuple[str, int, str]:
    request = Request(
        url,
        headers={
            "Accept": "*/*",
            "User-Agent": "DMML-Experiential-Learning/1.0",
        },
    )
    partial_path = destination.with_name(destination.name + ".part")

    for attempt in range(retries + 1):
        partial_path.unlink(missing_ok=True)
        digest = hashlib.sha256()
        byte_count = 0
        try:
            with urlopen(request, timeout=timeout) as response:
                final_url = response.geturl()
                with partial_path.open("xb") as output:
                    while True:
                        chunk = response.read(_CHUNK_SIZE)
                        if not chunk:
                            break
                        output.write(chunk)
                        digest.update(chunk)
                        byte_count += len(chunk)
                    output.flush()
                    os.fsync(output.fileno())
            if byte_count == 0:
                raise ValueError(f"Download from {url} returned an empty response")
            os.replace(partial_path, destination)
            return final_url, byte_count, digest.hexdigest()
        except HTTPError as error:
            error.close()
            partial_path.unlink(missing_ok=True)
            retryable = error.code == 429 or 500 <= error.code < 600
            if not retryable or attempt == retries:
                raise RuntimeError(
                    f"Download failed with HTTP {error.code} for {url}"
                ) from error
            logging.warning(
                "HTTP %d downloading %s; retrying attempt %d/%d",
                error.code,
                url,
                attempt + 1,
                retries,
            )
        except (URLError, TimeoutError) as error:
            partial_path.unlink(missing_ok=True)
            if attempt == retries:
                raise RuntimeError(f"Could not download {url}: {error}") from error
            logging.warning(
                "Download error for %s; retrying attempt %d/%d: %s",
                url,
                attempt + 1,
                retries,
                error,
            )

        time.sleep(min(2**attempt, 8))

    raise RuntimeError(f"Download failed after {retries + 1} attempts: {url}")


def _write_json_atomic(folder: Path, filename: str, payload: dict) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    destination = folder / filename
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            dir=folder,
            prefix=".manifest-",
            suffix=".tmp",
            delete=False,
            encoding="utf-8",
            newline="\n",
        ) as output:
            temp_path = Path(output.name)
            json.dump(payload, output, indent=2, ensure_ascii=False)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temp_path, destination)
        temp_path = None
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


def _commit_processed_files(
    staging_dir: Path,
    destination_dir: Path,
    manifest: dict,
    filename_pattern: str,
) -> None:
    destination_dir.mkdir(parents=True, exist_ok=True)
    staged_files = sorted(
        path
        for path in staging_dir.iterdir()
        if path.is_file() and path.name != "manifest.json"
    )
    if not staged_files:
        raise ValueError(f"No processed output files were created for {destination_dir.name}")

    new_names = {path.name for path in staged_files}
    for path in staged_files:
        os.replace(path, destination_dir / path.name)
    for existing in destination_dir.glob(filename_pattern):
        if existing.name not in new_names:
            existing.unlink()
    _write_json_atomic(destination_dir, "manifest.json", manifest)


def _xlsx_column_index(cell_reference: str) -> int:
    index = 0
    for character in cell_reference:
        if not character.isalpha():
            break
        index = index * 26 + ord(character.upper()) - ord("A") + 1
    if index == 0:
        raise ValueError(f"Invalid Excel cell reference: {cell_reference}")
    return index - 1


def _shared_strings(archive: zipfile.ZipFile) -> list[str]:
    try:
        shared_strings_file = archive.open("xl/sharedStrings.xml")
    except KeyError:
        return []

    strings = []
    with shared_strings_file:
        for _, element in ElementTree.iterparse(shared_strings_file, events=("end",)):
            if element.tag == f"{_EXCEL_NS}si":
                strings.append(
                    "".join(text.text or "" for text in element.iter(f"{_EXCEL_NS}t"))
                )
                element.clear()
    return strings


def _xlsx_cell_value(cell: ElementTree.Element, strings: list[str]) -> str:
    cell_type = cell.get("t")
    if cell_type == "inlineStr":
        return "".join(text.text or "" for text in cell.iter(f"{_EXCEL_NS}t"))

    value = cell.find(f"{_EXCEL_NS}v")
    if value is None or value.text is None:
        return ""
    if cell_type == "s":
        return strings[int(value.text)]
    return value.text


def _iter_transaction_rows(
    workbook_path: Path,
) -> Iterator[tuple[list[str], int, dict[str, str]]]:
    with zipfile.ZipFile(workbook_path) as archive:
        missing_files = _XLSX_REQUIRED_FILES - set(archive.namelist())
        if missing_files:
            raise ValueError(
                "Workbook is missing required Excel components: "
                + ", ".join(sorted(missing_files))
            )
        strings = _shared_strings(archive)
        with archive.open("xl/worksheets/sheet1.xml") as worksheet:
            iterator = ElementTree.iterparse(worksheet, events=("start", "end"))
            next(iterator)
            sheet_data: ElementTree.Element | None = None
            headers: list[str] | None = None
            data_row = 0
            for event, element in iterator:
                if event == "start" and element.tag == f"{_EXCEL_NS}sheetData":
                    sheet_data = element
                    continue
                if event != "end" or element.tag != f"{_EXCEL_NS}row":
                    continue
                values = {
                    _xlsx_column_index(cell.get("r", "")): _xlsx_cell_value(cell, strings)
                    for cell in element.findall(f"{_EXCEL_NS}c")
                }
                if headers is None:
                    headers = [
                        value
                        for _, value in sorted(values.items())
                    ]
                    missing_columns = set(_TRANSACTION_COLUMNS) - set(headers)
                    if missing_columns:
                        raise ValueError(
                            "Workbook is missing required columns: "
                            + ", ".join(sorted(missing_columns))
                        )
                else:
                    data_row += 1
                    record = {
                        header: values.get(index, "")
                        for index, header in enumerate(headers)
                    }
                    yield headers, data_row, record
                if sheet_data is not None:
                    sheet_data.clear()

            if headers is None:
                raise ValueError("Workbook worksheet is empty")


def _invoice_datetime(value: str, row_number: int) -> datetime:
    try:
        return datetime(1899, 12, 30) + timedelta(days=float(value))
    except (ValueError, OverflowError) as error:
        raise ValueError(
            f"Invalid Excel InvoiceDate value at data row {row_number}: {value!r}"
        ) from error


def _build_monthly_transaction_files(
    workbook_path: Path,
    landing_root: Path,
    raw_manifest: dict,
) -> Path:
    landing_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".online-retail-processing-",
        dir=landing_root,
    ) as staging:
        staging_dir = Path(staging)
        output_writers: dict[str, csv.DictWriter] = {}
        row_counts: dict[str, int] = defaultdict(int)
        headers: list[str] | None = None

        with ExitStack() as stack:
            for headers_for_row, row_number, record in _iter_transaction_rows(workbook_path):
                if headers is None:
                    headers = headers_for_row
                invoice_datetime = _invoice_datetime(record["InvoiceDate"], row_number)
                record["InvoiceDate"] = invoice_datetime.isoformat(timespec="seconds")
                month = invoice_datetime.strftime("%Y-%m")
                filename = f"transactions_{month}.csv"
                if month not in output_writers:
                    output_file = stack.enter_context(
                        (staging_dir / filename).open("w", encoding="utf-8", newline="")
                    )
                    writer = csv.DictWriter(output_file, fieldnames=headers)
                    writer.writeheader()
                    output_writers[month] = writer
                output_writers[month].writerow(record)
                row_counts[month] += 1

        if headers is None or not row_counts:
            raise ValueError("No transaction rows were found in the workbook")

        processed_files = []
        for month in sorted(row_counts):
            filename = f"transactions_{month}.csv"
            byte_count, digest = _sha256_file(staging_dir / filename)
            processed_files.append(
                {
                    "file": filename,
                    "month": month,
                    "row_count": row_counts[month],
                    "file_size_bytes": byte_count,
                    "sha256": digest,
                }
            )

        landing_dir = landing_root / "online_retail"
        _commit_processed_files(
            staging_dir,
            landing_dir,
            {
                "dataset": "online_retail",
                "source_type": "uci_batch",
                "created_at_utc": datetime.now(timezone.utc).isoformat(),
                "raw_source": {
                    "directory": raw_manifest["directory"],
                    "files": raw_manifest["assets"],
                },
                "format": "CSV, one file per InvoiceDate month",
                "columns": headers,
                "total_rows": sum(row_counts.values()),
                "monthly_batches": processed_files,
            },
            "transactions_*.csv",
        )

    logging.info(
        "Created %d monthly transaction batches (%d rows) at %s",
        len(row_counts),
        sum(row_counts.values()),
        landing_dir,
    )
    return landing_dir


def _extract_workbook(archive_path: Path, workbook_path: Path) -> tuple[int, str]:
    with zipfile.ZipFile(archive_path) as archive:
        candidates = [
            info
            for info in archive.infolist()
            if PurePosixPath(info.filename.replace("\\", "/")).name.lower()
            == "online retail.xlsx"
            and not PurePosixPath(info.filename.replace("\\", "/")).is_absolute()
            and ".." not in PurePosixPath(info.filename.replace("\\", "/")).parts
        ]
        if len(candidates) != 1:
            raise ValueError(
                "UCI archive must contain exactly one safe 'Online Retail.xlsx' file"
            )
        digest = hashlib.sha256()
        byte_count = 0
        with archive.open(candidates[0]) as source, workbook_path.open("xb") as output:
            while True:
                chunk = source.read(_CHUNK_SIZE)
                if not chunk:
                    break
                output.write(chunk)
                digest.update(chunk)
                byte_count += len(chunk)
            output.flush()
            os.fsync(output.fileno())
    return byte_count, digest.hexdigest()


def ingest_uci_archive(
    source_url: str = _DEFAULT_UCI_URL,
    raw_root: Path = _DEFAULT_RAW_ROOT,
    landing_root: Path = _DEFAULT_LANDING_ROOT,
    timeout: int = 30,
    retries: int = 3,
) -> Path:
    raw_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".uci-download-", dir=raw_root) as staging:
        staging_dir = Path(staging)
        archive_path = staging_dir / "source.zip"
        final_url, archive_size, archive_digest = _download(
            source_url,
            archive_path,
            timeout,
            retries,
        )
        workbook_path = staging_dir / "Online Retail.xlsx"
        workbook_size, workbook_digest = _extract_workbook(archive_path, workbook_path)

        raw_dir = raw_root / "online_retail"
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw_archive = raw_dir / "source.zip"
        raw_workbook = raw_dir / "Online Retail.xlsx"
        os.replace(archive_path, raw_archive)
        os.replace(workbook_path, raw_workbook)
        raw_manifest = {
            "dataset": "online_retail",
            "source_type": "uci_batch_archive",
            "source_url": final_url,
            "directory": str(raw_dir.resolve()),
            "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
            "assets": [
                {
                    "file": raw_archive.name,
                    "file_size_bytes": archive_size,
                    "sha256": archive_digest,
                },
                {
                    "file": raw_workbook.name,
                    "file_size_bytes": workbook_size,
                    "sha256": workbook_digest,
                },
            ],
        }
        _write_json_atomic(raw_dir, "manifest.json", raw_manifest)

    return _build_monthly_transaction_files(raw_workbook, landing_root, raw_manifest)


def ingest_catalog_api(
    api_url: str = _DEFAULT_CATALOG_API_URL,
    raw_root: Path = _DEFAULT_RAW_ROOT,
    landing_root: Path = _DEFAULT_LANDING_ROOT,
    timeout: int = 30,
    retries: int = 3,
) -> Path:
    raw_root.mkdir(parents=True, exist_ok=True)
    landing_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".catalog-download-", dir=raw_root) as staging:
        staging_dir = Path(staging)
        response_path = staging_dir / "raw_response.json"
        final_url, response_size, response_digest = _download(
            api_url,
            response_path,
            timeout,
            retries,
        )
        try:
            response_data = json.loads(response_path.read_text(encoding="utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("Catalog API response is not valid UTF-8 JSON") from error
        products = response_data.get("products") if isinstance(response_data, dict) else None
        if not isinstance(products, list):
            raise ValueError("Catalog API response must contain a 'products' list")

        processed_path = staging_dir / "products.csv"
        with processed_path.open("w", encoding="utf-8", newline="") as output:
            writer = csv.DictWriter(output, fieldnames=("stock_code", "description"))
            writer.writeheader()
            for index, product in enumerate(products, start=1):
                if not isinstance(product, dict):
                    raise ValueError(f"Catalog product at index {index} is not an object")
                stock_code = product.get("stock_code")
                description = product.get("description")
                if not isinstance(stock_code, str) or not stock_code.strip():
                    raise ValueError(f"Catalog product at index {index} has no stock_code")
                if description is not None and not isinstance(description, str):
                    raise ValueError(f"Catalog product at index {index} has invalid description")
                writer.writerow(
                    {
                        "stock_code": stock_code,
                        "description": description or "",
                    }
                )

        product_count = len(products)
        processed_size, processed_digest = _sha256_file(processed_path)
        raw_dir = raw_root / "product_catalog"
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw_response = raw_dir / "raw_response.json"
        os.replace(response_path, raw_response)
        raw_manifest = {
            "dataset": "product_catalog",
            "source_type": "rest_api",
            "source_url": final_url,
            "directory": str(raw_dir.resolve()),
            "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
            "record_count": product_count,
            "assets": [
                {
                    "file": raw_response.name,
                    "file_size_bytes": response_size,
                    "sha256": response_digest,
                }
            ],
            "catalog_provenance": (
                "Product records are derived from the UCI Online Retail workbook; "
                "they are not an independent external catalog."
            ),
        }
        _write_json_atomic(raw_dir, "manifest.json", raw_manifest)

        landing_dir = landing_root / "product_catalog"
        _commit_processed_files(
            staging_dir,
            landing_dir,
            {
                "dataset": "product_catalog",
                "source_type": "rest_api",
                "created_at_utc": datetime.now(timezone.utc).isoformat(),
                "raw_source": {
                    "directory": raw_manifest["directory"],
                    "file": raw_response.name,
                    "sha256": response_digest,
                },
                "format": "CSV",
                "columns": ["stock_code", "description"],
                "row_count": product_count,
                "processed_file": {
                    "file": "products.csv",
                    "file_size_bytes": processed_size,
                    "sha256": processed_digest,
                },
                "catalog_provenance": raw_manifest["catalog_provenance"],
            },
            "products.csv",
        )

    logging.info(
        "Fetched and converted %d products into %s",
        product_count,
        landing_dir / "products.csv",
    )
    return landing_dir


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch source data into raw and create monthly/processed landing files."
    )
    parser.add_argument("--raw-root", type=Path, default=_DEFAULT_RAW_ROOT)
    parser.add_argument("--landing-root", type=Path, default=_DEFAULT_LANDING_ROOT)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Number of retries after a transient download failure.",
    )
    subparsers = parser.add_subparsers(dest="source", required=True)
    uci_parser = subparsers.add_parser("uci", help="Download and batch the UCI workbook.")
    uci_parser.add_argument("--url", default=_DEFAULT_UCI_URL)
    api_parser = subparsers.add_parser("api", help="Fetch and convert the catalog API.")
    api_parser.add_argument("--api-url", default=_DEFAULT_CATALOG_API_URL)
    all_parser = subparsers.add_parser(
        "all",
        help="Download and batch the UCI workbook, then fetch the catalog API.",
    )
    all_parser.add_argument("--uci-url", default=_DEFAULT_UCI_URL)
    all_parser.add_argument("--api-url", default=_DEFAULT_CATALOG_API_URL)
    arguments = parser.parse_args()

    if arguments.timeout <= 0 or arguments.retries < 0:
        parser.error("--timeout must be positive and --retries cannot be negative")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if arguments.source == "uci":
        output_dirs = [
            ingest_uci_archive(
                arguments.url,
                arguments.raw_root,
                arguments.landing_root,
                arguments.timeout,
                arguments.retries,
            )
        ]
    elif arguments.source == "api":
        output_dirs = [
            ingest_catalog_api(
                arguments.api_url,
                arguments.raw_root,
                arguments.landing_root,
                arguments.timeout,
                arguments.retries,
            )
        ]
    else:
        output_dirs = [
            ingest_uci_archive(
                arguments.uci_url,
                arguments.raw_root,
                arguments.landing_root,
                arguments.timeout,
                arguments.retries,
            ),
            ingest_catalog_api(
                arguments.api_url,
                arguments.raw_root,
                arguments.landing_root,
                arguments.timeout,
                arguments.retries,
            ),
        ]
    for output_dir in output_dirs:
        print(output_dir)


if __name__ == "__main__":
    main()
