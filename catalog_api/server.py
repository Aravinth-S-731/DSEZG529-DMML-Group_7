import argparse
import json
import logging
import re
import zipfile
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlsplit
from xml.etree import ElementTree


_MAIN_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
_DEFAULT_WORKBOOK = (
    Path(__file__).resolve().parents[1]
    / "data"
    / "raw"
    / "online_retail"
    / "Online Retail.xlsx"
)


def _column_index(cell_reference: str) -> int:
    letters = re.match(r"[A-Z]+", cell_reference.upper())
    if letters is None:
        raise ValueError(f"Invalid Excel cell reference: {cell_reference}")

    index = 0
    for letter in letters.group(0):
        index = index * 26 + ord(letter) - ord("A") + 1
    return index - 1


def _load_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    try:
        shared_strings_file = archive.open("xl/sharedStrings.xml")
    except KeyError:
        return []

    shared_strings = []
    with shared_strings_file:
        for _, element in ElementTree.iterparse(shared_strings_file, events=("end",)):
            if element.tag == f"{_MAIN_NS}si":
                shared_strings.append(
                    "".join(text.text or "" for text in element.iter(f"{_MAIN_NS}t"))
                )
                element.clear()
    return shared_strings


def _cell_value(cell: ElementTree.Element, shared_strings: list[str]) -> str | None:
    cell_type = cell.get("t")

    if cell_type == "inlineStr":
        return "".join(text.text or "" for text in cell.iter(f"{_MAIN_NS}t"))

    value = cell.find(f"{_MAIN_NS}v")
    if value is None or value.text is None:
        return None
    if cell_type == "s":
        return shared_strings[int(value.text)]
    return value.text


def build_product_catalog(workbook_path: Path) -> list[dict[str, str | None]]:
    if not workbook_path.is_file():
        raise FileNotFoundError(f"Workbook not found: {workbook_path}")

    with zipfile.ZipFile(workbook_path) as archive:
        shared_strings = _load_shared_strings(archive)
        try:
            worksheet = archive.open("xl/worksheets/sheet1.xml")
        except KeyError as error:
            raise ValueError("Workbook does not contain the expected first worksheet") from error

        headers: dict[str, int] | None = None
        descriptions_by_code: dict[str, Counter[str]] = {}
        with worksheet:
            for _, row in ElementTree.iterparse(worksheet, events=("end",)):
                if row.tag != f"{_MAIN_NS}row":
                    continue

                values = {
                    _column_index(cell.get("r", "")): _cell_value(cell, shared_strings)
                    for cell in row.findall(f"{_MAIN_NS}c")
                }
                if headers is None:
                    headers = {
                        str(value): index
                        for index, value in values.items()
                        if value is not None
                    }
                    missing_headers = {"StockCode", "Description"} - headers.keys()
                    if missing_headers:
                        raise ValueError(
                            "Workbook is missing required columns: "
                            + ", ".join(sorted(missing_headers))
                        )
                else:
                    stock_code = values.get(headers["StockCode"])
                    description = values.get(headers["Description"])
                    if stock_code and stock_code.strip():
                        code = stock_code.strip()
                        product_descriptions = descriptions_by_code.setdefault(code, Counter())
                        if description and description.strip():
                            product_descriptions[description.strip()] += 1

                row.clear()

    if headers is None:
        raise ValueError("Workbook worksheet is empty")

    catalog = []
    for stock_code, descriptions in sorted(descriptions_by_code.items()):
        description = descriptions.most_common(1)[0][0] if descriptions else None
        catalog.append({"stock_code": stock_code, "description": description})
    return catalog


def _handler_for(catalog: list[dict[str, str | None]]):
    products_by_code = {product["stock_code"]: product for product in catalog}

    class CatalogHandler(BaseHTTPRequestHandler):
        def _send_json(self, status_code: int, payload: dict) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            path = urlsplit(self.path).path.rstrip("/") or "/"
            if path == "/health":
                self._send_json(200, {"status": "ok", "product_count": len(catalog)})
            elif path == "/products":
                self._send_json(
                    200,
                    {
                        "source": (
                            "Catalog derived from the UCI Online Retail transaction workbook"
                        ),
                        "count": len(catalog),
                        "products": catalog,
                    },
                )
            elif path.startswith("/products/"):
                stock_code = unquote(path[len("/products/") :])
                product = products_by_code.get(stock_code)
                if product is None:
                    self._send_json(404, {"error": "Product not found"})
                else:
                    self._send_json(200, product)
            else:
                self._send_json(
                    404,
                    {"error": "Not found", "available_endpoints": ["/health", "/products"]},
                )

        def log_message(self, format_string: str, *args) -> None:
            logging.info("%s - %s", self.address_string(), format_string % args)

    return CatalogHandler


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Serve a REST catalog derived from the Online Retail workbook."
    )
    parser.add_argument("--workbook", type=Path, default=_DEFAULT_WORKBOOK)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    arguments = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    catalog = build_product_catalog(arguments.workbook)
    logging.info("Loaded %d distinct products from %s", len(catalog), arguments.workbook)

    server = ThreadingHTTPServer(
        (arguments.host, arguments.port),
        _handler_for(catalog),
    )
    logging.info("Catalog API listening at http://%s:%d", arguments.host, arguments.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logging.info("Stopping catalog API")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
