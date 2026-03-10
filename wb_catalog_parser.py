from __future__ import annotations

import argparse
import json
import random
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def chunked(items: List[Any], size: int) -> Iterable[List[Any]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def nested_get(data: Any, *path: str) -> Any:
    current = data
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
        if current is None:
            return None
    return current


def first_not_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def safe_filename(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"\s+", "_", value)
    value = re.sub(r"[^a-zа-я0-9_]+", "", value, flags=re.IGNORECASE)
    return value or "wb_catalog"


def stringify(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        s = value.strip()
        return s or None
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        parts = []
        for item in value:
            item_str = stringify(item)
            if item_str:
                parts.append(item_str)
        return ", ".join(parts) if parts else None
    if isinstance(value, dict):
        # приоритет простым полям
        for key in ("value", "name", "label", "title", "text"):
            if key in value:
                item_str = stringify(value[key])
                if item_str:
                    return item_str
        return json.dumps(value, ensure_ascii=False)
    return str(value)


class WildberriesCatalogParser:
    SEARCH_ENDPOINTS = [
        "https://u-search.wb.ru/exactmatch/ru/common/v18/search",
        "https://search.wb.ru/exactmatch/ru/common/v18/search",
        "https://search.wb.ru/exactmatch/ru/common/v4/search",
    ]

    DETAIL_ENDPOINTS = [
        "https://card.wb.ru/cards/v2/detail",
        "https://card.wb.ru/cards/detail",
    ]

    BASKET_HOSTS = [f"basket-{i:02d}.wbbasket.ru" for i in range(1, 19)]

    HEADERS = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Referer": "https://www.wildberries.ru/",
        "Origin": "https://www.wildberries.ru",
    }

    COUNTRY_KEYS = {
        "страна производства",
        "страна изготовитель",
        "страна изготовителя",
        "countryproduction",
        "country of origin",
    }

    CHARACTERISTIC_KEYS = {
        "options",
        "characteristics",
        "grouped_options",
        "groupedoptions",
        "specifications",
        "details",
        "compositions",
        "descriptionblocks",
    }

    def __init__(
        self,
        query: str,
        dest: int = -1257786,
        max_pages: int = 100,
        output_dir: str = "output",
        min_delay: float = 0.15,
        max_delay: float = 0.45,
    ) -> None:
        self.query = query
        self.dest = dest
        self.max_pages = max_pages
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.session = self._build_session()
        self._basket_host_cache: Dict[int, str] = {}
        self._working_search_endpoint: Optional[str] = None
        self._working_detail_endpoint: Optional[str] = None

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=5,
            connect=5,
            read=5,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update(self.HEADERS)
        return session

    def _sleep(self) -> None:
        time.sleep(random.uniform(self.min_delay, self.max_delay))

    def _request_json(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: int = 20,
        allow_404: bool = False,
    ) -> Optional[Any]:
        try:
            response = self.session.get(url, params=params, timeout=timeout)
            if allow_404 and response.status_code == 404:
                return None
            response.raise_for_status()
            return response.json()
        except Exception:
            return None

    @staticmethod
    def _extract_products(payload: Any) -> List[Dict[str, Any]]:
        if not isinstance(payload, dict):
            return []
        if isinstance(nested_get(payload, "data", "products"), list):
            return nested_get(payload, "data", "products")
        if isinstance(payload.get("products"), list):
            return payload["products"]
        return []

    def search_all_products(self) -> List[Dict[str, Any]]:
        collected: List[Dict[str, Any]] = []
        seen_ids = set()

        search_endpoints = (
            [self._working_search_endpoint] if self._working_search_endpoint else []
        ) + [ep for ep in self.SEARCH_ENDPOINTS if ep != self._working_search_endpoint]

        for page in range(1, self.max_pages + 1):
            print(f"[search] page={page}")
            page_products: List[Dict[str, Any]] = []

            for endpoint in search_endpoints:
                params = {
                    "appType": 1,
                    "curr": "rub",
                    "dest": self.dest,
                    "lang": "ru",
                    "page": page,
                    "query": self.query,
                    "resultset": "catalog",
                    "sort": "popular",
                    "spp": 30,
                    "suppressSpellcheck": "false",
                }
                payload = self._request_json(endpoint, params=params)
                products = self._extract_products(payload)
                if products:
                    self._working_search_endpoint = endpoint
                    page_products = products
                    break

            if not page_products:
                print("[search] no more products or endpoint unavailable")
                break

            added = 0
            for product in page_products:
                nm_id = product.get("id")
                if not nm_id or nm_id in seen_ids:
                    continue
                seen_ids.add(nm_id)
                collected.append(product)
                added += 1

            print(f"[search] added={added}, total={len(collected)}")
            self._sleep()

        return collected

    def fetch_details_batch(self, nm_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        result: Dict[int, Dict[str, Any]] = {}
        detail_endpoints = (
            [self._working_detail_endpoint] if self._working_detail_endpoint else []
        ) + [ep for ep in self.DETAIL_ENDPOINTS if ep != self._working_detail_endpoint]

        for batch in chunked(nm_ids, 100):
            print(f"[detail] batch size={len(batch)}")
            batch_products: List[Dict[str, Any]] = []

            for endpoint in detail_endpoints:
                params = {
                    "appType": 1,
                    "curr": "rub",
                    "dest": self.dest,
                    "spp": 30,
                    "ab_testing": "false",
                    "lang": "ru",
                    "nm": ";".join(map(str, batch)),
                }
                payload = self._request_json(endpoint, params=params)
                products = self._extract_products(payload)
                if products:
                    self._working_detail_endpoint = endpoint
                    batch_products = products
                    break

            for product in batch_products:
                nm_id = product.get("id")
                if nm_id:
                    result[nm_id] = product

            self._sleep()

        return result

    def resolve_card_json(self, nm_id: int) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        vol = nm_id // 100000
        part = nm_id // 1000

        candidate_hosts: List[str] = []
        cached_host = self._basket_host_cache.get(vol)
        if cached_host:
            candidate_hosts.append(cached_host)
        candidate_hosts.extend(host for host in self.BASKET_HOSTS if host != cached_host)

        for host in candidate_hosts:
            url = f"https://{host}/vol{vol}/part{part}/{nm_id}/info/ru/card.json"
            payload = self._request_json(url, allow_404=True, timeout=15)
            if isinstance(payload, dict):
                self._basket_host_cache[vol] = host
                return payload, host

        return None, None

    @staticmethod
    def price_from_u(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return round(float(value) / 100, 2)
        except Exception:
            return None

    def extract_characteristics(self, card_json: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not isinstance(card_json, dict):
            return {}

        found: Dict[str, Any] = {}

        def walk(node: Any) -> None:
            if isinstance(node, dict):
                for key, value in node.items():
                    if key.lower() in self.CHARACTERISTIC_KEYS and key not in found:
                        found[key] = value
                    walk(value)
            elif isinstance(node, list):
                for item in node:
                    walk(item)

        walk(card_json)
        return found

    def extract_country(self, data: Any) -> Optional[str]:
        def walk(node: Any) -> Optional[str]:
            if isinstance(node, dict):
                for key, value in node.items():
                    if key.lower() in self.COUNTRY_KEYS:
                        return stringify(value)

                name_candidate = first_not_none(
                    node.get("name"),
                    node.get("label"),
                    node.get("title"),
                    node.get("type"),
                    node.get("key"),
                )
                if isinstance(name_candidate, str) and name_candidate.strip().lower() in self.COUNTRY_KEYS:
                    for value_key in ("value", "values", "text", "data"):
                        if value_key in node:
                            return stringify(node[value_key])

                for value in node.values():
                    result = walk(value)
                    if result:
                        return result

            elif isinstance(node, list):
                for item in node:
                    result = walk(item)
                    if result:
                        return result

            return None

        return walk(data)

    @staticmethod
    def is_russia(country: Optional[str]) -> bool:
        if not country:
            return False
        normalized = re.sub(r"[^a-zа-я0-9 ]+", " ", country.lower())
        return "россия" in normalized or "российская федерация" in normalized or "russia" in normalized

    @staticmethod
    def build_product_link(nm_id: int) -> str:
        return f"https://www.wildberries.ru/catalog/{nm_id}/detail.aspx"

    @staticmethod
    def build_seller_link(seller_id: Any) -> Optional[str]:
        if seller_id is None:
            return None
        return f"https://www.wildberries.ru/seller/{seller_id}"

    def build_image_urls(
        self,
        nm_id: int,
        card_json: Optional[Dict[str, Any]],
        basket_host: Optional[str],
        pics_count: Optional[int],
    ) -> List[str]:
        # 1) если в JSON уже есть готовые photo url
        photos = first_not_none(
            nested_get(card_json, "media", "photos"),
            card_json.get("photos") if isinstance(card_json, dict) else None,
        )

        if isinstance(photos, list):
            ready = []
            for photo in photos:
                if isinstance(photo, dict):
                    url = first_not_none(
                        photo.get("big"),
                        photo.get("c516x688"),
                        photo.get("c246x328"),
                        photo.get("square"),
                        photo.get("tm"),
                    )
                    if url:
                        ready.append(url)
            if ready:
                return ready

        # 2) иначе строим по шаблону basket-хранилища
        if not basket_host or not pics_count:
            return []

        vol = nm_id // 100000
        part = nm_id // 1000
        return [
            f"https://{basket_host}/vol{vol}/part{part}/{nm_id}/images/big/{i}.webp"
            for i in range(1, int(pics_count) + 1)
        ]

    @staticmethod
    def extract_sizes(product: Optional[Dict[str, Any]], card_json: Optional[Dict[str, Any]]) -> List[str]:
        sizes = []

        source_sizes = first_not_none(
            product.get("sizes") if isinstance(product, dict) else None,
            card_json.get("sizes") if isinstance(card_json, dict) else None,
        )

        if not isinstance(source_sizes, list):
            return sizes

        for size in source_sizes:
            if not isinstance(size, dict):
                continue
            value = first_not_none(
                size.get("name"),
                size.get("origName"),
                size.get("techSize"),
            )
            if value is None:
                continue
            value = str(value).strip()
            if value and value != "0" and value not in sizes:
                sizes.append(value)

        return sizes

    @staticmethod
    def extract_stock_count(product: Optional[Dict[str, Any]]) -> Optional[int]:
        if not isinstance(product, dict):
            return None

        total_quantity = product.get("totalQuantity")
        if isinstance(total_quantity, int):
            return total_quantity

        qty = 0
        found = False
        for size in product.get("sizes", []):
            if not isinstance(size, dict):
                continue
            for stock in size.get("stocks", []):
                if not isinstance(stock, dict):
                    continue
                stock_qty = stock.get("qty")
                if isinstance(stock_qty, int):
                    qty += stock_qty
                    found = True
        return qty if found else None

    @staticmethod
    def extract_description(card_json: Optional[Dict[str, Any]]) -> Optional[str]:
        if not isinstance(card_json, dict):
            return None

        direct = first_not_none(
            card_json.get("description"),
            nested_get(card_json, "data", "description"),
            nested_get(card_json, "productInfo", "description"),
        )
        if isinstance(direct, str) and direct.strip():
            return direct.strip()

        # рекурсивный fallback
        def walk(node: Any) -> Optional[str]:
            if isinstance(node, dict):
                for key, value in node.items():
                    if key.lower() == "description" and isinstance(value, str) and value.strip():
                        return value.strip()
                for value in node.values():
                    result = walk(value)
                    if result:
                        return result
            elif isinstance(node, list):
                for item in node:
                    result = walk(item)
                    if result:
                        return result
            return None

        return walk(card_json)

    def collect_rows(self) -> List[Dict[str, Any]]:
        search_products = self.search_all_products()
        nm_ids = [item["id"] for item in search_products if item.get("id")]
        detail_map = self.fetch_details_batch(nm_ids)

        rows: List[Dict[str, Any]] = []

        for index, search_item in enumerate(search_products, start=1):
            nm_id = search_item["id"]
            detail_item = detail_map.get(nm_id, {})
            card_json, basket_host = self.resolve_card_json(nm_id)

            product_for_sizes_and_stock = detail_item if detail_item else search_item
            characteristics = self.extract_characteristics(card_json)
            country = first_not_none(
                self.extract_country(characteristics),
                self.extract_country(card_json),
            )

            seller_name = first_not_none(
                detail_item.get("supplier"),
                detail_item.get("supplierName"),
                search_item.get("supplier"),
                nested_get(card_json, "selling", "supplier_name"),
                nested_get(card_json, "selling", "supplierName"),
            )
            seller_id = first_not_none(
                detail_item.get("supplierId"),
                search_item.get("supplierId"),
                nested_get(card_json, "selling", "supplier_id"),
                nested_get(card_json, "selling", "supplierId"),
            )

            price = first_not_none(
                self.price_from_u(detail_item.get("salePriceU")),
                self.price_from_u(detail_item.get("priceU")),
                self.price_from_u(search_item.get("salePriceU")),
                self.price_from_u(search_item.get("priceU")),
                self.price_from_u(nested_get(card_json, "extended", "clientPriceU")),
                self.price_from_u(nested_get(card_json, "extended", "basicPriceU")),
            )

            rating = first_not_none(
                detail_item.get("nmReviewRating"),
                detail_item.get("reviewRating"),
                detail_item.get("rating"),
                search_item.get("nmReviewRating"),
                search_item.get("reviewRating"),
                search_item.get("rating"),
                card_json.get("reviewRating") if isinstance(card_json, dict) else None,
                card_json.get("rating") if isinstance(card_json, dict) else None,
            )

            reviews_count = first_not_none(
                detail_item.get("nmFeedbacks"),
                detail_item.get("feedbacks"),
                search_item.get("nmFeedbacks"),
                search_item.get("feedbacks"),
                card_json.get("feedbackCount") if isinstance(card_json, dict) else None,
            )

            pics_count = first_not_none(
                detail_item.get("pics"),
                search_item.get("pics"),
            )

            image_urls = self.build_image_urls(
                nm_id=nm_id,
                card_json=card_json,
                basket_host=basket_host,
                pics_count=pics_count,
            )

            sizes = self.extract_sizes(product_for_sizes_and_stock, card_json)
            stock_count = self.extract_stock_count(product_for_sizes_and_stock)
            description = self.extract_description(card_json)
            title = first_not_none(
                detail_item.get("name"),
                search_item.get("name"),
                card_json.get("imt_name") if isinstance(card_json, dict) else None,
            )

            row = {
                "Ссылка на товар": self.build_product_link(nm_id),
                "Артикул": nm_id,
                "Название": title,
                "Цена": price,
                "Описание": description,
                "Ссылки на изображения": ", ".join(image_urls),
                "Все характеристики (JSON)": json.dumps(characteristics, ensure_ascii=False, indent=2),
                "Название селлера": seller_name,
                "Ссылка на селлера": self.build_seller_link(seller_id),
                "Размеры товара": ", ".join(sizes),
                "Остатки по товару": stock_count,
                "Рейтинг": rating,
                "Количество отзывов": reviews_count,
                "Страна производства": country,
            }
            rows.append(row)

            print(f"[item] {index}/{len(search_products)} nm_id={nm_id}")
            self._sleep()

        return rows

    def save_outputs(self, rows: List[Dict[str, Any]]) -> Tuple[Path, Path]:
        df = pd.DataFrame(rows)

        columns_order = [
            "Ссылка на товар",
            "Артикул",
            "Название",
            "Цена",
            "Описание",
            "Ссылки на изображения",
            "Все характеристики (JSON)",
            "Название селлера",
            "Ссылка на селлера",
            "Размеры товара",
            "Остатки по товару",
            "Рейтинг",
            "Количество отзывов",
            "Страна производства",
        ]
        df = df[columns_order]

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = safe_filename(self.query)

        catalog_path = self.output_dir / f"{base}_catalog_{timestamp}.xlsx"
        filtered_path = self.output_dir / f"{base}_filtered_{timestamp}.xlsx"

        df.to_excel(catalog_path, index=False)

        filtered_df = df[
            (pd.to_numeric(df["Рейтинг"], errors="coerce") >= 4.5)
            & (pd.to_numeric(df["Цена"], errors="coerce") <= 10000)
            & (df["Страна производства"].apply(self.is_russia))
        ].copy()

        filtered_df.to_excel(filtered_path, index=False)

        return catalog_path, filtered_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Wildberries catalog parser")
    parser.add_argument(
        "--query",
        default="пальто из натуральной шерсти",
        help="Поисковый запрос",
    )
    parser.add_argument(
        "--dest",
        type=int,
        default=-1257786,
        help="Регион WB (по умолчанию: -1257786)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=100,
        help="Максимум страниц поиска",
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Папка для xlsx-файлов",
    )
    args = parser.parse_args()

    app = WildberriesCatalogParser(
        query=args.query,
        dest=args.dest,
        max_pages=args.max_pages,
        output_dir=args.output_dir,
    )

    rows = app.collect_rows()
    if not rows:
        print("Ничего не собрано.")
        return

    catalog_path, filtered_path = app.save_outputs(rows)
    print(f"\nГотово:")
    print(f"Полный каталог:   {catalog_path}")
    print(f"Отфильтрованный:  {filtered_path}")


if __name__ == "__main__":
    main()