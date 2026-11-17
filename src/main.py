import asyncio
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import parse_qs, urlencode, urlparse

from apify import Actor
from curl_cffi.requests import AsyncSession
from pydantic import BaseModel, Field


class ScrapedRecord(BaseModel):
    item_id: str
    title: str
    price: Optional[float] = None
    currency: str = "EUR"
    location: Optional[str] = None
    url: str
    raw_metadata: dict[str, Any] = Field(default_factory=dict)
    scraped_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class WallapopScraperEngine:
    API_SEARCH_URL = "https://api.wallapop.com/api/v3/search"

    def __init__(self, max_items: int, max_concurrency: int, proxy_config: Any):
        self.max_items = max_items
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.proxy_config = proxy_config
        self.seen_ids: set[str] = set()
        self.total_pushed = 0
        self.lock = asyncio.Lock()

    async def _get_proxy_url(self) -> Optional[str]:
        if not self.proxy_config:
            return None
        try:
            return await self.proxy_config.new_url()
        except Exception:
            return None

    def _build_api_url(self, user_url: str, next_page: Optional[str] = None) -> str:
        if next_page:
            return f"{self.API_SEARCH_URL}?{urlencode({'next_page': next_page})}"

        parsed = urlparse(user_url)
        qs = parse_qs(parsed.query)

        params: dict[str, str] = {
            "source": "search_box",
            "latitude": qs.get("latitude", ["40.4168"])[0],
            "longitude": qs.get("longitude", ["-3.7038"])[0],
        }

        for key in (
            "keywords",
            "category_ids",
            "min_sale_price",
            "max_sale_price",
            "distance",
            "order_by",
            "condition",
        ):
            if key in qs and qs[key]:
                params[key] = qs[key][0]

        if "keywords" not in params and parsed.path:
            slug = parsed.path.rstrip("/").split("/")[-1]
            if slug and slug not in ("search", "app"):
                params["keywords"] = slug.replace("-", " ")

        return f"{self.API_SEARCH_URL}?{urlencode(params)}"

    async def fetch_json_with_retry(
        self, api_url: str, retries: int = 4
    ) -> tuple[Optional[dict[str, Any]], Optional[str]]:
        async with self.semaphore:
            for attempt in range(1, retries + 1):
                proxy_url = await self._get_proxy_url()
                try:
                    async with AsyncSession(
                        impersonate="chrome124",
                        proxy=proxy_url,
                        timeout=25,
                    ) as session:
                        headers = {
                            "Accept": "application/json, text/plain, */*",
                            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
                            "X-DeviceOS": "0",
                            "Origin": "https://es.wallapop.com",
                            "Referer": "https://es.wallapop.com/",
                        }
                        response = await session.get(api_url, headers=headers)

                        if response.status_code == 200:
                            next_token = response.headers.get("x-nextpage")
                            return response.json(), next_token

                        if response.status_code in (403, 429, 503):
                            Actor.log.warning(
                                f"[HTTP {response.status_code}] Límite temporal en Wallapop. Reintento {attempt}/{retries}..."
                            )
                        else:
                            Actor.log.error(
                                f"[HTTP {response.status_code}] Error inesperado: {response.text[:200]}"
                            )
                except Exception as exc:
                    Actor.log.warning(
                        f"Excepción de red (intento {attempt}/{retries}): {exc}"
                    )

                await asyncio.sleep(1.5 ** attempt)

            return None, None

    def parse_items(self, payload: dict[str, Any]) -> list[ScrapedRecord]:
        records: list[ScrapedRecord] = []

        data = payload.get("data", {})
        section = data.get("section", {})
        sec_payload = section.get("payload", {})
        items = sec_payload.get("items") or payload.get("search_objects") or []

        for item in items:
            item_id = str(item.get("id", "")).strip()
            title = str(item.get("title", "")).strip()
            if not item_id or not title:
                continue

            raw_price = item.get("price")
            price_val: Optional[float] = None
            currency = "EUR"

            if isinstance(raw_price, dict):
                price_val = raw_price.get("amount")
                currency = raw_price.get("currency", "EUR")
            elif isinstance(raw_price, (int, float)):
                price_val = float(raw_price)

            loc_data = item.get("location") or {}
            city = loc_data.get("city", "")
            postal = loc_data.get("postal_code", "")
            location_str = f"{city} ({postal})".strip(" ()") if (city or postal) else None

            slug = item.get("web_slug") or item_id
            item_url = f"https://es.wallapop.com/item/{slug}"

            images = item.get("images") or []
            first_img = None
            if images and isinstance(images[0], dict):
                urls_dict = images[0].get("urls") or {}
                first_img = urls_dict.get("big") or urls_dict.get("medium")

            flags = item.get("flags") or {}
            user_info = item.get("user") or {}

            records.append(
                ScrapedRecord(
                    item_id=item_id,
                    title=title,
                    price=price_val,
                    currency=currency,
                    location=location_str,
                    url=item_url,
                    raw_metadata={
                        "description": item.get("description", ""),
                        "reserved": bool(flags.get("reserved", False)),
                        "sold": bool(flags.get("sold", False)),
                        "seller_id": user_info.get("id"),
                        "image_url": first_img,
                    },
                )
            )

        return records

    async def process_search_url(self, user_url: str) -> None:
        next_page: Optional[str] = None
        page_num = 1

        while True:
            if self.max_items > 0 and self.total_pushed >= self.max_items:
                break

            api_url = self._build_api_url(user_url, next_page=next_page)
            Actor.log.info(f"Extrayendo página {page_num} para: {user_url}")

            payload, header_next_page = await self.fetch_json_with_retry(api_url)
            if not payload:
                break

            extracted = self.parse_items(payload)
            if not extracted:
                Actor.log.info("No hay más anuncios disponibles en esta búsqueda.")
                break

            batch_to_push: list[dict[str, Any]] = []
            async with self.lock:
                for record in extracted:
                    if self.max_items > 0 and self.total_pushed >= self.max_items:
                        break
                    if record.item_id in self.seen_ids:
                        continue

                    self.seen_ids.add(record.item_id)
                    batch_to_push.append(record.model_dump())
                    self.total_pushed += 1

            if batch_to_push:
                await Actor.push_data(batch_to_push)
                try:
                    await Actor.charge(
                        event_name="result-scraped", count=len(batch_to_push)
                    )
                except Exception:
                    pass

                Actor.log.info(
                    f"Guardados {len(batch_to_push)} anuncios (Total acumulado: {self.total_pushed})."
                )

            meta_next = payload.get("meta", {}).get("next_page")
            next_page = header_next_page or meta_next
            if not next_page:
                break

            page_num += 1
            await asyncio.sleep(0.5)


async def main() -> None:
    async with Actor:
        actor_input = await Actor.get_input() or {}
        start_urls_raw = actor_input.get("startUrls", [])
        max_items = int(actor_input.get("maxItems", 40))
        max_concurrency = int(actor_input.get("maxConcurrency", 3))
        proxy_settings = actor_input.get("proxyConfiguration")

        urls = [
            entry["url"] if isinstance(entry, dict) else entry
            for entry in start_urls_raw
            if entry
        ]

        if not urls:
            await Actor.fail(status_message="Debes introducir al menos una URL de búsqueda de Wallapop.")
            return

        proxy_config = None
        if proxy_settings and proxy_settings.get("useApifyProxy", True):
            try:
                proxy_config = await Actor.create_proxy_configuration(
                    actor_proxy_input=proxy_settings
                )
            except Exception as exc:
                Actor.log.warning(
                    f"No se pudo activar el proxy solicitado ({exc}). Usando proxy estándar..."
                )
                try:
                    proxy_config = await Actor.create_proxy_configuration()
                except Exception:
                    proxy_config = None

        engine = WallapopScraperEngine(
            max_items=max_items,
            max_concurrency=max_concurrency,
            proxy_config=proxy_config,
        )

        tasks = [engine.process_search_url(url) for url in urls]
        await asyncio.gather(*tasks)

        if engine.total_pushed == 0:
            await Actor.fail(
                status_message="0 anuncios extraídos. Prueba activando proxies residenciales o revisa la URL."
            )
        else:
            Actor.log.info(
                f"Extracción completada con éxito. Total anuncios entregados: {engine.total_pushed}"
            )


if __name__ == "__main__":
    asyncio.run(main())
