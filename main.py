import asyncio
import hashlib
import re
from datetime import datetime, timezone
from typing import Any, Optional

from apify import Actor
from curl_cffi.requests import AsyncSession
from pydantic import BaseModel, Field, field_validator
from selectolax.parser import HTMLParser


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

    @field_validator("title")
    @classmethod
    def clean_title(cls, v: str) -> str:
        return re.sub(r"\s+", " ", v).strip()


class EnterpriseScraperEngine:
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
        return await self.proxy_config.new_url()

    async def fetch_with_retry(
        self, url: str, retries: int = 4
    ) -> Optional[str]:
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
                            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json,*/*;q=0.8",
                            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
                            "Cache-Control": "no-cache",
                        }
                        response = await session.get(url, headers=headers)

                        if response.status_code == 200:
                            return response.text

                        if response.status_code in (403, 429, 503):
                            Actor.log.warning(
                                f"[HTTP {response.status_code}] Bloqueo temporal en {url}. Reintento {attempt}/{retries}..."
                            )
                        else:
                            Actor.log.error(
                                f"[HTTP {response.status_code}] Error inesperado en {url}"
                            )
                except Exception as exc:
                    Actor.log.warning(
                        f"Excepción de red en {url} (intento {attempt}/{retries}): {exc}"
                    )

                await asyncio.sleep(1.5 ** attempt)

            Actor.log.error(f"Fallo definitivo tras {retries} intentos en: {url}")
            return None

    def parse_payload(self, html_or_json: str, base_url: str) -> list[ScrapedRecord]:
        """
        ZONA DE EXTRACCIÓN ESPECÍFICA DEL PORTAL OBJETIVO.
        Aquí es donde la IA adapta los selectores CSS o claves JSON de la web elegida.
        """
        records: list[ScrapedRecord] = []
        tree = HTMLParser(html_or_json)

        # Ejemplo estructurado sobre tarjetas genéricas de listado
        for card in tree.css("article, .item-card, [data-item-id]"):
            title_node = card.css_first("h2, h3, .title, a[title]")
            link_node = card.css_first("a[href]")
            price_node = card.css_first(".price, [data-price], .amount")
            loc_node = card.css_first(".location, .city, .zone")

            if not title_node or not link_node:
                continue

            raw_url = link_node.attributes.get("href", "")
            full_url = (
                raw_url
                if raw_url.startswith("http")
                else f"{base_url.rstrip('/')}/{raw_url.lstrip('/')}"
            )

            raw_id = card.attributes.get("data-item-id") or hashlib.sha256(
                full_url.encode()
            ).hexdigest()[:16]

            parsed_price = None
            if price_node:
                digits = re.sub(r"[^\d.,]", "", price_node.text()).replace(",", ".")
                try:
                    parsed_price = float(digits)
                except ValueError:
                    parsed_price = None

            records.append(
                ScrapedRecord(
                    item_id=raw_id,
                    title=title_node.text(),
                    price=parsed_price,
                    location=loc_node.text().strip() if loc_node else None,
                    url=full_url,
                )
            )

        return records

    async def process_url(self, url: str) -> None:
        if self.max_items > 0 and self.total_pushed >= self.max_items:
            return

        Actor.log.info(f"Procesando objetivo: {url}")
        content = await self.fetch_with_retry(url)
        if not content:
            return

        extracted = self.parse_payload(content, url)
        if not extracted:
            Actor.log.warning(f"0 registros extraídos en {url}. Revisar selectores.")
            return

        batch_to_push: list[dict[str, Any]] = []
        async with self.lock:
            for item in extracted:
                if self.max_items > 0 and self.total_pushed >= self.max_items:
                    break
                if item.item_id in self.seen_ids:
                    continue

                self.seen_ids.add(item.item_id)
                batch_to_push.append(item.model_dump())
                self.total_pushed += 1

        if batch_to_push:
            await Actor.push_data(batch_to_push)
            try:
                await Actor.charge(
                    event_name="result-scraped", count=len(batch_to_push)
                )
            except Exception:
                # Permite pruebas locales o gratuitas si Pay-Per-Event aún no está activo
                pass

            Actor.log.info(
                f"Guardados {len(batch_to_push)} registros limpios (Total: {self.total_pushed})."
            )


async def main() -> None:
    async with Actor:
        actor_input = await Actor.get_input() or {}
        start_urls_raw = actor_input.get("startUrls", [])
        max_items = int(actor_input.get("maxItems", 50))
        max_concurrency = int(actor_input.get("maxConcurrency", 5))
        proxy_settings = actor_input.get("proxyConfiguration")

        urls = [
            entry["url"] if isinstance(entry, dict) else entry
            for entry in start_urls_raw
            if entry
        ]

        if not urls:
            await Actor.fail(status_message="No se proporcionaron URLs válidas en startUrls.")
            return

        proxy_config = (
            await Actor.create_proxy_configuration(actor_proxy_input=proxy_settings)
            if proxy_settings
            else None
        )

        engine = EnterpriseScraperEngine(
            max_items=max_items,
            max_concurrency=max_concurrency,
            proxy_config=proxy_config,
        )

        tasks = [engine.process_url(url) for url in urls]
        await asyncio.gather(*tasks)

        if engine.total_pushed == 0:
            await Actor.fail(
                status_message="Ejecución finalizada con 0 resultados. Posible cambio en el DOM/API del objetivo."
            )
        else:
            Actor.log.info(
                f"Misión completada con éxito. Total registros entregados: {engine.total_pushed}"
            )


if __name__ == "__main__":
    asyncio.run(main())
