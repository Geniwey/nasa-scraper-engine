# Wallapop Real-Time Scraper & Price Monitor API

Ultra-fast, high-concurrency data extractor for **Wallapop** (`es.wallapop.com`, `it.wallapop.com`, `pt.wallapop.com`). Built without headless browsers using TLS-fingerprinted HTTP/2 requests for sub-second latency and minimal compute cost.

## Key Features
- **No Browser Overhead:** Direct API extraction with Chrome TLS spoofing—10x faster and cheaper than Playwright/Puppeteer scrapers.
- **Automatic Pagination:** Crawls through search result pages automatically until reaching your `maxItems` limit.
- **Clean Structured Output:** Returns validated prices, currencies, cities, postal codes, direct listing URLs, images, and reservation flags.
- **Automation Ready:** Seamlessly integrates with **n8n**, **Make**, **Zapier**, Python, and Node.js.

## Use Cases
- **Resale & Arbitrage Monitoring:** Detect underpriced electronics, watches, vehicles, and collectibles the moment they are listed.
- **Market & Price Intelligence:** Track second-hand market supply, average pricing, and regional demand across Spain, Italy, and Portugal.
- **Automated Alerts:** Connect to Telegram or Slack via n8n/Make to trigger instant deal notifications.

## Input Parameters
| Parameter | Type | Description |
| :--- | :--- | :--- |
| `startUrls` | Array | One or more Wallapop search URLs (e.g., `https://es.wallapop.com/search?keywords=iphone+15`). |
| `maxItems` | Integer | Maximum number of listings to extract (`0` = unlimited). |
| `maxConcurrency` | Integer | Number of parallel search workers (`1` to `10`). |
| `proxyConfiguration` | Object | Apify Proxy configuration (Residential proxies recommended for heavy scraping). |

## Example Output (JSON)
```json
{
  "item_id": "987654321",
  "title": "iPhone 15 Pro 128GB Titanio Natural",
  "price": 790.0,
  "currency": "EUR",
  "location": "Barcelona (08032)",
  "url": "[https://es.wallapop.com/item/iphone-15-pro-128gb-titanio-natural-987654321](https://es.wallapop.com/item/iphone-15-pro-128gb-titanio-natural-987654321)",
  "raw_metadata": {
    "description": "En perfecto estado, siempre con funda y protector.",
    "reserved": false,
    "sold": false,
    "seller_id": "12345678",
    "image_url": "[https://cdn.wallapop.com/images/](https://cdn.wallapop.com/images/)..."
  },
  "scraped_at": "2026-11-17T19:15:00.123456+00:00"
}
