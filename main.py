from apify import Actor
from pydantic import BaseModel, Field
from loguru import logger
from playwright.async_api import async_playwright
import asyncio
import re
import json
import urllib.parse
import httpx
from bs4 import BeautifulSoup


class InputSchema(BaseModel):
    search: str = Field(..., description="Keyword or business type to search for.")
    max_results: int = Field(100, description="Maximum number of businesses to extract.")
    language: str = Field("en", description="Language for Google Maps interface.")
    country: str = Field("US", description="Country code for localized results.")
    include_website_data: bool = Field(False, description="Scrape company emails and phones from their website.")


async def extract_emails_and_phones(website_url: str):
    """Fetch website and extract emails/phones."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(website_url, follow_redirects=True)
            soup = BeautifulSoup(r.text, "html.parser")
            emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", r.text)
            phones = re.findall(r"\+?\d[\d\s().-]{7,}", r.text)
            return {"email": emails[0] if emails else None, "phone": phones[0] if phones else None}
    except Exception:
        return {"email": None, "phone": None}


async def scrape_google_maps(playwright, search_query, max_results, lang, country, include_website_data):
    results = []
    browser = await playwright.chromium.launch(headless=True)
    context = await browser.new_context(locale=lang)
    page = await context.new_page()
    encoded_query = urllib.parse.quote(search_query)
    maps_url = f"https://www.google.com/maps/search/{encoded_query}/?hl={lang}"
    logger.info(f"Navigating to {maps_url}")
    await page.goto(maps_url)
    await page.wait_for_selector("div[role='article']", timeout=15000)

    last_height = 0
    while len(results) < max_results:
        listings = await page.query_selector_all("div[role='article']")
        for el in listings[len(results):]:
            try:
                title = await el.query_selector_eval("div[aria-level='3']", "el => el.textContent", timeout=5000)
                category = await el.query_selector_eval("span[jsinstance]", "el => el.textContent", timeout=2000) if await el.query_selector("span[jsinstance]") else None
                url = await el.query_selector_eval("a", "el => el.href", timeout=2000)
                place_id = re.search(r"placeid=([^&]+)", url)
                await el.click()
                await page.wait_for_selector("h1 span", timeout=10000)
                name = await page.locator("h1 span").inner_text()
                address = await page.locator("button[data-item-id*='address']").inner_text() if await page.locator("button[data-item-id*='address']").count() > 0 else None
                phone = await page.locator("button[data-item-id*='phone']").inner_text() if await page.locator("button[data-item-id*='phone']").count() > 0 else None
                website = await page.locator("a[data-item-id*='authority']").get_attribute("href") if await page.locator("a[data-item-id*='authority']").count() > 0 else None
                rating = float(await page.locator("span[aria-label*='stars']").inner_text().split()[0]) if await page.locator("span[aria-label*='stars']").count() > 0 else None
                review_count = int(re.sub(r"\D", "", await page.locator("span[aria-label*='reviews']").inner_text())) if await page.locator("span[aria-label*='reviews']").count() > 0 else 0
                coordinates_match = re.search(r"@([-.\d]+),([-.\d]+)", page.url)
                lat, lng = (float(coordinates_match.group(1)), float(coordinates_match.group(2))) if coordinates_match else (None, None)

                data = {
                    "title": name or title,
                    "category": category,
                    "place_id": place_id.group(1) if place_id else None,
                    "url": page.url,
                    "address": address,
                    "coordinates": {"lat": lat, "lng": lng},
                    "phone": phone,
                    "website": website,
                    "rating": rating,
                    "review_count": review_count
                }

                if include_website_data and website:
                    extra = await extract_emails_and_phones(website)
                    data.update(extra)

                results.append(data)
                await Actor.push_data(data)

                if len(results) >= max_results:
                    break

            except Exception as e:
                logger.warning(f"Failed to parse element: {e}")
                continue

        # Scroll to load more
        await page.mouse.wheel(0, 2000)
        await asyncio.sleep(2)
        current_height = await page.evaluate("document.body.scrollHeight")
        if current_height == last_height:
            break
        last_height = current_height

    await browser.close()
    return results


async def main():
    async with Actor:
        actor_input = await Actor.get_input() or {}
        args = InputSchema(**actor_input)
        logger.info(f"Starting Google Places Crawler for: {args.search}")
        async with async_playwright() as p:
            results = await scrape_google_maps(
                p,
                args.search,
                args.max_results,
                args.language,
                args.country,
                args.include_website_data,
            )
        logger.success(f"Scraping completed. Total: {len(results)} results.")
        await Actor.set_value("OUTPUT", results)


if __name__ == "__main__":
    asyncio.run(main())
