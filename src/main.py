#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Apify Actor entry point for Facebook Page & Group Scraper.

Wraps the existing discovery.py and scraper.py so they run on the Apify platform.
Facebook login is required — credentials are read from actor input.

Input  → Actor.get_input()
Output → Actor.push_data()  (default dataset)
Images → Actor.set_value()  (key-value store, optional)
State  → Actor.set_value() / Actor.get_value() (key-value store)
"""

import asyncio
import json
import os
import sys
import logging
import random
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from apify import Actor

# ---------------------------------------------------------------------------
# Ensure the facebook-scraper root is importable (discovery.py, scraper.py …)
# ---------------------------------------------------------------------------
SCRIPTS_DIR = Path(__file__).parent.parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Region / location presets (business-oriented categories)
# ---------------------------------------------------------------------------
REGION_PRESETS: Dict[str, Dict] = {
    "us": {
        "categories": ["restaurant", "fitness", "retail", "beauty", "healthcare",
                       "real-estate", "entertainment", "education"],
        "locations": ["New York", "Los Angeles", "Miami", "Chicago", "Houston",
                      "Phoenix", "San Diego", "Dallas"],
    },
    "uk": {
        "categories": ["restaurant", "fitness", "retail", "beauty", "healthcare", "education"],
        "locations": ["London", "Manchester", "Birmingham", "Glasgow", "Leeds", "Liverpool"],
    },
    "ind": {
        "categories": ["restaurant", "fitness", "retail", "beauty", "healthcare",
                       "real-estate", "education", "fashion"],
        "locations": ["India", "Mumbai", "Delhi", "Bangalore", "Hyderabad",
                      "Chennai", "Kolkata", "Pune"],
    },
    "eur": {
        "categories": ["restaurant", "retail", "fitness", "beauty", "travel", "education"],
        "locations": ["Germany", "France", "Spain", "Italy", "Netherlands",
                      "Paris", "Berlin", "Amsterdam"],
    },
    "gulf": {
        "categories": ["restaurant", "retail", "beauty", "real-estate", "fitness", "education"],
        "locations": ["UAE", "Dubai", "Abu Dhabi", "Saudi Arabia", "Riyadh",
                      "Kuwait", "Qatar", "Doha"],
    },
    "east": {
        "categories": ["restaurant", "retail", "beauty", "fitness", "education", "entertainment"],
        "locations": ["Japan", "South Korea", "Thailand", "Indonesia", "Singapore",
                      "Malaysia", "Philippines", "Tokyo"],
    },
}


# ---------------------------------------------------------------------------
# Config builder
# ---------------------------------------------------------------------------
def build_config(actor_input: Dict) -> Dict:
    region = actor_input.get("region", "us").lower()
    preset = REGION_PRESETS.get(region, REGION_PRESETS["us"])

    categories = actor_input.get("categories") or preset["categories"]
    locations  = actor_input.get("locations")  or preset["locations"]

    google_api_key = actor_input.get("googleApiKey", "")
    google_cx      = actor_input.get("googleSearchEngineId", "")

    return {
        "proxy": {"enabled": False},
        "google_search": {
            "enabled": bool(google_api_key and google_cx),
            "api_key": google_api_key,
            "search_engine_id": google_cx,
            "queries_per_location": 3,
        },
        "scraper": {
            "headless": True,
            "min_likes": actor_input.get("minLikes", 1000),
            "download_thumbnails": actor_input.get("downloadThumbnails", False),
            "max_thumbnails": actor_input.get("maxThumbnailsPerPage", 6),
            "delay_between_pages": [5, 10],
            "timeout": 90000,
        },
        "cities": locations,
        "categories": categories,
    }


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------
def run_discovery(
    config: Dict,
    categories: List[str],
    locations: List[str],
    entity_type: str = "page",
) -> List[Dict]:
    """Use discovery.py to find Facebook page/group names."""
    from discovery import discover_pages_google, discover_pages_browser_sync

    found: List[Dict] = []
    seen: set = set()

    for location in locations:
        for category in categories:
            logger.info(f"Discovering {entity_type}s: {category} in {location}")
            try:
                if config["google_search"]["enabled"]:
                    names = discover_pages_google(location, category, entity_type, 10, config)
                else:
                    names = discover_pages_browser_sync(location, category, entity_type, 10)

                for n in names:
                    if n and n not in seen:
                        seen.add(n)
                        found.append({
                            "page_name": n,
                            "entity_type": entity_type,
                            "category": category,
                            "location": location,
                        })
            except Exception as exc:
                logger.warning(f"Discovery error ({location}/{category}): {exc}")

            import time
            time.sleep(random.uniform(1, 2))

    logger.info(f"Discovery complete. Found {len(found)} unique {entity_type}s.")
    return found


# ---------------------------------------------------------------------------
# Apify-aware scraper wrapper
# ---------------------------------------------------------------------------
class ApifyFacebookScraper:
    """
    Wraps FacebookScraper to:
    - inject Apify proxy into Playwright context
    - set Facebook credentials from actor input
    - push each scraped record to Apify dataset
    - optionally store thumbnails in KV store
    - respect maxPages limit
    """

    def __init__(
        self,
        proxy_url: Optional[str],
        fb_email: str,
        fb_password: str,
        min_likes: int,
        max_pages: int,
        download_thumbnails: bool,
        max_thumbnails: int,
        config_path: str,
    ):
        self.proxy_url           = proxy_url
        self.fb_email            = fb_email
        self.fb_password         = fb_password
        self.min_likes           = min_likes
        self.max_pages           = max_pages
        self.download_thumbnails = download_thumbnails
        self.max_thumbnails      = max_thumbnails
        self.config_path         = config_path
        self._scraped            = 0

    async def scrape_pages(self, pages: List[Any]) -> Dict:
        """Scrape a list of page/group dicts or name strings, push to dataset."""
        from scraper import (
            FacebookScraper,
            PageNotFoundException,
            PageSkippedException,
            RateLimitException,
            TemporaryBlockException,
        )

        stats = {"success": 0, "failed": 0, "skipped": 0}

        # Set credentials so FacebookScraper picks them up via os.getenv
        os.environ["FACEBOOK_EMAIL"]    = self.fb_email
        os.environ["FACEBOOK_PASSWORD"] = self.fb_password

        inst = FacebookScraper(config_path=Path(self.config_path))

        # Inject Apify proxy — Playwright needs split server/username/password keys,
        # NOT the raw URL returned by proxy_configuration.new_url().
        if self.proxy_url:
            inst.proxy_mgr.is_enabled = True
            inst.proxy_mgr.get_playwright_proxy = lambda: _parse_proxy_url(self.proxy_url)
            parsed = _parse_proxy_url(self.proxy_url)
            logger.info(f"Proxy injected: {parsed.get('server')} (credentials hidden)")

        await inst.start_browser(headless=True)

        logged_in = await inst.login()
        if not logged_in:
            logger.error("Facebook login failed. Cannot scrape.")
            await inst.cleanup()
            return stats

        try:
            for entry in pages:
                if self.max_pages and self._scraped >= self.max_pages:
                    logger.info(f"Reached maxPages limit ({self.max_pages}). Stopping.")
                    break

                if isinstance(entry, str):
                    page_name   = entry
                    entity_type = "page"
                    category    = ""
                    location    = ""
                else:
                    page_name   = entry.get("page_name", "")
                    entity_type = entry.get("entity_type", "page")
                    category    = entry.get("category", "")
                    location    = entry.get("location", "")

                if not page_name:
                    continue

                try:
                    profile = await inst.scrape_page(page_name, entity_type, category, location)

                    if not profile:
                        stats["skipped"] += 1
                        continue

                    # Secondary filter in case config min_likes differs
                    metric = (
                        profile.get("page_likes", 0)
                        if entity_type != "group"
                        else profile.get("members", 0)
                    )
                    if self.min_likes and metric < self.min_likes:
                        metric_label = "likes" if entity_type != "group" else "members"
                        logger.info(
                            f"Skipping {page_name}: {metric:,} {metric_label} < {self.min_likes:,}"
                        )
                        stats["skipped"] += 1
                        continue

                    # Handle thumbnails
                    if not self.download_thumbnails:
                        profile.pop("profile_pic_local", None)
                        profile.pop("cover_photo_local", None)
                        profile.pop("content_thumbnails_local", None)
                    else:
                        await self._store_thumbnails(profile, page_name)

                    await Actor.push_data(profile)
                    logger.info(
                        f"[{self._scraped + 1}] Pushed: {page_name} "
                        f"(likes={metric:,}, tier={profile.get('page_tier')})"
                    )
                    self._scraped += 1
                    stats["success"] += 1

                except PageNotFoundException:
                    logger.warning(f"Page not found: {page_name}")
                    stats["failed"] += 1
                except PageSkippedException:
                    logger.info(f"Page skipped: {page_name}")
                    stats["skipped"] += 1
                except RateLimitException:
                    logger.warning("Rate limited by Facebook — sleeping 120 s…")
                    await asyncio.sleep(120)
                    stats["failed"] += 1
                except TemporaryBlockException:
                    logger.error("Temporarily blocked by Facebook — stopping scrape.")
                    break
                except Exception as exc:
                    logger.error(f"Error scraping {page_name}: {exc}")
                    stats["failed"] += 1

                await asyncio.sleep(random.uniform(5, 10))

        finally:
            await inst.cleanup()

        return stats

    async def _store_thumbnails(self, profile: Dict, page_name: str):
        """Upload downloaded thumbnails to the Apify Key-Value store."""
        async def _upload(local_path: str, key: str):
            p = Path(local_path)
            if p.exists():
                with open(p, "rb") as fh:
                    data = fh.read()
                await Actor.set_value(key, data, content_type="image/jpeg")
                logger.debug(f"Stored: {key}")

        if profile.get("profile_pic_local"):
            await _upload(profile["profile_pic_local"], f"fb_{page_name}_profile")
        if profile.get("cover_photo_local"):
            await _upload(profile["cover_photo_local"], f"fb_{page_name}_cover")

        for i, local_path in enumerate(profile.get("content_thumbnails_local", []), 1):
            if local_path:
                await _upload(local_path, f"fb_{page_name}_content_{i}")


# ---------------------------------------------------------------------------
# Proxy helpers
# ---------------------------------------------------------------------------
def _parse_proxy_url(proxy_url: str) -> Dict[str, str]:
    """Break an Apify proxy URL into Playwright-friendly components.

    ``proxy_configuration.new_url()`` returns a URL with embedded credentials,
    e.g. ``http://user:pass@proxy.apify.com:8000``.
    Playwright does **not** parse credentials from the ``server`` value — it
    requires separate ``username`` / ``password`` keys.
    """
    from urllib.parse import urlparse

    parsed = urlparse(proxy_url)
    server = f"{parsed.scheme}://{parsed.hostname}:{parsed.port}"
    result: Dict[str, str] = {"server": server}
    if parsed.username:
        result["username"] = parsed.username
    if parsed.password:
        result["password"] = parsed.password
    return result


async def validate_proxy(proxy_url: str) -> bool:
    """Concurrent proxy connectivity check across multiple endpoints.

    Runs checks in parallel with ``asyncio.FIRST_COMPLETED`` so the function
    returns as soon as any probe succeeds.  Total wall-clock time is capped at
    ``overall_timeout`` ms to prevent Apify actor timeouts.

    Set env var ``SKIP_PROXY_VALIDATION=1`` to bypass this check entirely.
    """
    if os.getenv("SKIP_PROXY_VALIDATION"):
        logger.info("Skipping proxy validation (SKIP_PROXY_VALIDATION env var set)")
        return True

    from playwright.async_api import async_playwright

    endpoints = [
        "https://www.facebook.com",
        "https://www.google.com",
        "https://httpbin.org/ip",
    ]

    is_residential       = "RESIDENTIAL" in proxy_url.upper()
    per_endpoint_timeout = 30000 if is_residential else 20000  # ms
    overall_timeout      = 60000  # ms — total budget

    async def _check(ctx, endpoint: str) -> bool:
        try:
            logger.info(f"Validating proxy via {endpoint}…")
            page = await ctx.new_page()
            await page.goto(endpoint, timeout=per_endpoint_timeout, wait_until="domcontentloaded")
            await page.close()
            logger.info(f"✓ Proxy validation successful via {endpoint}")
            return True
        except Exception as exc:
            logger.debug(f"  Endpoint {endpoint} failed: {exc}")
            return False

    pw = None
    try:
        pw = await async_playwright().start()
        browser = await pw.chromium.launch(headless=True)
        proxy_opts = _parse_proxy_url(proxy_url)
        ctx = await browser.new_context(proxy=proxy_opts)

        tasks = [asyncio.create_task(_check(ctx, ep)) for ep in endpoints]
        done, pending = await asyncio.wait(
            tasks, timeout=overall_timeout / 1000, return_when=asyncio.FIRST_COMPLETED
        )

        success = any(t.result() for t in done)

        for task in pending:
            task.cancel()

        await browser.close()
        await pw.stop()

        if not success:
            logger.error("Proxy validation failed: no endpoint responded within budget")
        return success

    except Exception as exc:
        logger.error(f"Proxy validation error: {exc}")
        if pw:
            try:
                await pw.stop()
            except Exception:
                pass
        return False


# ---------------------------------------------------------------------------
# Main actor logic
# ---------------------------------------------------------------------------
async def main():
    async with Actor:
        # ----------------------------------------------------------------
        # 1. Read input
        # ----------------------------------------------------------------
        actor_input: Dict = await Actor.get_input() or {}
        logger.info(f"Actor input: {json.dumps(actor_input, indent=2, default=str)}")

        mode              = actor_input.get("mode", "full")
        entity_type       = actor_input.get("entityType", "page")
        region            = actor_input.get("region", "us").lower()
        min_likes         = actor_input.get("minLikes", 1000)
        max_pages         = actor_input.get("maxPages", 50)
        download_thumbs   = actor_input.get("downloadThumbnails", False)
        max_thumbs        = actor_input.get("maxThumbnailsPerPage", 6)
        page_names_input  = actor_input.get("pageNames", [])
        fb_email          = actor_input.get("facebookEmail", "")
        fb_password       = actor_input.get("facebookPassword", "")

        if not fb_email or not fb_password:
            await Actor.fail(
                status_message="facebookEmail and facebookPassword are required."
            )
            return

        # ----------------------------------------------------------------
        # 2. Proxy configuration
        # ----------------------------------------------------------------
        proxy_url: Optional[str] = None
        proxy_cfg_input = actor_input.get("proxyConfiguration")
        if proxy_cfg_input:
            try:
                proxy_configuration = await Actor.create_proxy_configuration(
                    actor_proxy_input=proxy_cfg_input
                )
                if proxy_configuration:
                    proxy_url = await proxy_configuration.new_url()
                safe = _parse_proxy_url(proxy_url).get("server")
                logger.info(f"Using Apify proxy: {safe}")
            except Exception as exc:
                logger.warning(f"Could not create proxy configuration: {exc}")

        # Validate proxy connectivity (unless user opts out)
        if proxy_url:
            if actor_input.get("skipProxyValidation", False):
                logger.info("Skipping proxy validation (skipProxyValidation=true)")
            else:
                logger.info("Validating proxy connectivity…")
                ok = await validate_proxy(proxy_url)
                if not ok:
                    if actor_input.get("dropProxyOnFailure", False):
                        logger.warning(
                            "Proxy validation failed — removing proxy and running direct."
                        )
                        proxy_url = None
                    else:
                        logger.warning(
                            "Proxy validation failed — continuing with proxy anyway. "
                            "Set dropProxyOnFailure=true to disable it on failure."
                        )

        # ----------------------------------------------------------------
        # 3. Build config and write temp config file
        # ----------------------------------------------------------------
        config = build_config(actor_input)
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        )
        json.dump(config, tmp)
        tmp.close()
        config_path = tmp.name
        logger.info(f"Wrote temp config to {config_path}")

        preset     = REGION_PRESETS.get(region, REGION_PRESETS["us"])
        categories = actor_input.get("categories") or preset["categories"]
        locations  = actor_input.get("locations")  or preset["locations"]

        # ----------------------------------------------------------------
        # 4. Execute based on mode
        # ----------------------------------------------------------------
        pages_to_scrape: List[Any] = []

        if mode == "scrape_pages":
            if not page_names_input:
                await Actor.fail(
                    status_message="mode=scrape_pages requires pageNames to be set."
                )
                return
            pages_to_scrape = page_names_input
            logger.info(f"scrape_pages mode: {len(pages_to_scrape)} page names provided")

        elif mode == "discovery_only":
            logger.info("discovery_only mode: discovering pages and pushing names to dataset…")
            discovered = run_discovery(config, categories, locations, entity_type)
            logger.info(f"Discovered {len(discovered)} pages")
            for p in discovered:
                await Actor.push_data(p)
            logger.info("Discovery complete. Exiting.")
            return

        else:  # full
            logger.info("full mode: discovering pages then scraping…")

            state_key = f"fb_state_{region}_{entity_type}"
            state = await Actor.get_value(state_key) or {}

            if state.get("pages") and state.get("phase") not in ("completed", None):
                logger.info(f"Resuming from saved state ({len(state['pages'])} pages)")
                pages_to_scrape = state["pages"]
            else:
                discovered = run_discovery(config, categories, locations, entity_type)
                logger.info(f"Discovery found {len(discovered)} pages")
                pages_to_scrape = discovered
                await Actor.set_value(
                    state_key,
                    {
                        "pages": pages_to_scrape,
                        "phase": "scraping",
                        "discovered_at": datetime.now(timezone.utc).isoformat(),
                    },
                )

        # ----------------------------------------------------------------
        # 5. Scrape pages
        # ----------------------------------------------------------------
        if not pages_to_scrape:
            logger.warning("No pages to scrape. Finishing.")
            return

        logger.info(
            f"Starting to scrape {len(pages_to_scrape)} {entity_type}s "
            f"(max={max_pages or 'unlimited'})…"
        )

        scraper_wrapper = ApifyFacebookScraper(
            proxy_url=proxy_url,
            fb_email=fb_email,
            fb_password=fb_password,
            min_likes=min_likes,
            max_pages=max_pages,
            download_thumbnails=download_thumbs,
            max_thumbnails=max_thumbs,
            config_path=config_path,
        )

        stats = await scraper_wrapper.scrape_pages(pages_to_scrape)

        logger.info(
            f"Scraping complete — success={stats['success']}, "
            f"failed={stats['failed']}, skipped={stats['skipped']}"
        )

        # Mark state as completed
        if mode == "full":
            await Actor.set_value(
                f"fb_state_{region}_{entity_type}",
                {
                    "phase": "completed",
                    "completed_at": datetime.now(timezone.utc).isoformat(),
                    "stats": stats,
                },
            )

        # ----------------------------------------------------------------
        # 6. Clean up temp config
        # ----------------------------------------------------------------
        try:
            Path(config_path).unlink(missing_ok=True)
        except Exception:
            pass


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    asyncio.run(main())
