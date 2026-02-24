#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Facebook Page & Group Discovery Script
Discovers Facebook pages and groups using Google Custom Search API
Outputs queue files for the Facebook browser scraper
"""

import sys
import io
import json
import logging
import os
import re
import time
import random
import asyncio
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
import requests

# Force UTF-8 encoding for stdout/stderr on Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Base directory for the skill
BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / 'config' / 'scraper_config.json'
QUEUE_DIR = BASE_DIR / 'data' / 'queue'

# Facebook URL patterns to exclude (non-page/group URLs)
FACEBOOK_BLACKLIST = [
    'login', 'help', 'policies', 'legal', 'privacy', 'terms',
    'marketplace', 'watch', 'gaming', 'events', 'stories',
    'pages', 'groups', 'friends', 'bookmarks', 'notifications',
    'settings', 'recover', 'signup', 'checkpoint', 'photo',
    'photo.php', 'video', 'reel', 'reels', 'share', 'sharer',
    'sharer.php', 'dialog', 'ads', 'business', 'about',
    'developers', 'messenger', 'lite', 'fundraisers',
    # common Bing / Facebook redirect / UI paths that slip through
    'l.php', 'l', 'r', 'security', 'ajax', 'ajax.php', 'tr',
    'extern', 'plugins', 'common', 'platform', 'xd_arbiter',
    'translations', 'rsrc.php', 'static', 'images', 'css',
    'blank', 'search', 'hashtag', 'public', 'undefined',
    'composer', 'toolbar', 'profile.php', 'home.php', 'pg',
    'media', 'permalink.php', 'p', 'posts', 'contact',
    'videos', 'photos', 'notes', 'reviews', 'community',
    'offers', 'jobs', 'services', 'info', 'mentions', 'tagged',
]


def load_config(config_path: Path = None) -> Dict:
    """Load configuration from JSON file"""
    if config_path is None:
        config_path = CONFIG_PATH
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Could not load config: {e}. Using defaults.")
        return {
            'cities': ['New York', 'Los Angeles', 'Miami'],
            'categories': ['restaurant', 'retail', 'fitness', 'real-estate', 'healthcare', 'beauty'],
            'google_search': {
                'enabled': True,
                'api_key': '',
                'search_engine_id': '',
                'queries_per_location': 3
            }
        }


def discover_pages_google(
    location: str, 
    category: str, 
    entity_type: str = 'page',
    num_results: int = 10, 
    config: Dict = None
) -> List[str]:
    """
    Discover Facebook pages/groups using Google Custom Search API
    
    Args:
        location: Location/city to search (e.g., 'New York', 'Miami')
        category: Category to search (e.g., 'restaurant', 'fitness')
        entity_type: Type of entity to discover ('page', 'group', 'profile')
        num_results: Number of results to fetch per query (max 10)
        config: Configuration dictionary (optional)
    
    Returns:
        List of Facebook page/group names
    """
    try:
        if config is None:
            config = load_config()
        
        google_config = config.get('google_search', {})
        if not google_config.get('enabled', False):
            logger.warning("Google Search API is disabled in config")
            return []
        
        api_key = google_config.get('api_key')
        cx = google_config.get('search_engine_id')
        queries_per_location = google_config.get('queries_per_location', 3)
        
        if not api_key or not cx:
            logger.warning("Google API key or Search Engine ID not configured. Using Chromium browser search (Google) fallback.")
            return discover_pages_browser_sync(location, category, entity_type, num_results)
        
        # Generate search queries based on entity type
        if entity_type == 'group':
            search_queries = [
                f'site:facebook.com/groups "{location}" "{category}"',
                f'site:facebook.com/groups "{location}" {category} community',
                f'site:facebook.com/groups {category} "{location}"',
            ][:queries_per_location]
        else:
            search_queries = [
                f'site:facebook.com "{location}" "{category}" page',
                f'site:facebook.com "{location}" {category} business',
                f'site:facebook.com {category} "{location}"',
            ][:queries_per_location]
        
        all_names = []
        
        for query in search_queries:
            try:
                logger.info(f"Searching Google: '{query}'")
                
                # Make API request
                url = "https://www.googleapis.com/customsearch/v1"
                params = {
                    'key': api_key,
                    'cx': cx,
                    'q': query,
                    'num': min(num_results, 10)  # Google API max is 10
                }
                
                response = requests.get(url, params=params, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    total_results = data.get('searchInformation', {}).get('totalResults', 0)
                    logger.info(f"  Found {total_results} total results")
                    
                    # Extract Facebook page/group names from URLs
                    for item in data.get('items', []):
                        link = item.get('link', '')
                        name = _extract_facebook_name(link, entity_type)
                        if name:
                            all_names.append(name)
                            logger.info(f"  Found: {name}")
                    
                elif response.status_code == 429:
                    logger.warning("Google API rate limit reached")
                    break
                else:
                    logger.warning(f"Google API error {response.status_code} for query: {query}")
                
                # Small delay between queries
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Error processing query '{query}': {e}")
                continue
        
        # Remove duplicates and return
        unique_names = list(set(all_names))
        logger.info(f"✅ Discovered {len(unique_names)} unique Facebook {entity_type}s from Google")
        return unique_names
            
    except Exception as e:
        logger.error(f"Error in Google Facebook discovery: {e}")
        return []


def _extract_facebook_name(url: str, entity_type: str = 'page') -> Optional[str]:
    """
    Extract Facebook page/group name from a URL
    
    Handles:
        - facebook.com/pagename
        - facebook.com/groups/groupname
        - facebook.com/profile.php?id=12345
    """
    if not url or 'facebook.com' not in url:
        return None
    
    if entity_type == 'group':
        # Match facebook.com/groups/groupname
        match = re.search(r'facebook\.com/groups/([a-zA-Z0-9._-]+)/?', url)
        if match:
            name = match.group(1)
            if name not in FACEBOOK_BLACKLIST and len(name) > 2:
                return name
    else:
        # Match facebook.com/pagename (not groups, not profile.php)
        # First try: direct page name
        match = re.search(r'facebook\.com/([a-zA-Z0-9._-]{3,60})/?', url)
        if match:
            name = match.group(1)
            # Must have at least 3 consecutive alphanumeric chars (filters out '...' etc)
            if (name.lower() not in FACEBOOK_BLACKLIST
                    and len(name) > 2
                    and not name.isdigit()
                    and re.search(r'[a-zA-Z0-9]{3,}', name)):
                return name

        # Second try: profile.php?id=
        match = re.search(r'facebook\.com/profile\.php\?id=(\d+)', url)
        if match:
            return f"profile.php?id={match.group(1)}"

    return None


async def _discover_pages_browser_async(
    location: str,
    category: str,
    entity_type: str = 'page',
    num_results: int = 10
) -> List[str]:
    """
    Discover Facebook pages/groups by driving a real Chromium browser through Google.
    Uses the same anti-detection fingerprinting as the scraper.
    Google returns far better Facebook results than Bing for niche queries.
    """
    from playwright.async_api import async_playwright
    from urllib.parse import quote_plus

    try:
        from anti_detection import BrowserFingerprint
        fp_mgr = BrowserFingerprint(BASE_DIR / 'data')
        fingerprint = fp_mgr.get_random_fingerprint()
        ctx_opts = fp_mgr.get_context_options(fingerprint)
        stealth_js = fp_mgr.get_stealth_scripts(fingerprint)
    except Exception:
        ctx_opts = {
            'viewport': {'width': 1280, 'height': 800},
            'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'locale': 'en-US',
        }
        stealth_js = "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"

    # Build search queries — diverse mix to find pages, profiles, bloggers
    if entity_type == 'group':
        queries = [
            f'site:facebook.com/groups {location} {category}',
            f'{location} {category} facebook group',
        ]
    elif entity_type == 'profile':
        queries = [
            f'site:facebook.com {location} {category}',
            f'{location} {category} facebook profile',
            f'{location} {category} facebook',
        ]
    else:
        # Default: mix pages + profiles to get both types
        queries = [
            f'site:facebook.com {location} {category}',
            f'{location} {category} facebook',
            f'{location} {category} facebook profile',
        ]

    found_names: set = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
            ]
        )
        context = await browser.new_context(**ctx_opts)
        page = await context.new_page()
        await page.add_init_script(stealth_js)

        for query in queries:
            if len(found_names) >= num_results:
                break
            try:
                logger.info(f"Google search: '{query}'")
                search_url = f'https://www.google.com/search?q={quote_plus(query)}&num=20'
                await page.goto(search_url, wait_until='domcontentloaded', timeout=25000)
                await asyncio.sleep(random.uniform(2.5, 4))

                html = await page.content()

                # Detect CAPTCHA / unusual traffic block
                if 'captcha' in html.lower() or 'unusual traffic' in html.lower():
                    logger.warning("  Google CAPTCHA detected — pausing 30s")
                    await asyncio.sleep(30)
                    continue

                # --- Strategy 1: <cite> breadcrumbs (Google shows real URLs) ---
                try:
                    cite_texts = await page.evaluate(
                        "() => Array.from(document.querySelectorAll('cite')).map(e => e.innerText)"
                    )
                    for cite in cite_texts:
                        if 'facebook.com' not in cite.lower():
                            continue
                        parts = re.split(r'[›/\s]+', cite.strip())
                        for i, part in enumerate(parts):
                            if 'facebook.com' in part.lower() and i + 1 < len(parts):
                                name = parts[i + 1].strip()
                                name = re.sub(r'[^\w._-]', '', name)
                                name = name.strip('.').strip('-')
                                if (name and name.lower() not in FACEBOOK_BLACKLIST
                                        and len(name) > 2
                                        and not name.isdigit()
                                        and re.search(r'[a-zA-Z0-9]{3,}', name)):
                                    found_names.add(name)
                                    logger.info(f"  Found (cite): {name}")
                except Exception:
                    pass

                # --- Strategy 2: regex on raw HTML for facebook.com URLs ---
                html_decoded = html.replace('\\u002F', '/').replace('&#x2F;', '/').replace('&amp;', '&')
                raw_hits = re.findall(
                    r'facebook\.com/([a-zA-Z0-9._-]{3,60})(?:[/?#\"\'\s<]|$)',
                    html_decoded
                )
                for name in raw_hits:
                    name = name.strip('.').strip('-')
                    if (name.lower() not in FACEBOOK_BLACKLIST
                            and len(name) > 2
                            and not name.isdigit()
                            and re.search(r'[a-zA-Z0-9]{3,}', name)):
                        found_names.add(name)
                        logger.info(f"  Found (html): {name}")

                await asyncio.sleep(random.uniform(3, 5))

            except Exception as e:
                logger.warning(f"Google query failed ({query[:50]}...): {e}")
                continue

        await browser.close()

    # ── Fallback: if still not enough, try Facebook search ────────────
    if len(found_names) < num_results:
        logger.info("Google yielded fewer results than needed — trying Facebook Search fallback...")
        fb_names = await _discover_via_facebook_search_async(
            location, category, entity_type, num_results - len(found_names)
        )
        found_names.update(fb_names)

    results = list(found_names)[:num_results]
    logger.info(f"✅ Browser discovery found {len(results)} unique Facebook {entity_type}s")
    return results


def discover_pages_browser_sync(
    location: str,
    category: str,
    entity_type: str = 'page',
    num_results: int = 10
) -> List[str]:
    """
    Synchronous wrapper around the async Chromium browser discovery.
    Drop-in replacement for the old DuckDuckGo requests-based fallback.
    """
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(
                    asyncio.run,
                    _discover_pages_browser_async(location, category, entity_type, num_results)
                )
                return future.result(timeout=180)
        else:
            return loop.run_until_complete(
                _discover_pages_browser_async(location, category, entity_type, num_results)
            )
    except Exception as e:
        logger.error(f"Browser discovery failed: {e}")
        return []


async def _discover_via_facebook_search_async(
    location: str,
    category: str,
    entity_type: str = 'page',
    num_results: int = 10
) -> set:
    """
    Discover Facebook pages by searching directly on Facebook.
    Uses a saved Playwright session if one exists (from a previous scrape run).
    Gracefully returns empty set if not logged in.
    """
    from playwright.async_api import async_playwright
    from urllib.parse import quote_plus

    ctx_opts = {'viewport': {'width': 1280, 'height': 800}}
    stealth_js = "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"

    try:
        from anti_detection import BrowserFingerprint, SessionManager
        fp_mgr = BrowserFingerprint(BASE_DIR / 'data')
        session_mgr = SessionManager(BASE_DIR / 'data')
        fingerprint = fp_mgr.get_random_fingerprint()
        ctx_opts = fp_mgr.get_context_options(fingerprint)
        stealth_js = fp_mgr.get_stealth_scripts(fingerprint)
        email = os.getenv('FACEBOOK_EMAIL', '')
        if email:
            session_path = session_mgr.get_session_path(email)
            if session_path:
                ctx_opts['storage_state'] = str(session_path)
                logger.info("Facebook Search: using saved session")
    except Exception:
        pass

    found: set = set()
    query = f'{location} {category}'

    if entity_type == 'group':
        search_url = f'https://www.facebook.com/search/groups/?q={quote_plus(query)}'
    else:
        search_url = f'https://www.facebook.com/search/pages/?q={quote_plus(query)}'

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=['--disable-blink-features=AutomationControlled', '--no-sandbox']
        )
        context = await browser.new_context(**ctx_opts)
        page = await context.new_page()
        await page.add_init_script(stealth_js)

        try:
            await page.goto(search_url, wait_until='domcontentloaded', timeout=30000)
            await asyncio.sleep(4)

            if 'login' in page.url or 'checkpoint' in page.url:
                logger.info("Facebook Search requires login (no saved session). Skipping.")
                await browser.close()
                return found

            # Scroll several times to load more results
            for _ in range(4):
                await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                await asyncio.sleep(2)

            html = await page.content()
            skip_set = set(FACEBOOK_BLACKLIST) | {'search', 'hashtag', 'public', 'undefined', 'pages'}
            hits = re.findall(
                r'facebook\.com/([a-zA-Z0-9._-]{3,60})(?:[/?#"\'\\]|&amp;|$)',
                html
            )
            for name in hits:
                name = name.strip('.').strip('-')
                if (name.lower() not in skip_set
                        and len(name) > 2
                        and not name.isdigit()
                        and re.search(r'[a-zA-Z0-9]{3,}', name)):
                    found.add(name)
                    logger.info(f"  FB Search found: {name}")

        except Exception as e:
            logger.debug(f"Facebook Search fallback error: {e}")

        await browser.close()

    logger.info(f"Facebook Search fallback found {len(found)} pages")
    return found


def create_queue_file(
    location: str, 
    category: str, 
    page_names: List[str],
    entity_type: str = 'page',
    output_dir: Path = None
) -> str:
    """
    Create a queue file for the scraper
    
    Args:
        location: Location name
        category: Category name
        page_names: List of discovered page/group names
        entity_type: Type of entity ('page', 'group', 'profile')
        output_dir: Output directory for queue file
    
    Returns:
        Path to created queue file
    """
    if output_dir is None:
        output_dir = QUEUE_DIR
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    safe_location = re.sub(r'[^\w\-]', '_', location)
    filename = f"{safe_location}_{category}_{entity_type}_{timestamp}.json"
    filepath = output_dir / filename
    
    queue_data = {
        'location': location,
        'category': category,
        'entity_type': entity_type,
        'total': len(page_names),
        'page_names': page_names,
        'completed': [],
        'failed': {},
        'current_index': 0,
        'created_at': datetime.now().isoformat(),
        'source': 'google_api'
    }
    
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(queue_data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"📁 Created queue file: {filepath}")
    return str(filepath)


def interactive_discovery():
    """Interactive mode - prompts for single location/category"""
    config = load_config()
    
    print("\n" + "="*50)
    print("🔍 Facebook Page & Group Discovery")
    print("="*50)
    
    # Get entity type
    print("\nEntity types:")
    print("  1. Page (business, brand, public figure)")
    print("  2. Group (community, interest group)")
    
    while True:
        try:
            type_choice = input("\nSelect type (1 or 2, default 1): ").strip() or "1"
            if type_choice == "1":
                entity_type = "page"
                break
            elif type_choice == "2":
                entity_type = "group"
                break
        except:
            print("Invalid input. Try again.")
    
    # Get location
    cities = config.get('cities', [])
    print("\nAvailable cities:")
    for i, city in enumerate(cities, 1):
        print(f"  {i}. {city}")
    print(f"  {len(cities)+1}. Enter custom location")
    
    while True:
        try:
            choice = input("\nSelect city (number) or enter custom: ").strip()
            if choice.isdigit():
                idx = int(choice) - 1
                if 0 <= idx < len(cities):
                    location = cities[idx]
                    break
                elif idx == len(cities):
                    location = input("Enter custom location: ").strip()
                    break
            else:
                location = choice
                break
        except:
            print("Invalid input. Try again.")
    
    # Get category
    categories = config.get('categories', [])
    print("\nAvailable categories:")
    for i, cat in enumerate(categories, 1):
        print(f"  {i}. {cat}")
    print(f"  {len(categories)+1}. Enter custom category")
    
    while True:
        try:
            choice = input("\nSelect category (number) or enter custom: ").strip()
            if choice.isdigit():
                idx = int(choice) - 1
                if 0 <= idx < len(categories):
                    category = categories[idx]
                    break
                elif idx == len(categories):
                    category = input("Enter custom category: ").strip()
                    break
            else:
                category = choice
                break
        except:
            print("Invalid input. Try again.")
    
    # Get count
    while True:
        try:
            count = int(input("\nNumber of pages/groups to discover (default 10): ").strip() or "10")
            if count > 0:
                break
        except:
            print("Please enter a valid number.")
    
    print(f"\n🔎 Discovering {category} {entity_type}s in {location}...")
    
    # Discover pages/groups
    page_names = discover_pages_google(location, category, entity_type, count, config)
    
    if page_names:
        queue_file = create_queue_file(location, category, page_names, entity_type)
        print(f"\n✅ Successfully discovered {len(page_names)} {entity_type}s!")
        print(f"📁 Queue file: {queue_file}")
        print(f"\n🚀 Next step: Run the scraper with:")
        print(f"   python main.py scrape {queue_file}")
    else:
        print(f"\n❌ No {entity_type}s discovered. Check your API credentials.")


def batch_discovery():
    """Batch mode - discover for multiple locations/categories"""
    config = load_config()
    
    print("\n" + "="*50)
    print("🔍 Batch Facebook Discovery")
    print("="*50)
    
    # Get entity type
    print("\nEntity types:")
    print("  1. Page (business, brand, public figure)")
    print("  2. Group (community, interest group)")
    
    type_choice = input("\nSelect type (1 or 2, default 1): ").strip() or "1"
    entity_type = "group" if type_choice == "2" else "page"
    
    cities = config.get('cities', [])
    categories = config.get('categories', [])
    
    # Select cities
    print("\nAvailable cities:")
    for i, city in enumerate(cities, 1):
        print(f"  {i}. {city}")
    
    city_input = input("\nSelect cities (comma-separated numbers or 'all'): ").strip()
    if city_input.lower() == 'all':
        selected_cities = cities
    else:
        indices = [int(x.strip())-1 for x in city_input.split(',') if x.strip().isdigit()]
        selected_cities = [cities[i] for i in indices if 0 <= i < len(cities)]
    
    # Select categories
    print("\nAvailable categories:")
    for i, cat in enumerate(categories, 1):
        print(f"  {i}. {cat}")
    
    cat_input = input("\nSelect categories (comma-separated numbers or 'all'): ").strip()
    if cat_input.lower() == 'all':
        selected_categories = categories
    else:
        indices = [int(x.strip())-1 for x in cat_input.split(',') if x.strip().isdigit()]
        selected_categories = [categories[i] for i in indices if 0 <= i < len(categories)]
    
    # Get count
    count = int(input("\nPages/groups per combination (default 10): ").strip() or "10")
    
    print(f"\n📊 Processing {len(selected_cities)} cities × {len(selected_categories)} categories")
    print(f"   = {len(selected_cities) * len(selected_categories)} total combinations")
    
    created_files = []
    
    for city in selected_cities:
        for category in selected_categories:
            print(f"\n🔎 {city} - {category} ({entity_type})...")
            page_names = discover_pages_google(city, category, entity_type, count, config)
            
            if page_names:
                queue_file = create_queue_file(city, category, page_names, entity_type)
                created_files.append(queue_file)
            
            # Rate limit protection
            time.sleep(1)
    
    print(f"\n" + "="*50)
    print(f"✅ Batch discovery complete!")
    print(f"📁 Created {len(created_files)} queue files")


def discover_command(
    location: str = None,
    category: str = None,
    entity_type: str = 'page',
    count: int = 10,
    output_json: bool = False
) -> Optional[Dict]:
    """
    Command-line discover function for agent integration
    
    Returns JSON-compatible dict if output_json=True
    """
    if not location or not category:
        if output_json:
            return {"error": "location and category are required"}
        interactive_discovery()
        return None
    
    config = load_config()
    page_names = discover_pages_google(location, category, entity_type, count, config)
    
    if page_names:
        queue_file = create_queue_file(location, category, page_names, entity_type)
        result = {
            "success": True,
            "location": location,
            "category": category,
            "entity_type": entity_type,
            "pages_found": len(page_names),
            "page_names": page_names,
            "queue_file": queue_file
        }
    else:
        result = {
            "success": False,
            "error": f"No {entity_type}s discovered",
            "location": location,
            "category": category,
            "entity_type": entity_type
        }
    
    if output_json:
        return result
    else:
        if result["success"]:
            print(f"\n✅ Discovered {len(page_names)} {entity_type}s")
            print(f"📁 Queue file: {queue_file}")
        else:
            print(f"\n❌ No {entity_type}s discovered")
        return result


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Discover Facebook pages/groups via Google Search API')
    parser.add_argument('--location', '-l', type=str, help='Location/city to search')
    parser.add_argument('--category', '-c', type=str, help='Category to search')
    parser.add_argument('--type', '-t', type=str, choices=['page', 'group', 'profile'], default='page', help='Entity type to discover')
    parser.add_argument('--count', '-n', type=int, default=10, help='Number of pages/groups to discover')
    parser.add_argument('--batch', '-b', action='store_true', help='Batch mode for multiple locations/categories')
    parser.add_argument('--output', '-o', type=str, choices=['json', 'text'], default='text', help='Output format')
    
    args = parser.parse_args()
    
    if args.batch:
        batch_discovery()
    elif args.location and args.category:
        result = discover_command(args.location, args.category, args.type, args.count, args.output == 'json')
        if args.output == 'json' and result:
            print(json.dumps(result, indent=2))
    else:
        interactive_discovery()
