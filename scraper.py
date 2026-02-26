#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Facebook Scraper with Playwright Browser Automation
Handles authentication and anti-bot detection for Facebook pages and groups
"""

import asyncio
import json
import os
import sys
import logging
import time
import csv
import re
from typing import List, Dict, Optional
from pathlib import Path
from playwright.async_api import async_playwright, Page, Browser
from datetime import datetime
import random
import html as html_module
from dotenv import load_dotenv
import aiohttp
import hashlib
from PIL import Image
import io

# Set UTF-8 encoding for stdout
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Base directory for the skill
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / 'data'
OUTPUT_DIR = DATA_DIR / 'output'
QUEUE_DIR = DATA_DIR / 'queue'
THUMBNAILS_DIR = BASE_DIR / 'thumbnails'
CONFIG_PATH = BASE_DIR / 'config' / 'scraper_config.json'


class PageSkippedException(Exception):
    """Exception raised when a page/group should be skipped"""
    pass


class PageNotFoundException(Exception):
    """Exception raised when a page/group doesn't exist"""
    pass


class RateLimitException(Exception):
    """Exception raised when Facebook rate limits the request"""
    pass


class TemporaryBlockException(Exception):
    """Exception raised when Facebook temporarily blocks the account"""
    pass


class FacebookScraper:
    """Facebook scraper using Playwright for browser automation"""

    def __init__(self, config_path: Path = None):
        self.config = self._load_config(config_path or CONFIG_PATH)
        self.browser = None
        self.context = None
        self.page = None
        self.logged_in = False
        self.playwright = None

        # Credentials from environment
        self.email = os.getenv('FACEBOOK_EMAIL', '')
        self.password = os.getenv('FACEBOOK_PASSWORD', '')

        # Setup directories
        self.thumbnails_dir = THUMBNAILS_DIR
        self.output_dir = OUTPUT_DIR
        self.thumbnails_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize anti-detection
        from anti_detection import AntiDetectionManager
        self.anti_detection_mgr = AntiDetectionManager(DATA_DIR)

        # Initialize proxy manager
        from proxy_manager import ProxyManager
        self.proxy_mgr = ProxyManager.from_config(str(CONFIG_PATH))
        if self.proxy_mgr.is_enabled:
            logger.info(f"Proxy enabled: {self.proxy_mgr}")
        else:
            logger.info("Proxy disabled — running without residential proxy.")

        # Session state: set True in start_browser() when a saved session is loaded
        self._session_restored = False

    def _load_config(self, config_path: Path) -> Dict:
        """Load configuration from JSON file"""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load config: {e}. Using defaults.")
            return {
                'scraper': {
                    'headless': False,
                    'min_likes': 1000,
                    'download_thumbnails': True,
                    'max_thumbnails': 6
                }
            }

    async def start_browser(self, headless: bool = None):
        """Start Playwright browser with anti-detection"""
        if headless is None:
            headless = self.config.get('scraper', {}).get('headless', False)
        
        logger.info("Starting browser with anti-detection...")
        from anti_detection import BrowserFingerprint
        
        self.playwright = await async_playwright().start()

        self.browser = await self.playwright.chromium.launch(
            headless=headless,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-setuid-sandbox',
            ]
        )

        # Apply fingerprint
        fingerprint_mgr = BrowserFingerprint(DATA_DIR)
        fingerprint = fingerprint_mgr.get_random_fingerprint(self.email)
        context_options = fingerprint_mgr.get_context_options(fingerprint)

        # Restore saved session if available (skips login on subsequent runs)
        session_path = self.anti_detection_mgr.session_mgr.get_session_path(self.email)
        if session_path:
            logger.info(f"Restoring saved session for {self.email}...")
            context_options['storage_state'] = str(session_path)
            self._session_restored = True

        # Apply residential proxy if enabled
        proxy_dict = self.proxy_mgr.get_playwright_proxy()
        if proxy_dict:
            context_options['proxy'] = proxy_dict
            logger.info(f"Browser using proxy: {self.proxy_mgr.provider} → {self.proxy_mgr.host}:{self.proxy_mgr.port}")

        self.context = await self.browser.new_context(**context_options)

        self.page = await self.context.new_page()

        # Inject stealth scripts
        stealth_js = fingerprint_mgr.get_stealth_scripts(fingerprint)
        await self.page.add_init_script(stealth_js)

        logger.info("Browser started with anti-detection")

    async def login(self) -> bool:
        """Login to Facebook"""
        if not self.email or not self.password:
            logger.error("Facebook credentials not set. Set FACEBOOK_EMAIL and FACEBOOK_PASSWORD.")
            return False

        try:
            behavior_sim = self.anti_detection_mgr.behavior_sim
            checkpoint_handler = self.anti_detection_mgr.checkpoint_handler
            session_mgr = self.anti_detection_mgr.session_mgr

            # ── Fast path: resume a saved session ────────────────────────────
            if self._session_restored:
                logger.info("Checking restored session validity...")
                try:
                    await self.page.goto('https://www.facebook.com/', wait_until='domcontentloaded', timeout=30000)
                    await asyncio.sleep(3)
                    current_url = self.page.url
                    if 'facebook.com' in current_url and 'login' not in current_url and 'checkpoint' not in current_url:
                        self.logged_in = True
                        logger.info("✅ Session restored — skipped login!")
                        return True
                    else:
                        logger.info("Restored session is invalid, falling through to full login...")
                        session_mgr.invalidate_session(self.email)
                        self._session_restored = False
                except Exception as e:
                    logger.warning(f"Session restore check failed: {e}, falling through to full login")
                    self._session_restored = False

            # ── Cold path: full login ─────────────────────────────────────────
            logger.info(f"Logging into Facebook as {self.email}...")

            # Warm-up: visit a neutral site first to avoid cold-start detection
            await behavior_sim.warm_up_browsing(self.page)

            await self.page.goto('https://www.facebook.com/', timeout=30000)
            await asyncio.sleep(4)

            # Handle cookie consent dialog if present
            try:
                cookie_buttons = [
                    'button[data-cookiebanner="accept_button"]',
                    'button:has-text("Allow all cookies")',
                    'button:has-text("Accept All")',
                    'button:has-text("Allow essential and optional cookies")',
                ]
                for selector in cookie_buttons:
                    try:
                        btn = await self.page.wait_for_selector(selector, timeout=3000, state='visible')
                        if btn:
                            await btn.click()
                            await asyncio.sleep(1)
                            break
                    except:
                        continue
            except:
                pass

            # Find and human-type email
            email_selectors = [
                'input[name="email"]',
                'input#email',
                'input[type="email"]',
                'input[aria-label*="email" i]',
                'input[aria-label*="Email" i]',
            ]
            found_email = False
            for selector in email_selectors:
                try:
                    el = await self.page.wait_for_selector(selector, timeout=5000, state='visible')
                    if el:
                        await behavior_sim.simulate_human_typing(self.page, selector, self.email)
                        found_email = True
                        break
                except:
                    continue

            if not found_email:
                logger.error("Could not find email input")
                return False

            await asyncio.sleep(random.uniform(0.5, 1.5))

            # Find and human-type password
            password_selectors = [
                'input[name="pass"]',
                'input#pass',
                'input[type="password"]',
            ]
            found_pass = False
            for selector in password_selectors:
                try:
                    el = await self.page.wait_for_selector(selector, timeout=3000, state='visible')
                    if el:
                        await behavior_sim.simulate_human_typing(self.page, selector, self.password)
                        found_pass = True
                        break
                except:
                    continue

            if not found_pass:
                logger.error("Could not find password input")
                return False

            await asyncio.sleep(random.uniform(0.8, 1.8))

            # Click login button
            login_selectors = [
                'button[name="login"]',
                'button[type="submit"]',
                'button:has-text("Log In")',
                'button:has-text("Log in")',
                'input[type="submit"][value="Log In"]',
            ]
            for selector in login_selectors:
                try:
                    login_button = await self.page.wait_for_selector(selector, timeout=3000, state='visible')
                    if login_button:
                        await login_button.click()
                        break
                except:
                    continue

            await asyncio.sleep(5)

            # Auto-resolve any checkpoint / security challenge — zero human input
            current_url = self.page.url
            if ('checkpoint' in current_url or 'two_factor' in current_url
                    or 'login/identify' in current_url or '/login' in current_url):
                resolved = await checkpoint_handler.handle(self.page, 'post-login')
                if not resolved:
                    logger.error("❌ Unresolvable checkpoint — login aborted")
                    session_mgr.invalidate_session(self.email)
                    return False

            # Verify login success
            await asyncio.sleep(2)
            current_url = self.page.url
            if 'facebook.com' in current_url and 'login' not in current_url and 'checkpoint' not in current_url:
                self.logged_in = True
                logger.info("✅ Login successful!")
                # Persist session so the next run can skip login entirely
                await session_mgr.save_session(self.context, self.email)
                return True
            else:
                logger.error("❌ Login failed")
                return False

        except Exception as e:
            logger.error(f"Login failed: {e}")
            return False

    async def download_image(self, url: str, page_name: str, image_type: str, index: int = 0) -> Optional[str]:
        """Download and resize image to ~150KB"""
        try:
            # Unescape HTML entities: &amp; -> &, etc.
            url = html_module.unescape(url)

            user_dir = self.thumbnails_dir / page_name
            user_dir.mkdir(parents=True, exist_ok=True)

            url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
            if image_type == 'profile':
                filename = f"profile_{url_hash}.jpg"
            elif image_type == 'cover':
                filename = f"cover_{url_hash}.jpg"
            else:
                filename = f"content_{index}_{url_hash}.jpg"
            filepath = user_dir / filename

            import ssl
            import certifi
            ssl_ctx = ssl.create_default_context(cafile=certifi.where())
            connector = aiohttp.TCPConnector(ssl=ssl_ctx)
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        content = await response.read()

                        img = Image.open(io.BytesIO(content))

                        # Convert to RGB
                        if img.mode in ('RGBA', 'LA', 'P'):
                            rgb_img = Image.new('RGB', img.size, (255, 255, 255))
                            if img.mode == 'P':
                                img = img.convert('RGBA')
                            rgb_img.paste(img, mask=img.split()[-1] if img.mode in ('RGBA', 'LA') else None)
                            img = rgb_img

                        # Resize to max 1000px
                        max_dimension = 1000
                        if max(img.size) > max_dimension:
                            ratio = max_dimension / max(img.size)
                            new_size = (int(img.size[0] * ratio), int(img.size[1] * ratio))
                            img = img.resize(new_size, Image.Resampling.LANCZOS)

                        # Save with compression
                        quality = 85
                        output = io.BytesIO()
                        img.save(output, format='JPEG', quality=quality, optimize=True)

                        # Adjust quality to meet ~150KB target
                        while output.tell() > 150 * 1024 and quality > 50:
                            output = io.BytesIO()
                            quality -= 5
                            img.save(output, format='JPEG', quality=quality, optimize=True)

                        with open(filepath, 'wb') as f:
                            f.write(output.getvalue())

                        logger.info(f"Downloaded: {filename} ({output.tell()/1024:.1f}KB)")
                        return str(filepath)

            return None
        except Exception as e:
            logger.error(f"Error downloading image: {e}")
            return None

    async def scrape_page(self, page_name: str, entity_type: str = 'page', category: str = '', location: str = '') -> Optional[Dict]:
        """Scrape a single Facebook page or group"""
        if not self.logged_in:
            logger.error("Not logged in. Call login() first.")
            return None

        try:
            from anti_detection import HumanBehaviorSimulator, NetworkPatternRandomizer
            behavior_sim = HumanBehaviorSimulator()
            network_sim = NetworkPatternRandomizer()
            
            # Build URL based on entity type
            if entity_type == 'group':
                url = f'https://www.facebook.com/groups/{page_name}/'
            elif page_name.startswith('profile.php'):
                url = f'https://www.facebook.com/{page_name}'
            else:
                url = f'https://www.facebook.com/{page_name}/'
            
            logger.info(f"Scraping Facebook {entity_type}: {page_name}")

            # ── Set up XHR response interceptor for GraphQL post data ───────
            # Facebook loads post text/reactions/comments via api/graphql/ calls.
            # The initial HTML only has images, not post metadata.
            captured_graphql_bodies = []

            async def _capture_graphql(response):
                try:
                    if 'graphql' in response.url and response.status == 200:
                        ct = response.headers.get('content-type', '')
                        if 'json' in ct or 'text' in ct:
                            body = await response.text()
                            if body and len(body) > 100:
                                captured_graphql_bodies.append(body)
                except Exception:
                    pass

            self.page.on('response', _capture_graphql)

            await network_sim.randomize_network(self.page)
            await behavior_sim.simulate_pre_navigation(self.page)

            # --- KEY FIX: use networkidle so Facebook's React SPA fully hydrates ---
            # domcontentloaded fires before React renders any real content;
            # the page is just the "f" logo splash screen at that point.
            response = await self.page.goto(url, wait_until='networkidle', timeout=60000)
            await behavior_sim.simulate_post_navigation(self.page)

            # Wait for main content to load
            try:
                await self.page.wait_for_selector('[role="main"], [role="banner"], h1, h2', timeout=15000)
            except:
                pass

            # --- Content-readiness loop ---
            # Facebook's SPA can take 10-20s to fully hydrate.
            # Wait until body text is > 200 chars (splash screen is ~30 chars).
            for attempt in range(8):
                body_len = await self.page.evaluate('() => document.body.innerText.length')
                if body_len > 200:
                    break
                logger.debug(f"Waiting for FB content to render… (body {body_len} chars, attempt {attempt+1}/8)")
                await asyncio.sleep(2)

            await behavior_sim.simulate_content_render(self.page)

            # Check HTTP status
            if response and response.status >= 400:
                if response.status == 404:
                    raise PageNotFoundException(f"Facebook {entity_type} {page_name} not found")
                elif response.status == 429:
                    raise RateLimitException("Rate limited")

            # Check page content for blocks/errors
            page_content = await self.page.content()
            page_content_lower = page_content.lower()

            # Check for temporary block
            if "you're temporarily blocked" in page_content_lower or 'temporarily blocked' in page_content_lower:
                raise TemporaryBlockException("Temporarily blocked by Facebook")

            # Check for checkpoint/security — try to auto-resolve before giving up
            if 'checkpoint' in self.page.url:
                logger.warning("Checkpoint triggered mid-scrape, attempting auto-resolve...")
                resolved = await self.anti_detection_mgr.checkpoint_handler.handle(self.page, 'mid-scrape')
                if not resolved:
                    raise TemporaryBlockException("Facebook security checkpoint triggered and could not be auto-resolved")

            # Check for not found
            not_found_indicators = [
                "this content isn't available",
                "this page isn't available",
                "the link you followed may be broken",
                "page not found",
                "content not found",
                "this group is no longer available",
            ]
            for indicator in not_found_indicators:
                if indicator in page_content_lower:
                    raise PageNotFoundException(f"Facebook {entity_type} {page_name} not found")

            # Scroll to load more content — aggressive multi-scroll to trigger GraphQL post loads
            for scroll_pass in range(5):
                await self.page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                await asyncio.sleep(random.uniform(2, 3.5))
            await self.page.evaluate('window.scrollTo(0, 0)')  # scroll back to top
            await asyncio.sleep(1)

            # Wait a bit more for remaining XHR responses to arrive
            await asyncio.sleep(3)

            # Remove the response listener
            self.page.remove_listener('response', _capture_graphql)
            logger.info(f"Captured {len(captured_graphql_bodies)} GraphQL responses")

            # Debug: log how much text the page actually has
            body_text_len = await self.page.evaluate('() => document.body.innerText.length')
            current_url = self.page.url
            page_title = await self.page.title()
            logger.info(f"Page body text length for {page_name}: {body_text_len} chars")
            logger.info(f"Current URL: {current_url}")
            logger.info(f"Page title: {page_title}")
            if body_text_len < 100:
                logger.warning(f"Facebook page {page_name} appears not to have rendered — body text < 100 chars")
                # Dump first 500 chars of raw HTML for diagnosis
                html_snippet = await self.page.evaluate('() => document.documentElement.outerHTML.substring(0, 500)')
                logger.info(f"HTML snippet: {html_snippet}")

            # Extract data based on entity type
            if entity_type == 'group':
                profile_data = await self._extract_group_data(page_name)
            else:
                profile_data = await self._extract_page_data(page_name, captured_graphql_bodies)

            if not profile_data:
                return None

            await behavior_sim.simulate_final_wait(self.page)

            # Validate data
            if not profile_data.get('page_name'):
                profile_data['page_name'] = page_name

            # Check for private group
            if entity_type == 'group' and profile_data.get('privacy', '').lower() == 'private':
                logger.warning(f"Skipping private group: {page_name}")
                raise PageSkippedException(f"Group {page_name} is private")

            # Check minimum likes/members
            min_likes = self.config.get('scraper', {}).get('min_likes', 1000)
            metric = profile_data.get('page_likes', 0) if entity_type == 'page' else profile_data.get('members', 0)
            if metric < min_likes:
                metric_name = 'likes' if entity_type == 'page' else 'members'
                logger.warning(f"Skipping {page_name}: {metric} {metric_name} < {min_likes}")
                return None

            # Classify tier
            if entity_type == 'page':
                tier_metric = profile_data.get('page_likes', 0)
            else:
                tier_metric = profile_data.get('members', 0)
            
            if tier_metric < 1000:
                tier = 'nano'
            elif tier_metric < 10000:
                tier = 'micro'
            elif tier_metric < 100000:
                tier = 'mid'
            elif tier_metric < 1000000:
                tier = 'macro'
            else:
                tier = 'mega'

            profile_data['page_tier'] = tier
            profile_data['entity_type'] = entity_type

            # Download profile picture
            if profile_data.get('profile_pic_url'):
                profile_pic_local = await self.download_image(
                    profile_data['profile_pic_url'],
                    page_name,
                    'profile'
                )
                profile_data['profile_pic_local'] = profile_pic_local

            # Download cover photo
            if profile_data.get('cover_photo_url'):
                cover_local = await self.download_image(
                    profile_data['cover_photo_url'],
                    page_name,
                    'cover'
                )
                profile_data['cover_photo_local'] = cover_local

            # Download post / content images
            # Go through each post's image_url and also remaining content_thumbnails
            downloaded_urls = set()
            content_locals = []

            for idx, post in enumerate(profile_data.get('recent_posts', [])):
                img_url = post.get('image_url', '')
                if img_url and img_url not in downloaded_urls:
                    try:
                        local_path = await self.download_image(img_url, page_name, 'post', index=idx)
                        post['image_local'] = local_path
                        content_locals.append(local_path)
                        downloaded_urls.add(img_url)
                    except Exception as e:
                        logger.debug(f"Failed to download post image {idx}: {e}")
                        post['image_local'] = None

            # Download remaining content thumbnails not already downloaded
            for idx, img_url in enumerate(profile_data.get('content_thumbnails', [])):
                if img_url and img_url not in downloaded_urls:
                    try:
                        local_path = await self.download_image(
                            img_url, page_name, 'content',
                            index=len(content_locals)
                        )
                        content_locals.append(local_path)
                        downloaded_urls.add(img_url)
                    except Exception as e:
                        logger.debug(f"Failed to download content image {idx}: {e}")
                        content_locals.append(None)

            profile_data['content_thumbnails_local'] = content_locals
            logger.info(f"Downloaded {len([x for x in content_locals if x])} content images")

            # Add metadata
            profile_data['category'] = category or profile_data.get('category', '')
            profile_data['location'] = location or profile_data.get('location', '')
            profile_data['scrape_timestamp'] = datetime.now().isoformat()

            tier_metric_display = f"{tier_metric:,}"
            metric_label = 'likes' if entity_type == 'page' else 'members'
            logger.info(f"✅ Scraped: {page_name} ({tier_metric_display} {metric_label}, {tier})")
            return profile_data

        except (PageNotFoundException, PageSkippedException, RateLimitException, TemporaryBlockException):
            raise
        except Exception as e:
            logger.error(f"Error scraping {page_name}: {e}")
            return None

    async def _extract_page_data(self, page_name: str, graphql_bodies: list = None) -> Optional[Dict]:
        """
        Extract data from a Facebook page.
        
        Strategy: Facebook's React SPA often doesn't hydrate under Playwright
        (body.innerText = 0 chars — the "f" logo splash never resolves).
        But the raw HTML source contains ALL page metadata in embedded JSON
        inside <script> tags. We parse that instead of the DOM.
        
        Fallback: if DOM IS rendered (body text > 200 chars), use DOM too.
        """
        try:
            html = await self.page.content()

            # DEBUG: dump raw HTML for pattern analysis (keep last page only)
            debug_path = BASE_DIR / 'data' / 'debug_last_html.txt'
            try:
                safe_html = html[:500000].encode('utf-8', 'replace').decode('utf-8')
                debug_path.write_text(safe_html, encoding='utf-8')
            except:
                pass
            data = {
                'page_name': page_name,
                'display_name': page_name,
                'followers': 0,
                'page_likes': 0,
                'about': '',
                'is_verified': False,
                'profile_pic_url': '',
                'cover_photo_url': '',
                'phone': '',
                'email': '',
                'website': '',
                'address': '',
                'hours': '',
                'recent_posts': [],
            }

            # ── Strategy 1: Parse embedded JSON from <script> tags ──────────
            # Facebook bakes page data into script blocks like:
            #   "name":"PageName","page_likers":{"global_likers_count":1234}
            #   "followers_count":5678
            #   "category_name":"Restaurant"
            #   "description":{"text":"About text..."}

            # Display name from <title> tag
            title_match = re.search(r'<title[^>]*>([^<]+)</title>', html)
            if title_match:
                raw_title = title_match.group(1).strip()
                # Remove " | Facebook" or " - Facebook" suffix
                raw_title = re.sub(r'\s*[\|–—-]\s*Facebook.*$', '', raw_title).strip()
                # Remove " | City" suffix (e.g. "Kanchiappatakkars | Kanchipuram")
                raw_title = re.sub(r'\s*\|.*$', '', raw_title).strip()
                if raw_title:
                    data['display_name'] = raw_title

            # Likes count — multiple patterns Facebook uses in embedded JSON
            likes_patterns = [
                r'"page_likers"\s*:\s*\{\s*"global_likers_count"\s*:\s*(\d+)',
                r'"overall_star_rating_count"\s*:\s*\{[^}]*"count"\s*:\s*(\d+)',
                r'"likers"\s*:\s*\{\s*"count"\s*:\s*(\d+)',
                r'"likes"\s*:\s*(\d+)',
                r'"page_like_count"\s*:\s*(\d+)',
                r'"fan_count"\s*:\s*(\d+)',
                # Text patterns from meta tags / OG data
                r'(\d[\d,]+)\s+(?:people\s+)?like\s+this',
                r'(\d[\d,]+)\s+(?:total\s+)?likes?',
            ]
            for pattern in likes_patterns:
                m = re.search(pattern, html, re.IGNORECASE)
                if m:
                    raw = m.group(1).replace(',', '')
                    val = int(raw)
                    if val > data['page_likes']:
                        data['page_likes'] = val
                    break

            # Followers count
            followers_patterns = [
                r'"followers_count"\s*:\s*(\d+)',
                r'"follower_count"\s*:\s*(\d+)',
                r'"global_followers_count"\s*:\s*(\d+)',
                r'(\d[\d,]+)\s+(?:people\s+)?follow',
                r'(\d[\d,]+)\s+followers?',
            ]
            for pattern in followers_patterns:
                m = re.search(pattern, html, re.IGNORECASE)
                if m:
                    raw = m.group(1).replace(',', '')
                    val = int(raw)
                    if val > data['followers']:
                        data['followers'] = val
                    break

            # About / description
            about_patterns = [
                r'"description"\s*:\s*\{\s*"text"\s*:\s*"([^"]{5,500})"',
                r'"about"\s*:\s*"([^"]{5,500})"',
                r'"blurb"\s*:\s*"([^"]{5,500})"',
                r'"biography"\s*:\s*"([^"]{5,500})"',
                r'"intro_text"\s*:\s*"([^"]{5,500})"',
                r'"page_about_fields"\s*:\s*\{[^}]*"text"\s*:\s*"([^"]{5,500})"',
                r'<meta\s+name="description"\s+content="([^"]{5,500})"',
                r'<meta\s+property="og:description"\s+content="([^"]{5,500})"',
            ]
            for pattern in about_patterns:
                m = re.search(pattern, html, re.IGNORECASE)
                if m:
                    text = m.group(1)
                    # Unescape JSON Unicode
                    try:
                        text = text.encode().decode('unicode_escape')
                    except:
                        pass
                    data['about'] = text.strip()
                    break

            # Category
            category_patterns = [
                r'"category_name"\s*:\s*"([^"]+)"',
                r'"category_type"\s*:\s*"([^"]+)"',
                r'"categories"\s*:\s*\[\s*\{\s*"name"\s*:\s*"([^"]+)"',
            ]
            for pattern in category_patterns:
                m = re.search(pattern, html)
                if m:
                    data['page_category'] = m.group(1)
                    break

            # Verified
            if ('"is_verified":true' in html or '"isVerified":true' in html
                    or 'aria-label="Verified"' in html):
                data['is_verified'] = True

            # Profile picture URL
            pic_patterns = [
                r'"profilePicLarge"\s*:\s*\{\s*"uri"\s*:\s*"([^"]+)"',
                r'"profilePicMedium"\s*:\s*\{\s*"uri"\s*:\s*"([^"]+)"',
                r'"profile_picture"\s*:\s*\{\s*"uri"\s*:\s*"([^"]+)"',
                r'"profile_pic_uri"\s*:\s*"([^"]+)"',
                r'"profilePhoto"\s*:\s*\{\s*"uri"\s*:\s*"([^"]+)"',
                r'"profile_pic_large"\s*:\s*\{\s*"uri"\s*:\s*"([^"]+)"',
                r'<meta\s+property="og:image"\s+content="([^"]+)"',
            ]
            for pattern in pic_patterns:
                m = re.search(pattern, html)
                if m:
                    url = m.group(1).replace('\\/', '/')
                    if url.startswith('http'):
                        data['profile_pic_url'] = url
                        break

            # Cover photo URL
            cover_patterns = [
                r'"coverPhoto"\s*:\s*\{[^}]*"uri"\s*:\s*"([^"]+)"',
                r'"cover_photo"\s*:\s*\{[^}]*"uri"\s*:\s*"([^"]+)"',
                r'"CoverPhoto"\s*:\s*\{[^}]*"uri"\s*:\s*"([^"]+)"',
            ]
            for pattern in cover_patterns:
                m = re.search(pattern, html)
                if m:
                    url = m.group(1).replace('\\/', '/')
                    if url.startswith('http'):
                        data['cover_photo_url'] = url
                        break

            # Phone
            phone_patterns = [
                r'"phone"\s*:\s*"([^"]+)"',
                r'"single_line_address"\s*:\s*"([^"]*\d{3,}[^"]*)"',
            ]
            for pattern in phone_patterns:
                m = re.search(pattern, html)
                if m:
                    data['phone'] = m.group(1)
                    break

            # Email
            email_match = re.search(r'"email"\s*:\s*"([^"]+@[^"]+)"', html)
            if not email_match:
                email_match = re.search(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', html)
            if email_match:
                data['email'] = email_match.group(1)

            # Website
            web_patterns = [
                r'"website"\s*:\s*"(https?://[^"]+)"',
                r'"external_url"\s*:\s*"(https?://[^"]+)"',
                r'"url"\s*:\s*"(https?://(?!(?:www\.)?facebook\.com)[^"]+)"',
            ]
            for pattern in web_patterns:
                m = re.search(pattern, html)
                if m:
                    data['website'] = m.group(1).replace('\\/', '/')
                    break

            # Address
            addr_patterns = [
                r'"single_line_address"\s*:\s*"([^"]+)"',
                r'"address"\s*:\s*\{[^}]*"text"\s*:\s*"([^"]+)"',
                r'"street"\s*:\s*"([^"]+)"',
            ]
            for pattern in addr_patterns:
                m = re.search(pattern, html)
                if m:
                    data['address'] = m.group(1)
                    break

            # ── Recent Posts / Content Portfolio ─────────────────────────
            # Facebook embeds post images in the initial HTML but loads
            # post text/reactions/comments via GraphQL XHR (captured above).
            posts = []
            seen_post_texts = set()

            # --- Parse post metadata from captured GraphQL XHR responses ---
            if graphql_bodies:
                all_graphql = '\n'.join(graphql_bodies)

                # Extract post message texts
                msg_texts = re.findall(
                    r'"message"\s*:\s*\{\s*"text"\s*:\s*"([^"]{5,1000})"',
                    all_graphql
                )

                # Extract creation timestamps
                creation_times = re.findall(
                    r'"creation_time"\s*:\s*(\d{10})',
                    all_graphql
                )

                # Extract reaction counts
                reaction_counts = re.findall(
                    r'"reaction_count"\s*:\s*\{\s*"count"\s*:\s*(\d+)',
                    all_graphql
                )

                # Extract comment counts
                comment_counts = re.findall(
                    r'"comment_count"\s*:\s*\{\s*"total_count"\s*:\s*(\d+)',
                    all_graphql
                )

                # Extract share counts
                share_counts = re.findall(
                    r'"share_count"\s*:\s*\{\s*"count"\s*:\s*(\d+)',
                    all_graphql
                )

                # Extract post image URIs from GraphQL (escaped \/ format)
                graphql_images = re.findall(
                    r'"uri"\s*:\s*"(https:\\/\\/scontent[^"]+t51\.82787[^"]+)"',
                    all_graphql
                )
                graphql_images = [u.replace('\\/', '/') for u in graphql_images]

                # Build posts from extracted data
                for i, text in enumerate(msg_texts[:10]):
                    short = text[:80].lower()
                    if short in seen_post_texts:
                        continue
                    seen_post_texts.add(short)
                    if len(posts) >= 6:
                        break
                    try:
                        decoded_text = text.encode().decode('unicode_escape')
                    except:
                        decoded_text = text
                    posts.append({
                        'text': decoded_text[:500],
                        'timestamp': int(creation_times[i]) if i < len(creation_times) else 0,
                        'image_url': '',
                        'reactions': int(reaction_counts[i]) if i < len(reaction_counts) else 0,
                        'comments': int(comment_counts[i]) if i < len(comment_counts) else 0,
                        'shares': int(share_counts[i]) if i < len(share_counts) else 0,
                    })

                logger.info(f"GraphQL yielded {len(posts)} posts, {len(graphql_images)} post images")

            # --- Fallback: try extracting from initial page HTML ---
            if not posts:
                msg_texts = re.findall(
                    r'"message"\s*:\s*\{\s*"text"\s*:\s*"([^"]{10,800})"',
                    html
                )
                for text in msg_texts[:6]:
                    short = text[:80].lower()
                    if short in seen_post_texts:
                        continue
                    seen_post_texts.add(short)
                    try:
                        decoded_text = text.encode().decode('unicode_escape')
                    except:
                        decoded_text = text
                    posts.append({
                        'text': decoded_text[:500],
                        'timestamp': 0,
                        'image_url': '',
                        'reactions': 0,
                        'comments': 0,
                        'shares': 0,
                    })

            # ── Extract content/post images ────────────────────────────
            # Use three sources: preload tags, JSON URIs, and GraphQL URIs.
            # Filter OUT profile pic (t39.30808-1) and cover photo (t39.30808-6).
            content_image_urls = []
            profile_cover_urls = set()

            # Remember profile/cover URLs to exclude them
            if data.get('profile_pic_url'):
                profile_cover_urls.add(re.sub(r'\?.*', '', data['profile_pic_url']))
            if data.get('cover_photo_url'):
                profile_cover_urls.add(re.sub(r'\?.*', '', data['cover_photo_url']))

            def _is_post_image(url: str) -> bool:
                """Return True if URL is a post/content image, not profile/cover"""
                key = re.sub(r'\?.*', '', url)
                if key in profile_cover_urls:
                    return False
                # t39.30808-1 = profile pic, t39.30808-6 = cover photo
                if 't39.30808-1' in url or 't39.30808-6' in url:
                    return False
                return True

            # Source 1: <link rel="preload"> tags
            preload_hits = re.findall(
                r'<link[^>]+rel="preload"[^>]+href="(https://scontent[^"]+)"',
                html
            )
            for url in preload_hits:
                url = html_module.unescape(url)
                if _is_post_image(url):
                    content_image_urls.append(url)

            # Source 2: JSON-escaped URIs from initial HTML
            json_img_hits = re.findall(
                r'"uri"\s*:\s*"(https:\\/\\/scontent[^"]+)"',
                html
            )
            for raw_url in json_img_hits:
                url = raw_url.replace('\\/', '/')
                url = html_module.unescape(url)
                if _is_post_image(url):
                    content_image_urls.append(url)

            # Source 3: GraphQL XHR images (already collected above)
            if graphql_bodies:
                all_gql = '\n'.join(graphql_bodies)
                gql_imgs = re.findall(
                    r'"uri"\s*:\s*"(https:\\/\\/scontent[^"]+)"',
                    all_gql
                )
                for raw_url in gql_imgs:
                    url = raw_url.replace('\\/', '/')
                    url = html_module.unescape(url)
                    if _is_post_image(url):
                        content_image_urls.append(url)

            # Deduplicate (keep order, prefer higher-res — longer URLs)
            seen_keys = set()
            unique_images = []
            for url in content_image_urls:
                # Normalize — strip query params for dedup
                key = re.sub(r'\?.*', '', url)
                # Also strip size modifiers like stp=dst-jpg_s960x960
                key = re.sub(r'/v/t', '//t', key)
                if key not in seen_keys:
                    seen_keys.add(key)
                    unique_images.append(url)
            content_image_urls = unique_images[:12]  # up to 12 images

            # Attach images to posts where possible
            for i, post in enumerate(posts):
                if i < len(content_image_urls):
                    post['image_url'] = content_image_urls[i]

            # Any remaining images go into content_thumbnails (portfolio)
            data['recent_posts'] = posts
            data['content_thumbnails'] = content_image_urls

            # ── Strategy 2 (fallback): if DOM IS rendered, enrich from it ───
            body_len = await self.page.evaluate('() => document.body.innerText.length')
            if body_len > 200:
                dom_data = await self.page.evaluate(r'''() => {
                    const d = {};
                    const bodyText = document.body.innerText;
                    // Display name
                    const h1 = document.querySelector('h1');
                    if (h1 && h1.innerText.trim()) d.display_name = h1.innerText.trim();
                    // Followers from text
                    const fm = bodyText.match(/([\d,KkMm.]+)\s+(?:people\s+)?follow/i);
                    if (fm) d.followers_raw = fm[1];
                    const lm = bodyText.match(/([\d,KkMm.]+)\s+(?:people\s+)?like(?:s)?\s+this/i);
                    if (lm) d.likes_raw = lm[1];
                    // Profile pic from DOM
                    const pic = document.querySelector('svg image[preserveAspectRatio], [role="main"] image, img[alt*="profile" i]');
                    if (pic) d.profile_pic_url = pic.getAttribute('xlink:href') || pic.src || '';
                    return d;
                }''')
                if dom_data:
                    if dom_data.get('display_name') and data['display_name'] == page_name:
                        data['display_name'] = dom_data['display_name']
                    if dom_data.get('profile_pic_url') and not data['profile_pic_url']:
                        data['profile_pic_url'] = dom_data['profile_pic_url']

            logger.info(f"Extracted: likes={data['page_likes']}, followers={data['followers']}, about={len(data['about'])} chars, posts={len(data['recent_posts'])}, images={len(data['content_thumbnails'])}")
            return data

        except Exception as e:
            logger.error(f"Error extracting page data for {page_name}: {e}")
            return None

    async def _extract_group_data(self, page_name: str) -> Optional[Dict]:
        """Extract data from a Facebook group"""
        try:
            profile_data = await self.page.evaluate(r'''() => {
                const data = {};
                
                // Group name
                const pathParts = window.location.pathname.split('/').filter(x => x);
                data.page_name = pathParts.length > 1 ? pathParts[1] : pathParts[0] || '';
                
                // Display name
                const nameSelectors = [
                    'h1',
                    '[role="main"] h1',
                ];
                for (const sel of nameSelectors) {
                    const el = document.querySelector(sel);
                    if (el && el.innerText && el.innerText.trim().length > 0) {
                        data.display_name = el.innerText.trim();
                        break;
                    }
                }
                if (!data.display_name) data.display_name = data.page_name;
                
                const bodyText = document.body.innerText;
                
                // Parse numeric values
                function parseCount(text) {
                    if (!text) return 0;
                    text = text.toUpperCase().replace(/,/g, '');
                    if (text.includes('K')) return Math.floor(parseFloat(text) * 1000);
                    if (text.includes('M')) return Math.floor(parseFloat(text) * 1000000);
                    return parseInt(text) || 0;
                }
                
                // Members count
                const membersPatterns = [
                    /([\d,KkMm.]+)\s+(?:total\s+)?members?/i,
                    /Members?\s*[:\s]+([\d,KkMm.]+)/i,
                ];
                for (const pattern of membersPatterns) {
                    const match = bodyText.match(pattern);
                    if (match) {
                        data.members = parseCount(match[1]);
                        break;
                    }
                }
                if (!data.members) data.members = 0;
                
                // Privacy status (Public/Private)
                if (bodyText.toLowerCase().includes('public group') || bodyText.toLowerCase().includes('public · ')) {
                    data.privacy = 'Public';
                } else if (bodyText.toLowerCase().includes('private group') || bodyText.toLowerCase().includes('private · ')) {
                    data.privacy = 'Private';
                } else {
                    data.privacy = 'Unknown';
                }
                
                // Posts per day
                const postsPerDayMatch = bodyText.match(/([\d,]+)\s+(?:new\s+)?posts?\s+(?:a|per|today|this)\s+(?:day|week)/i);
                data.posts_per_day = postsPerDayMatch ? parseInt(postsPerDayMatch[1].replace(/,/g, '')) : 0;
                
                // About/description
                const aboutSelectors = [
                    '[aria-label="About this group"]',
                    '[data-pagelet*="about"]',
                ];
                for (const sel of aboutSelectors) {
                    const el = document.querySelector(sel);
                    if (el && el.innerText) {
                        data.about = el.innerText.trim().substring(0, 500);
                        break;
                    }
                }
                if (!data.about) data.about = '';
                
                // Profile picture (group icon)
                const profilePicSelectors = [
                    '[role="main"] image',
                    'svg image[preserveAspectRatio]',
                    'img[alt*="group" i]',
                ];
                for (const sel of profilePicSelectors) {
                    const el = document.querySelector(sel);
                    if (el) {
                        const src = el.getAttribute('xlink:href') || el.src || el.getAttribute('href');
                        if (src && src.startsWith('http')) {
                            data.profile_pic_url = src;
                            break;
                        }
                    }
                }
                if (!data.profile_pic_url) data.profile_pic_url = '';
                
                // Cover photo
                const coverSelectors = [
                    'img[alt*="cover" i]',
                    '[data-imgperflogname*="cover" i] img',
                ];
                for (const sel of coverSelectors) {
                    const el = document.querySelector(sel);
                    if (el && el.src && el.src.startsWith('http')) {
                        data.cover_photo_url = el.src;
                        break;
                    }
                }
                if (!data.cover_photo_url) data.cover_photo_url = '';
                
                return data;
            }''')

            return profile_data

        except Exception as e:
            logger.error(f"Error extracting group data for {page_name}: {e}")
            return None

    @staticmethod
    def _sanitize_for_json(obj):
        """Recursively replace surrogate characters that can't be encoded to UTF-8"""
        if isinstance(obj, str):
            # Remove surrogates and decode HTML entities
            return obj.encode('utf-8', 'replace').decode('utf-8')
        elif isinstance(obj, dict):
            return {k: FacebookScraper._sanitize_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [FacebookScraper._sanitize_for_json(v) for v in obj]
        return obj

    def save_profile(self, profile: Dict):
        """Save profile/page data to JSON file"""
        page_name = profile.get('page_name', 'unknown')
        filepath = self.output_dir / f"{page_name}.json"

        # Sanitize surrogates before JSON encoding
        clean_profile = self._sanitize_for_json(profile)

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(clean_profile, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved: {filepath}")

    async def cleanup(self):
        """Close browser"""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        logger.info("Browser closed")


def load_queue_file(filepath: str) -> Dict:
    """Load queue file with checkpoint data"""
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if 'completed' not in data:
        data['completed'] = []
    if 'current_index' not in data:
        data['current_index'] = 0
    if 'failed' not in data:
        data['failed'] = {}

    return data


def save_queue_file(filepath: str, data: Dict):
    """Save queue file with checkpoint"""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


async def scrape_from_queue(queue_file: str, resume: bool = True) -> List[Dict]:
    """Scrape Facebook pages/groups from a queue file"""
    queue_data = load_queue_file(queue_file)
    
    page_names = queue_data.get('page_names', [])
    completed = set(queue_data.get('completed', []))
    location = queue_data.get('location', '')
    category = queue_data.get('category', '')
    entity_type = queue_data.get('entity_type', 'page')
    
    # Filter remaining page names
    remaining = [p for p in page_names if p not in completed]
    
    print(f"\n{'='*50}")
    print(f"📋 Queue: {Path(queue_file).name}")
    print(f"   Location: {location}")
    print(f"   Category: {category}")
    print(f"   Type: {entity_type}")
    print(f"   Total: {len(page_names)} | Completed: {len(completed)} | Remaining: {len(remaining)}")
    print(f"{'='*50}\n")
    
    if not remaining:
        print("✅ All pages/groups already scraped!")
        return []
    
    scraper = FacebookScraper()
    results = []
    
    try:
        await scraper.start_browser()
        
        if not await scraper.login():
            logger.error("Failed to login")
            return []
        
        for i, name in enumerate(remaining, 1):
            print(f"\n[{i}/{len(remaining)}] Scraping: {name}")
            
            try:
                profile = await scraper.scrape_page(name, entity_type, category, location)
                
                if profile:
                    results.append(profile)
                    scraper.save_profile(profile)
                    queue_data['completed'].append(name)
                else:
                    queue_data['failed'][name] = 'no_data'
                
            except PageNotFoundException:
                queue_data['failed'][name] = 'not_found'
                logger.warning(f"Page/group not found: {name}")
            except PageSkippedException:
                queue_data['failed'][name] = 'skipped'
            except RateLimitException:
                logger.error("Rate limited! Waiting 120 seconds...")
                await asyncio.sleep(120)
            except TemporaryBlockException:
                logger.error("Temporarily blocked! Stopping scrape.")
                break
            except Exception as e:
                queue_data['failed'][name] = str(e)
                logger.error(f"Error: {e}")
            
            # Save checkpoint
            save_queue_file(queue_file, queue_data)
            
            # Delay between pages (longer than Instagram - Facebook is more aggressive)
            delay_range = scraper.config.get('scraper', {}).get('delay_between_profiles', [5, 10])
            delay = random.uniform(delay_range[0], delay_range[1])
            logger.info(f"Waiting {delay:.1f}s...")
            await asyncio.sleep(delay)
        
    finally:
        await scraper.cleanup()
    
    return results


async def scrape_single(page_name: str, entity_type: str = 'page', output_json: bool = False) -> Optional[Dict]:
    """Scrape a single Facebook page or group"""
    scraper = FacebookScraper()
    
    try:
        await scraper.start_browser()
        
        if not await scraper.login():
            if output_json:
                return {"error": "Login failed"}
            return None
        
        profile = await scraper.scrape_page(page_name, entity_type)
        
        if profile:
            scraper.save_profile(profile)
            if output_json:
                return profile
            
            metric_name = 'likes' if entity_type == 'page' else 'members'
            metric_value = profile.get('page_likes', 0) if entity_type == 'page' else profile.get('members', 0)
            
            print(f"\n✅ Scraped: {page_name}")
            print(f"   {metric_name.title()}: {metric_value:,}")
            print(f"   Tier: {profile.get('page_tier', 'unknown')}")
            return profile
        else:
            if output_json:
                return {"error": f"Could not scrape {entity_type}"}
            print(f"\n❌ Could not scrape: {page_name}")
            return None
        
    finally:
        await scraper.cleanup()


def export_data(output_format: str = 'both'):
    """Export all scraped data to JSON and/or CSV"""
    output_files = list(OUTPUT_DIR.glob('*.json'))
    
    if not output_files:
        print("No data to export")
        return
    
    profiles = []
    for f in output_files:
        with open(f, 'r', encoding='utf-8') as file:
            profiles.append(json.load(file))
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    if output_format in ('json', 'both'):
        json_path = DATA_DIR / f"export_{timestamp}.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(profiles, f, indent=2, ensure_ascii=False)
        print(f"📁 JSON export: {json_path}")
    
    if output_format in ('csv', 'both'):
        csv_path = DATA_DIR / f"export_{timestamp}.csv"
        if profiles:
            keys = ['page_name', 'display_name', 'entity_type', 'followers', 'page_likes', 
                    'members', 'is_verified', 'about', 'page_tier', 'category', 'location',
                    'address', 'phone', 'email', 'website', 'hours', 'privacy']
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=keys, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(profiles)
        print(f"📁 CSV export: {csv_path}")


def list_queue_files():
    """List all queue files"""
    queue_files = sorted(QUEUE_DIR.glob('*.json'))
    
    if not queue_files:
        print("No queue files found")
        return
    
    print(f"\n{'='*60}")
    print("📋 Available Queue Files")
    print(f"{'='*60}")
    
    for i, qf in enumerate(queue_files, 1):
        try:
            with open(qf, 'r') as f:
                data = json.load(f)
            total = len(data.get('page_names', []))
            completed = len(data.get('completed', []))
            entity_type = data.get('entity_type', 'page')
            pct = int(completed/total*100) if total > 0 else 0
            print(f"{i}. {qf.name}")
            print(f"   Location: {data.get('location', 'N/A')} | Category: {data.get('category', 'N/A')} | Type: {entity_type}")
            print(f"   Progress: {completed}/{total} ({pct}%)")
        except:
            print(f"{i}. {qf.name} (error reading)")


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Facebook Page & Group Scraper')
    parser.add_argument('queue_file', nargs='?', help='Queue file to scrape')
    parser.add_argument('--page-name', '-p', type=str, help='Single page/group name to scrape')
    parser.add_argument('--type', '-t', type=str, choices=['page', 'group', 'profile'], default='page', help='Entity type')
    parser.add_argument('--list', '-l', action='store_true', help='List queue files')
    parser.add_argument('--resume', '-r', action='store_true', default=True, help='Resume from checkpoint')
    parser.add_argument('--export', '-e', type=str, choices=['json', 'csv', 'both'], help='Export data')
    parser.add_argument('--output', '-o', type=str, choices=['json', 'text'], default='text', help='Output format')
    
    args = parser.parse_args()
    
    if args.list:
        list_queue_files()
    elif args.export:
        export_data(args.export)
    elif args.page_name:
        result = asyncio.run(scrape_single(args.page_name, args.type, args.output == 'json'))
        if args.output == 'json' and result:
            print(json.dumps(result, indent=2))
    elif args.queue_file:
        asyncio.run(scrape_from_queue(args.queue_file, args.resume))
    else:
        parser.print_help()
