"""
Anti-detection and browser fingerprinting module for Facebook scraping
Implements various techniques to avoid bot detection
"""
import random
import json
import string
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime
import asyncio
from playwright.async_api import Page, BrowserContext
import logging

logger = logging.getLogger(__name__)


class BrowserFingerprint:
    """Manages browser fingerprinting to avoid detection"""
    
    def __init__(self, data_dir: Path = None):
        self.data_dir = data_dir or Path(__file__).parent / 'data'
        self.fingerprints_file = self.data_dir / 'browser_fingerprints.json'
        self.load_fingerprints()
    
    def load_fingerprints(self):
        if self.fingerprints_file.exists():
            with open(self.fingerprints_file, 'r') as f:
                self.fingerprints = json.load(f)
        else:
            self.fingerprints = self._generate_fingerprints()
            self.save_fingerprints()
    
    def save_fingerprints(self):
        self.fingerprints_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.fingerprints_file, 'w') as f:
            json.dump(self.fingerprints, f, indent=2)
    
    def _generate_fingerprints(self) -> Dict:
        return {
            "profiles": [
                {
                    "os": "Windows",
                    "browser": "Chrome",
                    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "viewport": {"width": 1920, "height": 1080},
                    "screen": {"width": 1920, "height": 1080, "depth": 24},
                    "timezone": "America/New_York",
                    "locale": "en-US",
                    "webgl_vendor": "Google Inc. (Intel)",
                    "webgl_renderer": "ANGLE (Intel, Intel(R) UHD Graphics 620 Direct3D11 vs_5_0 ps_5_0)",
                    "fonts": ["Arial", "Calibri", "Cambria", "Comic Sans MS", "Consolas", "Courier New", "Georgia", "Impact", "Lucida Console", "Tahoma", "Times New Roman", "Trebuchet MS", "Verdana"],
                    "plugins": ["Chrome PDF Plugin", "Chrome PDF Viewer", "Native Client"],
                    "hardware_concurrency": 8,
                    "device_memory": 8,
                    "max_touch_points": 0,
                    "color_depth": 24,
                    "pixel_ratio": 1
                },
                {
                    "os": "Windows",
                    "browser": "Chrome",
                    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                    "viewport": {"width": 1366, "height": 768},
                    "screen": {"width": 1366, "height": 768, "depth": 24},
                    "timezone": "America/Los_Angeles",
                    "locale": "en-US",
                    "webgl_vendor": "Google Inc. (NVIDIA)",
                    "webgl_renderer": "ANGLE (NVIDIA, NVIDIA GeForce GTX 1050 Ti Direct3D11 vs_5_0 ps_5_0)",
                    "fonts": ["Arial", "Calibri", "Cambria", "Comic Sans MS", "Consolas", "Courier New", "Georgia", "Impact", "Lucida Console", "Tahoma", "Times New Roman", "Trebuchet MS", "Verdana"],
                    "plugins": ["Chrome PDF Plugin", "Chrome PDF Viewer", "Native Client"],
                    "hardware_concurrency": 4,
                    "device_memory": 4,
                    "max_touch_points": 0,
                    "color_depth": 24,
                    "pixel_ratio": 1.25
                },
                {
                    "os": "Mac",
                    "browser": "Chrome",
                    "user_agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "viewport": {"width": 1440, "height": 900},
                    "screen": {"width": 2880, "height": 1800, "depth": 24},
                    "timezone": "America/Chicago",
                    "locale": "en-US",
                    "webgl_vendor": "Google Inc. (Apple)",
                    "webgl_renderer": "ANGLE (Apple, Apple M1 Pro, OpenGL 4.1)",
                    "fonts": ["Helvetica Neue", "Helvetica", "Arial", "Times", "Times New Roman", "Courier", "Courier New", "Verdana", "Georgia", "Palatino", "Garamond", "Bookman", "Comic Sans MS", "Trebuchet MS", "Arial Black", "Impact"],
                    "plugins": ["Chrome PDF Plugin", "Chrome PDF Viewer", "Native Client"],
                    "hardware_concurrency": 10,
                    "device_memory": 16,
                    "max_touch_points": 0,
                    "color_depth": 30,
                    "pixel_ratio": 2
                },
                {
                    "os": "Windows",
                    "browser": "Firefox",
                    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
                    "viewport": {"width": 1920, "height": 1080},
                    "screen": {"width": 1920, "height": 1080, "depth": 24},
                    "timezone": "America/Denver",
                    "locale": "en-US",
                    "webgl_vendor": "Google Inc.",
                    "webgl_renderer": "ANGLE (Intel(R) UHD Graphics 620 Direct3D11 vs_5_0 ps_5_0)",
                    "fonts": ["Arial", "Calibri", "Cambria", "Comic Sans MS", "Consolas", "Courier New", "Georgia", "Impact", "Lucida Console", "Tahoma", "Times New Roman", "Trebuchet MS", "Verdana"],
                    "plugins": [],
                    "hardware_concurrency": 8,
                    "device_memory": 8,
                    "max_touch_points": 0,
                    "color_depth": 24,
                    "pixel_ratio": 1
                }
            ]
        }
    
    def get_random_fingerprint(self, account_identifier: Optional[str] = None) -> Dict:
        """Get a fingerprint - consistent for same account, random otherwise"""
        if account_identifier:
            index = hash(account_identifier) % len(self.fingerprints['profiles'])
            return self.fingerprints['profiles'][index]
        return random.choice(self.fingerprints['profiles'])
    
    def get_context_options(self, fingerprint: Dict) -> Dict:
        """Get Playwright context options from fingerprint"""
        user_agent = fingerprint['user_agent']
        if 'Mobile' in user_agent or 'Android' in user_agent or 'iPhone' in user_agent:
            user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

        os_name = fingerprint.get('os', 'Windows')
        return {
            'viewport': fingerprint['viewport'],
            'user_agent': user_agent,
            'locale': fingerprint['locale'],
            'timezone_id': fingerprint['timezone'],
            'color_scheme': 'light',
            'reduced_motion': 'no-preference',
            'device_scale_factor': fingerprint.get('pixel_ratio', 1),
            'is_mobile': False,
            'has_touch': False,
            'bypass_csp': False,
            'ignore_https_errors': False,
            'java_script_enabled': True,
            'accept_downloads': False,
            'permissions': [],
            'extra_http_headers': {
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'Sec-Ch-Ua-Mobile': '?0',
                'Sec-Ch-Ua-Platform': f'"{os_name}"',
                'Upgrade-Insecure-Requests': '1',
            }
        }
    
    def get_stealth_scripts(self, fingerprint: Dict) -> str:
        """
        Comprehensive JS stealth injection covering all major vectors Facebook
        uses to detect automation: webdriver, plugins, languages, hardware,
        platform/vendor, window outer dimensions, WebGL, canvas noise,
        chrome object, permissions, battery, connection, hasFocus,
        visibilityState, Notification, Error stack sanitization,
        and iframe contentWindow propagation.
        """
        plugins_json = json.dumps(fingerprint.get('plugins', []))
        locale = fingerprint['locale']
        hw_concurrency = fingerprint.get('hardware_concurrency', 4)
        device_memory = fingerprint.get('device_memory', 4)
        max_touch = fingerprint.get('max_touch_points', 0)
        color_depth = fingerprint.get('color_depth', 24)
        pixel_ratio = fingerprint.get('pixel_ratio', 1)
        webgl_vendor = fingerprint.get('webgl_vendor', 'Google Inc.')
        webgl_renderer = fingerprint.get('webgl_renderer', 'ANGLE')
        vp_width = fingerprint['viewport']['width']
        vp_height = fingerprint['viewport']['height']
        os_name = fingerprint.get('os', 'Windows')
        rtt = random.choice([50, 75, 100, 125])
        downlink = random.choice([1.5, 5.0, 10.0, 20.0])
        platform_map = {'Windows': 'Win32', 'Mac': 'MacIntel', 'Linux': 'Linux x86_64'}
        platform = platform_map.get(os_name, 'Win32')

        return f'''
        // 1. Remove webdriver flag
        Object.defineProperty(navigator, 'webdriver', {{ get: () => undefined }});
        delete navigator.__proto__.webdriver;

        // 2. Plugins as real PluginArray objects
        (function() {{
            const pluginNames = {plugins_json};
            const pluginData = pluginNames.map((name) => ({{
                name,
                filename: name.toLowerCase().replace(/ /g, '-') + '.so',
                description: name,
                length: 1,
                item: () => null,
                namedItem: () => null,
            }}));
            Object.defineProperty(navigator, 'plugins', {{
                get: () => {{
                    const arr = pluginData;
                    arr.__proto__ = PluginArray.prototype;
                    return arr;
                }}
            }});
            Object.defineProperty(navigator, 'mimeTypes', {{
                get: () => {{
                    const arr = [];
                    arr.__proto__ = MimeTypeArray.prototype;
                    return arr;
                }}
            }});
        }})();

        // 3. Languages
        Object.defineProperty(navigator, 'languages', {{ get: () => ['{locale}', 'en-US', 'en'] }});
        Object.defineProperty(navigator, 'language',  {{ get: () => '{locale}' }});

        // 4. Hardware
        Object.defineProperty(navigator, 'hardwareConcurrency', {{ get: () => {hw_concurrency} }});
        Object.defineProperty(navigator, 'deviceMemory',        {{ get: () => {device_memory} }});
        Object.defineProperty(navigator, 'maxTouchPoints',      {{ get: () => {max_touch} }});

        // 5. Platform & Vendor
        Object.defineProperty(navigator, 'platform',   {{ get: () => '{platform}' }});
        Object.defineProperty(navigator, 'vendor',     {{ get: () => 'Google Inc.' }});
        Object.defineProperty(navigator, 'vendorSub',  {{ get: () => '' }});
        Object.defineProperty(navigator, 'productSub', {{ get: () => '20030107' }});

        // 6. Screen
        Object.defineProperty(screen, 'colorDepth',        {{ get: () => {color_depth} }});
        Object.defineProperty(screen, 'pixelDepth',        {{ get: () => {color_depth} }});
        Object.defineProperty(window, 'devicePixelRatio',  {{ get: () => {pixel_ratio} }});

        // 7. Window outer dimensions (headless sets inner==outer, a strong signal)
        Object.defineProperty(window, 'outerWidth',  {{ get: () => {vp_width} }});
        Object.defineProperty(window, 'outerHeight', {{ get: () => {vp_height + 74} }});

        // 8. WebGL vendor & renderer
        (function() {{
            const getParam = WebGLRenderingContext.prototype.getParameter;
            WebGLRenderingContext.prototype.getParameter = function(p) {{
                if (p === 37445) return '{webgl_vendor}';
                if (p === 37446) return '{webgl_renderer}';
                return getParam.apply(this, arguments);
            }};
            if (typeof WebGL2RenderingContext !== 'undefined') {{
                const getParam2 = WebGL2RenderingContext.prototype.getParameter;
                WebGL2RenderingContext.prototype.getParameter = function(p) {{
                    if (p === 37445) return '{webgl_vendor}';
                    if (p === 37446) return '{webgl_renderer}';
                    return getParam2.apply(this, arguments);
                }};
            }}
        }})();

        // 9. Canvas fingerprint noise
        (function() {{
            const orig = HTMLCanvasElement.prototype.toDataURL;
            HTMLCanvasElement.prototype.toDataURL = function(type) {{
                const ctx = this.getContext('2d');
                if (ctx) {{
                    const imgData = ctx.getImageData(0, 0, this.width, this.height);
                    for (let i = 0; i < imgData.data.length; i += 100) {{
                        imgData.data[i] = Math.min(255, imgData.data[i] + (Math.random() * 2 - 1));
                    }}
                    ctx.putImageData(imgData, 0, 0);
                }}
                return orig.apply(this, arguments);
            }};
        }})();

        // 10. chrome object
        if (!window.chrome) {{
            window.chrome = {{
                app: {{ isInstalled: false }},
                runtime: {{
                    id: undefined,
                    connect: () => {{}},
                    sendMessage: () => {{}},
                    onConnect: {{ addListener: () => {{}}, removeListener: () => {{}} }},
                    onMessage: {{ addListener: () => {{}}, removeListener: () => {{}} }},
                }},
                loadTimes: function() {{
                    return {{
                        requestTime: Date.now() / 1000 - Math.random() * 2,
                        startLoadTime: Date.now() / 1000 - Math.random(),
                        finishLoadTime: Date.now() / 1000,
                        firstPaintTime: Date.now() / 1000,
                        navigationType: 'Other',
                        wasFetchedViaSpdy: true,
                        npnNegotiatedProtocol: 'h2',
                        connectionInfo: 'h2',
                    }};
                }},
                csi: function() {{
                    return {{ startE: Date.now(), onloadT: Date.now(), pageT: Math.random() * 5000, tran: 15 }};
                }},
            }};
        }}

        // 11. Permissions
        (function() {{
            const origQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (params) => {{
                if (params.name === 'notifications') return Promise.resolve({{ state: 'denied' }});
                if (params.name === 'geolocation')   return Promise.resolve({{ state: 'prompt' }});
                if (params.name === 'camera')        return Promise.resolve({{ state: 'prompt' }});
                if (params.name === 'microphone')    return Promise.resolve({{ state: 'prompt' }});
                return origQuery.call(navigator.permissions, params);
            }};
        }})();

        // 12. Notification.permission must be 'default' not 'denied'
        try {{ Object.defineProperty(Notification, 'permission', {{ get: () => 'default' }}); }} catch(e) {{}}

        // 13. Battery API
        navigator.getBattery = () => Promise.resolve({{
            charging: true, chargingTime: 0, dischargingTime: Infinity,
            level: 0.87 + Math.random() * 0.1,
            addEventListener: () => {{}}, removeEventListener: () => {{}},
        }});

        // 14. Network connection
        try {{
            if (navigator.connection) {{
                Object.defineProperty(navigator.connection, 'rtt',           {{ get: () => {rtt} }});
                Object.defineProperty(navigator.connection, 'downlink',      {{ get: () => {downlink} }});
                Object.defineProperty(navigator.connection, 'effectiveType', {{ get: () => '4g' }});
                Object.defineProperty(navigator.connection, 'saveData',      {{ get: () => false }});
            }}
        }} catch(e) {{}}

        // 15. document.hasFocus — Facebook polls this to detect inactive tabs
        Document.prototype.hasFocus = function() {{ return true; }};
        document.hasFocus = function() {{ return true; }};

        // 16. visibilityState — Facebook checks if tab is in background
        try {{
            Object.defineProperty(document, 'visibilityState', {{ get: () => 'visible' }});
            Object.defineProperty(document, 'hidden',          {{ get: () => false }});
        }} catch(e) {{}}

        // 17. Error stack sanitization — hides Playwright/chromium paths
        (function() {{
            const origError = Error;
            window.Error = function(...args) {{
                const err = new origError(...args);
                if (err.stack) {{
                    err.stack = err.stack
                        .replace(/playwright[^\\n]*/gi, '')
                        .replace(/chromium[^\\n]*/gi, '');
                }}
                return err;
            }};
            window.Error.prototype = origError.prototype;
        }})();

        // 18. iframe contentWindow stealth propagation
        const _origCW = Object.getOwnPropertyDescriptor(HTMLIFrameElement.prototype, 'contentWindow');
        if (_origCW) {{
            Object.defineProperty(HTMLIFrameElement.prototype, 'contentWindow', {{
                get: function() {{
                    const win = _origCW.get.call(this);
                    if (!win) return win;
                    try {{ Object.defineProperty(win.navigator, 'webdriver', {{ get: () => undefined }}); }} catch(e) {{}}
                    return win;
                }}
            }});
        }}
        '''


class HumanBehaviorSimulator:
    """Simulates human-like behavior patterns - tuned for Facebook's heavier pages"""

    async def simulate_human_typing(self, page: Page, selector: str, text: str):
        """
        Type text character-by-character with realistic delays.
        Occasional micro-pauses and rare typo+correction.
        Much harder to detect than Playwright fill().
        """
        element = await page.wait_for_selector(selector, state='visible', timeout=10000)
        await element.click()
        await asyncio.sleep(random.uniform(0.3, 0.8))

        i = 0
        while i < len(text):
            char = text[i]
            # 5% chance of typo+correction per alphabetic char
            if random.random() < 0.05 and i > 0 and char.isalpha():
                wrong = random.choice(string.ascii_lowercase)
                await page.keyboard.type(wrong, delay=random.randint(60, 150))
                await asyncio.sleep(random.uniform(0.15, 0.35))
                await page.keyboard.press('Backspace')
                await asyncio.sleep(random.uniform(0.1, 0.25))
            await page.keyboard.type(char, delay=random.randint(50, 180))
            # Micro-pause every 5-12 chars (like reading while typing)
            if i > 0 and i % random.randint(5, 12) == 0:
                await asyncio.sleep(random.uniform(0.1, 0.4))
            i += 1

        await asyncio.sleep(random.uniform(0.3, 0.7))

    async def warm_up_browsing(self, page: Page):
        """
        Visit one neutral site before Facebook to build a realistic
        browsing history in the session. Avoids cold-start detection.
        """
        sites = [
            ('https://www.google.com', 3, 5),
            ('https://www.wikipedia.org', 2, 4),
        ]
        site, min_wait, max_wait = random.choice(sites)
        try:
            logger.info(f"Warm-up: visiting {site}...")
            await page.goto(site, wait_until='domcontentloaded', timeout=20000)
            await asyncio.sleep(random.uniform(min_wait, max_wait))
            await page.evaluate(f'window.scrollTo(0, {random.randint(80, 350)})')
            await asyncio.sleep(random.uniform(1, 2))
            for _ in range(random.randint(2, 4)):
                await page.mouse.move(random.randint(200, 900), random.randint(100, 500))
                await asyncio.sleep(random.uniform(0.2, 0.5))
        except Exception as e:
            logger.debug(f"Warm-up failed (non-critical): {e}")

    async def simulate_pre_navigation(self, page: Page):
        await asyncio.sleep(random.uniform(0.5, 2.0))

    async def simulate_post_navigation(self, page: Page):
        await asyncio.sleep(random.uniform(3, 6))
        for _ in range(random.randint(2, 5)):
            await page.mouse.move(random.randint(100, 800), random.randint(100, 600))
            await asyncio.sleep(random.uniform(0.1, 0.4))

    async def simulate_content_render(self, page: Page):
        await asyncio.sleep(random.uniform(4, 8))

    async def simulate_scroll(self, page: Page):
        scroll_amount = random.randint(400, 800)
        await page.evaluate(f'window.scrollTo(0, {scroll_amount})')
        await asyncio.sleep(random.uniform(2, 5))

    async def simulate_deep_scroll(self, page: Page):
        for _ in range(random.randint(2, 4)):
            await page.evaluate(f'window.scrollBy(0, {random.randint(300, 600)})')
            await asyncio.sleep(random.uniform(1.5, 3))

    async def simulate_post_load(self, page: Page):
        await asyncio.sleep(random.uniform(1.5, 3))

    async def simulate_final_wait(self, page: Page):
        await asyncio.sleep(random.uniform(2, 4))

    async def simulate_error_recovery(self, page: Page):
        await asyncio.sleep(random.uniform(8, 20))


class NetworkPatternRandomizer:
    """Randomizes network patterns to avoid detection"""
    
    async def randomize_network(self, page: Page):
        """Apply network randomization"""
        await asyncio.sleep(random.uniform(0.2, 0.8))


class SessionManager:
    """
    Manages Playwright storage-state persistence.
    Saves full browser cookies + localStorage after login so future
    runs can restore the session and skip login entirely.
    """

    SESSION_MAX_AGE_HOURS = 12

    def __init__(self, data_dir: Path = None):
        self.data_dir = data_dir or Path(__file__).parent / 'data'
        self.session_dir = self.data_dir / 'sessions'
        self.session_dir.mkdir(parents=True, exist_ok=True)

    def _session_path(self, account_id: str) -> Path:
        safe_id = account_id.replace('@', '_at_').replace('.', '_').replace('+', '_')
        return self.session_dir / f"{safe_id}_state.json"

    def has_session(self, account_id: str) -> bool:
        """Return True if a fresh (< SESSION_MAX_AGE_HOURS) session exists"""
        path = self._session_path(account_id)
        if not path.exists():
            return False
        age_hours = (datetime.now().timestamp() - path.stat().st_mtime) / 3600
        if age_hours > self.SESSION_MAX_AGE_HOURS:
            logger.info(f"Session for {account_id} is stale ({age_hours:.1f}h), will re-login")
            return False
        return True

    def get_session_path(self, account_id: str) -> Optional[Path]:
        """Return session file path if valid, else None"""
        return self._session_path(account_id) if self.has_session(account_id) else None

    async def save_session(self, context: BrowserContext, account_id: str):
        """Save Playwright browser storage state (cookies + localStorage) to disk"""
        path = self._session_path(account_id)
        await context.storage_state(path=str(path))
        logger.info(f"Session saved for {account_id} → {path.name}")

    def invalidate_session(self, account_id: str):
        """Delete saved session (after block, failed auth, or stale checkpoint)"""
        path = self._session_path(account_id)
        if path.exists():
            path.unlink()
            logger.info(f"Session invalidated for {account_id}")


class CheckpointHandler:
    """
    Automatically resolves Facebook post-login security challenges
    with zero human intervention.

    Handles:
    - "Was this you?" / "This was me" confirm dialogs
    - "Remember this browser?" → clicks Save/Continue
    - Notification popups → dismisses
    - Checkpoint redirect loops → navigates away to break them
    - SMS/email code screens → logs warning, returns False (unresolvable)
    """

    CONFIRM_SELECTORS = [
        'button:has-text("This Was Me")',
        'button:has-text("This was me")',
        'button:has-text("That Was Me")',
        'button:has-text("That was me")',
        'button:has-text("Yes, it was me")',
        'button:has-text("Confirm")',
        'a:has-text("This Was Me")',
        'a:has-text("That Was Me")',
    ]

    SAVE_BROWSER_SELECTORS = [
        'button:has-text("Save")',
        'button:has-text("Save Browser")',
        'button:has-text("OK")',
        'button:has-text("Continue")',
        'button:has-text("Yes")',
    ]

    DISMISS_SELECTORS = [
        'button:has-text("Not Now")',
        'button:has-text("Not now")',
        'button:has-text("Skip")',
        'button:has-text("Close")',
        'button[aria-label="Close"]',
        '[aria-label="Dismiss"]',
    ]

    CODE_REQUIRED_INDICATORS = [
        'enter the code', 'enter a login code', 'enter the 6-digit code',
        'check your email', 'check your phone', 'we sent a code',
        'two-factor authentication', 'two factor authentication',
        'login code', 'security code', 'authentication code',
    ]

    def _is_checkpoint_url(self, url: str) -> bool:
        patterns = [
            'checkpoint', 'two_factor', 'login/identify',
            'login/device-based', 'recover', 'confirmemail',
            'confirmidentity',
        ]
        return any(p in url.lower() for p in patterns)

    def _has_challenge_text(self, text: str) -> bool:
        phrases = [
            'confirm your identity', 'verify your identity', 'unusual activity',
            'suspicious login', 'was this you', 'did you try to log in',
            'confirm it\'s you', 'you\'ve been temporarily locked out',
        ]
        return any(p in text for p in phrases)

    async def _try_click_any(self, page: Page, selectors: list) -> bool:
        for sel in selectors:
            try:
                el = await page.wait_for_selector(sel, timeout=2000, state='visible')
                if el:
                    await el.click()
                    return True
            except:
                continue
        return False

    async def handle(self, page: Page, context_label: str = 'post-login') -> bool:
        """
        Attempt to auto-resolve any checkpoint on the current page.
        Returns True if resolved (or no checkpoint), False if unresolvable.
        """
        logger.info(f"Checkpoint handler running ({context_label})...")

        for attempt in range(6):
            current_url = page.url
            page_text = ''
            try:
                page_text = (await page.inner_text('body')).lower()
            except:
                pass

            # No longer on a challenge page
            if not self._is_checkpoint_url(current_url) and not self._has_challenge_text(page_text):
                logger.info("No checkpoint detected, proceeding")
                return True

            logger.info(f"  Checkpoint attempt {attempt + 1}: {current_url}")

            # SMS/email code required → cannot auto-resolve
            if any(ind in page_text for ind in self.CODE_REQUIRED_INDICATORS):
                logger.warning(
                    "SMS/email code checkpoint — cannot auto-resolve. "
                    "Consider enabling 2FA recovery email or disabling 2FA."
                )
                return False

            # "This was me" confirmation
            if await self._try_click_any(page, self.CONFIRM_SELECTORS):
                logger.info("  Clicked confirm/this-was-me button")
                await asyncio.sleep(random.uniform(3, 5))
                continue

            # "Remember this browser" / Save
            if await self._try_click_any(page, self.SAVE_BROWSER_SELECTORS):
                logger.info("  Clicked save/continue on remember-browser dialog")
                await asyncio.sleep(random.uniform(2, 4))
                continue

            # Dismiss notification / other popups
            if await self._try_click_any(page, self.DISMISS_SELECTORS):
                logger.info("  Dismissed popup")
                await asyncio.sleep(random.uniform(1.5, 3))
                continue

            # Stale checkpoint loop — navigate home to break it
            if 'checkpoint' in current_url or '/login' in current_url:
                logger.info("  Navigating to facebook.com to break checkpoint loop")
                try:
                    await page.goto('https://www.facebook.com/', wait_until='domcontentloaded', timeout=20000)
                    await asyncio.sleep(random.uniform(3, 5))
                except:
                    pass
                continue

            await asyncio.sleep(random.uniform(2, 4))

        # Final check after all attempts
        if not self._is_checkpoint_url(page.url):
            return True
        logger.warning(f"Could not auto-resolve checkpoint at: {page.url}")
        return False


class AntiDetectionManager:
    """Central manager for all anti-detection techniques"""

    def __init__(self, data_dir: Path = None):
        self.data_dir = data_dir or Path(__file__).parent / 'data'
        self.fingerprint_mgr = BrowserFingerprint(self.data_dir)
        self.behavior_sim = HumanBehaviorSimulator()
        self.network_sim = NetworkPatternRandomizer()
        self.session_mgr = SessionManager(self.data_dir)
        self.checkpoint_handler = CheckpointHandler()
        logger.info("AntiDetectionManager initialized")

    def get_fingerprint_for_account(self, account_id: str):
        return self.fingerprint_mgr.get_random_fingerprint(account_id)

    async def apply_pre_navigation_behavior(self, page: Page):
        await self.network_sim.randomize_network(page)
        await self.behavior_sim.simulate_pre_navigation(page)

    async def apply_post_navigation_behavior(self, page: Page):
        await self.behavior_sim.simulate_post_navigation(page)
