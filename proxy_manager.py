#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Residential Proxy Manager for Facebook Scraper
Supports multiple providers: Bright Data, IProyal, Storm Proxies, NetNut, or any custom proxy.

Usage:
    from proxy_manager import ProxyManager

    # From config file (recommended)
    pm = ProxyManager.from_config("config/scraper_config.json")

    # Or from environment variables
    pm = ProxyManager.from_env()

    # Or manual
    pm = ProxyManager(provider="netnut", username="user", password="pass")

    # Get Playwright proxy dict for browser context
    proxy = pm.get_playwright_proxy()

    # Get requests-style proxy dict (for aiohttp / requests)
    proxies = pm.get_requests_proxy()
"""

import os
import json
import uuid
import logging
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────
# Default gateway settings per provider
# ──────────────────────────────────────────────────────────────────────
PROVIDER_DEFAULTS: Dict[str, Dict] = {
    "brightdata": {
        "host": "brd.superproxy.io",
        "port": 22225,
        "description": "Bright Data Residential Proxy",
        "signup_url": "https://get.brightdata.com/o1kpd2da8iv4",
        "supports_country": True,
        "supports_sticky": True,
        "sticky_separator": "-session-",
        "country_separator": "-country-",
    },
    "iproyal": {
        "host": "proxy.iproyal.com",
        "port": 12321,
        "description": "IProyal Residential Proxy",
        "signup_url": "https://iproyal.com/?r=ScrapeClaw",
        "supports_country": True,
        "supports_sticky": True,
        "sticky_separator": "-session-",
        "country_separator": "-country-",
    },
    "stormproxies": {
        "host": "rotating.stormproxies.com",
        "port": 9999,
        "description": "Storm Proxies Residential Proxy",
        "signup_url": "https://stormproxies.com/clients/aff/go/scrapeclaw",
        "supports_country": True,
        "supports_sticky": True,
        "sticky_separator": "-session-",
        "country_separator": "-country-",
    },
    "netnut": {
        "host": "gw-resi.netnut.io",
        "port": 5959,
        "description": "NetNut Residential Proxy",
        "signup_url": "https://netnut.io?ref=mwrlzwv",
        "supports_country": True,
        "supports_sticky": True,
        "sticky_separator": "-session-",
        "country_separator": "-country-",
    },
    "custom": {
        "host": "",
        "port": 0,
        "description": "Custom proxy provider",
        "signup_url": "",
        "supports_country": False,
        "supports_sticky": False,
        "sticky_separator": "",
        "country_separator": "",
    },
}


class ProxyManager:
    """
    Generic residential proxy manager.

    Supports rotating IPs, sticky sessions, and country targeting
    across Bright Data, IProyal, Storm Proxies, NetNut, or any custom HTTPS proxy.
    """

    def __init__(
        self,
        provider: str = "netnut",
        host: str = None,
        port: int = None,
        username: str = "",
        password: str = "",
        country: str = None,
        sticky: bool = False,
        sticky_ttl_minutes: int = 10,
        protocol: str = "http",
    ):
        self.provider = provider.lower().strip()
        defaults = PROVIDER_DEFAULTS.get(self.provider, PROVIDER_DEFAULTS["custom"])

        self.host = host or defaults["host"]
        self.port = port or defaults["port"]
        self.username = username
        self.password = password
        self.country = country
        self.sticky = sticky
        self.sticky_ttl = sticky_ttl_minutes
        self.protocol = protocol
        self.provider_meta = defaults

        # Generate a sticky session id (reused for the lifetime of this object)
        self._session_id = uuid.uuid4().hex[:8]

        if not self.host or not self.port:
            logger.warning("Proxy host/port not configured — proxy will be disabled.")

    # ── Factory constructors ──────────────────────────────────────────

    @classmethod
    def from_config(cls, config_path: str = "config/scraper_config.json") -> "ProxyManager":
        """
        Build a ProxyManager from the 'proxy' section of scraper_config.json.

        Expected config shape:
        {
            "proxy": {
                "enabled": true,
                "provider": "netnut",
                "host": "",          # optional, uses provider default
                "port": 0,           # optional, uses provider default
                "username": "user",
                "password": "pass",
                "country": "us",     # optional
                "sticky": true,      # optional, default false
                "sticky_ttl_minutes": 10,
                "protocol": "http"
            }
        }
        """
        path = Path(config_path)
        if not path.is_absolute():
            path = Path(__file__).parent / path

        try:
            with open(path, "r") as f:
                full_cfg = json.load(f)
        except Exception as e:
            logger.warning(f"Could not load proxy config from {path}: {e}")
            return cls()

        cfg = full_cfg.get("proxy", {})
        if not cfg.get("enabled", False):
            logger.info("Proxy is disabled in config.")
            return cls()

        # Allow env-var overrides for credentials (never store passwords in JSON)
        username = os.getenv("PROXY_USERNAME", cfg.get("username", ""))
        password = os.getenv("PROXY_PASSWORD", cfg.get("password", ""))

        return cls(
            provider=cfg.get("provider", "netnut"),
            host=cfg.get("host") or None,
            port=cfg.get("port") or None,
            username=username,
            password=password,
            country=cfg.get("country"),
            sticky=cfg.get("sticky", False),
            sticky_ttl_minutes=cfg.get("sticky_ttl_minutes", 10),
            protocol=cfg.get("protocol", "http"),
        )

    @classmethod
    def from_env(cls) -> "ProxyManager":
        """
        Build a ProxyManager entirely from environment variables.

        Env vars:
            PROXY_ENABLED    - "true" to enable  (default: false)
            PROXY_PROVIDER   - brightdata | iproyal | stormproxies | netnut | custom
            PROXY_HOST       - override gateway host
            PROXY_PORT       - override gateway port
            PROXY_USERNAME   - proxy username
            PROXY_PASSWORD   - proxy password
            PROXY_COUNTRY    - two-letter country code (e.g. us, gb)
            PROXY_STICKY     - "true" for sticky sessions
            PROXY_PROTOCOL   - http | socks5 (default: http)
        """
        if os.getenv("PROXY_ENABLED", "false").lower() != "true":
            logger.info("Proxy disabled (PROXY_ENABLED != true).")
            return cls()

        return cls(
            provider=os.getenv("PROXY_PROVIDER", "netnut"),
            host=os.getenv("PROXY_HOST") or None,
            port=int(os.getenv("PROXY_PORT", "0")) or None,
            username=os.getenv("PROXY_USERNAME", ""),
            password=os.getenv("PROXY_PASSWORD", ""),
            country=os.getenv("PROXY_COUNTRY"),
            sticky=os.getenv("PROXY_STICKY", "false").lower() == "true",
            protocol=os.getenv("PROXY_PROTOCOL", "http"),
        )

    # ── Core helpers ──────────────────────────────────────────────────

    @property
    def is_enabled(self) -> bool:
        """Return True if proxy is properly configured."""
        return bool(self.host and self.port and self.username)

    def _build_username(self) -> str:
        """
        Compose the effective username with optional country and sticky tokens.
        Each provider encodes these differently in the username string.
        """
        user = self.username
        meta = self.provider_meta

        # Country targeting
        if self.country and meta.get("supports_country"):
            sep = meta["country_separator"]
            user = f"{user}{sep}{self.country}"

        # Sticky session
        if self.sticky and meta.get("supports_sticky"):
            sep = meta["sticky_separator"]
            user = f"{user}{sep}{self._session_id}"

        return user

    def _build_url(self) -> str:
        """Build the full proxy URL."""
        effective_user = self._build_username()
        return f"{self.protocol}://{effective_user}:{self.password}@{self.host}:{self.port}"

    def rotate_session(self):
        """Force a new sticky session ID (next request gets a new IP)."""
        self._session_id = uuid.uuid4().hex[:8]
        logger.info(f"Proxy session rotated → {self._session_id}")

    # ── Output formats ────────────────────────────────────────────────

    def get_playwright_proxy(self) -> Optional[Dict]:
        """
        Return a proxy dict suitable for Playwright browser.new_context(proxy=...).
        Returns None if proxy is not enabled.

        Example return:
            {
                "server": "http://gw-resi.netnut.io:5959",
                "username": "user-country-us-session-abc123",
                "password": "pass"
            }
        """
        if not self.is_enabled:
            return None

        return {
            "server": f"{self.protocol}://{self.host}:{self.port}",
            "username": self._build_username(),
            "password": self.password,
        }

    def get_requests_proxy(self) -> Optional[Dict[str, str]]:
        """
        Return a proxy dict suitable for `requests` or `aiohttp`.
        Returns None if proxy is not enabled.

        Example return:
            {
                "http": "http://user:pass@host:port",
                "https": "http://user:pass@host:port"
            }
        """
        if not self.is_enabled:
            return None

        url = self._build_url()
        return {"http": url, "https": url}

    def get_aiohttp_proxy(self) -> Optional[str]:
        """
        Return a single proxy URL for aiohttp.ClientSession(proxy=...).
        Returns None if proxy is not enabled.
        """
        if not self.is_enabled:
            return None
        return self._build_url()

    # ── Info / debugging ──────────────────────────────────────────────

    def info(self) -> Dict:
        """Return a dict summarising the current proxy configuration (no password)."""
        return {
            "enabled": self.is_enabled,
            "provider": self.provider,
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "country": self.country,
            "sticky": self.sticky,
            "sticky_session_id": self._session_id if self.sticky else None,
            "protocol": self.protocol,
            "signup_url": self.provider_meta.get("signup_url", ""),
        }

    def __repr__(self) -> str:
        state = "enabled" if self.is_enabled else "disabled"
        return f"<ProxyManager provider={self.provider} {state} host={self.host}:{self.port}>"
