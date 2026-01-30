import asyncio
import logging
import re
import base64
import json
import os
import gzip
import zlib
import random
import time
from urllib.parse import urlparse, urljoin
from typing import Dict, Any, Optional, List

import aiohttp
from aiohttp import ClientSession, ClientTimeout, TCPConnector, FormData
from aiohttp_socks import ProxyConnector

try:
    import zstandard
except ImportError:
    zstandard = None

from mediaflow_proxy.extractors.base import BaseExtractor, ExtractorError, HttpResponse
from mediaflow_proxy.configs import settings

logger = logging.getLogger(__name__)

# Global state to persist between extractor instances
_GLOBAL_SESSION: Optional[ClientSession] = None
_GLOBAL_PROXY_URL: Optional[str] = None
_SESSION_LOCK = asyncio.Lock()
_IFRAME_HOSTS: List[str] = []
_STREAM_DATA_CACHE: Dict[str, Any] = {}
_DLHD_CONFIG = {
    'auth_url': 'https://security.kiko2.ru/auth2.php',
    'stream_cdn_template': 'https://top1.kiko2.ru/top1/cdn/{CHANNEL}/mono.css',
    'stream_other_template': 'https://{SERVER_KEY}new.kiko2.ru/{SERVER_KEY}/{CHANNEL}/mono.css',
    'server_lookup_url': 'https://chevy.kiko2.ru/server_lookup',
    'base_domain': 'kiko2.ru'
}

class DLHDExtractor(BaseExtractor):
    """DLHD Extractor ported from EasyProxy with persistent session and advanced anti-bot handling."""

    USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
    CHANNEL_ID_PATTERNS = [
        r'/premium(\d+)/mono',
        r'/(?:watch|stream|cast|player)/stream-(\d+)\.php',
        r'watch\.php\?id=(\d+)',
        r'(?:%2F|/)stream-(\d+)\.php',
        r'stream-(\d+)\.php',
        r'[?&]id=(\d+)',
        r'daddyhd\.php\?id=(\d+)',
    ]

    def __init__(self, request_headers: dict):
        super().__init__(request_headers)
        self.mediaflow_endpoint = "hls_manifest_proxy"
        self._extraction_locks: Dict[str, asyncio.Lock] = {}

    async def _get_session(self) -> tuple[ClientSession, Optional[str]]:
        global _GLOBAL_SESSION, _GLOBAL_PROXY_URL
        async with _SESSION_LOCK:
            if _GLOBAL_SESSION is None or _GLOBAL_SESSION.closed:
                from mediaflow_proxy.utils.http_client import get_routing_config, _create_connector
                
                # Get default routing for a DLHD-like URL to determine proxy
                routing_config = get_routing_config()
                # Use a more generic pattern for DaddyLive
                route_match = routing_config.match_url("https://dlhd.dad")
                
                connector, _GLOBAL_PROXY_URL = _create_connector(route_match.proxy_url, verify_ssl=False)
                
                timeout = ClientTimeout(total=60, connect=30, sock_read=30)
                _GLOBAL_SESSION = ClientSession(
                    timeout=timeout,
                    connector=connector,
                    headers={"user-agent": self.USER_AGENT},
                    cookie_jar=aiohttp.CookieJar()
                )
            return _GLOBAL_SESSION, _GLOBAL_PROXY_URL

    @staticmethod
    def extract_channel_id(url: str) -> Optional[str]:
        for pattern in DLHDExtractor.CHANNEL_ID_PATTERNS:
            match = re.search(pattern, url, re.IGNORECASE)
            if match:
                return match.group(1)
        return None

    def _build_stream_url(self, server_key: str, channel_key: str) -> str:
        if server_key == 'top1/cdn':
            return _DLHD_CONFIG['stream_cdn_template'].replace('{CHANNEL}', channel_key)
        else:
            return _DLHD_CONFIG['stream_other_template'].replace('{SERVER_KEY}', server_key).replace('{CHANNEL}', channel_key)

    def _build_stream_headers(self, iframe_url: str, channel_key: str, auth_token: str, secret_key: str = None) -> dict:
        iframe_origin = f"https://{urlparse(iframe_url).netloc}"
        headers = {
            'User-Agent': self.USER_AGENT,
            'Referer': iframe_url,
            'Origin': iframe_origin,
            'Authorization': f'Bearer {auth_token}',
            'X-Channel-Key': channel_key,
            'X-User-Agent': self.USER_AGENT,
        }
        if secret_key:
            headers['X-Secret-Key'] = secret_key
        return headers

    async def _handle_response_content(self, response: aiohttp.ClientResponse) -> str:
        content_encoding = response.headers.get('Content-Encoding')
        raw_body = await response.read()
        
        try:
            if content_encoding == 'zstd' and zstandard:
                dctx = zstandard.ZstdDecompressor()
                with dctx.stream_reader(raw_body) as reader:
                    decompressed_body = reader.read()
                return decompressed_body.decode(response.charset or 'utf-8')
            elif content_encoding == 'gzip':
                decompressed_body = gzip.decompress(raw_body)
                return decompressed_body.decode(response.charset or 'utf-8')
            elif content_encoding == 'deflate':
                decompressed_body = zlib.decompress(raw_body)
                return decompressed_body.decode(response.charset or 'utf-8')
            else:
                return raw_body.decode(response.charset or 'utf-8', errors='replace')
        except Exception as e:
            logger.error(f"Decompression/decoding error from {response.url}: {e}")
            raise ExtractorError(f"Decompression failure for {response.url}: {e}")

    async def _make_robust_request(self, url: str, headers: dict = None, retries=3, initial_delay=2):
        final_headers = headers or {}
        # Apply specific headers for stream domain logic if needed
        parsed_url = urlparse(url)
        if _DLHD_CONFIG['base_domain'] in parsed_url.netloc:
            origin = f"{parsed_url.scheme}://{parsed_url.netloc}"
            final_headers.update({
                'User-Agent': self.USER_AGENT,
                'Referer': origin,
                'Origin': origin
            })
        
        final_headers['Accept-Encoding'] = 'gzip, deflate, zstd'
        
        for attempt in range(retries):
            try:
                session, proxy_url = await self._get_session()
                async with session.get(url, headers=final_headers, ssl=False, auto_decompress=False, proxy=proxy_url) as response:
                    response.raise_for_status()
                    content = await self._handle_response_content(response)
                    
                    # Return a compatibility object that looks like HttpResponse
                    return HttpResponse(
                        status=response.status,
                        headers=dict(response.headers),
                        text=content,
                        content=raw_body,
                        url=str(response.url)
                    )
            except Exception as e:
                if attempt == retries - 1:
                    raise ExtractorError(f"All {retries} attempts failed for {url}: {str(e)}")
                await asyncio.sleep(initial_delay * (2 ** attempt))

    async def _fetch_server_key(self, channel_key: str, iframe_url: str) -> str:
        server_lookup_url = f"{_DLHD_CONFIG['server_lookup_url']}?channel_id={channel_key}"
        iframe_origin = f"https://{urlparse(iframe_url).netloc}"
        lookup_headers = {
            'User-Agent': self.USER_AGENT,
            'Accept': '*/*',
            'Referer': iframe_url,
            'Origin': iframe_origin,
        }
        resp = await self._make_robust_request(server_lookup_url, headers=lookup_headers, retries=2)
        server_data = resp.json()
        server_key = server_data.get('server_key')
        if not server_key:
            raise ExtractorError(f"No server_key in response: {server_data}")
        return server_key

    async def _fetch_iframe_hosts(self) -> bool:
        global _IFRAME_HOSTS
        encoded_url = "aHR0cHM6Ly9pZnJhbWUuZGxoZC5kcGRucy5vcmcv"
        url = base64.b64decode(encoded_url).decode('utf-8')
        
        logger.info(f"🔄 Updating iframe host list...")
        try:
            session, proxy_url = await self._get_session()
            async with session.get(url, ssl=False, timeout=ClientTimeout(total=10), proxy=proxy_url) as response:
                if response.status == 200:
                    text = await response.text()
                    lines = [line.strip() for line in text.splitlines() if line.strip()]
                    new_hosts = []
                    
                    for line in lines:
                        if line.startswith('#AUTH_URL:'):
                            _DLHD_CONFIG['auth_url'] = line.replace('#AUTH_URL:', '').strip()
                        elif line.startswith('#STREAM_CDN_TEMPLATE:'):
                            _DLHD_CONFIG['stream_cdn_template'] = line.replace('#STREAM_CDN_TEMPLATE:', '').strip()
                        elif line.startswith('#STREAM_OTHER_TEMPLATE:'):
                            _DLHD_CONFIG['stream_other_template'] = line.replace('#STREAM_OTHER_TEMPLATE:', '').strip()
                        elif line.startswith('#SERVER_LOOKUP_URL:'):
                            _DLHD_CONFIG['server_lookup_url'] = line.replace('#SERVER_LOOKUP_URL:', '').strip()
                        elif line.startswith('#BASE_DOMAIN:'):
                            _DLHD_CONFIG['base_domain'] = line.replace('#BASE_DOMAIN:', '').strip()
                        elif not line.startswith('#'):
                            new_hosts.append(line)
                    
                    if new_hosts:
                        _IFRAME_HOSTS[:] = new_hosts
                        logger.info(f"✅ Iframe host list updated: {_IFRAME_HOSTS}")
                        return True
        except Exception as e:
            logger.error(f"Error updating iframe host: {e}")
        return False

    def _extract_secret_key(self, iframe_html: str, channel_key: str = None) -> Optional[str]:
        hmac_pattern = r'CryptoJS\.HmacSHA256\(resource,\s*([a-zA-Z_$][\w$]*)\)'
        hmac_match = re.search(hmac_pattern, iframe_html)
        if not hmac_match:
            hmac_pattern_general = r'HmacSHA256\([^,]+,\s*([a-zA-Z_$][\w$]*)\)'
            hmac_match = re.search(hmac_pattern_general, iframe_html)
        
        if not hmac_match:
            return None

        secret_var_name = hmac_match.group(1)
        # Try both 'let' and 'const' patterns
        let_pattern = rf'(?:let|const)\s+{re.escape(secret_var_name)}\s*='
        let_match = re.search(let_pattern, iframe_html)
        if not let_match:
            return None

        line_start = let_match.start()
        while line_start > 0 and iframe_html[line_start - 1] not in '\n\r':
            line_start -= 1
        
        line_end = iframe_html.find(';', let_match.end())
        if line_end == -1:
            line_end = iframe_html.find('\n', let_match.end())
            if line_end == -1: line_end = len(iframe_html)
            
        line_content = iframe_html[line_start:line_end + 1]
        base64_parts = re.findall(r'\"([A-Za-z0-9+/=]+)\"', line_content)
        if not base64_parts:
            return None

        combined_b64 = "".join(base64_parts)
        try:
            decoded = base64.b64decode(combined_b64).decode("utf-8")
            if 8 <= len(decoded) <= 128 and decoded != channel_key:
                return decoded
        except:
            pass
        return None

    def _extract_obfuscated_session_data(self, iframe_html: str) -> Optional[Dict[str, str]]:
        token_pattern = r'const\s+var_[a-f0-9]+\s*=\s*"(eyJ[^"]+)"'
        key_pattern = r'const\s+var_[a-f0-9]+\s*=\s*"eyJ[^"]+";[\s\n]*const\s+var_[a-f0-9]+\s*=\s*"([^"]+)"'
        lookup_pattern = r"fetchWithRetry\s*\(\s*'([^']+server_lookup\?channel_id=)"

        token_match = re.search(token_pattern, iframe_html)
        key_match = re.search(key_pattern, iframe_html)
        lookup_match = re.search(lookup_pattern, iframe_html)

        if token_match and key_match:
            result = {
                "session_token": token_match.group(1),
                "channel_key": key_match.group(1),
            }
            if lookup_match:
                result["server_lookup_url"] = lookup_match.group(1) + result["channel_key"]
            
            secret_key = self._extract_secret_key(iframe_html, result["channel_key"])
            if secret_key:
                result["secret_key"] = secret_key
            return result
        return None

    async def _extract_new_auth_flow(self, iframe_url: str, iframe_content: str) -> Dict[str, Any]:
        logger.info("Tentativo rilevamento nuovo flusso auth obfuscated...")
        obfuscated_data = self._extract_obfuscated_session_data(iframe_content)
        params = {}
        secret_key = None

        if obfuscated_data:
            logger.info("✅ Rilevato pattern obfuscated (var_xxx)")
            params['auth_token'] = obfuscated_data.get('session_token')
            params['channel_key'] = obfuscated_data.get('channel_key')
            secret_key = obfuscated_data.get('secret_key')
        else:
            logger.info("Pattern obfuscated non trovato, provo estrazione euristica...")
            jwt_match = re.search(r'["\'](eyJ[a-zA-Z0-9\-_]+\.[a-zA-Z0-9\-_]+\.[a-zA-Z0-9\-_]+)["\']', iframe_content)
            if jwt_match:
                params['auth_token'] = jwt_match.group(1)
            
            key_matches = re.finditer(r'["\']([a-z]+[0-9]+)["\']', iframe_content)
            for m in key_matches:
                val = m.group(1)
                if re.match(r'^(premium|dad|sport|live)[0-9]+$', val):
                    params['channel_key'] = val
                    break
            
            if params.get('channel_key'):
                secret_key = self._extract_secret_key(iframe_content, params['channel_key'])

        country_match = re.search(r'["\']([A-Z]{2})["\']', iframe_content)
        params['auth_country'] = country_match.group(1) if country_match else 'DE'

        ts_matches = re.findall(r'["\']([0-9]{10})["\']', iframe_content)
        if ts_matches:
            ts_values = sorted([int(x) for x in ts_matches])
            params['auth_ts'] = str(ts_values[0])
            params['auth_expiry'] = str(ts_values[-1]) if len(ts_values) > 1 else str(ts_values[0] + 3600)

        if not params.get('auth_token'):
            raise ExtractorError("Unable to extract JWT from new flow.")

        channel_key = params.get('channel_key')
        if not channel_key:
            m_url = re.search(r'id=([0-9]+)', iframe_url)
            if m_url: channel_key = f"premium{m_url.group(1)}"
            else: raise ExtractorError("Channel Key missing.")

        logger.info("🚀 Skipping auth2.php POST (new flow detected). Proceeding directly to server lookup.")
        server_key = await self._fetch_server_key(channel_key, iframe_url)
        stream_url = self._build_stream_url(server_key, channel_key)
        stream_headers = self._build_stream_headers(iframe_url, channel_key, params['auth_token'], secret_key)
        
        # Add session cookies
        session, _ = await self._get_session()
        cookies = session.cookie_jar.filter_cookies(stream_url)
        cookie_str = "; ".join([f"{k}={v.value}" for k, v in cookies.items()])
        if cookie_str:
            stream_headers['Cookie'] = cookie_str

        return {
            "destination_url": stream_url,
            "request_headers": stream_headers,
            "mediaflow_endpoint": self.mediaflow_endpoint,
            "expires_at": float(params.get('auth_expiry', 0))
        }

    async def _extract_lovecdn_stream(self, iframe_url: str, iframe_content: str) -> Dict[str, Any]:
        m3u8_patterns = [
            r'["\']([^"\']*\.m3u8[^"\']*)["\']',
            r'source[:\s]+["\']([^"\']+)["\']',
            r'file[:\s]+["\']([^"\']+\.m3u8[^"\']*)["\']',
            r'hlsManifestUrl[:\s]*["\']([^"\']+)["\']',
        ]
        
        stream_url = None
        for pattern in m3u8_patterns:
            matches = re.findall(pattern, iframe_content)
            for match in matches:
                if '.m3u8' in match and match.startswith('http'):
                    stream_url = match
                    break
            if stream_url: break
        
        if not stream_url:
            channel_match = re.search(r'(?:stream|channel)["\s:=]+["\']([^"\']+)["\']', iframe_content)
            server_match = re.search(r'(?:server|domain|host)["\s:=]+["\']([^"\']+)["\']', iframe_content)
            if channel_match:
                server = server_match.group(1) if server_match else _DLHD_CONFIG['base_domain']
                stream_url = f"https://{server}/{channel_match.group(1)}/mono.m3u8"
        
        if not stream_url:
            url_pattern = r'https?://[^\s"\'<>]+\.m3u8[^\s"\'<>]*'
            matches = re.findall(url_pattern, iframe_content)
            if matches:
                 stream_url = matches[0]

        if not stream_url:
            raise ExtractorError("Could not find stream URL in lovecdn.ru iframe")
        
        return {
            "destination_url": stream_url,
            "request_headers": {
                'User-Agent': self.USER_AGENT,
                'Referer': iframe_url,
                'Origin': f"https://{urlparse(iframe_url).netloc}"
            },
            "mediaflow_endpoint": self.mediaflow_endpoint,
        }

    async def extract(self, url: str, force_refresh: bool = False, **kwargs) -> Dict[str, Any]:
        channel_id = self.extract_channel_id(url)
        if not channel_id:
            raise ExtractorError(f"Unable to extract channel ID from {url}")

        # Check in-memory cache
        if not force_refresh and channel_id in _STREAM_DATA_CACHE:
            cached = _STREAM_DATA_CACHE[channel_id]
            if not cached.get("expires_at") or time.time() < (cached["expires_at"] - 30):
                logger.info(f"Using in-memory cache for DLHD channel {channel_id}")
                return cached

        async def do_extraction(cid, hosts):
            last_err = None
            for host in hosts:
                try:
                    iframe_url = f'https://{host}/premiumtv/daddyhd.php?id={cid}'
                    logger.info(f"🔍 Attempting extraction from: {iframe_url}")
                    
                    embed_headers = {
                        'User-Agent': self.USER_AGENT,
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.9',
                        'Referer': 'https://dlhd.dad/',
                        'sec-ch-ua': '"Chromium";v="136", "Google Chrome";v="136"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"macOS"',
                    }
                    
                    resp = await self._make_robust_request(iframe_url, headers=embed_headers, retries=2)
                    content = resp.text
                    
                    if 'lovecdn.ru' in content:
                        logger.info("Detected lovecdn.ru iframe")
                        return await self._extract_lovecdn_stream(iframe_url, content)
                    
                    # Pattern matching for auth parameters
                    params = {}
                    patterns = {
                        'channel_key': r'(?:const|var|let)\s+(?:CHANNEL_KEY|channelKey)\s*=\s*["\']([^"\']+)["\']',
                        'auth_token': r'(?:const|var|let)\s+AUTH_TOKEN\s*=\s*["\']([^"\']+)["\']',
                        'auth_country': r'(?:const|var|let)\s+AUTH_COUNTRY\s*=\s*["\']([^"\']+)["\']',
                        'auth_ts': r'(?:const|var|let)\s+AUTH_TS\s*=\s*["\']([^"\']+)["\']',
                        'auth_expiry': r'(?:const|var|let)\s+AUTH_EXPIRY\s*=\s*["\']([^"\']+)["\']',
                    }
                    for key, pattern in patterns.items():
                        match = re.search(pattern, content)
                        params[key] = match.group(1) if match else None
                    
                    missing_params = [k for k, v in params.items() if not v]
                    if missing_params:
                        logger.info(f"Missing params {missing_params}, trying new auth flow...")
                        return await self._extract_new_auth_flow(iframe_url, content)
                    
                    # Classic auth flow
                    logger.info(f"Using classic auth flow for {channel_id}")
                    auth_url = _DLHD_CONFIG['auth_url']
                    iframe_origin = f"https://{host}"
                    
                    form_data = FormData()
                    form_data.add_field('channelKey', params['channel_key'])
                    form_data.add_field('country', params['auth_country'])
                    form_data.add_field('timestamp', params['auth_ts'])
                    form_data.add_field('expiry', params['auth_expiry'])
                    form_data.add_field('token', params['auth_token'])
                    
                    auth_headers = {
                        'User-Agent': self.USER_AGENT,
                        'Origin': iframe_origin,
                        'Referer': iframe_url,
                    }
                    
                    session, proxy_url = await self._get_session()
                    async with session.post(auth_url, data=form_data, headers=auth_headers, ssl=False, proxy=proxy_url) as auth_resp:
                        auth_text = await auth_resp.text()
                        if auth_resp.status != 200 or 'Blocked' in auth_text:
                             logger.warning(f"Classic auth failed, trying new flow as fallback...")
                             return await self._extract_new_auth_flow(iframe_url, content)
                        
                        auth_data = json.loads(auth_text)
                        if not (auth_data.get('success') or auth_data.get('valid')):
                             return await self._extract_new_auth_flow(iframe_url, content)

                    server_key = await self._fetch_server_key(params['channel_key'], iframe_url)
                    stream_url = self._build_stream_url(server_key, params['channel_key'])
                    stream_headers = self._build_stream_headers(iframe_url, params['channel_key'], params['auth_token'])
                    
                    cookies = session.cookie_jar.filter_cookies(stream_url)
                    cookie_str = "; ".join([f"{k}={v.value}" for k, v in cookies.items()])
                    if cookie_str:
                        stream_headers['Cookie'] = cookie_str
                        
                    return {
                        "destination_url": stream_url,
                        "request_headers": stream_headers,
                        "mediaflow_endpoint": self.mediaflow_endpoint,
                        "expires_at": float(params.get('auth_expiry', 0))
                    }
                except Exception as e:
                    logger.warning(f"Host {host} failed: {e}")
                    last_err = e
                    continue
            raise ExtractorError(f"All hosts failed. Last error: {last_err}")

        # Refresh hosts if empty
        if not _IFRAME_HOSTS:
            await self._fetch_iframe_hosts()
        
        try:
            result = await do_extraction(channel_id, _IFRAME_HOSTS)
        except ExtractorError:
            # Force refresh host list on total failure
            if await self._fetch_iframe_hosts():
                result = await do_extraction(channel_id, _IFRAME_HOSTS)
            else:
                raise

        _STREAM_DATA_CACHE[channel_id] = result
        return result
