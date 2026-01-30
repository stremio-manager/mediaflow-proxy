"""
Helper functions for automatic stream extraction in proxy routes.

This module provides extraction helpers for DLHD/DaddyLive
and Sportsonline/Sportzonline streams that are auto-detected in proxy routes.
"""

import logging
import re
import time
from urllib.parse import urlparse

from fastapi import Request, HTTPException

from mediaflow_proxy.extractors.base import ExtractorError
from mediaflow_proxy.extractors.factory import ExtractorFactory
from mediaflow_proxy.utils.http_utils import ProxyRequestHeaders, DownloadError


logger = logging.getLogger(__name__)

# Silent extraction cache: {url: {"data": result, "timestamp": time}}
_extraction_cache: dict = {}
_cache_duration = 300  # 5 minutes



async def check_and_extract_dlhd_stream(
    request: Request, destination: str, proxy_headers: ProxyRequestHeaders, force_refresh: bool = False
) -> dict | None:
    """
    Check if destination contains DLHD/DaddyLive patterns and extract stream directly.

    Args:
        request (Request): The incoming HTTP request.
        destination (str): The destination URL to check.
        proxy_headers (ProxyRequestHeaders): The headers to include in the request.
        force_refresh (bool): Force re-extraction (this is now always the case as cache is removed).

    Returns:
        dict | None: Extracted stream data if DLHD link detected, None otherwise.
    """
    # Check for common DLHD/DaddyLive patterns in the URL
    netloc = urlparse(destination).netloc.lower()
    is_dlhd_link = (
        re.search(r"stream-\d+", destination)
        or any(
            domain in netloc
            for domain in [
                "dlhd.dad",
                "daddylive",
                "daddyhd.com",
                "daddylive.voto",
                "daddylive.sx",
                "daddylive.it",
                "daddytv.live",
            ]
        )
    )

    if not is_dlhd_link:
        return None

    logger.info(f"DLHD link detected: {destination}")

    # Silent cache check
    if not force_refresh and destination in _extraction_cache:
        cached = _extraction_cache[destination]
        if time.time() - cached["timestamp"] < _cache_duration:
            return cached["data"]

    # Extract stream data
    extractor = None
    try:
        logger.info(f"Extracting DLHD stream data from: {destination}")
        extractor = ExtractorFactory.get_extractor("DLHD", proxy_headers.request)
        result = await extractor.extract(destination)

        logger.info(f"DLHD extraction successful. Stream URL: {result.get('destination_url')}")
        
        # Silent cache storage
        _extraction_cache[destination] = {"data": result, "timestamp": time.time()}

        return result

    except (ExtractorError, DownloadError) as e:
        logger.error(f"DLHD extraction failed: {str(e)}")
        raise HTTPException(status_code=400, detail=f"DLHD extraction failed: {str(e)}")
    except Exception as e:
        logger.exception(f"Unexpected error during DLHD extraction: {str(e)}")
        raise HTTPException(status_code=500, detail=f"DLHD extraction failed: {str(e)}")
    finally:
        if extractor and hasattr(extractor, "close"):
            await extractor.close()


async def check_and_extract_sportsonline_stream(
    request: Request, destination: str, proxy_headers: ProxyRequestHeaders, force_refresh: bool = False
) -> dict | None:
    """
    Check if destination contains Sportsonline/Sportzonline patterns and extract stream directly.

    Args:
        request (Request): The incoming HTTP request.
        destination (str): The destination URL to check.
        proxy_headers (ProxyRequestHeaders): The headers to include in the request.
        force_refresh (bool): Force re-extraction (this is now always the case as cache is removed).

    Returns:
        dict | None: Extracted stream data if Sportsonline link detected, None otherwise.
    """
    parsed_netloc = urlparse(destination).netloc
    is_sportsonline_link = "sportzonline." in parsed_netloc or "sportsonline." in parsed_netloc

    if not is_sportsonline_link:
        return None

    logger.info(f"Sportsonline link detected: {destination}")
    
    # Silent cache check
    if not force_refresh and destination in _extraction_cache:
        cached = _extraction_cache[destination]
        if time.time() - cached["timestamp"] < _cache_duration:
            return cached["data"]

    extractor = None
    try:
        logger.info(f"Extracting Sportsonline stream data from: {destination}")
        extractor = ExtractorFactory.get_extractor("Sportsonline", proxy_headers.request)
        result = await extractor.extract(destination)
        logger.info(f"Sportsonline extraction successful. Stream URL: {result.get('destination_url')}")
        
        # Silent cache storage
        _extraction_cache[destination] = {"data": result, "timestamp": time.time()}

        return result
    except (ExtractorError, DownloadError) as e:
        logger.error(f"Sportsonline extraction failed: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Sportsonline extraction failed: {str(e)}")
    except Exception as e:
        logger.exception(f"Unexpected error during Sportsonline extraction: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Sportsonline extraction failed: {str(e)}")
    finally:
        if extractor and hasattr(extractor, "close"):
            await extractor.close()
