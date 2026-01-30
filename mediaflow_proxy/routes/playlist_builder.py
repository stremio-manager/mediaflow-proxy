import json
import logging
import urllib.parse
from typing import Iterator, Dict, Optional
from fastapi import APIRouter, Request, HTTPException, Query
from fastapi.responses import StreamingResponse
from starlette.responses import RedirectResponse

from mediaflow_proxy.configs import settings
from mediaflow_proxy.utils.http_utils import get_original_scheme
from mediaflow_proxy.utils.http_client import create_aiohttp_session
import asyncio

logger = logging.getLogger(__name__)
playlist_builder_router = APIRouter()


def process_entry(entry_lines: list[str], base_url: str, api_password: Optional[str]) -> list[str]:
    """
    Processa un singolo blocco di canale (#EXTINF + tags + URL).
    Ritorna le linee modificate.
    """
    if not entry_lines:
        return []

    headers = {}
    kodi_props = {}
    url_indices = []
    processed_lines = list(entry_lines)

    # Primo passaggio: analizza tutti i tag e trova l'URL (o gli URL)
    for idx, line in enumerate(processed_lines):
        logical_line = line.strip()
        if not logical_line:
            continue

        if logical_line.startswith("#EXTVLCOPT:"):
            try:
                opt = logical_line.split(":", 1)[1]
                if "=" in opt:
                    k, v = opt.split("=", 1)
                    k, v = k.strip(), v.strip()
                    if k == "http-header" and ":" in v:
                        hk, hv = v.split(":", 1)
                        headers[hk.strip()] = hv.strip()
                    elif k.startswith("http-"):
                        headers[k[len("http-") :]] = v
            except Exception: pass
        elif logical_line.startswith("#EXTHTTP:"):
            try:
                headers.update(json.loads(logical_line.split(":", 1)[1]))
            except Exception: pass
        elif logical_line.startswith("#KODIPROP:"):
            try:
                prop = logical_line.split(":", 1)[1]
                if "=" in prop:
                    pk, pv = prop.split("=", 1)
                    kodi_props[pk.strip()] = pv.strip()
            except Exception: pass
        elif logical_line.startswith("http") and not logical_line.startswith("#"):
            url_indices.append(idx)

    if not url_indices:
        return processed_lines

    # Secondo passaggio: riscrivi l'URL (prendiamo il primo URL trovato nell'entry)
    u_idx = url_indices[0]
    original_url = processed_lines[u_idx].strip()
    manifest_type = kodi_props.get("inputstream.adaptive.manifest_type", "").lower()
    
    processed_url = original_url
    if "pluto.tv" in original_url:
        pass
    elif "vavoo.to" in original_url:
        encoded = urllib.parse.quote(original_url, safe="")
        processed_url = f"{base_url}/proxy/hls/manifest.m3u8?d={encoded}"
    elif "vixsrc.to" in original_url:
        encoded = urllib.parse.quote(original_url, safe="")
        processed_url = f"{base_url}/extractor/video?host=VixCloud&redirect_stream=true&d={encoded}&max_res=true&no_proxy=true"
    elif manifest_type == "mpd" or ".mpd" in original_url.lower():
        from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
        parsed = urlparse(original_url)
        query = parse_qs(parsed.query)
        kid = query.get("key_id", [None])[0]
        k = query.get("key", [None])[0]
        
        clean_q = urlencode({k: v for k, v in query.items() if k not in ["key_id", "key"]}, doseq=True)
        clean_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, clean_q, ""))
        
        processed_url = f"{base_url}/proxy/mpd/manifest.m3u8?d={urllib.parse.quote(clean_url, safe='')}"
        if kid: processed_url += f"&key_id={kid}"
        if k: processed_url += f"&key={k}"
    else:
        # Default as HLS
        encoded = urllib.parse.quote(original_url, safe="")
        processed_url = f"{base_url}/proxy/hls/manifest.m3u8?d={encoded}"

    # Aggiungi chiavi KODI
    lic_key = kodi_props.get("inputstream.adaptive.license_key")
    if lic_key and ":" in lic_key:
        kid_k, k_k = lic_key.split(":", 1)
        if "&key_id=" not in processed_url: processed_url += f"&key_id={kid_k}"
        if "&key=" not in processed_url: processed_url += f"&key={k_k}"

    # Aggiungi Headers
    if headers:
        h_str = "".join([f"&h_{urllib.parse.quote(k)}={urllib.parse.quote(v)}" for k, v in headers.items()])
        processed_url += h_str

    if api_password:
        processed_url += f"&api_password={api_password}"

    processed_lines[u_idx] = processed_url + "\n"
    return processed_lines


def rewrite_m3u_links_streaming(
    m3u_lines_iterator: Iterator[str], base_url: str, api_password: Optional[str]
) -> Iterator[str]:
    """
    Riscrive i link da un iteratore di linee M3U raggruppando per entry (#EXTINF).
    """
    lines = list(m3u_lines_iterator)
    if not lines:
        return

    # Gestione header #EXTM3U
    rest = lines
    if lines and lines[0].strip().startswith("#EXTM3U"):
        yield lines[0]
        rest = lines[1:]

    entries = parse_channel_entries(rest)
    for entry in entries:
        for p_line in process_entry(entry, base_url, api_password):
            yield p_line


async def async_download_m3u_playlist(url: str) -> list[str]:
    """Scarica una playlist M3U in modo asincrono e restituisce le righe."""
    headers = {
        "User-Agent": settings.user_agent,
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    }
    lines = []
    try:
        async with create_aiohttp_session(url, timeout=30) as (session, proxy_url):
            response = await session.get(url, headers=headers, proxy=proxy_url)
            response.raise_for_status()
            content = await response.text()
            # Split content into lines
            for line in content.splitlines():
                lines.append(line + "\n" if line else "")
    except Exception as e:
        logger.error(f"Error downloading playlist (async): {str(e)}")
        raise
    return lines


def parse_channel_entries(lines: list[str]) -> list[list[str]]:
    """
    Analizza le linee di una playlist M3U e le raggruppa in entry di canali.
    Ogni entry contiene tutto ciò che si trova tra un #EXTINF e il successivo.
    """
    entries = []
    current_entry = []
    for line in lines:
        if line.strip().startswith("#EXTINF:"):
            if current_entry:
                entries.append(current_entry)
            current_entry = [line]
        elif current_entry:
            current_entry.append(line)
        elif line.strip().startswith("#EXTM3U"): # Skip additional EXTM3U headers within the playlist
            continue
        else: # Handle lines before the first #EXTINF or other non-channel lines
            current_entry.append(line)
    if current_entry:
        entries.append(current_entry)
    return entries


async def async_generate_combined_playlist(playlist_definitions: list[str], base_url: str, api_password: Optional[str]):
    """Genera una playlist combinata da multiple definizioni, scaricando in parallelo."""
    download_tasks = []
    for definition in playlist_definitions:
        url_str = definition
        sort = False
        proxy = True
        
        if definition.startswith("sort:"):
            sort = True
            definition = definition[len("sort:"):]
            
        if definition.startswith("no_proxy:"):
            proxy = False
            url_str = definition[len("no_proxy:"):]
        else:
            url_str = definition

        download_tasks.append({"url": url_str, "proxy": proxy, "sort": sort})

    results = await asyncio.gather(
        *[async_download_m3u_playlist(task["url"]) for task in download_tasks], return_exceptions=True
    )

    combined_sorted_entries = []
    header_yielded = False

    for idx, result in enumerate(results):
        task_info = download_tasks[idx]
        if isinstance(result, Exception):
            if not header_yielded:
                yield "#EXTM3U\n"
                header_yielded = True
            yield f"# ERROR processing {task_info['url']}: {str(result)}\n"
            continue

        # Estrai header e canali
        header = []
        rest = result
        if result and result[0].strip().startswith("#EXTM3U"):
            header = [result[0]]
            rest = result[1:]

        if not header_yielded and header:
            yield header[0]
            header_yielded = True

        entries = parse_channel_entries(rest)
        
        # Processa e riscrive se necessario
        processed_entries = []
        for entry in entries:
            if task_info["proxy"]:
                processed_entries.append(process_entry(entry, base_url, api_password))
            else:
                processed_entries.append(entry)

        if task_info["sort"]:
            combined_sorted_entries.extend(processed_entries)
        else:
            if not header_yielded:
                yield "#EXTM3U\n"
                header_yielded = True
            for entry in processed_entries:
                for line in entry:
                    yield line

    if combined_sorted_entries:
        if not header_yielded:
            yield "#EXTM3U\n"
            header_yielded = True
            
        # Ordina per nome canale (prima riga dell'entry, dopo la virgola)
        combined_sorted_entries.sort(key=lambda e: e[0].split(",")[-1].strip() if "," in e[0] else e[0])
        for entry in combined_sorted_entries:
            for line in entry:
                yield line


@playlist_builder_router.get("/playlist")
async def proxy_handler(
    request: Request,
    d: str = Query(..., description="Query string con le definizioni delle playlist", alias="d"),
    api_password: Optional[str] = Query(None, description="Password API per MFP"),
):
    """
    Endpoint per il proxy delle playlist M3U con supporto MFP.

    Formato query string: playlist1&url1;playlist2&url2
    Esempio: https://mfp.com:pass123&http://provider.com/playlist.m3u
    """
    try:
        if not d:
            raise HTTPException(status_code=400, detail="Query string mancante")

        if not d.strip():
            raise HTTPException(status_code=400, detail="Query string cannot be empty")

        # Validate that we have at least one valid definition
        playlist_definitions = [def_.strip() for def_ in d.split(";") if def_.strip()]
        if not playlist_definitions:
            raise HTTPException(status_code=400, detail="No valid playlist definitions found")

        # Costruisci base_url con lo schema corretto
        original_scheme = get_original_scheme(request)
        base_url = f"{original_scheme}://{request.url.netloc}"

        # Estrai base_url dalla prima definizione se presente
        if playlist_definitions and "&" in playlist_definitions[0]:
            parts = playlist_definitions[0].split("&", 1)
            if ":" in parts[0] and not parts[0].startswith("http"):
                # Estrai base_url dalla prima parte se contiene password
                base_url_part = parts[0].rsplit(":", 1)[0]
                if base_url_part.startswith("http"):
                    base_url = base_url_part

        async def generate_response():
            async for line in async_generate_combined_playlist(playlist_definitions, base_url, api_password):
                yield line

        return StreamingResponse(
            generate_response(),
            media_type="application/vnd.apple.mpegurl",
            headers={"Content-Disposition": 'attachment; filename="playlist.m3u"', "Access-Control-Allow-Origin": "*"},
        )

    except Exception as e:
        logger.error(f"General error in playlist handler: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}") from e


@playlist_builder_router.get("/builder")
async def url_builder():
    """
    Pagina con un'interfaccia per generare l'URL del proxy MFP.
    """
    return RedirectResponse(url="/playlist_builder.html")
