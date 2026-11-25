import asyncio
import io
import json
import re
from typing import Dict, List, Optional, Tuple, Any

import httpx
import pandas as pd
from bs4 import BeautifulSoup
from fastapi import FastAPI, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from pydantic import BaseModel

app = FastAPI(title="Mobile App Store & Play Store Lookup API", version="2.0.0")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


class BatchRequest(BaseModel):
    ids: List[str]
    platform: str = "auto"  # "auto", "ios", "android"
    countries: List[str] = ["in", "tw", "us", "gb"]
    scrape_fallback: bool = True


class QuickRequest(BaseModel):
    inputs: List[str]
    platform: str = "auto"
    countries: str = "in,tw,us,gb"
    scrape_fallback: bool = True


# ============================================================================
# Identifier Extractors
# ============================================================================


def extract_ios_id(value: object) -> Optional[str]:
    """Extract numeric iOS App Store ID from raw ID, 'id123', or Apple store URL."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    # Handle Excel float notation (e.g. "1617391485.0")
    if re.fullmatch(r"\d+\.0+", text):
        text = text.split(".")[0]
    url_match = re.search(r"/id(\d+)", text)
    if url_match:
        return url_match.group(1)
    id_match = re.fullmatch(r"(?:id)?(\d{6,15})", text, flags=re.IGNORECASE)
    if id_match:
        return id_match.group(1)
    return None


def extract_android_package(value: object) -> Optional[str]:
    """Extract Android package name from package string, Play Store URL, or market:// link."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None

    # Handle URL: https://play.google.com/store/apps/details?id=com.whatsapp&...
    url_match = re.search(r"[?&]id=([a-zA-Z0-9_\.]+)", text)
    if url_match:
        pkg = url_match.group(1).rstrip("/")
        if "." in pkg and not pkg.endswith("."):
            return pkg

    # Handle market://details?id=...
    market_match = re.search(r"market://details\?id=([a-zA-Z0-9_\.]+)", text)
    if market_match:
        return market_match.group(1).rstrip("/")

    # Handle raw package name format (e.g., com.whatsapp, org.mozilla.firefox)
    pkg_match = re.fullmatch(r"([a-zA-Z][a-zA-Z0-9_]*(\.[a-zA-Z][a-zA-Z0-9_]*)+)", text)
    if pkg_match:
        pkg = pkg_match.group(1)
        # Exclude common file extensions and general domains
        if pkg.lower().endswith((".xlsx", ".xls", ".csv", ".py", ".txt", ".json", ".pdf", ".zip", ".html")):
            return None
        return pkg

    return None


def extract_app_info(value: object, platform_hint: str = "auto") -> Tuple[Optional[str], Optional[str]]:
    """
    Detects platform ('iOS' or 'Android') and extracts the clean App ID / Package Name.
    Returns (platform, id_or_package).
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None, None
    text = str(value).strip()
    if not text:
        return None, None

    hint = (platform_hint or "auto").lower()

    if hint == "ios":
        ios_id = extract_ios_id(text)
        return ("iOS", ios_id) if ios_id else (None, None)

    if hint == "android":
        pkg = extract_android_package(text)
        return ("Android", pkg) if pkg else (None, None)

    # Auto-detect mode
    # 1. Explicit domain cues
    if "play.google.com" in text or "market://" in text:
        pkg = extract_android_package(text)
        return ("Android", pkg) if pkg else (None, None)

    if "apps.apple.com" in text or "itunes.apple.com" in text or "/id" in text:
        ios_id = extract_ios_id(text)
        return ("iOS", ios_id) if ios_id else (None, None)

    # 2. Pattern cues: pure numeric or id\d+ -> iOS
    ios_id = extract_ios_id(text)
    if ios_id:
        return ("iOS", ios_id)

    # 3. Dotted package name -> Android
    pkg = extract_android_package(text)
    if pkg:
        return ("Android", pkg)

    return None, None


def clean_ios_title(title: str) -> str:
    """Strip Apple directional markers and store suffix."""
    cleaned = title.replace("\u200e", "").replace("\u200f", "").strip()
    cleaned = re.sub(r"\s+on the App Store$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*[-–|]\s*App Store$", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip()


def clean_android_title(title: str) -> str:
    """Strip Google Play Store title suffixes and markers."""
    cleaned = title.replace("\u200e", "").replace("\u200f", "").strip()
    cleaned = re.sub(r"\s*-\s*Apps on Google Play$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+on Google Play$", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip()


# ============================================================================
# iOS Store Resolvers
# ============================================================================


async def lookup_itunes_batch(
    client: httpx.AsyncClient, app_ids: List[str], country: str
) -> Dict[str, dict]:
    """Query Apple iTunes Lookup API in batches of 100."""
    found: Dict[str, dict] = {}
    batch_size = 100

    for i in range(0, len(app_ids), batch_size):
        batch = app_ids[i : i + batch_size]
        url = "https://itunes.apple.com/lookup"
        params = {"id": ",".join(batch), "country": country.lower()}

        for attempt in range(3):
            try:
                resp = await client.get(url, params=params, timeout=20.0)
                if resp.status_code == 429:
                    await asyncio.sleep(3 * (attempt + 1))
                    continue
                if resp.status_code == 200:
                    data = resp.json()
                    for item in data.get("results", []):
                        app_id = str(item.get("trackId", ""))
                        if app_id:
                            found[app_id] = {
                                "platform": "iOS",
                                "app_name": item.get("trackName", ""),
                                "bundle_id": item.get("bundleId", ""),
                                "developer": item.get("artistName", ""),
                                "category": item.get("primaryGenreName", ""),
                                "store_url": item.get("trackViewUrl", ""),
                                "storefront": country.upper(),
                                "source": "itunes_api",
                            }
                break
            except Exception:
                await asyncio.sleep(1)

        await asyncio.sleep(0.2)

    return found


async def scrape_app_store_page(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    app_id: str,
    country: str,
) -> Optional[dict]:
    """Fetch apps.apple.com/{country}/app/id{app_id} and extract metadata."""
    url = f"https://apps.apple.com/{country.lower()}/app/id{app_id}"
    async with sem:
        try:
            resp = await client.get(url, headers=HEADERS, follow_redirects=True, timeout=15.0)
            if resp.status_code != 200:
                return None

            soup = BeautifulSoup(resp.text, "html.parser")
            app_name = ""
            developer = ""
            category = ""

            # 1. SoftwareApplication JSON-LD
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    ld = json.loads(script.string or "")
                    if isinstance(ld, dict) and ld.get("@type") == "SoftwareApplication":
                        app_name = ld.get("name", "") or app_name
                        author = ld.get("author", {})
                        if isinstance(author, dict):
                            developer = author.get("name", "") or developer
                        elif isinstance(author, str):
                            developer = author or developer
                        category = ld.get("applicationCategory", "") or category
                        break
                except Exception:
                    continue

            # 2. Fallbacks
            if not app_name:
                og = soup.find("meta", property="og:title")
                if og and og.get("content"):
                    app_name = og["content"]

            if not app_name and soup.title and soup.title.string:
                app_name = soup.title.string

            app_name = clean_ios_title(app_name)
            if not app_name or app_name.lower() in {"app store", "connecting to the app store"}:
                return None

            return {
                "platform": "iOS",
                "app_name": app_name,
                "bundle_id": "",
                "developer": developer,
                "category": category,
                "store_url": str(resp.url),
                "storefront": country.upper(),
                "source": "app_store_page",
            }
        except Exception:
            return None
        finally:
            await asyncio.sleep(0.15)


async def resolve_ios_ids(
    raw_ids: List[str], countries: List[str], scrape_fallback: bool = True
) -> Dict[str, dict]:
    """Resolve iOS App Store IDs across storefronts with direct page scraping fallback."""
    unique_ids = list(dict.fromkeys([i for i in raw_ids if i]))
    results: Dict[str, dict] = {}
    clean_countries = [c.strip().lower() for c in countries if c.strip()] or ["in", "us", "tw", "gb"]

    async with httpx.AsyncClient() as client:
        # Step 1: Fast iTunes batch query
        for country in clean_countries:
            missing = [aid for aid in unique_ids if aid not in results]
            if not missing:
                break
            batch_hits = await lookup_itunes_batch(client, missing, country)
            results.update(batch_hits)

        # Step 2: Web page scraping fallback
        if scrape_fallback:
            sem = asyncio.Semaphore(5)
            for country in clean_countries:
                missing = [aid for aid in unique_ids if aid not in results]
                if not missing:
                    break
                tasks = [scrape_app_store_page(client, sem, aid, country) for aid in missing]
                scraped = await asyncio.gather(*tasks)
                for aid, info in zip(missing, scraped):
                    if info:
                        results[aid] = info

    return results


# ============================================================================
# Android Store Resolvers
# ============================================================================


async def scrape_google_play_page(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    package_name: str,
    country: str,
) -> Optional[dict]:
    """Fetch Google Play Store page and extract app metadata."""
    url = "https://play.google.com/store/apps/details"
    params = {"id": package_name, "gl": country.lower(), "hl": "en"}

    async with sem:
        try:
            resp = await client.get(url, params=params, headers=HEADERS, follow_redirects=True, timeout=15.0)
            if resp.status_code != 200:
                return None

            soup = BeautifulSoup(resp.text, "html.parser")
            app_name = ""
            developer = ""
            category = ""

            # 1. SoftwareApplication JSON-LD
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    ld = json.loads(script.string or "")
                    if isinstance(ld, dict) and ld.get("@type") == "SoftwareApplication":
                        app_name = ld.get("name", "") or app_name
                        author = ld.get("author", {})
                        if isinstance(author, dict):
                            developer = author.get("name", "") or developer
                        elif isinstance(author, str):
                            developer = author or developer
                        category = ld.get("applicationCategory", "") or category
                        break
                except Exception:
                    continue

            # 2. Fallbacks
            if not app_name:
                h1 = soup.find("h1")
                if h1:
                    app_name = h1.get_text(strip=True)

            if not app_name:
                og = soup.find("meta", property="og:title")
                if og and og.get("content"):
                    app_name = og["content"]

            if not app_name and soup.title and soup.title.string:
                app_name = soup.title.string

            app_name = clean_android_title(app_name)
            if not app_name or app_name.lower() in {"google play", "apps on google play"}:
                return None

            return {
                "platform": "Android",
                "app_name": app_name,
                "bundle_id": package_name,
                "developer": developer,
                "category": category,
                "store_url": f"https://play.google.com/store/apps/details?id={package_name}",
                "storefront": country.upper(),
                "source": "google_play_store",
            }
        except Exception:
            return None
        finally:
            await asyncio.sleep(0.15)


async def resolve_android_packages(
    packages: List[str], countries: List[str]
) -> Dict[str, dict]:
    """Resolve Android package names across specified storefront countries."""
    unique_pkgs = list(dict.fromkeys([p for p in packages if p]))
    results: Dict[str, dict] = {}
    clean_countries = [c.strip().lower() for c in countries if c.strip()] or ["in", "us", "tw", "gb"]

    async with httpx.AsyncClient() as client:
        sem = asyncio.Semaphore(5)
        for country in clean_countries:
            missing = [pkg for pkg in unique_pkgs if pkg not in results]
            if not missing:
                break
            tasks = [scrape_google_play_page(client, sem, pkg, country) for pkg in missing]
            scraped = await asyncio.gather(*tasks)
            for pkg, info in zip(missing, scraped):
                if info:
                    results[pkg] = info

    return results


# ============================================================================
# Unified Multi-Platform Batch Resolver
# ============================================================================


async def resolve_mixed_batch(
    raw_inputs: List[str],
    platform_hint: str = "auto",
    countries: Optional[List[str]] = None,
    scrape_fallback: bool = True,
) -> List[dict]:
    """Resolve a mixed list of iOS and Android identifiers concurrently."""
    clean_countries = [c.strip().lower() for c in (countries or ["in", "tw", "us", "gb"]) if c.strip()]

    parsed_items = []
    ios_ids = []
    android_pkgs = []

    for raw in raw_inputs:
        platform, aid = extract_app_info(raw, platform_hint)
        parsed_items.append((raw, platform, aid))
        if platform == "iOS" and aid:
            ios_ids.append(aid)
        elif platform == "Android" and aid:
            android_pkgs.append(aid)

    # Concurrently execute iOS and Android lookups
    ios_task = resolve_ios_ids(ios_ids, clean_countries, scrape_fallback) if ios_ids else asyncio.sleep(0, result={})
    android_task = resolve_android_packages(android_pkgs, clean_countries) if android_pkgs else asyncio.sleep(0, result={})

    resolved_ios, resolved_android = await asyncio.gather(ios_task, android_task)

    output = []
    for raw, platform, aid in parsed_items:
        if not platform or not aid:
            output.append({
                "input": raw,
                "platform": "Unknown",
                "app_id": "",
                "app_name": "Invalid ID / Format",
                "bundle_id": "",
                "developer": "",
                "category": "",
                "store_url": "",
                "storefront": "",
                "source": "invalid_input",
                "found": False,
            })
            continue

        if platform == "iOS":
            info = resolved_ios.get(aid)
        else:
            info = resolved_android.get(aid)

        if info:
            output.append({
                "input": raw,
                "platform": platform,
                "app_id": aid,
                "app_name": info["app_name"],
                "bundle_id": info.get("bundle_id", "") or aid,
                "developer": info.get("developer", ""),
                "category": info.get("category", ""),
                "store_url": info.get("store_url", ""),
                "storefront": info.get("storefront", ""),
                "source": info.get("source", ""),
                "found": True,
            })
        else:
            output.append({
                "input": raw,
                "platform": platform,
                "app_id": aid,
                "app_name": "Not Found / Removed",
                "bundle_id": aid,
                "developer": "",
                "category": "",
                "store_url": f"https://apps.apple.com/app/id{aid}" if platform == "iOS" else f"https://play.google.com/store/apps/details?id={aid}",
                "storefront": "",
                "source": "not_found",
                "found": False,
            })

    return output


def detect_id_column(df: pd.DataFrame, preferred: Optional[str] = None, platform_hint: str = "auto") -> str:
    """Detect the most likely column containing app identifiers/URLs."""
    if preferred and preferred in df.columns:
        return preferred
    best_col = df.columns[0]
    best_score = -1
    for col in df.columns:
        sample = df[col].dropna().head(50)
        score = sum(1 for v in sample if extract_app_info(v, platform_hint)[1] is not None)
        if score > best_score:
            best_score = score
            best_col = col
    return best_col


# ============================================================================
# API Endpoints
# ============================================================================


@app.get("/", response_class=HTMLResponse)
def index_ui():
    return HTML_PAGE


@app.get("/lookup/ios/{app_id}")
async def lookup_ios_single(
    app_id: str,
    countries: str = Query("in,tw,us,gb", description="Comma-separated country codes"),
    scrape_fallback: bool = Query(True),
):
    clean_id = extract_ios_id(app_id)
    if not clean_id:
        raise HTTPException(status_code=400, detail="Invalid iOS App Store ID or URL")
    res = await resolve_ios_ids([clean_id], countries.split(","), scrape_fallback)
    info = res.get(clean_id)
    if not info:
        return {"platform": "iOS", "app_id": clean_id, "app_name": "Not Found / Removed", "source": "not_found"}
    return {"app_id": clean_id, **info}


@app.get("/lookup/android/{package_name}")
async def lookup_android_single(
    package_name: str,
    countries: str = Query("in,tw,us,gb", description="Comma-separated country codes"),
):
    clean_pkg = extract_android_package(package_name)
    if not clean_pkg:
        raise HTTPException(status_code=400, detail="Invalid Android package name or Play Store URL")
    res = await resolve_android_packages([clean_pkg], countries.split(","))
    info = res.get(clean_pkg)
    if not info:
        return {"platform": "Android", "app_id": clean_pkg, "app_name": "Not Found / Removed", "source": "not_found"}
    return {"app_id": clean_pkg, **info}


@app.get("/lookup/auto/{identifier:path}")
async def lookup_auto_single(
    identifier: str,
    countries: str = Query("in,tw,us,gb"),
    scrape_fallback: bool = Query(True),
):
    results = await resolve_mixed_batch([identifier], platform_hint="auto", countries=countries.split(","), scrape_fallback=scrape_fallback)
    if results:
        return results[0]
    raise HTTPException(status_code=404, detail="Could not process identifier")


@app.post("/lookup/quick")
async def lookup_quick_interactive(payload: QuickRequest):
    """Fast endpoint for the interactive UI quick search table."""
    country_list = [c.strip().lower() for c in payload.countries.split(",") if c.strip()]
    results = await resolve_mixed_batch(
        raw_inputs=payload.inputs,
        platform_hint=payload.platform,
        countries=country_list,
        scrape_fallback=payload.scrape_fallback,
    )
    resolved_count = sum(1 for r in results if r.get("found"))
    return {"total": len(results), "resolved": resolved_count, "results": results}


@app.post("/lookup/batch")
async def lookup_batch(payload: BatchRequest):
    results = await resolve_mixed_batch(
        raw_inputs=payload.ids,
        platform_hint=payload.platform,
        countries=payload.countries,
        scrape_fallback=payload.scrape_fallback,
    )
    resolved_count = sum(1 for r in results if r.get("found"))
    return {"total": len(results), "resolved": resolved_count, "results": results}


@app.post("/lookup/excel")
async def lookup_excel(
    file: UploadFile = File(...),
    platform: str = Form("auto"),
    countries: str = Form("in,tw,us,gb"),
    column_name: Optional[str] = Form(None),
    scrape_fallback: bool = Form(True),
):
    raw_bytes = await file.read()
    filename = (file.filename or "apps.xlsx").lower()

    try:
        if filename.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(raw_bytes), dtype=str)
            if len(df.columns) >= 1 and extract_app_info(df.columns[0], platform)[1] is not None:
                df = pd.read_csv(io.BytesIO(raw_bytes), header=None, dtype=str)
                df.columns = ["app_id"] + [f"col_{i}" for i in range(1, len(df.columns))]
        else:
            df = pd.read_excel(io.BytesIO(raw_bytes), dtype=str)
            if len(df.columns) >= 1 and extract_app_info(df.columns[0], platform)[1] is not None:
                df = pd.read_excel(io.BytesIO(raw_bytes), header=None, dtype=str)
                df.columns = ["app_id"] + [f"col_{i}" for i in range(1, len(df.columns))]
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not read spreadsheet file: {exc}")

    id_col = detect_id_column(df, column_name.strip() if column_name else None, platform)
    raw_list = df[id_col].fillna("").astype(str).tolist()

    resolved_list = await resolve_mixed_batch(
        raw_inputs=raw_list,
        platform_hint=platform,
        countries=countries.split(","),
        scrape_fallback=scrape_fallback,
    )

    # Attach enriched metadata columns
    df["Platform"] = [r["platform"] for r in resolved_list]
    df["Extracted_ID"] = [r["app_id"] for r in resolved_list]
    df["App_Name"] = [r["app_name"] for r in resolved_list]
    df["Developer"] = [r["developer"] for r in resolved_list]
    df["Category"] = [r["category"] for r in resolved_list]
    df["Store_URL"] = [r["store_url"] for r in resolved_list]
    df["Storefront"] = [r["storefront"] for r in resolved_list]
    df["Source"] = [r["source"] for r in resolved_list]

    output_buffer = io.BytesIO()
    with pd.ExcelWriter(output_buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Resolved Apps")
    output_buffer.seek(0)

    out_name = re.sub(r"\.(xlsx|xls|csv)$", "", file.filename or "apps", flags=re.IGNORECASE)
    return StreamingResponse(
        output_buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{out_name}_resolved.xlsx"'},
    )


# ============================================================================
# Minimal, Subtle, Unified User Interface
# ============================================================================

HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>App Lookup &bull; iOS &amp; Android</title>
  <style>
    :root {
      --bg: #fafafa;
      --surface: #ffffff;
      --text: #111827;
      --text-muted: #6b7280;
      --text-subtle: #9ca3af;
      --border: #e5e7eb;
      --border-focus: #111827;
      --primary: #111827;
      --primary-hover: #1f2937;
      --primary-active: #030712;
      --badge-bg: #f3f4f6;
      --badge-text: #374151;
      --radius: 14px;
      --radius-sm: 8px;
      --transition: all 0.15s cubic-bezier(0.16, 1, 0.3, 1);
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      font-family: -apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      background: var(--bg);
      color: var(--text);
      padding: 56px 20px 80px;
      line-height: 1.5;
      -webkit-font-smoothing: antialiased;
    }

    .container {
      max-width: 660px;
      margin: 0 auto;
    }

    /* Header */
    .header {
      margin-bottom: 24px;
      text-align: center;
    }
    .header-logo {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 44px;
      height: 44px;
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 12px;
      margin-bottom: 14px;
      box-shadow: 0 1px 3px rgba(0,0,0,0.03);
    }
    .header h1 {
      font-size: 22px;
      font-weight: 600;
      letter-spacing: -0.025em;
      color: var(--text);
      margin-bottom: 4px;
    }
    .header p {
      font-size: 13.5px;
      color: var(--text-muted);
    }

    /* Card */
    .card {
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      box-shadow: 0 1px 3px rgba(0,0,0,0.02), 0 8px 24px -4px rgba(0,0,0,0.04);
      padding: 24px;
    }

    /* Mode Switcher Tabs */
    .segmented {
      display: flex;
      background: #f3f4f6;
      padding: 3px;
      border-radius: 10px;
      margin-bottom: 22px;
      gap: 3px;
    }
    .segmented button {
      flex: 1;
      border: none;
      background: transparent;
      padding: 7px 14px;
      font-size: 13px;
      font-weight: 500;
      color: var(--text-muted);
      border-radius: 7px;
      cursor: pointer;
      transition: var(--transition);
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 6px;
    }
    .segmented button.active {
      background: var(--surface);
      color: var(--text);
      font-weight: 600;
      box-shadow: 0 1px 2px rgba(0,0,0,0.06);
    }

    .tab-pane { display: none; }
    .tab-pane.active { display: block; }

    /* Dropzone */
    .dropzone {
      border: 1.5px dashed #d1d5db;
      border-radius: 12px;
      padding: 38px 20px;
      text-align: center;
      background: #fafafa;
      cursor: pointer;
      position: relative;
      transition: var(--transition);
      margin-bottom: 18px;
    }
    .dropzone:hover, .dropzone.dragover {
      border-color: #6b7280;
      background: #f3f4f6;
    }
    .dropzone input[type="file"] {
      position: absolute;
      top: 0; left: 0; width: 100%; height: 100%;
      opacity: 0;
      cursor: pointer;
    }
    .dropzone-icon {
      width: 28px;
      height: 28px;
      margin: 0 auto 10px;
      color: var(--text-muted);
      display: block;
    }
    .dropzone-title {
      font-size: 13.5px;
      font-weight: 500;
      color: var(--text);
      margin-bottom: 3px;
    }
    .dropzone-hint {
      font-size: 12px;
      color: var(--text-subtle);
    }

    /* Selected File Pill */
    .file-pill {
      display: none;
      align-items: center;
      justify-content: space-between;
      padding: 10px 14px;
      background: #f9fafb;
      border: 1px solid var(--border);
      border-radius: 9px;
      margin-bottom: 18px;
      font-size: 13px;
    }
    .file-pill-left {
      display: flex;
      align-items: center;
      gap: 8px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .file-pill-left svg {
      flex-shrink: 0;
      color: var(--text-muted);
    }
    .file-pill-name {
      font-weight: 500;
      color: var(--text);
    }
    .file-pill-size {
      color: var(--text-subtle);
      font-size: 12px;
    }
    .file-pill-remove {
      background: transparent;
      border: none;
      color: var(--text-muted);
      cursor: pointer;
      font-size: 16px;
      padding: 2px 6px;
      border-radius: 4px;
      line-height: 1;
    }
    .file-pill-remove:hover {
      color: #ef4444;
      background: #fee2e2;
    }

    /* Buttons */
    .btn-main {
      width: 100%;
      padding: 11px 18px;
      background: var(--primary);
      color: #ffffff;
      border: 1px solid transparent;
      border-radius: var(--radius-sm);
      font-size: 14px;
      font-weight: 500;
      cursor: pointer;
      transition: var(--transition);
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
    }
    .btn-main:hover {
      background: var(--primary-hover);
    }
    .btn-main:disabled {
      opacity: 0.6;
      cursor: not-allowed;
    }

    /* Collapsible Advanced Options */
    details.advanced {
      margin-top: 18px;
      border-top: 1px solid #f3f4f6;
      padding-top: 14px;
    }
    details.advanced summary {
      font-size: 12.5px;
      font-weight: 500;
      color: var(--text-muted);
      cursor: pointer;
      user-select: none;
      outline: none;
      display: inline-flex;
      align-items: center;
      gap: 5px;
      transition: var(--transition);
    }
    details.advanced summary:hover {
      color: var(--text);
    }
    .advanced-body {
      padding-top: 14px;
      display: grid;
      gap: 12px;
    }
    .form-group label {
      display: block;
      font-size: 12px;
      font-weight: 500;
      color: var(--text-muted);
      margin-bottom: 5px;
    }
    input[type="text"], textarea {
      width: 100%;
      padding: 9px 11px;
      border: 1px solid var(--border);
      border-radius: var(--radius-sm);
      font-size: 13px;
      color: var(--text);
      background: var(--surface);
      outline: none;
      transition: var(--transition);
    }
    input[type="text"]:focus, textarea:focus {
      border-color: var(--border-focus);
    }
    .input-hint {
      font-size: 11.5px;
      color: var(--text-subtle);
      margin-top: 4px;
    }

    /* Platform Radio Buttons in Advanced */
    .radio-group {
      display: flex;
      gap: 8px;
    }
    .radio-group label {
      flex: 1;
      padding: 7px 10px;
      border: 1px solid var(--border);
      border-radius: 6px;
      font-size: 12px;
      font-weight: 500;
      color: var(--text-muted);
      text-align: center;
      cursor: pointer;
      transition: var(--transition);
    }
    .radio-group input { display: none; }
    .radio-group label:has(input:checked) {
      border-color: var(--primary);
      background: #f9fafb;
      color: var(--text);
      font-weight: 600;
    }

    .checkbox-row {
      display: flex;
      align-items: center;
      gap: 7px;
      font-size: 12px;
      color: var(--text-muted);
      cursor: pointer;
      margin-top: 2px;
    }
    .checkbox-row input { accent-color: var(--primary); }

    /* Instant Search View */
    textarea {
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      resize: vertical;
      min-height: 105px;
      line-height: 1.6;
      margin-bottom: 8px;
    }
    .sample-row {
      display: flex;
      align-items: center;
      gap: 6px;
      flex-wrap: wrap;
      margin-bottom: 16px;
      font-size: 12px;
      color: var(--text-subtle);
    }
    .sample-chip {
      background: #f3f4f6;
      border: 1px solid transparent;
      padding: 3px 8px;
      border-radius: 6px;
      color: var(--text-muted);
      font-size: 11.5px;
      cursor: pointer;
      transition: var(--transition);
    }
    .sample-chip:hover {
      background: #e5e7eb;
      color: var(--text);
    }

    .quick-actions {
      display: flex;
      gap: 8px;
    }
    .btn-ghost {
      padding: 9px 14px;
      background: transparent;
      border: 1px solid var(--border);
      border-radius: var(--radius-sm);
      font-size: 13px;
      font-weight: 500;
      color: var(--text-muted);
      cursor: pointer;
      transition: var(--transition);
    }
    .btn-ghost:hover {
      background: #f9fafb;
      color: var(--text);
    }

    /* Results Table */
    .results-card {
      margin-top: 20px;
      border-top: 1px solid var(--border);
      padding-top: 18px;
      display: none;
    }
    .results-topbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 12px;
      font-size: 12.5px;
      color: var(--text-muted);
    }
    .table-box {
      border: 1px solid var(--border);
      border-radius: 8px;
      overflow-x: auto;
      max-height: 380px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 12.5px;
      text-align: left;
    }
    th {
      background: #fafafa;
      padding: 8px 12px;
      font-weight: 500;
      color: var(--text-muted);
      border-bottom: 1px solid var(--border);
      position: sticky;
      top: 0;
      white-space: nowrap;
    }
    td {
      padding: 9px 12px;
      border-bottom: 1px solid #f3f4f6;
      vertical-align: middle;
      white-space: nowrap;
    }
    tr:last-child td { border-bottom: none; }
    tr:hover td { background: #fafafa; }
    .badge-plat {
      display: inline-block;
      padding: 2px 6px;
      border-radius: 4px;
      font-size: 11px;
      font-weight: 500;
      background: #f3f4f6;
      color: #374151;
    }
    .store-link {
      color: #2563eb;
      text-decoration: none;
      font-weight: 500;
    }
    .store-link:hover { text-decoration: underline; }

    /* Spinner */
    .spinner {
      border: 2px solid rgba(255,255,255,0.3);
      border-radius: 50%;
      border-top: 2px solid #ffffff;
      width: 14px;
      height: 14px;
      animation: spin 0.6s linear infinite;
      display: inline-block;
    }
    @keyframes spin {
      to { transform: rotate(360deg); }
    }

    /* Footer */
    .footer {
      text-align: center;
      margin-top: 28px;
      font-size: 12px;
      color: var(--text-subtle);
    }
    .footer a {
      color: var(--text-muted);
      text-decoration: none;
    }
    .footer a:hover {
      text-decoration: underline;
    }
  </style>
</head>
<body>

<div class="container">
  <!-- Header -->
  <div class="header">
    <div class="header-logo">
      <svg width="22" height="22" fill="none" stroke="currentColor" stroke-width="1.8" viewBox="0 0 24 24">
        <rect x="5" y="2" width="14" height="20" rx="3"></rect>
        <line x1="12" y1="18" x2="12.01" y2="18" stroke-width="2.5"></line>
      </svg>
    </div>
    <h1>App Lookup</h1>
    <p>Enrich iOS App Store &amp; Google Play metadata in bulk or individually.</p>
  </div>

  <!-- Card -->
  <div class="card">
    <!-- Mode Switcher -->
    <div class="segmented">
      <button type="button" class="active" id="tab-btn-file" onclick="setMode('file')">
        <svg width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path><polyline points="14 2 14 8 20 8"></polyline></svg>
        Spreadsheet Enricher
      </button>
      <button type="button" id="tab-btn-quick" onclick="setMode('quick')">
        <svg width="14" height="14" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><circle cx="11" cy="11" r="8"></circle><line x1="21" y1="21" x2="16.65" y2="16.65"></line></svg>
        Instant Search
      </button>
    </div>

    <!-- PANE 1: SPREADSHEET ENRICHER -->
    <div class="tab-pane active" id="pane-file">
      <form action="/lookup/excel" method="post" enctype="multipart/form-data" id="excel-form" onsubmit="handleFormSubmit(event)">
        <!-- Dropzone -->
        <div class="dropzone" id="dropzone">
          <input type="file" name="file" id="file-input" accept=".xlsx,.xls,.csv" required onchange="handleFile(this)">
          <svg class="dropzone-icon" fill="none" stroke="currentColor" stroke-width="1.6" viewBox="0 0 24 24">
            <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
            <polyline points="17 8 12 3 7 8"></polyline>
            <line x1="12" y1="3" x2="12" y2="15"></line>
          </svg>
          <div class="dropzone-title" id="dropzone-title">Drop your Excel (.xlsx) or CSV here</div>
          <div class="dropzone-hint">or click to browse from your device</div>
        </div>

        <!-- Selected File Pill -->
        <div class="file-pill" id="file-pill">
          <div class="file-pill-left">
            <svg width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.8" viewBox="0 0 24 24"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path><polyline points="14 2 14 8 20 8"></polyline></svg>
            <span class="file-pill-name" id="pill-name">file.xlsx</span>
            <span class="file-pill-size" id="pill-size">0 KB</span>
          </div>
          <button type="button" class="file-pill-remove" onclick="removeFile()" title="Remove file">&times;</button>
        </div>

        <!-- Primary Action Button -->
        <button type="submit" class="btn-main" id="btn-submit-file">
          <span id="btn-submit-label">Enrich &amp; Download Excel &darr;</span>
        </button>

        <!-- Collapsible Advanced Settings -->
        <details class="advanced">
          <summary>Advanced settings</summary>
          <div class="advanced-body">
            <div class="form-group">
              <label>Target Platform</label>
              <div class="radio-group">
                <label><input type="radio" name="platform" value="auto" checked> Auto-Detect</label>
                <label><input type="radio" name="platform" value="ios"> iOS Only</label>
                <label><input type="radio" name="platform" value="android"> Android Only</label>
              </div>
            </div>
            <div class="form-group">
              <label>Storefront Countries (priority order)</label>
              <input type="text" name="countries" value="in,tw,us,gb">
              <div class="input-hint">Searches in this country order for region-locked apps.</div>
            </div>
            <div class="form-group">
              <label>ID Column Header (optional)</label>
              <input type="text" name="column_name" placeholder="Auto-detected if blank">
            </div>
            <label class="checkbox-row">
              <input type="checkbox" name="scrape_fallback" value="true" checked>
              Enable deep web page scraping fallback
            </label>
          </div>
        </details>
      </form>
    </div>

    <!-- PANE 2: INSTANT SEARCH -->
    <div class="tab-pane" id="pane-quick">
      <textarea id="quick-input" placeholder="Paste App IDs, package names, or store URLs (one per line)..."></textarea>

      <!-- Sample Chips -->
      <div class="sample-row">
        <span>Try:</span>
        <button type="button" class="sample-chip" onclick="addSample('com.whatsapp')">+ WhatsApp (Android)</button>
        <button type="button" class="sample-chip" onclick="addSample('310633997')">+ WhatsApp (iOS)</button>
        <button type="button" class="sample-chip" onclick="addSample('org.mozilla.firefox')">+ Firefox</button>
        <button type="button" class="sample-chip" onclick="addSample('com.spotify.music')">+ Spotify</button>
      </div>

      <div class="quick-actions">
        <button type="button" class="btn-main" style="flex: 2;" id="btn-quick-run" onclick="runSearch()">
          <span id="quick-label">Look Up Apps</span>
        </button>
        <button type="button" class="btn-ghost" onclick="clearSearch()">Clear</button>
      </div>

      <!-- Collapsible Advanced Settings for Search -->
      <details class="advanced">
        <summary>Advanced settings</summary>
        <div class="advanced-body">
          <div class="form-group">
            <label>Target Platform</label>
            <div class="radio-group">
              <label><input type="radio" name="quick_platform" value="auto" checked> Auto-Detect</label>
              <label><input type="radio" name="quick_platform" value="ios"> iOS Only</label>
              <label><input type="radio" name="quick_platform" value="android"> Android Only</label>
            </div>
          </div>
          <div class="form-group">
            <label>Storefront Countries</label>
            <input type="text" id="quick-countries" value="in,tw,us,gb">
          </div>
          <label class="checkbox-row">
            <input type="checkbox" id="quick-fallback" checked>
            Enable deep web page scraping fallback
          </label>
        </div>
      </details>

      <!-- Results Table -->
      <div class="results-card" id="results-card">
        <div class="results-topbar">
          <span id="results-count">Results</span>
          <button type="button" class="btn-ghost" style="padding: 3px 8px; font-size: 11.5px;" onclick="copyTableAsCsv()">Copy CSV</button>
        </div>
        <div class="table-box">
          <table>
            <thead>
              <tr>
                <th>Platform</th>
                <th>App Name</th>
                <th>ID / Package</th>
                <th>Developer</th>
                <th>Category</th>
                <th>Store</th>
              </tr>
            </thead>
            <tbody id="results-tbody"></tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <!-- Footer -->
  <div class="footer">
    App Lookup &bull; <a href="/docs" target="_blank">Swagger API (/docs)</a>
  </div>
</div>

<script>
  let lastResults = [];

  function setMode(mode) {
    document.getElementById('pane-file').classList.toggle('active', mode === 'file');
    document.getElementById('pane-quick').classList.toggle('active', mode === 'quick');
    document.getElementById('tab-btn-file').classList.toggle('active', mode === 'file');
    document.getElementById('tab-btn-quick').classList.toggle('active', mode === 'quick');
  }

  // File Handling
  function handleFile(input) {
    if (input.files && input.files[0]) {
      const file = input.files[0];
      document.getElementById('pill-name').textContent = file.name;
      document.getElementById('pill-size').textContent = (file.size / 1024).toFixed(1) + ' KB';
      document.getElementById('file-pill').style.display = 'flex';
      document.getElementById('dropzone').style.display = 'none';
    }
  }

  function removeFile() {
    const input = document.getElementById('file-input');
    input.value = '';
    document.getElementById('file-pill').style.display = 'none';
    document.getElementById('dropzone').style.display = 'block';
  }

  function handleFormSubmit(e) {
    const btn = document.getElementById('btn-submit-file');
    const label = document.getElementById('btn-submit-label');
    btn.disabled = true;
    label.innerHTML = '<span class="spinner"></span> Processing &amp; Downloading...';
    setTimeout(() => {
      btn.disabled = false;
      label.innerHTML = 'Enrich &amp; Download Excel &darr;';
    }, 6000);
  }

  // Search Handling
  function addSample(sample) {
    const ta = document.getElementById('quick-input');
    const cur = ta.value.trim();
    if (cur) {
      ta.value = cur + '\\n' + sample;
    } else {
      ta.value = sample;
    }
  }

  function clearSearch() {
    document.getElementById('quick-input').value = '';
    document.getElementById('results-card').style.display = 'none';
    document.getElementById('results-tbody').innerHTML = '';
    lastResults = [];
  }

  async function runSearch() {
    const raw = document.getElementById('quick-input').value.trim();
    if (!raw) {
      alert("Please enter at least one App ID or package name.");
      return;
    }
    const lines = raw.split('\\n').map(s => s.trim()).filter(Boolean);
    const platform = document.querySelector('input[name="quick_platform"]:checked').value;
    const countries = document.getElementById('quick-countries').value.trim() || 'in,tw,us,gb';
    const fallback = document.getElementById('quick-fallback').checked;

    const btn = document.getElementById('btn-quick-run');
    const label = document.getElementById('quick-label');
    btn.disabled = true;
    label.innerHTML = '<span class="spinner"></span> Looking up...';

    try {
      const resp = await fetch('/lookup/quick', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          inputs: lines,
          platform: platform,
          countries: countries,
          scrape_fallback: fallback
        })
      });

      if (!resp.ok) throw new Error('Request failed with status ' + resp.status);

      const data = await resp.json();
      lastResults = data.results || [];
      renderTable(data);
    } catch (err) {
      alert('Error fetching metadata: ' + err.message);
    } finally {
      btn.disabled = false;
      label.textContent = 'Look Up Apps';
    }
  }

  function renderTable(data) {
    const card = document.getElementById('results-card');
    const tbody = document.getElementById('results-tbody');
    const count = document.getElementById('results-count');

    count.textContent = `Found ${data.resolved} of ${data.total} apps`;
    tbody.innerHTML = '';

    (data.results || []).forEach(r => {
      const tr = document.createElement('tr');
      const plat = r.platform === 'iOS' ? 'iOS' : r.platform === 'Android' ? 'Android' : '-';
      const link = r.store_url ? `<a href="${esc(r.store_url)}" target="_blank" class="store-link">Open &nearr;</a>` : '-';

      tr.innerHTML = `
        <td><span class="badge-plat">${plat}</span></td>
        <td><strong>${esc(r.app_name)}</strong></td>
        <td><code>${esc(r.app_id || r.input)}</code></td>
        <td>${esc(r.developer || '-')}</td>
        <td>${esc(r.category || '-')}</td>
        <td>${link}</td>
      `;
      tbody.appendChild(tr);
    });

    card.style.display = 'block';
  }

  function copyTableAsCsv() {
    if (!lastResults.length) return;
    const headers = ['Platform', 'App Name', 'ID', 'Developer', 'Category', 'Store URL'];
    const rows = lastResults.map(r => [
      `"${r.platform || ''}"`,
      `"${(r.app_name || '').replace(/"/g, '""')}"`,
      `"${r.app_id || r.input || ''}"`,
      `"${(r.developer || '').replace(/"/g, '""')}"`,
      `"${r.category || ''}"`,
      `"${r.store_url || ''}"`
    ]);
    const csvContent = [headers.join(','), ...rows.map(row => row.join(','))].join('\\n');
    navigator.clipboard.writeText(csvContent).then(() => {
      alert('Copied CSV to clipboard!');
    });
  }

  function esc(str) {
    if (!str) return '';
    return String(str).replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));
  }
</script>

</body>
</html>
"""

# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app_store_api:app", host="127.0.0.1", port=8080, reload=True)

