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
      --bg: #f8fafc;
      --card-bg: #ffffff;
      --text-main: #0f172a;
      --text-muted: #64748b;
      --border: #e2e8f0;
      --border-hover: #cbd5e1;
      --primary: #2563eb;
      --primary-hover: #1d4ed8;
      --primary-subtle: #eff6ff;
      --success: #10b981;
      --success-subtle: #ecfdf5;
      --ios-color: #1e293b;
      --android-color: #047857;
      --radius: 12px;
      --transition: all 0.15s ease-in-out;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
      background: var(--bg);
      color: var(--text-main);
      padding: 40px 20px 80px;
      line-height: 1.5;
      -webkit-font-smoothing: antialiased;
    }

    .container {
      max-width: 820px;
      margin: 0 auto;
    }

    /* Header */
    .header {
      text-align: center;
      margin-bottom: 28px;
    }
    .badge-icon {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      background: #ffffff;
      border: 1px solid var(--border);
      padding: 6px 14px;
      border-radius: 999px;
      font-size: 13px;
      font-weight: 500;
      color: var(--text-muted);
      margin-bottom: 12px;
      box-shadow: 0 1px 2px rgba(0,0,0,0.03);
    }
    .badge-icon span.dot {
      width: 7px;
      height: 7px;
      border-radius: 50%;
      background: var(--success);
    }
    h1 {
      font-size: 26px;
      font-weight: 700;
      letter-spacing: -0.02em;
      color: var(--text-main);
      margin-bottom: 6px;
    }
    .subtitle {
      font-size: 14.5px;
      color: var(--text-muted);
    }

    /* Main Card */
    .card {
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      box-shadow: 0 4px 18px -4px rgba(15, 23, 42, 0.05);
      overflow: hidden;
    }

    /* Segmented Navigation Tabs */
    .tabs-header {
      display: flex;
      border-bottom: 1px solid var(--border);
      background: #fafbfc;
      padding: 6px;
      gap: 6px;
    }
    .tab-btn {
      flex: 1;
      padding: 10px 16px;
      border: 1px solid transparent;
      background: transparent;
      font-size: 14px;
      font-weight: 600;
      color: var(--text-muted);
      border-radius: 8px;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      transition: var(--transition);
    }
    .tab-btn:hover {
      color: var(--text-main);
      background: rgba(0,0,0,0.02);
    }
    .tab-btn.active {
      color: var(--text-main);
      background: #ffffff;
      border-color: var(--border);
      box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    }

    /* Tab Content Panes */
    .tab-pane {
      display: none;
      padding: 28px 24px;
    }
    .tab-pane.active {
      display: block;
    }

    /* Platform Radio Selector */
    .platform-selector {
      margin-bottom: 22px;
    }
    .field-label {
      font-size: 13px;
      font-weight: 600;
      color: var(--text-main);
      margin-bottom: 8px;
      display: block;
    }
    .segmented-control {
      display: flex;
      background: #f1f5f9;
      padding: 3px;
      border-radius: 9px;
      gap: 3px;
    }
    .segmented-control label {
      flex: 1;
      text-align: center;
      font-size: 13px;
      font-weight: 500;
      padding: 7px 10px;
      border-radius: 7px;
      cursor: pointer;
      color: var(--text-muted);
      user-select: none;
      transition: var(--transition);
    }
    .segmented-control input {
      display: none;
    }
    .segmented-control input:checked + span {
      display: block;
      background: #ffffff;
      color: var(--text-main);
      font-weight: 600;
      border-radius: 7px;
      padding: 7px 10px;
      margin: -7px -10px;
      box-shadow: 0 1px 2px rgba(0,0,0,0.06);
    }

    /* Drag & Drop File Upload */
    .dropzone {
      border: 2px dashed var(--border);
      border-radius: 10px;
      padding: 32px 20px;
      text-align: center;
      background: #fafbfc;
      cursor: pointer;
      transition: var(--transition);
      margin-bottom: 20px;
      position: relative;
    }
    .dropzone:hover, .dropzone.dragover {
      border-color: var(--primary);
      background: var(--primary-subtle);
    }
    .dropzone input[type="file"] {
      position: absolute;
      top: 0; left: 0; width: 100%; height: 100%;
      opacity: 0;
      cursor: pointer;
    }
    .dropzone-icon {
      font-size: 32px;
      margin-bottom: 8px;
      display: block;
    }
    .dropzone-text {
      font-size: 14px;
      font-weight: 600;
      color: var(--text-main);
      margin-bottom: 4px;
    }
    .dropzone-hint {
      font-size: 12.5px;
      color: var(--text-muted);
    }
    .file-preview {
      display: none;
      margin-top: 12px;
      padding: 8px 14px;
      background: #ffffff;
      border: 1px solid var(--border);
      border-radius: 8px;
      font-size: 13px;
      font-weight: 500;
      color: var(--text-main);
      align-items: center;
      justify-content: space-between;
    }

    /* Form Inputs */
    .form-row {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 14px;
      margin-bottom: 18px;
    }
    @media (max-width: 600px) {
      .form-row { grid-template-columns: 1fr; }
    }
    .form-group {
      margin-bottom: 16px;
    }
    input[type="text"], textarea {
      width: 100%;
      padding: 10px 12px;
      border: 1px solid var(--border);
      border-radius: 8px;
      font-size: 13.5px;
      color: var(--text-main);
      background: #ffffff;
      outline: none;
      transition: var(--transition);
    }
    input[type="text"]:focus, textarea:focus {
      border-color: var(--primary);
      box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.12);
    }
    textarea {
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      resize: vertical;
      min-height: 110px;
      line-height: 1.6;
    }
    .form-hint {
      font-size: 12px;
      color: var(--text-muted);
      margin-top: 5px;
    }

    .checkbox-label {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      font-size: 13px;
      color: var(--text-main);
      cursor: pointer;
      margin-bottom: 22px;
    }
    .checkbox-label input {
      accent-color: var(--primary);
      width: 16px;
      height: 16px;
    }

    /* Buttons */
    .btn-primary {
      width: 100%;
      padding: 12px 18px;
      background: var(--primary);
      color: #ffffff;
      border: none;
      border-radius: 8px;
      font-size: 14.5px;
      font-weight: 600;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      transition: var(--transition);
    }
    .btn-primary:hover {
      background: var(--primary-hover);
    }
    .btn-primary:disabled {
      opacity: 0.6;
      cursor: not-allowed;
    }
    .btn-row {
      display: flex;
      gap: 10px;
      margin-top: 12px;
    }
    .btn-secondary {
      padding: 9px 15px;
      background: #ffffff;
      border: 1px solid var(--border);
      border-radius: 8px;
      font-size: 13px;
      font-weight: 500;
      color: var(--text-main);
      cursor: pointer;
      transition: var(--transition);
    }
    .btn-secondary:hover {
      background: #f8fafc;
      border-color: var(--border-hover);
    }

    /* Live Results Table */
    .results-section {
      margin-top: 26px;
      border-top: 1px solid var(--border);
      padding-top: 22px;
      display: none;
    }
    .results-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 14px;
    }
    .results-title {
      font-size: 15px;
      font-weight: 700;
      color: var(--text-main);
    }
    .results-summary {
      font-size: 12.5px;
      color: var(--text-muted);
    }
    .table-wrapper {
      border: 1px solid var(--border);
      border-radius: 8px;
      overflow-x: auto;
      max-height: 440px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
      text-align: left;
    }
    th {
      background: #f8fafc;
      padding: 10px 12px;
      font-size: 12px;
      font-weight: 600;
      color: var(--text-muted);
      border-bottom: 1px solid var(--border);
      position: sticky;
      top: 0;
    }
    td {
      padding: 10px 12px;
      border-bottom: 1px solid var(--border);
      vertical-align: middle;
    }
    tr:last-child td { border-bottom: none; }
    tr:hover td { background: #fafbfc; }

    /* Badges */
    .badge {
      display: inline-flex;
      align-items: center;
      padding: 3px 8px;
      border-radius: 6px;
      font-size: 11px;
      font-weight: 600;
      letter-spacing: 0.02em;
    }
    .badge-ios {
      background: #f1f5f9;
      color: var(--ios-color);
      border: 1px solid #cbd5e1;
    }
    .badge-android {
      background: #ecfdf5;
      color: var(--android-color);
      border: 1px solid #a7f3d0;
    }
    .badge-found {
      background: #f0fdf4;
      color: #15803d;
    }
    .badge-missing {
      background: #f8fafc;
      color: #94a3b8;
    }
    .link-store {
      color: var(--primary);
      text-decoration: none;
      font-weight: 500;
      display: inline-flex;
      align-items: center;
      gap: 3px;
    }
    .link-store:hover { text-decoration: underline; }

    /* Loading Spinner */
    .spinner {
      border: 2px solid rgba(255,255,255,0.3);
      border-radius: 50%;
      border-top: 2px solid #ffffff;
      width: 16px;
      height: 16px;
      animation: spin 0.7s linear infinite;
      display: inline-block;
    }
    @keyframes spin {
      0% { transform: rotate(0deg); }
      100% { transform: rotate(360deg); }
    }

    /* Footer */
    .footer {
      text-align: center;
      margin-top: 24px;
      font-size: 12.5px;
      color: var(--text-muted);
    }
    .footer a {
      color: var(--primary);
      text-decoration: none;
    }
    .footer a:hover { text-decoration: underline; }
  </style>
</head>
<body>

<div class="container">
  <!-- Header -->
  <div class="header">
    <div class="badge-icon">
      <span class="dot"></span>
      Apple App Store &amp; Google Play Store
    </div>
    <h1>Mobile App Store Lookup</h1>
    <p class="subtitle">Quickly look up metadata or bulk enrich Excel spreadsheets for iOS and Android apps.</p>
  </div>

  <!-- Main Card -->
  <div class="card">
    <!-- Segmented Tab Header -->
    <div class="tabs-header">
      <button type="button" class="tab-btn active" id="tab-btn-file" onclick="switchTab('file')">
        <svg width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path><polyline points="14 2 14 8 20 8"></polyline><line x1="16" y1="13" x2="8" y2="13"></line><line x1="16" y1="17" x2="8" y2="17"></line></svg>
        Batch Excel / CSV Enricher
      </button>
      <button type="button" class="tab-btn" id="tab-btn-quick" onclick="switchTab('quick')">
        <svg width="16" height="16" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><circle cx="11" cy="11" r="8"></circle><line x1="21" y1="21" x2="16.65" y2="16.65"></line></svg>
        Quick Search &amp; Preview
      </button>
    </div>

    <!-- PANE 1: Excel / CSV Batch Enricher -->
    <div class="tab-pane active" id="pane-file">
      <form action="/lookup/excel" method="post" enctype="multipart/form-data" id="excel-form" onsubmit="handleExcelSubmit(event)">
        <!-- Platform Selection -->
        <div class="platform-selector">
          <label class="field-label">Target Platform</label>
          <div class="segmented-control">
            <label>
              <input type="radio" name="platform" value="auto" checked>
              <span>⚡ Auto-Detect (Both)</span>
            </label>
            <label>
              <input type="radio" name="platform" value="ios">
              <span>🍎 Apple iOS</span>
            </label>
            <label>
              <input type="radio" name="platform" value="android">
              <span>🤖 Google Android</span>
            </label>
          </div>
        </div>

        <!-- File Upload Dropzone -->
        <div class="dropzone" id="file-dropzone">
          <input type="file" name="file" id="file-input" accept=".xlsx,.xls,.csv" required onchange="handleFileSelected(this)">
          <span class="dropzone-icon">📁</span>
          <div class="dropzone-text" id="dropzone-label">Click to browse or drop your spreadsheet here</div>
          <div class="dropzone-hint">Supports Excel (.xlsx, .xls) and CSV (.csv) files</div>
        </div>
        <div class="file-preview" id="file-preview-pill">
          <span id="file-name-text">filename.xlsx</span>
          <button type="button" class="btn-secondary" style="padding:4px 8px;font-size:11px;" onclick="clearFile()">Remove</button>
        </div>

        <!-- Settings Row -->
        <div class="form-row">
          <div class="form-group">
            <label class="field-label">Storefront Countries (priority order)</label>
            <input type="text" name="countries" value="in,tw,us,gb">
            <div class="form-hint">Comma-separated ISO codes (e.g. in,tw,us,gb,jp)</div>
          </div>
          <div class="form-group">
            <label class="field-label">ID Column Header (optional)</label>
            <input type="text" name="column_name" placeholder="Auto-detects if left blank">
            <div class="form-hint">Leaves auto-detection enabled when blank</div>
          </div>
        </div>

        <label class="checkbox-label">
          <input type="checkbox" name="scrape_fallback" value="true" checked>
          Enable web page scraping fallback for unlisted / country-specific apps
        </label>

        <button type="submit" class="btn-primary" id="btn-submit-excel">
          <svg width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path><polyline points="7 10 12 15 17 10"></polyline><line x1="12" y1="15" x2="12" y2="3"></line></svg>
          <span id="btn-submit-text">Enrich Spreadsheet &amp; Download Excel</span>
        </button>
      </form>
    </div>

    <!-- PANE 2: Quick Search & Preview -->
    <div class="tab-pane" id="pane-quick">
      <!-- Platform Selection -->
      <div class="platform-selector">
        <label class="field-label">Target Platform</label>
        <div class="segmented-control">
          <label>
            <input type="radio" name="quick_platform" value="auto" checked>
            <span>⚡ Auto-Detect (Both)</span>
          </label>
          <label>
            <input type="radio" name="quick_platform" value="ios">
            <span>🍎 Apple iOS</span>
          </label>
          <label>
            <input type="radio" name="quick_platform" value="android">
            <span>🤖 Google Android</span>
          </label>
        </div>
      </div>

      <div class="form-group">
        <label class="field-label">App IDs, Package Names, or Store URLs (one per line)</label>
        <textarea id="quick-inputs" rows="5" placeholder="e.g.&#10;com.whatsapp&#10;https://apps.apple.com/in/app/whatsapp-messenger/id310633997&#10;org.mozilla.firefox&#10;1617391485"></textarea>
      </div>

      <div class="form-row">
        <div class="form-group" style="margin-bottom:0;">
          <label class="field-label">Storefront Countries</label>
          <input type="text" id="quick-countries" value="in,tw,us,gb">
        </div>
        <div class="form-group" style="margin-bottom:0;display:flex;align-items:flex-end;">
          <label class="checkbox-label" style="margin-bottom:8px;">
            <input type="checkbox" id="quick-scrape-fallback" checked>
            Enable scrape fallback
          </label>
        </div>
      </div>

      <div class="btn-row">
        <button type="button" class="btn-primary" style="flex:2;" id="btn-quick-run" onclick="runQuickLookup()">
          <span id="quick-btn-text">Lookup Apps</span>
        </button>
        <button type="button" class="btn-secondary" onclick="loadSampleData()">Load Samples</button>
        <button type="button" class="btn-secondary" onclick="clearQuick()">Clear</button>
      </div>

      <!-- Live Results Table -->
      <div class="results-section" id="results-area">
        <div class="results-header">
          <div class="results-title">Lookup Results</div>
          <div class="results-summary" id="results-stats">Total: 0 | Resolved: 0</div>
        </div>
        <div class="table-wrapper">
          <table id="results-table">
            <thead>
              <tr>
                <th>Platform</th>
                <th>App Name</th>
                <th>ID / Package</th>
                <th>Developer</th>
                <th>Category</th>
                <th>Country</th>
                <th>Store Link</th>
              </tr>
            </thead>
            <tbody id="results-tbody">
            </tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <!-- Footer -->
  <div class="footer">
    iOS App Store &amp; Google Play Store Enriched &bull; <a href="/docs" target="_blank">Interactive Swagger API Docs (/docs)</a>
  </div>
</div>

<script>
  function switchTab(tab) {
    document.getElementById('pane-file').classList.toggle('active', tab === 'file');
    document.getElementById('pane-quick').classList.toggle('active', tab === 'quick');
    document.getElementById('tab-btn-file').classList.toggle('active', tab === 'file');
    document.getElementById('tab-btn-quick').classList.toggle('active', tab === 'quick');
  }

  // File Dropzone Handling
  function handleFileSelected(input) {
    if (input.files && input.files[0]) {
      const file = input.files[0];
      document.getElementById('file-name-text').textContent = `${file.name} (${(file.size / 1024).toFixed(1)} KB)`;
      document.getElementById('file-preview-pill').style.display = 'flex';
      document.getElementById('dropzone-label').textContent = 'Selected: ' + file.name;
    }
  }

  function clearFile() {
    const input = document.getElementById('file-input');
    input.value = '';
    document.getElementById('file-preview-pill').style.display = 'none';
    document.getElementById('dropzone-label').textContent = 'Click to browse or drop your spreadsheet here';
  }

  function handleExcelSubmit(e) {
    const btn = document.getElementById('btn-submit-excel');
    const txt = document.getElementById('btn-submit-text');
    btn.disabled = true;
    txt.innerHTML = '<span class="spinner"></span> Processing &amp; Downloading...';
    setTimeout(() => {
      btn.disabled = false;
      txt.textContent = 'Enrich Spreadsheet & Download Excel';
    }, 8000);
  }

  // Quick Interactive Search
  function loadSampleData() {
    document.getElementById('quick-inputs').value = 
      "com.whatsapp\\n" +
      "https://apps.apple.com/us/app/whatsapp-messenger/id310633997\\n" +
      "org.mozilla.firefox\\n" +
      "1617391485";
  }

  function clearQuick() {
    document.getElementById('quick-inputs').value = '';
    document.getElementById('results-area').style.display = 'none';
    document.getElementById('results-tbody').innerHTML = '';
  }

  async function runQuickLookup() {
    const rawText = document.getElementById('quick-inputs').value.trim();
    if (!rawText) {
      alert("Please enter at least one App ID, package name, or store URL.");
      return;
    }
    const lines = rawText.split("\\n").map(l => l.trim()).filter(Boolean);
    const platform = document.querySelector('input[name="quick_platform"]:checked').value;
    const countries = document.getElementById('quick-countries').value.trim() || "in,tw,us,gb";
    const fallback = document.getElementById('quick-scrape-fallback').checked;

    const btn = document.getElementById('btn-quick-run');
    const txt = document.getElementById('quick-btn-text');
    btn.disabled = true;
    txt.innerHTML = '<span class="spinner"></span> Looking up...';

    try {
      const resp = await fetch("/lookup/quick", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          inputs: lines,
          platform: platform,
          countries: countries,
          scrape_fallback: fallback
        })
      });

      if (!resp.ok) {
        throw new Error("Server returned status " + resp.status);
      }

      const data = await resp.json();
      renderResults(data);
    } catch (err) {
      alert("Error resolving apps: " + err.message);
    } finally {
      btn.disabled = false;
      txt.textContent = 'Lookup Apps';
    }
  }

  function renderResults(data) {
    const area = document.getElementById('results-area');
    const tbody = document.getElementById('results-tbody');
    const stats = document.getElementById('results-stats');

    stats.textContent = `Total: ${data.total} | Found: ${data.resolved}`;
    tbody.innerHTML = '';

    (data.results || []).forEach(r => {
      const tr = document.createElement('tr');

      const platBadge = r.platform === 'iOS' 
        ? '<span class="badge badge-ios">🍎 iOS</span>' 
        : r.platform === 'Android' 
        ? '<span class="badge badge-android">🤖 Android</span>' 
        : '<span class="badge badge-missing">Unknown</span>';

      const statusBadge = r.found 
        ? '<span class="badge badge-found">' + escapeHtml(r.app_name) + '</span>'
        : '<span class="badge badge-missing">Not Found</span>';

      const storeLink = r.store_url 
        ? `<a href="${escapeHtml(r.store_url)}" target="_blank" class="link-store">View Store &#8599;</a>`
        : '-';

      tr.innerHTML = `
        <td>${platBadge}</td>
        <td><strong>${escapeHtml(r.app_name)}</strong></td>
        <td><code>${escapeHtml(r.app_id || r.input)}</code></td>
        <td>${escapeHtml(r.developer || '-')}</td>
        <td>${escapeHtml(r.category || '-')}</td>
        <td>${escapeHtml(r.storefront || '-')}</td>
        <td>${storeLink}</td>
      `;
      tbody.appendChild(tr);
    });

    area.style.display = 'block';
  }

  function escapeHtml(str) {
    if (!str) return '';
    return String(str).replace(/[&<>"']/g, function(m) {
      return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m];
    });
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
