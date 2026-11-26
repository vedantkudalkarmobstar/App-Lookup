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

app = FastAPI(title="Mobile App Store & Play Store Lookup API", version="3.1.0")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

DEFAULT_COUNTRIES = ["in", "us", "tw", "gb"]

COUNTRY_NAMES = {
    "IN": "India (IN)",
    "US": "United States (US)",
    "GB": "United Kingdom (GB)",
    "TW": "Taiwan (TW)",
    "JP": "Japan (JP)",
    "DE": "Germany (DE)",
    "CA": "Canada (CA)",
    "AU": "Australia (AU)",
    "FR": "France (FR)",
    "SG": "Singapore (SG)",
}


def format_region(code: str) -> str:
    """Format 2-letter ISO country code into readable region name."""
    c = (code or "").upper().strip()
    return COUNTRY_NAMES.get(c, c if c else "Global")


def parse_countries(countries: Union[str, List[str], None]) -> List[str]:
    """Parse comma-separated country string or list of codes into a clean lowercase list."""
    if not countries:
        return list(DEFAULT_COUNTRIES)
    if isinstance(countries, str):
        parts = [c.strip().lower() for c in countries.split(",") if c.strip()]
        return parts or list(DEFAULT_COUNTRIES)
    if isinstance(countries, (list, tuple)):
        parts = [str(c).strip().lower() for c in countries if str(c).strip()]
        return parts or list(DEFAULT_COUNTRIES)
    return list(DEFAULT_COUNTRIES)


class BatchRequest(BaseModel):
    ids: List[str]
    platform: str = "auto"
    countries: List[str] = DEFAULT_COUNTRIES
    scrape_fallback: bool = True


class QuickRequest(BaseModel):
    inputs: List[str]
    platform: str = "auto"
    countries: str = "in,us,tw,gb"
    scrape_fallback: bool = True


# ============================================================================
# Identifier Extractors & Helpers
# ============================================================================


def extract_ios_id(value: object) -> Optional[str]:
    """Extract numeric iOS App Store ID from raw ID, 'id123', or Apple store URL."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
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

    url_match = re.search(r"[?&]id=([a-zA-Z0-9_\.]+)", text)
    if url_match:
        pkg = url_match.group(1).rstrip("/")
        if "." in pkg and not pkg.endswith("."):
            return pkg

    market_match = re.search(r"market://details\?id=([a-zA-Z0-9_\.]+)", text)
    if market_match:
        return market_match.group(1).rstrip("/")

    pkg_match = re.fullmatch(r"([a-zA-Z][a-zA-Z0-9_]*(\.[a-zA-Z][a-zA-Z0-9_]*)+)", text)
    if pkg_match:
        pkg = pkg_match.group(1)
        if pkg.lower().endswith((".xlsx", ".xls", ".csv", ".py", ".txt", ".json", ".pdf", ".zip", ".html")):
            return None
        return pkg

    return None


def extract_app_info(value: object, platform_hint: str = "auto") -> Tuple[Optional[str], Optional[str]]:
    """Detect platform ('iOS' or 'Android') and clean App ID / Package Name."""
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

    if "play.google.com" in text or "market://" in text:
        pkg = extract_android_package(text)
        return ("Android", pkg) if pkg else (None, None)

    if "apps.apple.com" in text or "itunes.apple.com" in text or "/id" in text:
        ios_id = extract_ios_id(text)
        return ("iOS", ios_id) if ios_id else (None, None)

    ios_id = extract_ios_id(text)
    if ios_id:
        return ("iOS", ios_id)

    pkg = extract_android_package(text)
    if pkg:
        return ("Android", pkg)

    return None, None


def clean_ios_title(title: str) -> str:
    cleaned = title.replace("\u200e", "").replace("\u200f", "").strip()
    cleaned = re.sub(r"\s+on the App Store$", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*[-–|]\s*App Store$", "", cleaned, flags=re.IGNORECASE)
    return cleaned.strip()


def clean_android_title(title: str) -> str:
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
    """Query Apple iTunes Lookup API in batches of 100 with rich metadata."""
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
                            rating = item.get("averageUserRating")
                            found[app_id] = {
                                "platform": "iOS",
                                "app_name": item.get("trackName", ""),
                                "bundle_id": item.get("bundleId", ""),
                                "developer": item.get("artistName", ""),
                                "developer_id": str(item.get("artistId", "")),
                                "category": item.get("primaryGenreName", ""),
                                "icon_url": item.get("artworkUrl512") or item.get("artworkUrl100") or "",
                                "rating": round(float(rating), 2) if rating is not None else None,
                                "reviews_count": item.get("userRatingCount") or 0,
                                "price": item.get("formattedPrice") or "Free",
                                "version": item.get("version", ""),
                                "content_rating": item.get("contentAdvisoryRating", ""),
                                "min_os": f"iOS {item.get('minimumOsVersion')}+" if item.get("minimumOsVersion") else "",
                                "store_url": item.get("trackViewUrl", ""),
                                "deep_link": f"itms-apps://itunes.apple.com/app/id{app_id}",
                                "storefront": country.upper(),
                                "region": format_region(country),
                                "source": "itunes_api",
                                "status": "Live",
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
    """Fetch apps.apple.com/{country}/app/id{app_id} fallback with rich metadata."""
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
            icon_url = ""
            rating = None
            reviews_count = 0
            price = "Free"

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
                        icon_url = ld.get("image", "") or icon_url
                        agg_rating = ld.get("aggregateRating", {})
                        if agg_rating and isinstance(agg_rating, dict):
                            val = agg_rating.get("ratingValue")
                            rating = round(float(val), 2) if val is not None else None
                            reviews_count = int(agg_rating.get("reviewCount") or 0)
                        offers = ld.get("offers", {})
                        if isinstance(offers, dict):
                            p = offers.get("price")
                            if p is not None:
                                price = "Free" if str(p) == "0" else f"${p}"
                        break
                except Exception:
                    continue

            if not app_name:
                og = soup.find("meta", property="og:title")
                if og and og.get("content"):
                    app_name = og["content"]

            if not icon_url:
                og_img = soup.find("meta", property="og:image")
                if og_img and og_img.get("content"):
                    icon_url = og_img["content"]

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
                "developer_id": "",
                "category": category,
                "icon_url": icon_url,
                "rating": rating,
                "reviews_count": reviews_count,
                "price": price,
                "version": "",
                "content_rating": "",
                "min_os": "",
                "store_url": str(resp.url),
                "deep_link": f"itms-apps://itunes.apple.com/app/id{app_id}",
                "storefront": country.upper(),
                "region": format_region(country),
                "source": "app_store_page",
                "status": "Live",
            }
        except Exception:
            return None
        finally:
            await asyncio.sleep(0.15)


async def resolve_ios_ids(
    raw_ids: List[str], countries: Optional[List[str]] = None, scrape_fallback: bool = True
) -> Dict[str, dict]:
    """Resolve iOS App Store IDs automatically across storefronts."""
    unique_ids = list(dict.fromkeys([i for i in raw_ids if i]))
    results: Dict[str, dict] = {}
    clean_countries = [c.strip().lower() for c in (countries or DEFAULT_COUNTRIES) if c.strip()]

    async with httpx.AsyncClient() as client:
        for idx, country in enumerate(clean_countries):
            missing = [aid for aid in unique_ids if aid not in results]
            if not missing:
                break
            batch_hits = await lookup_itunes_batch(client, missing, country)
            for k, v in batch_hits.items():
                if idx > 0 and v.get("status") == "Live":
                    v["status"] = "Region-Restricted"
            results.update(batch_hits)

        if scrape_fallback:
            sem = asyncio.Semaphore(5)
            for idx, country in enumerate(clean_countries):
                missing = [aid for aid in unique_ids if aid not in results]
                if not missing:
                    break
                tasks = [scrape_app_store_page(client, sem, aid, country) for aid in missing]
                scraped = await asyncio.gather(*tasks)
                for aid, info in zip(missing, scraped):
                    if info:
                        if idx > 0 and info.get("status") == "Live":
                            info["status"] = "Region-Restricted"
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
    """Fetch Google Play Store page and extract rich app metadata."""
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
            developer_id = ""
            category = ""
            icon_url = ""
            rating = None
            reviews_count = 0
            price = "Free"
            content_rating = ""

            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    ld = json.loads(script.string or "")
                    if isinstance(ld, dict) and ld.get("@type") == "SoftwareApplication":
                        app_name = ld.get("name", "") or app_name
                        author = ld.get("author", {})
                        if isinstance(author, dict):
                            developer = author.get("name", "") or developer
                            developer_id = author.get("url", "").split("=")[-1] if "url" in author else ""
                        elif isinstance(author, str):
                            developer = author or developer
                        category = ld.get("applicationCategory", "") or category
                        icon_url = ld.get("image", "") or icon_url

                        agg = ld.get("aggregateRating", {})
                        if agg and isinstance(agg, dict):
                            val = agg.get("ratingValue")
                            rating = round(float(val), 2) if val is not None else None
                            reviews_count = int(agg.get("ratingCount") or agg.get("reviewCount") or 0)

                        offers = ld.get("offers", [])
                        if isinstance(offers, list) and offers:
                            p = offers[0].get("price")
                            price = "Free" if str(p) in {"0", "0.0", "0.00"} else f"${p}"
                        elif isinstance(offers, dict):
                            p = offers.get("price")
                            price = "Free" if str(p) in {"0", "0.0", "0.00"} else f"${p}"

                        content_rating = ld.get("contentRating", "") or content_rating
                        break
                except Exception:
                    continue

            if not app_name:
                h1 = soup.find("h1")
                if h1:
                    app_name = h1.get_text(strip=True)

            if not icon_url:
                og_img = soup.find("meta", property="og:image")
                if og_img and og_img.get("content"):
                    icon_url = og_img["content"]

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
                "developer_id": developer_id,
                "category": category,
                "icon_url": icon_url,
                "rating": rating,
                "reviews_count": reviews_count,
                "price": price,
                "version": "",
                "content_rating": content_rating,
                "min_os": "Android",
                "store_url": f"https://play.google.com/store/apps/details?id={package_name}",
                "deep_link": f"market://details?id={package_name}",
                "storefront": country.upper(),
                "region": format_region(country),
                "source": "google_play_store",
                "status": "Live",
            }
        except Exception:
            return None
        finally:
            await asyncio.sleep(0.15)


async def resolve_android_packages(
    packages: List[str], countries: Optional[List[str]] = None
) -> Dict[str, dict]:
    """Resolve Android package names across specified storefront countries."""
    unique_pkgs = list(dict.fromkeys([p for p in packages if p]))
    results: Dict[str, dict] = {}
    clean_countries = [c.strip().lower() for c in (countries or DEFAULT_COUNTRIES) if c.strip()]

    async with httpx.AsyncClient() as client:
        sem = asyncio.Semaphore(5)
        for idx, country in enumerate(clean_countries):
            missing = [pkg for pkg in unique_pkgs if pkg not in results]
            if not missing:
                break
            tasks = [scrape_google_play_page(client, sem, pkg, country) for pkg in missing]
            scraped = await asyncio.gather(*tasks)
            for pkg, info in zip(missing, scraped):
                if info:
                    if idx > 0 and info.get("status") == "Live":
                        info["status"] = "Region-Restricted"
                    results[pkg] = info

    return results


# ============================================================================
# Reverse Keyword Search & Developer Portfolio
# ============================================================================


async def search_apple_apps(query: str, country: str = "us", limit: int = 4) -> List[dict]:
    """Search Apple iTunes for apps by keyword/name."""
    url = "https://itunes.apple.com/search"
    params = {"term": query, "entity": "software", "limit": limit, "country": country.lower()}
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, params=params, timeout=10.0)
            if resp.status_code != 200:
                return []
            items = []
            for x in resp.json().get("results", []):
                rating = x.get("averageUserRating")
                items.append({
                    "platform": "iOS",
                    "app_id": str(x.get("trackId")),
                    "app_name": x.get("trackName", ""),
                    "developer": x.get("artistName", ""),
                    "developer_id": str(x.get("artistId", "")),
                    "category": x.get("primaryGenreName", ""),
                    "icon_url": x.get("artworkUrl100") or x.get("artworkUrl60") or "",
                    "rating": round(float(rating), 1) if rating is not None else None,
                    "reviews_count": x.get("userRatingCount") or 0,
                    "price": x.get("formattedPrice") or "Free",
                    "store_url": x.get("trackViewUrl", ""),
                    "region": format_region(country),
                })
            return items
    except Exception:
        return []


async def search_google_apps(query: str, country: str = "us", limit: int = 4) -> List[dict]:
    """Search Google Play for apps by keyword/name."""
    url = f"https://play.google.com/store/search?q={query}&c=apps&hl=en&gl={country.lower()}"
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(url, headers=HEADERS, timeout=10.0)
            if resp.status_code != 200:
                return []
            soup = BeautifulSoup(resp.text, "html.parser")
            hits = []
            for a in soup.find_all("a", href=re.compile(r"/store/apps/details\?id=")):
                href = a.get("href", "")
                m = re.search(r"id=([a-zA-Z0-9_\.]+)", href)
                if m:
                    pkg = m.group(1)
                    if pkg not in [h["app_id"] for h in hits]:
                        name = a.get_text(strip=True)
                        img = a.find("img")
                        icon = img["src"] if img and "src" in img.attrs else ""
                        hits.append({
                            "platform": "Android",
                            "app_id": pkg,
                            "app_name": clean_android_title(name) or pkg,
                            "developer": "",
                            "developer_id": "",
                            "category": "",
                            "icon_url": icon,
                            "rating": None,
                            "reviews_count": 0,
                            "price": "Free",
                            "store_url": f"https://play.google.com/store/apps/details?id={pkg}",
                            "region": format_region(country),
                        })
                        if len(hits) >= limit:
                            break
            return hits
    except Exception:
        return []


async def lookup_developer_apps(developer_id: str, platform: str = "ios", country: str = "us") -> List[dict]:
    """Find all apps published by a developer/artist."""
    if platform.lower() == "ios":
        url = "https://itunes.apple.com/lookup"
        params = {"id": developer_id, "entity": "software", "country": country.lower()}
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(url, params=params, timeout=10.0)
                if resp.status_code != 200:
                    return []
                results = resp.json().get("results", [])
                apps = []
                for x in results:
                    if x.get("wrapperType") == "software":
                        rating = x.get("averageUserRating")
                        apps.append({
                            "platform": "iOS",
                            "app_id": str(x.get("trackId")),
                            "app_name": x.get("trackName", ""),
                            "developer": x.get("artistName", ""),
                            "category": x.get("primaryGenreName", ""),
                            "icon_url": x.get("artworkUrl100") or "",
                            "rating": round(float(rating), 1) if rating is not None else None,
                            "price": x.get("formattedPrice") or "Free",
                            "store_url": x.get("trackViewUrl", ""),
                            "region": format_region(country),
                        })
                return apps
        except Exception:
            return []
    else:
        url = f"https://play.google.com/store/search?q=pub:{developer_id}&c=apps&hl=en&gl={country.lower()}"
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(url, headers=HEADERS, timeout=10.0)
                if resp.status_code != 200:
                    return []
                soup = BeautifulSoup(resp.text, "html.parser")
                hits = []
                for a in soup.find_all("a", href=re.compile(r"/store/apps/details\?id=")):
                    m = re.search(r"id=([a-zA-Z0-9_\.]+)", a.get("href", ""))
                    if m and m.group(1) not in [h["app_id"] for h in hits]:
                        pkg = m.group(1)
                        img = a.find("img")
                        icon = img["src"] if img and "src" in img.attrs else ""
                        hits.append({
                            "platform": "Android",
                            "app_id": pkg,
                            "app_name": clean_android_title(a.get_text(strip=True)) or pkg,
                            "developer": developer_id,
                            "category": "",
                            "icon_url": icon,
                            "rating": None,
                            "price": "Free",
                            "store_url": f"https://play.google.com/store/apps/details?id={pkg}",
                            "region": format_region(country),
                        })
                        if len(hits) >= 10:
                            break
                return hits
        except Exception:
            return []


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
    clean_countries = [c.strip().lower() for c in (countries or DEFAULT_COUNTRIES) if c.strip()]

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
                "developer_id": "",
                "category": "",
                "icon_url": "",
                "rating": None,
                "reviews_count": 0,
                "price": "",
                "version": "",
                "content_rating": "",
                "min_os": "",
                "store_url": "",
                "deep_link": "",
                "storefront": "",
                "region": "Unknown",
                "source": "invalid_input",
                "status": "Invalid ID",
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
                "developer_id": info.get("developer_id", ""),
                "category": info.get("category", ""),
                "icon_url": info.get("icon_url", ""),
                "rating": info.get("rating"),
                "reviews_count": info.get("reviews_count", 0),
                "price": info.get("price", "Free"),
                "version": info.get("version", ""),
                "content_rating": info.get("content_rating", ""),
                "min_os": info.get("min_os", ""),
                "store_url": info.get("store_url", ""),
                "deep_link": info.get("deep_link", ""),
                "storefront": info.get("storefront", ""),
                "region": info.get("region", format_region(info.get("storefront", ""))),
                "source": info.get("source", ""),
                "status": info.get("status", "Live"),
                "found": True,
            })
        else:
            output.append({
                "input": raw,
                "platform": platform,
                "app_id": aid,
                "app_name": "Not Found / Delisted",
                "bundle_id": aid,
                "developer": "",
                "developer_id": "",
                "category": "",
                "icon_url": "",
                "rating": None,
                "reviews_count": 0,
                "price": "",
                "version": "",
                "content_rating": "",
                "min_os": "",
                "store_url": f"https://apps.apple.com/app/id{aid}" if platform == "iOS" else f"https://play.google.com/store/apps/details?id={aid}",
                "deep_link": f"itms-apps://itunes.apple.com/app/id{aid}" if platform == "iOS" else f"market://details?id={aid}",
                "storefront": "",
                "region": "Not Available",
                "source": "not_found",
                "status": "Delisted / Not Found",
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


@app.get("/search/apps")
async def search_apps_endpoint(
    query: str = Query(..., min_length=2, description="App name or keyword"),
    platform: str = Query("auto"),
    country: str = Query("us"),
    limit: int = Query(4, ge=1, le=10),
):
    """Reverse search: find app IDs and metadata by app name/keyword."""
    plat = platform.lower()
    tasks = []
    if plat in {"auto", "ios"}:
        tasks.append(search_apple_apps(query, country, limit))
    if plat in {"auto", "android"}:
        tasks.append(search_google_apps(query, country, limit))

    results = await asyncio.gather(*tasks)
    merged = []
    for r in results:
        merged.extend(r)
    return {"query": query, "count": len(merged), "results": merged}


@app.get("/developer/apps")
async def developer_apps_endpoint(
    developer_id: str = Query(..., description="Developer ID or publisher name"),
    platform: str = Query("ios"),
    country: str = Query("us"),
):
    """Retrieve other apps published by a developer."""
    apps = await lookup_developer_apps(developer_id, platform, country)
    return {"developer_id": developer_id, "platform": platform, "count": len(apps), "apps": apps}


@app.get("/lookup/ios/{app_id}")
async def lookup_ios_single(
    app_id: str,
    countries: str = Query("in,us,tw,gb", description="Comma-separated country codes"),
    scrape_fallback: bool = Query(True),
):
    clean_id = extract_ios_id(app_id)
    if not clean_id:
        raise HTTPException(status_code=400, detail="Invalid iOS App Store ID or URL")
    res = await resolve_ios_ids([clean_id], parse_countries(countries), scrape_fallback)
    info = res.get(clean_id)
    if not info:
        return {"platform": "iOS", "app_id": clean_id, "app_name": "Not Found / Delisted", "source": "not_found", "status": "Delisted / Not Found", "region": "Not Available"}
    return {"app_id": clean_id, **info}


@app.get("/lookup/android/{package_name}")
async def lookup_android_single(
    package_name: str,
    countries: str = Query("in,us,tw,gb", description="Comma-separated country codes"),
):
    clean_pkg = extract_android_package(package_name)
    if not clean_pkg:
        raise HTTPException(status_code=400, detail="Invalid Android package name or Play Store URL")
    res = await resolve_android_packages([clean_pkg], parse_countries(countries))
    info = res.get(clean_pkg)
    if not info:
        return {"platform": "Android", "app_id": clean_pkg, "app_name": "Not Found / Delisted", "source": "not_found", "status": "Delisted / Not Found", "region": "Not Available"}
    return {"app_id": clean_pkg, **info}


@app.get("/lookup/auto/{identifier:path}")
async def lookup_auto_single(
    identifier: str,
    countries: str = Query("in,us,tw,gb"),
    scrape_fallback: bool = Query(True),
):
    results = await resolve_mixed_batch([identifier], platform_hint="auto", countries=parse_countries(countries), scrape_fallback=scrape_fallback)
    if results:
        return results[0]
    raise HTTPException(status_code=404, detail="Could not process identifier")


@app.post("/lookup/quick")
async def lookup_quick_interactive(payload: QuickRequest):
    """Fast endpoint for the interactive UI quick search table."""
    results = await resolve_mixed_batch(
        raw_inputs=payload.inputs,
        platform_hint=payload.platform,
        countries=parse_countries(payload.countries),
        scrape_fallback=payload.scrape_fallback,
    )
    live_count = sum(1 for r in results if r.get("status") in {"Live", "Region-Restricted"})
    delisted_count = sum(1 for r in results if not r.get("found"))
    return {
        "total": len(results),
        "resolved": live_count,
        "delisted": delisted_count,
        "results": results,
    }


@app.post("/lookup/batch")
async def lookup_batch(payload: BatchRequest):
    results = await resolve_mixed_batch(
        raw_inputs=payload.ids,
        platform_hint=payload.platform,
        countries=payload.countries,
        scrape_fallback=payload.scrape_fallback,
    )
    live_count = sum(1 for r in results if r.get("status") in {"Live", "Region-Restricted"})
    return {"total": len(results), "resolved": live_count, "results": results}


@app.post("/lookup/excel")
async def lookup_excel(
    file: UploadFile = File(...),
    platform: str = Form("auto"),
    countries: str = Form("in,us,tw,gb"),
    column_name: Optional[str] = Form(None),
    scrape_fallback: bool = Form(True),
):
    raw_bytes = await file.read()
    filename = (file.filename or "apps.xlsx").lower()

    try:
        if filename.endswith(".csv"):
            df = None
            enc_errors = []
            for enc in ["utf-8", "utf-8-sig", "latin-1", "cp1252"]:
                try:
                    df = pd.read_csv(io.BytesIO(raw_bytes), encoding=enc, dtype=str)
                    break
                except Exception as e:
                    enc_errors.append(str(e))
            if df is None:
                raise HTTPException(status_code=400, detail=f"Could not read CSV file: {enc_errors[-1]}")

            if len(df.columns) >= 1 and extract_app_info(df.columns[0], platform)[1] is not None:
                for enc in ["utf-8", "utf-8-sig", "latin-1", "cp1252"]:
                    try:
                        df = pd.read_csv(io.BytesIO(raw_bytes), header=None, encoding=enc, dtype=str)
                        df.columns = ["app_id"] + [f"col_{i}" for i in range(1, len(df.columns))]
                        break
                    except Exception:
                        continue
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
        countries=parse_countries(countries),
        scrape_fallback=scrape_fallback,
    )

    # Attach enriched metadata columns (with direct Region output)
    df["Store_Status"] = [r["status"] for r in resolved_list]
    df["Region"] = [r.get("region", "Global") for r in resolved_list]
    df["Platform"] = [r["platform"] for r in resolved_list]
    df["Extracted_ID"] = [r["app_id"] for r in resolved_list]
    df["App_Name"] = [r["app_name"] for r in resolved_list]
    df["Developer"] = [r["developer"] for r in resolved_list]
    df["Category"] = [r["category"] for r in resolved_list]
    df["Rating"] = [r.get("rating") for r in resolved_list]
    df["Reviews_Count"] = [r.get("reviews_count") for r in resolved_list]
    df["Price"] = [r.get("price") for r in resolved_list]
    df["Version"] = [r.get("version") for r in resolved_list]
    df["Content_Rating"] = [r.get("content_rating") for r in resolved_list]
    df["Min_OS"] = [r.get("min_os") for r in resolved_list]
    df["Store_URL"] = [r["store_url"] for r in resolved_list]
    df["Deep_Link"] = [r.get("deep_link") for r in resolved_list]
    df["Icon_URL"] = [r.get("icon_url") for r in resolved_list]
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
# Minimal, Subtle, Unified User Interface (With Real-Time Loading & Auto Region)
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
      --badge-bg: #f3f4f6;
      --badge-text: #374151;
      --success: #16a34a;
      --danger: #dc2626;
      --radius: 14px;
      --radius-sm: 8px;
      --transition: all 0.15s cubic-bezier(0.16, 1, 0.3, 1);
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      font-family: -apple-system, BlinkMacSystemFont, "Inter", "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      background: var(--bg);
      color: var(--text);
      padding: 50px 20px 80px;
      line-height: 1.5;
      -webkit-font-smoothing: antialiased;
    }

    .container {
      max-width: 720px;
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
      margin-bottom: 12px;
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
      margin-bottom: 20px;
      gap: 3px;
    }
    .segmented button {
      flex: 1;
      border: none;
      background: transparent;
      padding: 8px 14px;
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
      padding: 36px 20px;
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
    .file-pill-left svg { flex-shrink: 0; color: var(--text-muted); }
    .file-pill-name { font-weight: 500; color: var(--text); }
    .file-pill-size { color: var(--text-subtle); font-size: 12px; }
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
    .file-pill-remove:hover { color: #ef4444; background: #fee2e2; }

    /* Live Progress Bar Container for Spreadsheet Download */
    .progress-box {
      display: none;
      background: #f9fafb;
      border: 1px solid var(--border);
      border-radius: 10px;
      padding: 14px 16px;
      margin-bottom: 18px;
    }
    .progress-track {
      width: 100%;
      height: 6px;
      background: #e5e7eb;
      border-radius: 999px;
      overflow: hidden;
      margin-bottom: 10px;
      position: relative;
    }
    .progress-fill {
      height: 100%;
      width: 0%;
      background: var(--primary);
      border-radius: 999px;
      transition: width 0.35s ease;
      background-image: linear-gradient(45deg, rgba(255,255,255,0.2) 25%, transparent 25%, transparent 50%, rgba(255,255,255,0.2) 50%, rgba(255,255,255,0.2) 75%, transparent 75%, transparent);
      background-size: 16px 16px;
      animation: progress-stripes 1s linear infinite;
    }
    @keyframes progress-stripes {
      from { background-position: 16px 0; }
      to { background-position: 0 0; }
    }
    .progress-status-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      font-size: 12.5px;
    }
    .progress-status-text {
      color: var(--text);
      font-weight: 500;
      display: inline-flex;
      align-items: center;
      gap: 7px;
    }
    .progress-pct {
      color: var(--text-muted);
      font-weight: 600;
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
    .btn-main:hover { background: var(--primary-hover); }
    .btn-main:disabled { opacity: 0.6; cursor: not-allowed; }

    .btn-ghost {
      padding: 7px 12px;
      background: transparent;
      border: 1px solid var(--border);
      border-radius: var(--radius-sm);
      font-size: 12px;
      font-weight: 500;
      color: var(--text-muted);
      cursor: pointer;
      transition: var(--transition);
      display: inline-flex;
      align-items: center;
      gap: 5px;
    }
    .btn-ghost:hover { background: #f9fafb; color: var(--text); border-color: #d1d5db; }

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
    details.advanced summary:hover { color: var(--text); }
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
    input[type="text"]:focus, textarea:focus { border-color: var(--border-focus); }

    .radio-group { display: flex; gap: 8px; }
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
    }
    .checkbox-row input { accent-color: var(--primary); }

    /* Search Sub-Tabs */
    .search-subnav {
      display: flex;
      gap: 6px;
      margin-bottom: 12px;
    }
    .subnav-btn {
      padding: 5px 10px;
      background: transparent;
      border: none;
      font-size: 12px;
      font-weight: 500;
      color: var(--text-muted);
      border-radius: 6px;
      cursor: pointer;
    }
    .subnav-btn.active {
      background: #f3f4f6;
      color: var(--text);
      font-weight: 600;
    }

    /* Instant Search View */
    textarea {
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      resize: vertical;
      min-height: 95px;
      line-height: 1.6;
      margin-bottom: 8px;
    }
    .sample-row {
      display: flex;
      align-items: center;
      gap: 6px;
      flex-wrap: wrap;
      margin-bottom: 14px;
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
    .sample-chip:hover { background: #e5e7eb; color: var(--text); }

    .quick-actions { display: flex; gap: 8px; }

    /* Shimmer Skeleton Loaders for Live Fetching */
    .shimmer-cell {
      background: linear-gradient(90deg, #f0f1f3 25%, #e2e4e8 50%, #f0f1f3 75%);
      background-size: 200% 100%;
      animation: shimmer 1.5s infinite;
      border-radius: 5px;
      display: inline-block;
    }
    @keyframes shimmer {
      0% { background-position: 200% 0; }
      100% { background-position: -200% 0; }
    }

    /* Keyword Search Results Cards */
    .keyword-results {
      margin-top: 14px;
      display: grid;
      gap: 8px;
      display: none;
    }
    .app-card-item {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 10px 14px;
      background: #f9fafb;
      border: 1px solid var(--border);
      border-radius: 10px;
      transition: var(--transition);
    }
    .app-card-item:hover { border-color: #cbd5e1; background: #ffffff; }
    .app-card-left {
      display: flex;
      align-items: center;
      gap: 12px;
      overflow: hidden;
    }
    .app-card-icon {
      width: 40px;
      height: 40px;
      border-radius: 9px;
      object-fit: cover;
      border: 1px solid rgba(0,0,0,0.06);
      background: #e5e7eb;
    }
    .app-card-meta { overflow: hidden; }
    .app-card-title {
      font-size: 13px;
      font-weight: 600;
      color: var(--text);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .app-card-sub {
      font-size: 11.5px;
      color: var(--text-muted);
      display: flex;
      align-items: center;
      gap: 6px;
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
      flex-wrap: wrap;
      gap: 8px;
    }
    .results-summary-badge {
      font-size: 12.5px;
      font-weight: 500;
      color: var(--text-muted);
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .export-btns { display: flex; gap: 6px; }
    .table-box {
      border: 1px solid var(--border);
      border-radius: 8px;
      overflow-x: auto;
      max-height: 400px;
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 12.5px;
      text-align: left;
    }
    th {
      background: #fafafa;
      padding: 9px 12px;
      font-weight: 500;
      color: var(--text-muted);
      border-bottom: 1px solid var(--border);
      position: sticky;
      top: 0;
      white-space: nowrap;
      z-index: 2;
    }
    td {
      padding: 9px 12px;
      border-bottom: 1px solid #f3f4f6;
      vertical-align: middle;
      white-space: nowrap;
    }
    tr.clickable-row { cursor: pointer; transition: var(--transition); }
    tr.clickable-row:hover td { background: #f8fafc; }
    tr:last-child td { border-bottom: none; }

    .tbl-app-cell {
      display: flex;
      align-items: center;
      gap: 9px;
    }
    .tbl-icon {
      width: 26px;
      height: 26px;
      border-radius: 6px;
      object-fit: cover;
      background: #e5e7eb;
      border: 1px solid rgba(0,0,0,0.05);
      flex-shrink: 0;
    }

    .badge-plat {
      display: inline-block;
      padding: 2px 6px;
      border-radius: 4px;
      font-size: 11px;
      font-weight: 500;
      background: #f3f4f6;
      color: #374151;
    }
    .badge-region {
      display: inline-block;
      padding: 2px 7px;
      border-radius: 4px;
      font-size: 11px;
      font-weight: 500;
      background: #f1f5f9;
      color: #1e293b;
      border: 1px solid #e2e8f0;
    }
    .badge-status {
      display: inline-flex;
      align-items: center;
      gap: 4px;
      padding: 2px 6px;
      border-radius: 4px;
      font-size: 11px;
      font-weight: 500;
    }
    .status-live { background: #ecfdf5; color: var(--success); }
    .status-delisted { background: #fef2f2; color: var(--danger); }
    .status-restricted { background: #fefce8; color: #854d0e; }

    /* Interactive Slide-Over Drawer */
    .drawer-overlay {
      position: fixed;
      inset: 0;
      background: rgba(0, 0, 0, 0.35);
      backdrop-filter: blur(2px);
      z-index: 100;
      opacity: 0;
      pointer-events: none;
      transition: opacity 0.2s ease;
    }
    .drawer-overlay.open { opacity: 1; pointer-events: auto; }
    .drawer {
      position: fixed;
      top: 0;
      right: -420px;
      width: 400px;
      max-width: 90vw;
      height: 100%;
      background: #ffffff;
      box-shadow: -4px 0 24px rgba(0, 0, 0, 0.1);
      z-index: 101;
      transition: right 0.25s cubic-bezier(0.16, 1, 0.3, 1);
      overflow-y: auto;
      padding: 28px 24px;
      display: flex;
      flex-direction: column;
    }
    .drawer.open { right: 0; }
    .drawer-close {
      position: absolute;
      top: 20px;
      right: 20px;
      background: #f3f4f6;
      border: none;
      width: 28px;
      height: 28px;
      border-radius: 50%;
      font-size: 16px;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      color: var(--text-muted);
    }
    .drawer-close:hover { background: #e5e7eb; color: var(--text); }
    .drawer-icon {
      width: 72px;
      height: 72px;
      border-radius: 16px;
      object-fit: cover;
      box-shadow: 0 2px 10px rgba(0,0,0,0.08);
      border: 1px solid rgba(0,0,0,0.06);
      margin-bottom: 14px;
    }
    .drawer-title { font-size: 18px; font-weight: 700; color: var(--text); line-height: 1.3; margin-bottom: 4px; }
    .drawer-dev { font-size: 13px; color: var(--text-muted); margin-bottom: 12px; }
    .drawer-dev-link { color: #2563eb; text-decoration: none; cursor: pointer; }
    .drawer-dev-link:hover { text-decoration: underline; }

    .drawer-pills {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      margin-bottom: 20px;
    }
    .drawer-pill {
      background: #f3f4f6;
      padding: 4px 8px;
      border-radius: 6px;
      font-size: 11.5px;
      font-weight: 500;
      color: var(--text-muted);
    }
    .drawer-pill-region {
      background: #eff6ff;
      color: #1e40af;
      border: 1px solid #dbeafe;
    }

    .qr-card {
      background: #fafafa;
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 16px;
      text-align: center;
      margin-bottom: 20px;
    }
    .qr-img {
      width: 140px;
      height: 140px;
      margin: 0 auto 8px;
      display: block;
      border-radius: 8px;
      background: #ffffff;
      padding: 6px;
      border: 1px solid var(--border);
    }
    .qr-caption { font-size: 11.5px; color: var(--text-muted); }

    .drawer-actions {
      display: grid;
      gap: 8px;
      margin-top: auto;
    }

    /* Live Spinner */
    .spinner {
      border: 2px solid rgba(255,255,255,0.3);
      border-radius: 50%;
      border-top: 2px solid #ffffff;
      width: 14px;
      height: 14px;
      animation: spin 0.6s linear infinite;
      display: inline-block;
    }
    .spinner-dark {
      border: 2px solid rgba(17,24,39,0.2);
      border-top: 2px solid #111827;
    }
    @keyframes spin { to { transform: rotate(360deg); } }

    /* Footer */
    .footer {
      text-align: center;
      margin-top: 28px;
      font-size: 12px;
      color: var(--text-subtle);
    }
    .footer a { color: var(--text-muted); text-decoration: none; }
    .footer a:hover { text-decoration: underline; }
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
    <p>Enrich App Store &amp; Google Play metadata automatically across global regions.</p>
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
      <form id="excel-form" onsubmit="handleFormSubmit(event)">
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

        <!-- Live Download Progress Indicator -->
        <div class="progress-box" id="progress-box">
          <div class="progress-track">
            <div class="progress-fill" id="progress-fill"></div>
          </div>
          <div class="progress-status-row">
            <span class="progress-status-text" id="progress-status-text">
              <span class="spinner spinner-dark"></span> Starting enrichment...
            </span>
            <span class="progress-pct" id="progress-pct">0%</span>
          </div>
        </div>

        <!-- Primary Action Button -->
        <button type="submit" class="btn-main" id="btn-submit-file">
          <span id="btn-submit-label">Enrich &amp; Download Excel &darr;</span>
        </button>

        <!-- Collapsible Advanced Settings (No manual region required!) -->
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
      <!-- Search Sub-Navigation -->
      <div class="search-subnav">
        <button type="button" class="subnav-btn active" id="subnav-id" onclick="setSearchSubnav('id')">By ID / Store URL</button>
        <button type="button" class="subnav-btn" id="subnav-name" onclick="setSearchSubnav('name')">Reverse Search by Name</button>
      </div>

      <!-- Mode A: By ID / URL -->
      <div id="search-box-id">
        <textarea id="quick-input" placeholder="Paste App IDs, package names, or store URLs (one per line)..."></textarea>
        <!-- Sample Chips -->
        <div class="sample-row">
          <span>Try:</span>
          <button type="button" class="sample-chip" onclick="addSample('com.grofers.customerapp')">+ Blinkit (Android)</button>
          <button type="button" class="sample-chip" onclick="addSample('960335206')">+ Blinkit (iOS)</button>
          <button type="button" class="sample-chip" onclick="addSample('com.whatsapp')">+ WhatsApp</button>
          <button type="button" class="sample-chip" onclick="addSample('org.mozilla.firefox')">+ Firefox</button>
          <button type="button" class="sample-chip" onclick="addSample('com.spotify.music')">+ Spotify</button>
        </div>
        <div class="quick-actions">
          <button type="button" class="btn-main" style="flex: 2;" id="btn-quick-run" onclick="runSearch()">
            <span id="quick-label">Look Up Apps</span>
          </button>
          <button type="button" class="btn-ghost" onclick="clearSearch()">Clear</button>
        </div>
      </div>

      <!-- Mode B: By Name / Keyword -->
      <div id="search-box-name" style="display: none;">
        <div style="display: flex; gap: 8px; margin-bottom: 12px;">
          <input type="text" id="name-search-input" placeholder="Type app name (e.g. Blinkit, Notion, Duolingo)..." onkeydown="if(event.key==='Enter') runKeywordSearch()">
          <button type="button" class="btn-main" style="width: auto; padding: 0 16px;" onclick="runKeywordSearch()">Search</button>
        </div>
        <div class="keyword-results" id="keyword-results"></div>
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
          <label class="checkbox-row">
            <input type="checkbox" id="quick-fallback" checked>
            Enable deep web page scraping fallback
          </label>
        </div>
      </details>

      <!-- Results Table -->
      <div class="results-card" id="results-card">
        <div class="results-topbar">
          <div class="results-summary-badge" id="results-summary">
            Results
          </div>
          <div class="export-btns">
            <button type="button" class="btn-ghost" onclick="copyForGoogleSheets()">📋 Copy for Sheets</button>
            <button type="button" class="btn-ghost" onclick="downloadCsv()">📄 CSV</button>
            <button type="button" class="btn-ghost" onclick="downloadJson()">{} JSON</button>
          </div>
        </div>
        <div class="table-box">
          <table>
            <thead>
              <tr>
                <th>App</th>
                <th>Region</th>
                <th>Status</th>
                <th>Rating</th>
                <th>Price</th>
                <th>Category</th>
                <th>Store Link</th>
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

<!-- Slide-Over App Drawer -->
<div class="drawer-overlay" id="drawer-overlay" onclick="closeDrawer()"></div>
<div class="drawer" id="drawer">
  <button type="button" class="drawer-close" onclick="closeDrawer()">&times;</button>
  <img id="drawer-icon" src="" alt="App Icon" class="drawer-icon">
  <div class="drawer-title" id="drawer-title">-</div>
  <div class="drawer-dev">
    by <span id="drawer-dev">-</span>
    <span id="drawer-dev-explore" style="display:none;"> &bull; <a class="drawer-dev-link" onclick="exploreDevApps()">More apps by publisher &rarr;</a></span>
  </div>

  <div class="drawer-pills">
    <span class="drawer-pill drawer-pill-region" id="dpill-region">Region: Global</span>
    <span class="drawer-pill" id="dpill-plat">iOS</span>
    <span class="drawer-pill" id="dpill-price">Free</span>
    <span class="drawer-pill" id="dpill-rating">4.8 ★</span>
    <span class="drawer-pill" id="dpill-version" style="display:none;">v1.0</span>
    <span class="drawer-pill" id="dpill-os" style="display:none;">iOS 15.0+</span>
  </div>

  <!-- QR Code Card -->
  <div class="qr-card">
    <img id="drawer-qr" class="qr-img" src="" alt="Store QR Code">
    <div class="qr-caption">Scan with phone camera to view in store</div>
  </div>

  <div class="drawer-actions">
    <a id="drawer-store-btn" href="#" target="_blank" class="btn-main" style="text-decoration:none;">Open in Store &nearr;</a>
    <a id="drawer-deeplink-btn" href="#" class="btn-ghost" style="text-decoration:none; justify-content:center;">Open in Native App &nearr;</a>
    <button type="button" class="btn-ghost" onclick="copyDrawerId()">Copy App ID / Package</button>
    <button type="button" class="btn-ghost" onclick="copyDrawerUrl()">Copy Store URL</button>
  </div>
</div>

<script>
  let lastResults = [];
  let currentDrawerApp = null;
  let progressTimer = null;

  function setMode(mode) {
    document.getElementById('pane-file').classList.toggle('active', mode === 'file');
    document.getElementById('pane-quick').classList.toggle('active', mode === 'quick');
    document.getElementById('tab-btn-file').classList.toggle('active', mode === 'file');
    document.getElementById('tab-btn-quick').classList.toggle('active', mode === 'quick');
  }

  function setSearchSubnav(sub) {
    document.getElementById('search-box-id').style.display = sub === 'id' ? 'block' : 'none';
    document.getElementById('search-box-name').style.display = sub === 'name' ? 'block' : 'none';
    document.getElementById('subnav-id').classList.toggle('active', sub === 'id');
    document.getElementById('subnav-name').classList.toggle('active', sub === 'name');
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
    hideProgress();
  }

  // Live Loading Animation for File Download
  async function handleFormSubmit(e) {
    e.preventDefault();
    const fileInput = document.getElementById('file-input');
    if (!fileInput.files || !fileInput.files[0]) {
      alert("Please select an Excel or CSV file first.");
      return;
    }

    const file = fileInput.files[0];
    const form = document.getElementById('excel-form');
    const formData = new FormData(form);

    const btn = document.getElementById('btn-submit-file');
    btn.style.display = 'none';
    showProgress();

    // Stage progression
    updateProgress(20, 'Uploading spreadsheet...');
    progressTimer = setTimeout(() => {
      updateProgress(50, 'Querying Apple & Google stores...');
      progressTimer = setTimeout(() => {
        updateProgress(80, 'Generating enriched Excel spreadsheet...');
      }, 2500);
    }, 1200);

    try {
      const resp = await fetch('/lookup/excel', {
        method: 'POST',
        body: formData
      });

      if (!resp.ok) {
        const err = await resp.text();
        throw new Error(err || 'Failed to enrich spreadsheet');
      }

      clearTimeout(progressTimer);
      updateProgress(100, '✓ Download ready!');

      const blob = await resp.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      const baseName = file.name.replace(/\\.[^/.]+$/, "");
      a.download = `${baseName}_resolved.xlsx`;
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);

      setTimeout(() => {
        hideProgress();
        btn.style.display = 'inline-flex';
      }, 2000);

    } catch (err) {
      clearTimeout(progressTimer);
      alert("Error: " + err.message);
      hideProgress();
      btn.style.display = 'inline-flex';
    }
  }

  function showProgress() {
    const box = document.getElementById('progress-box');
    box.style.display = 'block';
    updateProgress(10, 'Initializing...');
  }

  function hideProgress() {
    document.getElementById('progress-box').style.display = 'none';
  }

  function updateProgress(pct, statusText) {
    document.getElementById('progress-fill').style.width = pct + '%';
    document.getElementById('progress-pct').textContent = pct + '%';
    document.getElementById('progress-status-text').innerHTML = 
      pct === 100 
      ? statusText 
      : `<span class="spinner spinner-dark"></span> ${statusText}`;
  }

  // Search Handling
  function addSample(sample) {
    const ta = document.getElementById('quick-input');
    const cur = ta.value.trim();
    ta.value = cur ? cur + '\\n' + sample : sample;
  }

  function clearSearch() {
    document.getElementById('quick-input').value = '';
    document.getElementById('results-card').style.display = 'none';
    document.getElementById('results-tbody').innerHTML = '';
    lastResults = [];
  }

  // Live Shimmer Loader for Instant Fetching
  function showSearchShimmer() {
    const card = document.getElementById('results-card');
    const tbody = document.getElementById('results-tbody');
    const summary = document.getElementById('results-summary');

    summary.innerHTML = `<span class="spinner spinner-dark"></span> Fetching real-time store metadata across regions...`;

    tbody.innerHTML = `
      <tr>
        <td><div class="tbl-app-cell"><span class="shimmer-cell tbl-icon"></span><div><span class="shimmer-cell" style="width:120px;height:14px;"></span><div style="margin-top:4px;"><span class="shimmer-cell" style="width:80px;height:11px;"></span></div></div></div></td>
        <td><span class="shimmer-cell" style="width:75px;height:18px;border-radius:4px;"></span></td>
        <td><span class="shimmer-cell" style="width:55px;height:18px;border-radius:4px;"></span></td>
        <td><span class="shimmer-cell" style="width:45px;height:14px;"></span></td>
        <td><span class="shimmer-cell" style="width:40px;height:14px;"></span></td>
        <td><span class="shimmer-cell" style="width:90px;height:14px;"></span></td>
        <td><span class="shimmer-cell" style="width:50px;height:14px;"></span></td>
      </tr>
      <tr>
        <td><div class="tbl-app-cell"><span class="shimmer-cell tbl-icon"></span><div><span class="shimmer-cell" style="width:140px;height:14px;"></span><div style="margin-top:4px;"><span class="shimmer-cell" style="width:90px;height:11px;"></span></div></div></div></td>
        <td><span class="shimmer-cell" style="width:85px;height:18px;border-radius:4px;"></span></td>
        <td><span class="shimmer-cell" style="width:55px;height:18px;border-radius:4px;"></span></td>
        <td><span class="shimmer-cell" style="width:45px;height:14px;"></span></td>
        <td><span class="shimmer-cell" style="width:40px;height:14px;"></span></td>
        <td><span class="shimmer-cell" style="width:80px;height:14px;"></span></td>
        <td><span class="shimmer-cell" style="width:50px;height:14px;"></span></td>
      </tr>
    `;
    card.style.display = 'block';
  }

  async function runSearch() {
    const raw = document.getElementById('quick-input').value.trim();
    if (!raw) {
      alert("Please enter at least one App ID or package name.");
      return;
    }
    const lines = raw.split('\\n').map(s => s.trim()).filter(Boolean);
    const platform = document.querySelector('input[name="quick_platform"]:checked').value;
    const fallback = document.getElementById('quick-fallback').checked;

    const btn = document.getElementById('btn-quick-run');
    const label = document.getElementById('quick-label');
    btn.disabled = true;
    label.innerHTML = '<span class="spinner"></span> Fetching...';

    showSearchShimmer();

    try {
      const resp = await fetch('/lookup/quick', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          inputs: lines,
          platform: platform,
          countries: 'in,us,tw,gb',
          scrape_fallback: fallback
        })
      });

      if (!resp.ok) throw new Error('Request failed with status ' + resp.status);

      const data = await resp.json();
      lastResults = data.results || [];
      renderTable(data);
    } catch (err) {
      alert('Error fetching metadata: ' + err.message);
      document.getElementById('results-card').style.display = 'none';
    } finally {
      btn.disabled = false;
      label.textContent = 'Look Up Apps';
    }
  }

  function renderTable(data) {
    const card = document.getElementById('results-card');
    const tbody = document.getElementById('results-tbody');
    const summary = document.getElementById('results-summary');

    const total = data.total || 0;
    const live = data.resolved || 0;
    const delisted = data.delisted || 0;

    summary.innerHTML = `Found ${total} apps &bull; <span style="color:var(--success);">&bull; ${live} Live</span>` +
      (delisted > 0 ? ` &bull; <span style="color:var(--danger);">&bull; ${delisted} Delisted</span>` : '');

    tbody.innerHTML = '';

    (data.results || []).forEach(r => {
      const tr = document.createElement('tr');
      tr.className = 'clickable-row';
      tr.onclick = () => openDrawer(r);

      const statusCls = r.status === 'Live' ? 'status-live' : (r.status === 'Region-Restricted' ? 'status-restricted' : 'status-delisted');
      const statusIcon = r.status === 'Live' ? '&bull;' : (r.status === 'Region-Restricted' ? '&bull;' : '&times;');

      const iconImg = r.icon_url 
        ? `<img src="${esc(r.icon_url)}" class="tbl-icon" alt="" onerror="this.style.display='none'">` 
        : '<div class="tbl-icon"></div>';

      const ratingTxt = r.rating ? `${r.rating} ★ <span style="color:#9ca3af;font-size:11px;">(${formatCount(r.reviews_count)})</span>` : '-';
      const link = r.store_url ? `<a href="${esc(r.store_url)}" target="_blank" class="store-link" onclick="event.stopPropagation()">Store &nearr;</a>` : '-';

      tr.innerHTML = `
        <td>
          <div class="tbl-app-cell">
            ${iconImg}
            <div>
              <strong>${esc(r.app_name)}</strong>
              <div style="font-size:11.5px;color:#9ca3af;">${esc(r.developer || r.app_id)}</div>
            </div>
          </div>
        </td>
        <td><span class="badge-region">${esc(r.region || 'Global')}</span></td>
        <td><span class="badge-status ${statusCls}">${statusIcon} ${esc(r.status || 'Live')}</span></td>
        <td>${ratingTxt}</td>
        <td>${esc(r.price || 'Free')}</td>
        <td>${esc(r.category || '-')}</td>
        <td>${link}</td>
      `;
      tbody.appendChild(tr);
    });

    card.style.display = 'block';
  }

  // Reverse Keyword Search
  async function runKeywordSearch() {
    const q = document.getElementById('name-search-input').value.trim();
    if (!q) return;

    const resBox = document.getElementById('keyword-results');
    resBox.innerHTML = '<div style="font-size:12.5px;color:#6b7280;padding:8px 0;"><span class="spinner spinner-dark"></span> Searching global stores...</div>';
    resBox.style.display = 'grid';

    try {
      const resp = await fetch(`/search/apps?query=${encodeURIComponent(q)}&country=us&limit=4`);
      if (!resp.ok) throw new Error('Search failed');
      const data = await resp.json();

      if (!data.results || !data.results.length) {
        resBox.innerHTML = '<div style="font-size:12.5px;color:#6b7280;padding:8px 0;">No apps found matching "' + esc(q) + '".</div>';
        return;
      }

      resBox.innerHTML = '';
      data.results.forEach(app => {
        const item = document.createElement('div');
        item.className = 'app-card-item';
        const ratingStr = app.rating ? `${app.rating} ★` : '';
        item.innerHTML = `
          <div class="app-card-left">
            <img src="${esc(app.icon_url)}" class="app-card-icon" onerror="this.src='data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22/>'">
            <div class="app-card-meta">
              <div class="app-card-title">${esc(app.app_name)}</div>
              <div class="app-card-sub">
                <span class="badge-plat">${app.platform}</span>
                <span>${esc(app.developer || '')}</span>
                ${ratingStr ? '<span>&bull; ' + ratingStr + '</span>' : ''}
              </div>
            </div>
          </div>
          <div style="display:flex;gap:6px;">
            <button type="button" class="btn-ghost" onclick="addSample('${esc(app.app_id)}'); setSearchSubnav('id'); runSearch();">Enrich &rarr;</button>
          </div>
        `;
        resBox.appendChild(item);
      });
    } catch (e) {
      resBox.innerHTML = '<div style="font-size:12.5px;color:#ef4444;padding:8px 0;">Error searching: ' + esc(e.message) + '</div>';
    }
  }

  // Drawer Interactions
  function openDrawer(app) {
    currentDrawerApp = app;
    document.getElementById('drawer-title').textContent = app.app_name;
    document.getElementById('drawer-dev').textContent = app.developer || 'Unknown Publisher';
    document.getElementById('drawer-icon').src = app.icon_url || 'data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22/>';

    document.getElementById('dpill-region').textContent = 'Region: ' + (app.region || 'Global');
    document.getElementById('dpill-plat').textContent = app.platform;
    document.getElementById('dpill-price').textContent = app.price || 'Free';
    document.getElementById('dpill-rating').textContent = app.rating ? `${app.rating} ★ (${formatCount(app.reviews_count)})` : 'No ratings';

    const vPill = document.getElementById('dpill-version');
    if (app.version) { vPill.textContent = 'v' + app.version; vPill.style.display = 'inline-block'; } else { vPill.style.display = 'none'; }

    const osPill = document.getElementById('dpill-os');
    if (app.min_os) { osPill.textContent = app.min_os; osPill.style.display = 'inline-block'; } else { osPill.style.display = 'none'; }

    const devExplore = document.getElementById('drawer-dev-explore');
    devExplore.style.display = (app.developer_id || app.developer) ? 'inline' : 'none';

    const storeUrl = app.store_url || `https://apps.apple.com/app/id${app.app_id}`;
    document.getElementById('drawer-qr').src = `https://api.qrserver.com/v1/create-qr-code/?size=140x140&margin=4&data=${encodeURIComponent(storeUrl)}`;
    document.getElementById('drawer-store-btn').href = storeUrl;

    const deepBtn = document.getElementById('drawer-deeplink-btn');
    if (app.deep_link) {
      deepBtn.href = app.deep_link;
      deepBtn.style.display = 'inline-flex';
    } else {
      deepBtn.style.display = 'none';
    }

    document.getElementById('drawer').classList.add('open');
    document.getElementById('drawer-overlay').classList.add('open');
  }

  function closeDrawer() {
    document.getElementById('drawer').classList.remove('open');
    document.getElementById('drawer-overlay').classList.remove('open');
    currentDrawerApp = null;
  }

  function copyDrawerId() {
    if (!currentDrawerApp) return;
    navigator.clipboard.writeText(currentDrawerApp.app_id).then(() => alert('Copied ID: ' + currentDrawerApp.app_id));
  }

  function copyDrawerUrl() {
    if (!currentDrawerApp) return;
    navigator.clipboard.writeText(currentDrawerApp.store_url).then(() => alert('Copied Store URL!'));
  }

  async function exploreDevApps() {
    if (!currentDrawerApp) return;
    const devId = currentDrawerApp.developer_id || currentDrawerApp.developer;
    const plat = currentDrawerApp.platform || 'iOS';
    try {
      const resp = await fetch(`/developer/apps?developer_id=${encodeURIComponent(devId)}&platform=${plat}`);
      const data = await resp.json();
      if (data.apps && data.apps.length) {
        closeDrawer();
        const ids = data.apps.map(a => a.app_id).join('\\n');
        document.getElementById('quick-input').value = ids;
        setSearchSubnav('id');
        runSearch();
      } else {
        alert('No other apps found for this developer.');
      }
    } catch (e) {
      alert('Could not retrieve developer apps.');
    }
  }

  function copyForGoogleSheets() {
    if (!lastResults.length) return;
    const headers = ['Platform', 'Region', 'Status', 'App Name', 'ID / Package', 'Developer', 'Category', 'Rating', 'Reviews', 'Price', 'Store URL'];
    const rows = lastResults.map(r => [
      r.platform || '',
      r.region || 'Global',
      r.status || '',
      r.app_name || '',
      r.app_id || r.input || '',
      r.developer || '',
      r.category || '',
      r.rating || '',
      r.reviews_count || '',
      r.price || '',
      r.store_url || ''
    ]);
    const tsv = [headers.join('\\t'), ...rows.map(row => row.join('\\t'))].join('\\n');
    navigator.clipboard.writeText(tsv).then(() => {
      alert('Copied table to clipboard! Paste directly into Google Sheets (Ctrl+V).');
    });
  }

  function downloadCsv() {
    if (!lastResults.length) return;
    const headers = ['Platform', 'Region', 'Status', 'App Name', 'ID', 'Developer', 'Category', 'Rating', 'Reviews', 'Price', 'Store URL'];
    const rows = lastResults.map(r => [
      `"${r.platform || ''}"`,
      `"${r.region || 'Global'}"`,
      `"${r.status || ''}"`,
      `"${(r.app_name || '').replace(/"/g, '""')}"`,
      `"${r.app_id || r.input || ''}"`,
      `"${(r.developer || '').replace(/"/g, '""')}"`,
      `"${r.category || ''}"`,
      `"${r.rating || ''}"`,
      `"${r.reviews_count || ''}"`,
      `"${r.price || ''}"`,
      `"${r.store_url || ''}"`
    ]);
    const csvContent = "data:text/csv;charset=utf-8," + [headers.join(','), ...rows.map(row => row.join(','))].join('\\n');
    const link = document.createElement('a');
    link.href = encodeURI(csvContent);
    link.download = `app_lookup_export_${Date.now()}.csv`;
    link.click();
  }

  function downloadJson() {
    if (!lastResults.length) return;
    const jsonStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(lastResults, null, 2));
    const link = document.createElement('a');
    link.href = jsonStr;
    link.download = `app_lookup_export_${Date.now()}.json`;
    link.click();
  }

  function formatCount(num) {
    if (!num) return '0';
    if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
    return String(num);
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
