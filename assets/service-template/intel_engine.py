#!/usr/bin/env python3
"""
WorldMonitor v2 Advanced Intelligence Engine
Pulls from 17 real-time data sources, computes Country Instability Index,
detects anomalies, tracks history in PostgreSQL, and outputs intelligence briefs.

Usage:
    python3 intel_engine.py                    # Full JSON output
    python3 intel_engine.py --section          # Formatted 🌍 GLOBAL SITUATION
    python3 intel_engine.py --brief            # Full intelligence brief
    python3 intel_engine.py --store            # Pull + store snapshot to NeonDB
    python3 intel_engine.py --cii              # Just CII scores
    python3 intel_engine.py --alerts           # Just anomalies and convergence
    python3 intel_engine.py --polymarket       # Just prediction market odds
"""

import json
import os
import sys
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from datetime import datetime, timezone, timedelta
from typing import Optional, Any, Dict, List, Tuple

# ==================== CONFIGURATION ====================

TIMEOUT_PER_REQUEST = 12  # seconds
TIMEOUT_TOTAL = 45  # seconds
USER_AGENT = "WorldMonitor-v2/2.0"
NWS_ALERT_POINT = os.environ.get("WORLDMONITOR_NWS_POINT", "39.8283,-98.5795")

MONITORED_COUNTRIES = {
    'US': 'United States', 'RU': 'Russia', 'CN': 'China', 'UA': 'Ukraine',
    'IR': 'Iran', 'IL': 'Israel', 'TW': 'Taiwan', 'KP': 'North Korea',
    'SA': 'Saudi Arabia', 'TR': 'Turkey', 'PL': 'Poland', 'DE': 'Germany',
    'FR': 'France', 'GB': 'United Kingdom', 'IN': 'India', 'PK': 'Pakistan',
    'SY': 'Syria', 'YE': 'Yemen', 'MM': 'Myanmar', 'VE': 'Venezuela',
    'BR': 'Brazil', 'AE': 'UAE'
}

BASELINE_RISK = {
    'US': 5, 'RU': 35, 'CN': 25, 'UA': 50, 'IR': 40, 'IL': 45,
    'TW': 30, 'KP': 45, 'SA': 20, 'TR': 25, 'PL': 10, 'DE': 5,
    'FR': 10, 'GB': 5, 'IN': 20, 'PK': 35, 'SY': 50, 'YE': 50,
    'MM': 45, 'VE': 40, 'BR': 15, 'AE': 10
}

COUNTRY_KEYWORDS = {
    'US': ['united states', 'usa', 'america', 'washington', 'biden', 'trump', 'pentagon'],
    'RU': ['russia', 'moscow', 'kremlin', 'putin'],
    'CN': ['china', 'beijing', 'xi jinping', 'prc'],
    'UA': ['ukraine', 'kyiv', 'zelensky', 'donbas'],
    'IR': ['iran', 'tehran', 'khamenei', 'irgc'],
    'IL': ['israel', 'tel aviv', 'netanyahu', 'idf', 'gaza'],
    'TW': ['taiwan', 'taipei'],
    'KP': ['north korea', 'pyongyang', 'kim jong'],
    'SA': ['saudi arabia', 'riyadh', 'mbs'],
    'TR': ['turkey', 'ankara', 'erdogan'],
    'PL': ['poland', 'warsaw'],
    'DE': ['germany', 'berlin'],
    'FR': ['france', 'paris', 'macron'],
    'GB': ['britain', 'uk', 'london', 'starmer'],
    'IN': ['india', 'delhi', 'modi'],
    'PK': ['pakistan', 'islamabad'],
    'SY': ['syria', 'damascus', 'assad'],
    'YE': ['yemen', 'sanaa', 'houthi'],
    'MM': ['myanmar', 'burma', 'rangoon'],
    'VE': ['venezuela', 'caracas', 'maduro'],
    'BR': ['brazil', 'brasilia', 'lula', 'bolsonaro'],
    'AE': ['uae', 'emirates', 'dubai', 'abu dhabi'],
}

# ==================== HTTP FETCHING ====================

def fetch_json(url: str, name: str) -> Optional[Any]:
    """Fetch JSON from URL with timeout and error handling."""
    try:
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=TIMEOUT_PER_REQUEST) as resp:
            return json.loads(resp.read())
    except Exception as e:
        print(f"  ⚠️ {name} failed: {e}", file=sys.stderr)
        return None


# ==================== DATA SOURCE FETCHERS ====================

def fetch_earthquakes() -> List[Dict[str, Any]]:
    """USGS M4.5+ earthquakes in last 24h."""
    data = fetch_json(
        "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson",
        "USGS Earthquakes"
    )
    if not data:
        return []
    quakes = []
    for f in data.get("features", []):
        p = f.get("properties", {})
        coords = f.get("geometry", {}).get("coordinates", [0, 0, 0])
        quakes.append({
            "magnitude": p.get("mag"),
            "place": p.get("place"),
            "time": datetime.fromtimestamp(p.get("time", 0) / 1000, tz=timezone.utc).isoformat(),
            "depth_km": round(coords[2], 1) if len(coords) > 2 else None,
            "lat": coords[1] if len(coords) > 1 else 0,
            "lon": coords[0] if len(coords) > 0 else 0,
            "alert": p.get("alert"),
            "tsunami": p.get("tsunami", 0),
        })
    quakes.sort(key=lambda q: q.get("magnitude", 0) or 0, reverse=True)
    return quakes


def fetch_gdacs_disasters() -> List[Dict[str, Any]]:
    """GDACS orange/red disasters (last 7 days)."""
    data = fetch_json(
        "https://www.gdacs.org/gdacsapi/api/events/geteventlist/SEARCH"
        "?alertlevel=orange;red&eventlist=EQ;TC;FL;VO;DR&limit=50",
        "GDACS Disasters"
    )
    if not data:
        return []
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    events = []
    for feature in data.get("features", []):
        p = feature.get("properties", {})
        date_str = p.get("fromdate") or p.get("todate", "")
        if date_str:
            try:
                event_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                if event_date < cutoff:
                    continue
            except (ValueError, TypeError):
                pass
        events.append({
            "type": p.get("eventtype"),
            "name": p.get("name") or p.get("eventname"),
            "country": p.get("country"),
            "alert_level": p.get("alertlevel"),
            "date": p.get("fromdate"),
        })
    # Dedup by type+country
    seen = {}
    for e in events:
        key = f"{e.get('type')}|{e.get('country','')}"
        if key not in seen or e.get("alert_level") == "Red":
            seen[key] = e
    events = list(seen.values())
    events.sort(key=lambda e: (0 if e.get("alert_level") == "Red" else 1, e.get("date", "") or ""))
    return events[:10]


def fetch_nasa_events() -> List[Dict[str, Any]]:
    """NASA EONET active natural events."""
    data = fetch_json("https://eonet.gsfc.nasa.gov/api/v3/events?status=open&limit=20", "NASA EONET")
    if not data:
        return []
    events = []
    for e in data.get("events", []):
        categories = [c.get("title") for c in e.get("categories", [])]
        events.append({
            "title": e.get("title"),
            "categories": categories,
            "date": e.get("geometry", [{}])[-1].get("date") if e.get("geometry") else None,
        })
    return events


def fetch_crypto() -> Dict[str, Any]:
    """CoinGecko crypto prices with 24h change."""
    data = fetch_json(
        "https://api.coingecko.com/api/v3/simple/price"
        "?ids=bitcoin,ethereum,solana&vs_currencies=usd&include_24hr_change=true&include_market_cap=true",
        "CoinGecko"
    )
    if not data:
        return {}
    result = {}
    name_map = {"bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL"}
    for coin_id, symbol in name_map.items():
        if coin_id in data:
            d = data[coin_id]
            result[symbol] = {
                "price": d.get("usd"),
                "change_24h": round(d.get("usd_24h_change", 0), 2),
                "market_cap": d.get("usd_market_cap"),
            }
    return result


def fetch_fear_greed() -> Dict[str, Any]:
    """Crypto Fear & Greed Index."""
    data = fetch_json("https://api.alternative.me/fng/?limit=2", "Fear & Greed")
    if not data or not data.get("data"):
        return {}
    today = data["data"][0]
    yesterday = data["data"][1] if len(data["data"]) > 1 else today
    return {
        "value": int(today.get("value", 0)),
        "label": today.get("value_classification", "Unknown"),
        "timestamp": today.get("timestamp"),
        "prev_value": int(yesterday.get("value", 0)),
        "change": int(today.get("value", 0)) - int(yesterday.get("value", 0)),
    }


def fetch_polymarket() -> List[Dict[str, Any]]:
    """Polymarket prediction markets (top by volume, geopolitical only)."""
    data = fetch_json(
        "https://gamma-api.polymarket.com/events?closed=false&order=volume&ascending=false&limit=20",
        "Polymarket"
    )
    if not data:
        return []
    markets = []
    for event in data:
        vol = event.get("volume") or event.get("volumeNum") or 0
        try:
            vol = float(vol)
        except (ValueError, TypeError):
            vol = 0
        if vol < 50000:
            continue
        title = event.get("title", "")
        # Filter out sports/entertainment
        if any(x in title.lower() for x in ["nfl", "nba", "mlb", "nhl", "ufc", "boxing", "award", "grammy", "oscar",
                                              "premier league", "champions league", "la liga", "serie a", "bundesliga",
                                              "world cup", "copa", "europa league", "mls", "liga mx", "ligue 1",
                                              "cricket", "rugby", "tennis", "golf", "f1", "formula",
                                              "super bowl", "playoffs", "march madness", "college"]):
            continue
        # Pick first market from event
        event_markets = event.get("markets", [])
        if not event_markets:
            continue
        market = event_markets[0]
        question = market.get("question", title)
        prices_str = market.get("outcomePrices", "[]")
        try:
            prices = json.loads(prices_str)
            yes_prob = round(float(prices[0]) * 100, 1) if prices else 50.0
        except:
            yes_prob = 50.0
        markets.append({
            "question": question,
            "yes_prob": yes_prob,
            "volume": vol,  # Already in USD
            "slug": event.get("slug", ""),
            "url": f"https://polymarket.com/event/{event.get('slug', '')}",
        })
    markets.sort(key=lambda m: m["volume"], reverse=True)
    return markets[:10]


def fetch_nws_alerts() -> List[Dict[str, Any]]:
    """NWS active alerts for configured point (WORLDMONITOR_NWS_POINT)."""
    data = fetch_json(f"https://api.weather.gov/alerts/active?point={NWS_ALERT_POINT}", "NWS Alerts")
    if not data:
        return []
    alerts = []
    for f in data.get("features", []):
        p = f.get("properties", {})
        alerts.append({
            "event": p.get("event"),
            "severity": p.get("severity"),
            "headline": p.get("headline"),
            "description": p.get("description", "")[:200],
        })
    return alerts[:5]


def fetch_ucdp_conflicts() -> List[Dict[str, Any]]:
    """UCDP armed conflict events (latest year)."""
    # UCDP API is slow, use a longer timeout
    try:
        req = urllib.request.Request(
            "https://ucdpapi.pcr.uu.se/api/gedevents/24.1?pagesize=50&page=0",
            headers={"User-Agent": USER_AGENT}
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        print(f"  ⚠️ UCDP Conflicts failed: {e}", file=sys.stderr)
        data = None
    # data = fetch_json("https://ucdpapi.pcr.uu.se/api/gedevents/24.1?pagesize=100&page=0", "UCDP Conflicts")
    if not data:
        return []
    events = []
    for e in data.get("Result", []):
        events.append({
            "country": e.get("country"),
            "event_date": e.get("date_start"),
            "type": e.get("type_of_violence"),
            "deaths_low": e.get("best_est", 0),
            "side_a": e.get("side_a"),
            "side_b": e.get("side_b"),
        })
    return events[:50]


def fetch_fed_funds_rate() -> Optional[float]:
    """Federal funds effective rate via FRED (free, no key needed via alternative)."""
    # Try Treasury.gov yield data as proxy (no API key)
    data = fetch_json(
        "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/accounting/od/avg_interest_rates?sort=-record_date&page[size]=1",
        "Fed Funds Rate"
    )
    if data and data.get("data"):
        try:
            return float(data["data"][0].get("avg_interest_rate_amt", 0))
        except (ValueError, IndexError):
            pass
    return None


def fetch_usa_spending() -> Dict[str, Any]:
    """USASpending agency spending totals (public, no auth)."""
    data = fetch_json(
        "https://api.usaspending.gov/api/v2/references/toptier_agencies/",
        "USASpending"
    )
    if not data or not data.get("results"):
        return {}
    # Find DOD
    for agency in data["results"]:
        name = agency.get("agency_name", "")
        if "Defense" in name:
            return {
                "agency": name,
                "budget_authority": agency.get("budget_authority_amount", 0),
                "obligated": agency.get("obligated_amount", 0),
                "fiscal_year": agency.get("current_total_budget_authority_amount", 0),
            }
    return {}


def fetch_open_meteo_climate() -> Dict[str, Any]:
    """Open-Meteo climate API (sample location)."""
    # Climate API requires specific coords; we'll use a sample
    data = fetch_json(
        "https://climate-api.open-meteo.com/v1/climate?latitude=35&longitude=-80&start_date=2024-01-01&end_date=2024-12-31&models=EC_Earth3P_HR",
        "Open-Meteo Climate"
    )
    if not data:
        return {}
    return {"model": "EC_Earth3P_HR", "status": "available"}


def fetch_gdelt_gkg() -> List[Dict[str, Any]]:
    """GDELT GKG 24h global articles (geopolitical focus)."""
    # Use a broader query and shorter timespan to avoid 429s
    data = fetch_json(
        "https://api.gdeltproject.org/api/v2/doc/doc?query=military+OR+conflict+OR+crisis&mode=ArtList&maxrecords=30&timespan=12h&format=json",
        "GDELT GKG"
    )
    if not data:
        return []
    articles = []
    for article in data.get("articles", []):
        articles.append({
            "title": article.get("title"),
            "url": article.get("url"),
            "domain": article.get("domain"),
            "seendate": article.get("seendate"),
        })
    return articles[:20]


def fetch_unhcr_displacement() -> Dict[str, Any]:
    """UNHCR global displacement data (refugees, asylum seekers, IDPs, stateless).

    NOTE: The UNHCR endpoint returns *global totals* as a single record when called normally.
    When called with `coo_all=true`, it returns per-origin breakdowns (paginated).
    We do BOTH so totals are accurate.
    """

    YEAR = 2023

    def _safe_int(v):
        try:
            # UNHCR sometimes returns numbers as strings
            return int(float(v)) if v not in (None, "") else 0
        except (ValueError, TypeError):
            return 0

    # 1) Global totals (single record)
    totals_resp = fetch_json(
        f"https://api.unhcr.org/population/v1/population/?year={YEAR}&limit=1&page=1",
        "UNHCR Displacement (totals)"
    )
    totals_item = (totals_resp or {}).get("items", [{}])[0] if totals_resp else {}
    totals = {
        "refugees": _safe_int(totals_item.get("refugees")),
        "asylum_seekers": _safe_int(totals_item.get("asylum_seekers")),
        "idps": _safe_int(totals_item.get("idps")),
        "stateless": _safe_int(totals_item.get("stateless")),
    }

    # 2) Per-origin breakdown (top origins)
    by_origin: Dict[str, int] = {}
    origin_pages = 0
    max_pages = 1

    # Keep runtime bounded: cap pages + total rows
    LIMIT = 500
    PAGE_CAP = 10
    ROW_CAP = 5000

    for page in range(1, PAGE_CAP + 1):
        url = f"https://api.unhcr.org/population/v1/population/?year={YEAR}&limit={LIMIT}&page={page}&coo_all=true"
        resp = fetch_json(url, f"UNHCR Displacement (origins p{page})")
        if not resp or not resp.get("items"):
            break
        origin_pages += 1
        max_pages = int(resp.get("maxPages") or max_pages)

        for item in resp.get("items", []):
            origin = item.get("coo_name") or "Unknown"
            displaced = (
                _safe_int(item.get("refugees")) +
                _safe_int(item.get("asylum_seekers")) +
                _safe_int(item.get("idps")) +
                _safe_int(item.get("stateless"))
            )
            by_origin[origin] = by_origin.get(origin, 0) + displaced

        if page >= max_pages:
            break
        if sum(by_origin.values()) > ROW_CAP:
            break

    top_origins = sorted(by_origin.items(), key=lambda x: x[1], reverse=True)[:5]
    grand_total = totals["refugees"] + totals["asylum_seekers"] + totals["idps"] + totals["stateless"]

    return {
        "year": YEAR,
        "totals": totals,
        "grand_total": grand_total,
        "top_origins": [{"country": c, "displaced": d} for c, d in top_origins],
        "origin_pages_fetched": origin_pages,
        "origin_max_pages": max_pages,
    }


def fetch_feodo_tracker() -> Dict[str, Any]:
    """Feodo Tracker — active C2/botnet servers."""
    data = fetch_json(
        "https://feodotracker.abuse.ch/downloads/ipblocklist.json",
        "Feodo Tracker"
    )
    if not data:
        return {}
    entries = data if isinstance(data, list) else data.get("data", data.get("entries", []))
    if not isinstance(entries, list):
        return {}
    online = [e for e in entries if (e.get("status") or "").lower() == "online"]
    # Count by country
    by_country = {}
    for e in online:
        c = e.get("country", "Unknown") or "Unknown"
        by_country[c] = by_country.get(c, 0) + 1
    top_countries = sorted(by_country.items(), key=lambda x: x[1], reverse=True)[:3]
    # Count by malware family
    by_malware = {}
    for e in online:
        m = e.get("malware", "Unknown") or "Unknown"
        by_malware[m] = by_malware.get(m, 0) + 1
    return {
        "active_c2": len(online),
        "total_listed": len(entries),
        "top_countries": [{"country": c, "count": n} for c, n in top_countries],
        "by_malware": by_malware,
    }


def fetch_opensky_conflict_zones() -> Dict[str, Any]:
    """OpenSky Network — aircraft counts in conflict zone bounding boxes."""
    zones = {
        "Ukraine": {"lamin": 44, "lomin": 22, "lamax": 53, "lomax": 40},
        "Taiwan Strait": {"lamin": 21, "lomin": 115, "lamax": 27, "lomax": 125},
        "Middle East": {"lamin": 25, "lomin": 34, "lamax": 40, "lomax": 55},
    }
    results = {}
    for zone_name, bbox in zones.items():
        url = (
            f"https://opensky-network.org/api/states/all"
            f"?lamin={bbox['lamin']}&lomin={bbox['lomin']}"
            f"&lamax={bbox['lamax']}&lomax={bbox['lomax']}"
        )
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=TIMEOUT_PER_REQUEST) as resp:
                data = json.loads(resp.read())
            states = data.get("states", []) or []
            results[zone_name] = len(states)
        except Exception as e:
            print(f"  ⚠️ OpenSky {zone_name} failed: {e}", file=sys.stderr)
            results[zone_name] = None  # None = failed, 0 = no aircraft
    return results


def fetch_eia_oil_prices() -> Dict[str, Any]:
    """EIA crude oil spot prices (WTI + Brent)."""
    url = (
        "https://api.eia.gov/v2/petroleum/pri/spt/data/"
        "?api_key=DEMO_KEY&frequency=daily&data[0]=value"
        "&facets[product][]=EPCBRENT&facets[product][]=EPCWTI"
        "&sort[0][column]=period&sort[0][direction]=desc&length=2"
    )
    data = fetch_json(url, "EIA Oil Prices")
    result = {"wti": None, "brent": None}
    if data and data.get("response", {}).get("data"):
        for row in data["response"]["data"]:
            product = row.get("product", "")
            value = row.get("value")
            try:
                value = float(value)
            except (ValueError, TypeError):
                continue
            if product == "EPCWTI" and result["wti"] is None:
                result["wti"] = round(value, 2)
            elif product == "EPCBRENT" and result["brent"] is None:
                result["brent"] = round(value, 2)
    return result


def fetch_worldbank_gdp() -> Dict[str, Any]:
    """World Bank GDP growth rates for CII countries."""
    # Map our 2-letter codes to World Bank 3-letter codes
    wb_codes = {
        'US': 'USA', 'RU': 'RUS', 'CN': 'CHN', 'UA': 'UKR', 'IR': 'IRN', 'IL': 'ISR',
        'TW': 'TWN', 'KP': 'PRK', 'SA': 'SAU', 'TR': 'TUR', 'PL': 'POL', 'DE': 'DEU',
        'FR': 'FRA', 'GB': 'GBR', 'IN': 'IND', 'PK': 'PAK', 'SY': 'SYR', 'YE': 'YEM',
        'MM': 'MMR', 'VE': 'VEN', 'BR': 'BRA', 'AE': 'ARE'
    }
    wb_to_iso2 = {v: k for k, v in wb_codes.items()}
    # Request only our specific countries (semicolon-separated 3-letter codes)
    country_str = ";".join(wb_codes.values())
    data = fetch_json(
        f"https://api.worldbank.org/v2/country/{country_str}/indicator/NY.GDP.MKTP.KD.ZG"
        "?format=json&per_page=200&date=2020:2024",
        "World Bank GDP"
    )
    if not data or not isinstance(data, list) or len(data) < 2:
        return {}
    # data[0] is pagination info, data[1] is the actual data
    entries = data[1] if len(data) > 1 else []
    if not isinstance(entries, list):
        return {}
    gdp_by_country = {}  # iso2 -> {"growth": float, "year": int}
    for entry in entries:
        if not entry or entry.get("value") is None:
            continue
        country_code_3 = entry.get("countryiso3code", "") or entry.get("country", {}).get("id", "")
        iso2 = wb_to_iso2.get(country_code_3)
        if not iso2:
            continue
        year = int(entry.get("date", "0"))
        growth = float(entry.get("value"))
        # Keep the most recent year with data
        if iso2 not in gdp_by_country or year > gdp_by_country[iso2]["year"]:
            gdp_by_country[iso2] = {"growth": round(growth, 2), "year": year}
    return gdp_by_country


# ==================== COUNTRY INSTABILITY INDEX (CII) ====================

def compute_cii(
    earthquakes: List[Dict],
    disasters: List[Dict],
    ucdp: List[Dict],
    gdelt: List[Dict],
    prev_scores: Dict[str, int],
    gdp_data: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """Compute Country Instability Index for all monitored countries."""
    country_data = {code: {"unrest": 0, "conflict": 0, "security": 0, "information": 0}
                    for code in MONITORED_COUNTRIES}

    # Unrest: GDELT article mentions as proxy for protests/news velocity
    for article in gdelt:
        title = article.get("title", "").lower()
        for code, keywords in COUNTRY_KEYWORDS.items():
            if any(kw in title for kw in keywords):
                country_data[code]["unrest"] += 5

    # Conflict: earthquakes near countries, UCDP events, GDACS disasters
    for quake in earthquakes:
        # Simplified geo-matching
        if quake["magnitude"] and quake["magnitude"] >= 5.0:
            place = quake.get("place", "").lower()
            for code, name in MONITORED_COUNTRIES.items():
                if name.lower() in place or any(kw in place for kw in COUNTRY_KEYWORDS.get(code, [])):
                    country_data[code]["conflict"] += 10

    for event in ucdp:
        country = event.get("country", "")
        for code, name in MONITORED_COUNTRIES.items():
            if name.lower() in country.lower():
                country_data[code]["conflict"] += 15
                break

    for disaster in disasters:
        country = disaster.get("country", "")
        for code, name in MONITORED_COUNTRIES.items():
            if name.lower() in country.lower():
                boost = 20 if disaster.get("alert_level") == "Red" else 10
                country_data[code]["conflict"] += boost
                break

    # Security: military activity mentions in GDELT
    for article in gdelt:
        title = article.get("title", "").lower()
        if any(x in title for x in ["military", "troops", "forces", "navy", "army", "strike"]):
            for code, keywords in COUNTRY_KEYWORDS.items():
                if any(kw in title for kw in keywords):
                    country_data[code]["security"] += 8

    # Information: news velocity from GDELT
    for article in gdelt:
        title = article.get("title", "").lower()
        for code, keywords in COUNTRY_KEYWORDS.items():
            if any(kw in title for kw in keywords):
                country_data[code]["information"] += 3

    # Economic: World Bank GDP growth (negative growth = instability boost)
    if gdp_data:
        for code in MONITORED_COUNTRIES:
            entry = gdp_data.get(code)
            if entry and entry.get("growth") is not None:
                if entry["growth"] < 0:
                    country_data[code]["unrest"] += 5  # negative GDP -> +5 instability

    # Compute scores
    scores = []
    for code, name in MONITORED_COUNTRIES.items():
        components = country_data[code]
        # Clamp components to 0-100
        for k in components:
            components[k] = min(100, components[k])

        baseline = BASELINE_RISK.get(code, 20)
        event_score = (
            components["unrest"] * 0.3 +
            components["conflict"] * 0.3 +
            components["security"] * 0.25 +
            components["information"] * 0.15
        )
        score = int(baseline + event_score * 0.6)
        score = max(0, min(100, score))

        # Level thresholds
        if score >= 80:
            level = "critical"
        elif score >= 60:
            level = "high"
        elif score >= 40:
            level = "elevated"
        elif score >= 20:
            level = "normal"
        else:
            level = "low"

        # Trend arrows
        prev = prev_scores.get(code, score)
        if score - prev >= 3:
            trend = "↑"
        elif prev - score >= 3:
            trend = "↓"
        else:
            trend = "→"

        scores.append({
            "code": code,
            "name": name,
            "score": score,
            "level": level,
            "trend": trend,
            "change": score - prev,
            "components": components,
        })

    scores.sort(key=lambda s: s["score"], reverse=True)
    return scores


# ==================== CONVERGENCE DETECTION ====================

def detect_convergence(
    earthquakes: List[Dict],
    disasters: List[Dict],
    ucdp: List[Dict],
    gdelt: List[Dict]
) -> List[str]:
    """Detect convergence: 3+ signal types active in same region."""
    convergence = []

    for code, name in MONITORED_COUNTRIES.items():
        signals = []

        # Check earthquake
        for quake in earthquakes:
            place = quake.get("place", "").lower()
            if name.lower() in place or any(kw in place for kw in COUNTRY_KEYWORDS.get(code, [])):
                signals.append("earthquake")
                break

        # Check disaster
        for disaster in disasters:
            country = disaster.get("country", "")
            if name.lower() in country.lower():
                signals.append("disaster")
                break

        # Check conflict
        for event in ucdp:
            country = event.get("country", "")
            if name.lower() in country.lower():
                signals.append("conflict")
                break

        # Check military news
        for article in gdelt:
            title = article.get("title", "").lower()
            if any(x in title for x in ["military", "strike", "troops"]) and any(kw in title for kw in COUNTRY_KEYWORDS.get(code, [])):
                signals.append("military")
                break

        if len(set(signals)) >= 3:
            convergence.append(f"{name}: {' + '.join(set(signals))}")

    return convergence


# ==================== ANOMALY DETECTION ====================

def detect_anomalies(
    current_data: Dict[str, Any],
    historical_avg: Dict[str, float]
) -> List[str]:
    """Compare current values against 7-day rolling average + stateless checks."""
    anomalies = []

    # Earthquake count anomaly (vs historical if available)
    eq_count = len(current_data.get("earthquakes", []))
    eq_avg = historical_avg.get("earthquake_count")
    if eq_avg and eq_avg > 0 and eq_count > eq_avg * 2:
        anomalies.append(f"Earthquake count {eq_count / eq_avg:.1f}x above weekly average ({eq_count} vs avg {eq_avg:.0f})")

    # Stateless: Major earthquake (M6.5+)
    for q in current_data.get("earthquakes", []):
        if (q.get("magnitude") or 0) >= 6.5:
            anomalies.append(f"Major earthquake M{q['magnitude']} — {q.get('place', 'Unknown')}")
            break  # Only flag the biggest

    # Stateless: Multiple red alerts
    red_count = sum(1 for d in current_data.get("disasters", []) if d.get("alert_level") == "Red")
    if red_count >= 3:
        anomalies.append(f"{red_count} simultaneous GDACS Red Alerts active")

    # Bitcoin price anomaly (stateless: >5% move)
    btc = current_data.get("crypto", {}).get("BTC", {})
    btc_change = btc.get("change_24h", 0)
    if abs(btc_change) > 5:
        direction = "crashed" if btc_change < 0 else "surged"
        anomalies.append(f"BTC {direction} {abs(btc_change):.1f}% in 24h")

    # Fear & Greed anomaly (stateless: extreme values or big shift)
    fng = current_data.get("fear_greed", {})
    fng_val = fng.get("value", 50)
    fng_change = fng.get("change", 0)
    if fng_val <= 10:
        anomalies.append(f"Fear & Greed at {fng_val} — Extreme Fear territory")
    elif fng_val >= 90:
        anomalies.append(f"Fear & Greed at {fng_val} — Extreme Greed territory")
    if abs(fng_change) > 15:
        anomalies.append(f"Fear & Greed shifted {abs(fng_change)} points in 24h")

    # Stateless: CII critical countries
    cii = current_data.get("cii_scores", [])
    critical = [s for s in cii if s.get("level") == "critical"]
    if critical:
        names = ", ".join(s["name"] for s in critical[:3])
        anomalies.append(f"CII Critical: {names}")

    return anomalies


# ==================== DATABASE OPERATIONS (OPTIONAL) ====================

def get_db_connection():
    """Get NeonDB connection from env or config file."""
    try:
        import asyncpg
    except ImportError:
        print("  ⚠️ asyncpg not installed, DB operations disabled", file=sys.stderr)
        return None

    db_url = os.environ.get("DATABASE_URL") or os.environ.get("NEON_DATABASE_URL")
    if not db_url:
        config_path = os.path.expanduser(os.environ.get("WORLDMONITOR_DB_ENV_FILE", "~/.config/worldmonitor/neon.env"))
        if os.path.exists(config_path):
            with open(config_path) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("NEON_DATABASE_URL=") or line.startswith("DATABASE_URL="):
                        db_url = line.split("=", 1)[1].strip()
                        break

    if not db_url:
        print("  ⚠️ DATABASE_URL not found, DB operations disabled", file=sys.stderr)
        return None

    return db_url


async def store_snapshot(intel: Dict[str, Any], cii_scores: List[Dict]):
    """Store snapshot to NeonDB."""
    import asyncpg

    db_url = get_db_connection()
    if not db_url:
        return

    conn = await asyncpg.connect(db_url)
    try:
        # Get yesterday's CII scores for comparison
        prev_scores = {}
        rows = await conn.fetch(
            "SELECT country_code, score FROM worldmonitor_cii_history WHERE snapshot_date = CURRENT_DATE - INTERVAL '1 day'"
        )
        for row in rows:
            prev_scores[row["country_code"]] = row["score"]

        # Insert snapshot
        earthquakes = intel.get("earthquakes", [])
        max_mag = max((e.get("magnitude", 0) or 0 for e in earthquakes), default=0)
        disasters = intel.get("disasters", [])
        red_alerts = sum(1 for d in disasters if d.get("alert_level") == "Red")
        crypto = intel.get("crypto", {})
        btc = crypto.get("BTC", {})
        fng = intel.get("fear_greed", {})
        polymarket = intel.get("polymarket", [])
        convergence = intel.get("convergence", [])
        anomalies = intel.get("anomalies", [])

        cii_critical = sum(1 for s in cii_scores if s["level"] == "critical")
        cii_high = sum(1 for s in cii_scores if s["level"] == "high")
        cii_top = cii_scores[0] if cii_scores else {"code": "XX", "score": 0}

        await conn.execute("""
            INSERT INTO worldmonitor_snapshots (
                earthquake_count, max_earthquake_mag, disaster_count, red_alert_count,
                nasa_event_count, btc_price, btc_change_24h, fear_greed_index, fear_greed_label,
                cii_top_country, cii_top_score, cii_critical_count, cii_high_count,
                polymarket_top_event, polymarket_top_probability,
                convergence_zones, anomaly_flags, total_signals
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
            ON CONFLICT (snapshot_date) DO UPDATE SET
                earthquake_count = EXCLUDED.earthquake_count,
                max_earthquake_mag = EXCLUDED.max_earthquake_mag,
                disaster_count = EXCLUDED.disaster_count,
                red_alert_count = EXCLUDED.red_alert_count,
                nasa_event_count = EXCLUDED.nasa_event_count,
                btc_price = EXCLUDED.btc_price,
                btc_change_24h = EXCLUDED.btc_change_24h,
                fear_greed_index = EXCLUDED.fear_greed_index,
                fear_greed_label = EXCLUDED.fear_greed_label,
                cii_top_country = EXCLUDED.cii_top_country,
                cii_top_score = EXCLUDED.cii_top_score,
                cii_critical_count = EXCLUDED.cii_critical_count,
                cii_high_count = EXCLUDED.cii_high_count,
                polymarket_top_event = EXCLUDED.polymarket_top_event,
                polymarket_top_probability = EXCLUDED.polymarket_top_probability,
                convergence_zones = EXCLUDED.convergence_zones,
                anomaly_flags = EXCLUDED.anomaly_flags,
                total_signals = EXCLUDED.total_signals
        """, len(earthquakes), max_mag, len(disasters), red_alerts,
             len(intel.get("nasa_events", [])), btc.get("price"), btc.get("change_24h"),
             fng.get("value"), fng.get("label"), cii_top["code"], cii_top["score"],
             cii_critical, cii_high,
             polymarket[0]["question"] if polymarket else None,
             polymarket[0]["yes_prob"] if polymarket else None,
             convergence, anomalies,
             len(earthquakes) + len(disasters) + len(intel.get("nasa_events", []))
        )

        # Insert CII history
        for score in cii_scores:
            await conn.execute("""
                INSERT INTO worldmonitor_cii_history (
                    country_code, country_name, score, level,
                    unrest_component, conflict_component, security_component, information_component
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (snapshot_date, country_code) DO UPDATE SET
                    score = EXCLUDED.score,
                    level = EXCLUDED.level,
                    unrest_component = EXCLUDED.unrest_component,
                    conflict_component = EXCLUDED.conflict_component,
                    security_component = EXCLUDED.security_component,
                    information_component = EXCLUDED.information_component
            """, score["code"], score["name"], score["score"], score["level"],
                 score["components"]["unrest"], score["components"]["conflict"],
                 score["components"]["security"], score["components"]["information"]
            )

        print("  ✓ Snapshot stored to NeonDB", file=sys.stderr)
    finally:
        await conn.close()


async def get_historical_avg() -> Dict[str, float]:
    """Get 7-day rolling averages from NeonDB."""
    import asyncpg

    db_url = get_db_connection()
    if not db_url:
        return {}

    conn = await asyncpg.connect(db_url)
    try:
        rows = await conn.fetch("""
            SELECT AVG(earthquake_count) as eq_avg, AVG(fear_greed_index) as fng_avg
            FROM worldmonitor_snapshots
            WHERE snapshot_date >= CURRENT_DATE - INTERVAL '7 days'
        """)
        if rows and rows[0]:
            return {
                "earthquake_count": float(rows[0]["eq_avg"] or 0),
                "fear_greed_index": float(rows[0]["fng_avg"] or 0),
            }
        return {}
    finally:
        await conn.close()


async def get_prev_cii_scores() -> Dict[str, int]:
    """Get yesterday's CII scores for trend arrows."""
    import asyncpg

    db_url = get_db_connection()
    if not db_url:
        return {}

    conn = await asyncpg.connect(db_url)
    try:
        rows = await conn.fetch("""
            SELECT country_code, score
            FROM worldmonitor_cii_history
            WHERE snapshot_date = CURRENT_DATE - INTERVAL '1 day'
        """)
        return {row["country_code"]: row["score"] for row in rows}
    finally:
        await conn.close()


# ==================== FORMATTING ====================

def format_section(intel: Dict[str, Any]) -> str:
    """Format 🌍 GLOBAL SITUATION section."""
    lines = ["🌍 GLOBAL SITUATION (WorldMonitor v2)", ""]

    # Earthquakes
    quakes = intel.get("earthquakes", [])
    sig_quakes = [q for q in quakes if (q.get("magnitude") or 0) >= 5.0]
    if sig_quakes:
        lines.append("🌋 Seismic Activity")
        for q in sig_quakes[:5]:
            mag = q.get("magnitude", "?")
            place = q.get("place", "Unknown")
            depth = q.get("depth_km")
            tsunami = " 🌊" if q.get("tsunami") else ""
            depth_str = f" ({depth}km deep)" if depth else ""
            lines.append(f"  • M{mag} — {place}{depth_str}{tsunami}")
    elif quakes:
        lines.append("🌋 Seismic Activity")
        lines.append(f"  • {len(quakes)} earthquakes M4.5+ in 24h, largest M{quakes[0].get('magnitude', '?')}")
    else:
        lines.append("🌋 Seismic: No significant activity")
    lines.append("")

    # GDACS Disasters
    disasters = intel.get("disasters", [])
    if disasters:
        lines.append("⚠️ Active Disasters (GDACS)")
        type_emoji = {"EQ": "🔴", "TC": "🌀", "FL": "🌊", "VO": "🌋", "DR": "☀️"}
        for d in disasters[:6]:
            emoji = type_emoji.get(d.get("type"), "🔶")
            name = d.get("name", "Unknown")
            country = d.get("country", "")[:50]
            alert_emoji = "🔴" if d.get("alert_level") == "Red" else "🟠"
            country_str = f" — {country}" if country else ""
            lines.append(f"  {emoji} {name}{country_str} {alert_emoji}")
        if len(disasters) > 6:
            lines.append(f"  ... +{len(disasters) - 6} more")
        lines.append("")

    # NASA Events
    nasa = intel.get("nasa_events", [])
    if nasa:
        lines.append("🛰️ NASA Active Events")
        for e in nasa[:4]:
            cats = ", ".join(e.get("categories", []))
            lines.append(f"  • {e.get('title', 'Unknown')} [{cats}]")
        if len(nasa) > 4:
            lines.append(f"  ... +{len(nasa) - 4} more")
        lines.append("")

    # NWS Weather Alerts (Configured Point)
    nws = intel.get("nws_alerts", [])
    if nws:
        lines.append("🌪️ Weather Alerts (Configured Point)")
        for alert in nws[:3]:
            severity = alert.get("severity", "")
            sev_emoji = "🔴" if severity == "Extreme" else "🟠" if severity == "Severe" else "🟡"
            lines.append(f"  {sev_emoji} {alert.get('event', 'Unknown')}: {alert.get('headline', '')[:80]}")
        lines.append("")

    # UNHCR Displacement
    unhcr = intel.get("unhcr_displacement", {})
    if unhcr and unhcr.get("grand_total"):
        lines.append("🚨 Displacement (UNHCR)")
        gt = unhcr["grand_total"]
        if gt >= 1_000_000:
            gt_str = f"{gt / 1_000_000:.1f}M"
        elif gt >= 1000:
            gt_str = f"{gt / 1000:.0f}K"
        else:
            gt_str = str(gt)
        totals = unhcr.get("totals", {})
        ref = totals.get("refugees", 0)
        idps = totals.get("idps", 0)
        ref_str = f"{ref / 1_000_000:.1f}M" if ref >= 1_000_000 else f"{ref / 1000:.0f}K" if ref >= 1000 else str(ref)
        idp_str = f"{idps / 1_000_000:.1f}M" if idps >= 1_000_000 else f"{idps / 1000:.0f}K" if idps >= 1000 else str(idps)
        lines.append(f"  • Global: {gt_str} displaced ({ref_str} refugees, {idp_str} IDPs)")
        top_origins = unhcr.get("top_origins", [])
        if top_origins:
            origin_parts = [f"{o['country']} ({o['displaced']:,})" for o in top_origins[:5]]
            lines.append(f"  • Top origins: {', '.join(origin_parts)}")
        lines.append("")

    # Feodo Tracker — Cyber Threats
    feodo = intel.get("feodo_tracker", {})
    if feodo and feodo.get("active_c2"):
        lines.append("🛡️ Cyber Threats (Feodo)")
        lines.append(f"  • Active C2 servers: {feodo['active_c2']} (of {feodo.get('total_listed', '?')} tracked)")
        top_c = feodo.get("top_countries", [])
        if top_c:
            c_parts = [f"{c['country']} ({c['count']})" for c in top_c]
            lines.append(f"  • Top hosting: {', '.join(c_parts)}")
        by_malware = feodo.get("by_malware", {})
        if by_malware:
            mal_parts = [f"{m}: {c}" for m, c in sorted(by_malware.items(), key=lambda x: x[1], reverse=True)[:3]]
            lines.append(f"  • Malware: {', '.join(mal_parts)}")
        lines.append("")

    # OpenSky — Air Activity in Conflict Zones
    opensky = intel.get("opensky_air", {})
    if opensky:
        has_data = any(v is not None for v in opensky.values())
        if has_data:
            lines.append("✈️ Air Activity (Conflict Zones)")
            for zone, count in opensky.items():
                if count is not None:
                    lines.append(f"  • {zone}: {count} aircraft tracked")
                else:
                    lines.append(f"  • {zone}: unavailable")
            lines.append("")

    # Prediction Markets
    polymarket = intel.get("polymarket", [])
    if polymarket:
        lines.append("🎰 Prediction Markets (Polymarket)")
        for m in polymarket[:3]:
            q = m["question"][:70]
            prob = m["yes_prob"]
            vol = m["volume"]
            if vol >= 1_000_000:
                vol_str = f"${vol / 1_000_000:.1f}M"
            elif vol >= 1000:
                vol_str = f"${vol / 1000:.0f}K"
            else:
                vol_str = f"${vol:.0f}"
            lines.append(f"  • \"{q}\" — {prob}% YES ({vol_str} volume)")
        lines.append("")

    # Country Instability Index
    cii = intel.get("cii_scores", [])
    if cii:
        lines.append("🏴 Country Instability Index")
        top5 = cii[:5]
        for s in top5:
            emoji = "🔴" if s["level"] == "critical" else "🟠" if s["level"] == "high" else "🟡"
            lines.append(f"  {emoji} {s['code']}: {s['score']} ({s['level'].title()} {s['trend']})")
        lines.append("")

    # Convergence
    convergence = intel.get("convergence", [])
    if convergence:
        lines.append("⚡ Convergence Alerts")
        for c in convergence:
            lines.append(f"  • {c}")
        lines.append("")

    # Anomalies
    anomalies = intel.get("anomalies", [])
    if anomalies:
        lines.append("📊 Anomalies")
        for a in anomalies:
            lines.append(f"  • {a}")
        lines.append("")

    # Crypto
    crypto = intel.get("crypto", {})
    fng = intel.get("fear_greed", {})
    if crypto:
        lines.append("🪙 Crypto Radar")
        parts = []
        for symbol in ["BTC", "ETH", "SOL"]:
            if symbol in crypto:
                c = crypto[symbol]
                price = c.get("price", 0)
                change = c.get("change_24h", 0)
                arrow = "↑" if change >= 0 else "↓"
                price_str = f"${price:,.0f}" if price >= 1000 else f"${price:,.2f}"
                parts.append(f"{symbol}: {price_str} ({arrow}{abs(change):.1f}%)")
        lines.append(f"  • {' | '.join(parts[:2])}")
        if len(parts) > 2:
            lines.append(f"  • {parts[2]}")
        if fng:
            lines.append(f"  • Fear & Greed: {fng.get('value', '?')} ({fng.get('label', '?')})")
        # Oil prices
        oil = intel.get("oil_prices", {})
        if oil and (oil.get("wti") or oil.get("brent")):
            oil_parts = []
            if oil.get("wti"):
                oil_parts.append(f"WTI ${oil['wti']:.2f}")
            if oil.get("brent"):
                oil_parts.append(f"Brent ${oil['brent']:.2f}")
            lines.append(f"  • 🛢️ Oil: {' | '.join(oil_parts)}")

    return "\n".join(lines)


def format_brief(intel: Dict[str, Any]) -> str:
    """Full intelligence brief."""
    now = datetime.now(timezone.utc)
    lines = [
        "🌐 WORLDMONITOR v2 INTELLIGENCE BRIEF",
        f"📅 {now.strftime('%A, %B %d, %Y')} | {now.strftime('%H:%M')} UTC",
        "━" * 50,
        "",
    ]
    lines.append(format_section(intel))
    lines.append("")
    lines.append("━" * 50)
    lines.append("Sources: USGS, GDACS, NASA EONET, CoinGecko, Alternative.me, Polymarket, UCDP, GDELT, UNHCR, Feodo, OpenSky, EIA, World Bank")
    lines.append("Dashboard: worldmonitor.app")
    return "\n".join(lines)


# ==================== MAIN ====================

def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "--json"

    print("WorldMonitor v2 Intelligence Engine — pulling from 17 sources...", file=sys.stderr)

    # Parallel fetch all sources
    with ThreadPoolExecutor(max_workers=17) as executor:
        futures = {
            executor.submit(fetch_earthquakes): "earthquakes",
            executor.submit(fetch_gdacs_disasters): "disasters",
            executor.submit(fetch_nasa_events): "nasa_events",
            executor.submit(fetch_crypto): "crypto",
            executor.submit(fetch_fear_greed): "fear_greed",
            executor.submit(fetch_polymarket): "polymarket",
            executor.submit(fetch_nws_alerts): "nws_alerts",
            executor.submit(fetch_ucdp_conflicts): "ucdp_conflicts",
            executor.submit(fetch_fed_funds_rate): "fed_funds_rate",
            executor.submit(fetch_usa_spending): "usa_spending",
            executor.submit(fetch_open_meteo_climate): "climate",
            executor.submit(fetch_gdelt_gkg): "gdelt_gkg",
            executor.submit(fetch_unhcr_displacement): "unhcr_displacement",
            executor.submit(fetch_feodo_tracker): "feodo_tracker",
            executor.submit(fetch_opensky_conflict_zones): "opensky_air",
            executor.submit(fetch_eia_oil_prices): "oil_prices",
            executor.submit(fetch_worldbank_gdp): "worldbank_gdp",
        }

        intel = {"timestamp": datetime.now(timezone.utc).isoformat()}
        for future in as_completed(futures, timeout=TIMEOUT_TOTAL):
            name = futures[future]
            try:
                intel[name] = future.result()
            except Exception as e:
                print(f"  ⚠️ {name} exception: {e}", file=sys.stderr)
                intel[name] = [] if name.endswith("s") else {}

    # Get previous CII scores for trend (try DB always)
    prev_scores = {}
    try:
        import asyncio
        prev_scores = asyncio.run(get_prev_cii_scores())
    except Exception:
        pass  # No DB or no history yet — trends will show →

    # Compute CII
    cii_scores = compute_cii(
        intel.get("earthquakes", []),
        intel.get("disasters", []),
        intel.get("ucdp_conflicts", []),
        intel.get("gdelt_gkg", []),
        prev_scores,
        intel.get("worldbank_gdp", {})
    )
    intel["cii_scores"] = cii_scores

    # Detect convergence
    convergence = detect_convergence(
        intel.get("earthquakes", []),
        intel.get("disasters", []),
        intel.get("ucdp_conflicts", []),
        intel.get("gdelt_gkg", [])
    )
    intel["convergence"] = convergence

    # Detect anomalies (try DB first, fall back to stateless detection)
    historical_avg = {}
    try:
        import asyncio
        historical_avg = asyncio.run(get_historical_avg())
    except Exception:
        pass
    anomalies = detect_anomalies(intel, historical_avg)
    intel["anomalies"] = anomalies

    # Store to DB if requested
    if mode == "--store":
        import asyncio
        asyncio.run(store_snapshot(intel, cii_scores))

    # Output
    counts = {
        "earthquakes": len(intel["earthquakes"]),
        "disasters": len(intel["disasters"]),
        "nasa_events": len(intel["nasa_events"]),
        "polymarket": len(intel["polymarket"]),
        "ucdp_conflicts": len(intel["ucdp_conflicts"]),
        "gdelt_articles": len(intel["gdelt_gkg"]),
        "unhcr": bool(intel.get("unhcr_displacement")),
        "feodo_c2": intel.get("feodo_tracker", {}).get("active_c2", 0),
        "opensky_zones": sum(1 for v in intel.get("opensky_air", {}).values() if v is not None),
        "oil": bool(intel.get("oil_prices", {}).get("wti") or intel.get("oil_prices", {}).get("brent")),
        "gdp_countries": len(intel.get("worldbank_gdp", {})),
    }
    print(f"✓ Collected: {counts}", file=sys.stderr)

    if mode == "--brief":
        print(format_brief(intel))
    elif mode == "--section":
        print(format_section(intel))
    elif mode == "--cii":
        for s in cii_scores:
            print(f"{s['code']}: {s['score']} ({s['level']}) {s['trend']}")
    elif mode == "--alerts":
        if convergence:
            print("⚡ CONVERGENCE:")
            for c in convergence:
                print(f"  • {c}")
        if anomalies:
            print("📊 ANOMALIES:")
            for a in anomalies:
                print(f"  • {a}")
        if not convergence and not anomalies:
            print("No alerts detected.")
    elif mode == "--polymarket":
        for m in intel["polymarket"]:
            vol = m['volume']
            vol_str = f"${vol/1_000_000:.1f}M" if vol >= 1_000_000 else f"${vol/1000:.0f}K"
            print(f"{m['yes_prob']}% — {m['question']} ({vol_str})")
    else:
        print(json.dumps(intel, indent=2, default=str))


if __name__ == "__main__":
    main()
