import argparse
import json
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

PRIORITY_PASS_AIRPORTS_URL = "https://www.prioritypass.com/airport-lounges"
OURAIRPORTS_CSV_URL = "https://ourairports.com/data/airports.csv"
BASE_URL = "https://www.prioritypass.com"
MY_PP_BASE = "https://my.prioritypass.com"
MY_PP_LANG_PREFIX = "/en-GB"


def build_requests_session() -> requests.Session:
    """
    Build a session that ignores system proxy settings.
    In this environment proxy settings may block outbound HTTPS.
    """
    session = requests.Session()
    session.trust_env = False
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            )
        }
    )
    return session


def fetch_text(url: str, session: requests.Session, timeout: int = 45) -> str:
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    return response.text


def to_my_prioritypass_url(url_or_path: str) -> str:
    """
    Convert a Priority Pass URL/path into my.prioritypass.com/en-GB form.
    """
    raw = (url_or_path or "").strip()
    if not raw:
        return raw

    parsed = urlparse(raw)
    path = parsed.path if parsed.netloc else raw
    if not path.startswith("/"):
        path = "/" + path

    # Normalize /lounges/... to /en-GB/lounges/...
    if path.startswith("/lounges/"):
        path = f"{MY_PP_LANG_PREFIX}{path}"
    elif path.startswith(f"{MY_PP_LANG_PREFIX}/lounges/"):
        pass

    return urlunparse(("https", "my.prioritypass.com", path, "", "", ""))


def is_lounge_detail_url(url: str) -> bool:
    """
    True only for lounge detail pages:
    /en-GB/lounges/<country>/<airport>/<lounge-slug>
    """
    if not url:
        return False
    parsed = urlparse(url)
    parts = [p for p in parsed.path.strip("/").split("/") if p]
    if len(parts) >= 5 and parts[0].lower() == "en-gb" and parts[1] == "lounges":
        return True
    if len(parts) >= 4 and parts[0] == "lounges":
        return True
    return False


def normalize_lounge_detail_url(country_slug: str, href: str) -> Optional[str]:
    """
    Build canonical lounge detail URL in my.prioritypass.com/en-GB format.
    """
    if not href:
        return None
    href = href.strip()
    if not href or href.startswith("#"):
        return None
    if href.startswith("http://") or href.startswith("https://"):
        candidate = to_my_prioritypass_url(href)
        return candidate if is_lounge_detail_url(candidate) else None
    if href.startswith("/"):
        candidate = to_my_prioritypass_url(href)
        return candidate if is_lounge_detail_url(candidate) else None
    if not country_slug:
        return None
    # Common card link is "<canonical-airport-slug>/<detail-slug>"
    candidate = to_my_prioritypass_url(f"/lounges/{country_slug}/{href.lstrip('/')}")
    return candidate if is_lounge_detail_url(candidate) else None


def repair_duplicated_airport_segment(url: str) -> str:
    """
    Repair malformed pattern:
    /lounges/<country>/<wrong-airport>/<correct-airport>/<detail-slug>
    -> /lounges/<country>/<correct-airport>/<detail-slug>
    """
    parsed = urlparse(url)
    path = parsed.path
    raw_parts = [p for p in path.strip("/").split("/") if p]

    if len(raw_parts) >= 6 and raw_parts[0].lower() == "en-gb" and raw_parts[1] == "lounges":
        # en-GB/lounges/country/wrong/correct/detail...
        fixed_parts = raw_parts[:3] + raw_parts[4:]
        fixed_path = "/" + "/".join(fixed_parts)
        return urlunparse((parsed.scheme or "https", parsed.netloc or "my.prioritypass.com", fixed_path, "", "", ""))

    if len(raw_parts) >= 5 and raw_parts[0] == "lounges":
        # lounges/country/wrong/correct/detail...
        fixed_parts = raw_parts[:2] + raw_parts[3:]
        fixed_path = "/" + "/".join(fixed_parts)
        return urlunparse((parsed.scheme or "https", parsed.netloc or "my.prioritypass.com", fixed_path, "", "", ""))

    return url


def check_url_ok(session: requests.Session, url: str, timeout: int = 35) -> Tuple[bool, str]:
    """
    Check whether URL opens to a non-404 page and return resolved URL.
    """
    if not is_lounge_detail_url(url):
        return False, url
    response = session.get(url, timeout=timeout, allow_redirects=True)
    if not is_lounge_detail_url(response.url):
        return False, response.url
    html_head = (response.text or "")[:2500].lower()
    looks_not_found = ("page not found" in html_head) or ("404" in html_head and "lounge" not in html_head)
    ok = response.status_code < 400 and not looks_not_found
    return ok, response.url


def try_recover_detail_url_from_redirect(original_url: str, redirected_url: str) -> Optional[str]:
    """
    Recover lounge detail URL when request lands on country/airport page.
    Typical fallback:
    /en-GB/lounges/usa/.../atl10-the-club-atl
      -> /en-GB/lounges/united-states-of-america/hartsfield-jackson-atlanta-international?fromLounge=1
    We rebuild:
      /en-GB/lounges/united-states-of-america/hartsfield-jackson-atlanta-international/atl10-the-club-atl
    """
    o_parts = [p for p in urlparse(original_url).path.strip("/").split("/") if p]
    r_parts = [p for p in urlparse(redirected_url).path.strip("/").split("/") if p]

    if len(o_parts) < 5:
        return None
    detail_slug = o_parts[-1]
    original_airport = o_parts[-2]

    # redirected airport page (preferred)
    if len(r_parts) >= 4 and r_parts[0].lower() == "en-gb" and r_parts[1] == "lounges":
        country_slug = r_parts[2]
        airport_slug = r_parts[3]
        rebuilt = to_my_prioritypass_url(f"/en-GB/lounges/{country_slug}/{airport_slug}/{detail_slug}")
        if is_lounge_detail_url(rebuilt):
            return rebuilt

    # redirected country page -> reuse original airport slug
    if len(r_parts) == 3 and r_parts[0].lower() == "en-gb" and r_parts[1] == "lounges":
        country_slug = r_parts[2]
        rebuilt = to_my_prioritypass_url(f"/en-GB/lounges/{country_slug}/{original_airport}/{detail_slug}")
        if is_lounge_detail_url(rebuilt):
            return rebuilt

    return None


def parse_priority_pass_airport_links(index_html: str) -> List[str]:
    soup = BeautifulSoup(index_html, "html.parser")
    links = set()
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"].strip()
        if href.startswith("/lounges/") and href.count("/") == 3 and not href.endswith("/"):
            links.add(urljoin(BASE_URL, href))
    return sorted(links)


def extract_iata_candidates(text: str) -> List[str]:
    if not text:
        return []
    candidates = re.findall(r"\b([A-Z]{3})\b", text.upper())
    seen = set()
    ordered = []
    for code in candidates:
        if code not in seen:
            seen.add(code)
            ordered.append(code)
    return ordered


def extract_outlet_items_from_embedded_payload(
    html: str,
    country_slug: str,
    airport_slug: str,
    airport_url: str,
    airport_name: str,
    airport_title: str,
    iata_from_title: Optional[str],
) -> List[Dict]:
    """
    Extract outlet items (all terminals/concourse tabs) from embedded page payload.
    Some airports only server-render the first tab as <a> cards, while additional
    tabs are still present in serialized JSON blocks in the HTML source.
    """
    html_norm = html.replace("\\/", "/")
    records: List[Dict] = []

    # Match payload objects that contain outletCategory + slug + name, and usually terminal.
    # Supports both plain JSON and escaped JSON string fragments.
    pattern = re.compile(
        r'(?:\\?"code\\?":\\?"(?P<code>[A-Z0-9]{3,6})\\?",)?'
        r'\\?"name\\?":\\?"(?P<name>[^"\\]+)\\?",'
        r'\\?"outletCategory\\?":\\?"(?P<category>LOUNGE|DINING|RELAX)\\?",'
        r'\\?"slug\\?":\\?"(?P<slug>[a-z0-9-]+)\\?"'
        r'.{0,1200}?'
        r'\\?"terminal\\?":\\?"(?P<terminal>[^"\\]+)\\?"',
        flags=re.IGNORECASE | re.DOTALL,
    )

    for m in pattern.finditer(html_norm):
        category = (m.group("category") or "").upper()
        name = (m.group("name") or "").strip()
        slug = (m.group("slug") or "").strip()
        code = (m.group("code") or "").strip().upper()
        if not category or not name or not slug:
            continue

        # Prefer canonical path found in page payload.
        path_match = re.search(
            rf"/(?:en-GB/)?lounges/{re.escape(country_slug)}/([a-z0-9-]+)/{re.escape(slug)}",
            html_norm,
            flags=re.IGNORECASE,
        )
        if path_match:
            detail_url = to_my_prioritypass_url(path_match.group(0))
        else:
            detail_url = to_my_prioritypass_url(f"/lounges/{country_slug}/{airport_slug}/{slug}")

        iata_from_code = None
        if code:
            m_code = re.match(r"([A-Z]{3})", code)
            if m_code:
                iata_from_code = m_code.group(1)
        if not iata_from_code:
            m_slug_code = re.match(r"([A-Z]{3})", slug.upper())
            if m_slug_code:
                iata_from_code = m_slug_code.group(1)

        records.append(
            {
                "airport_url": airport_url,
                "airport_slug": airport_slug,
                "country_slug": country_slug,
                "airport_name_pp": airport_name,
                "airport_title": airport_title,
                "experience_type": category,
                "experience_name": name,
                "experience_detail_url": detail_url,
                "experience_detail_slug": slug,
                "iata_from_code": iata_from_code,
                "iata_from_title": iata_from_title,
            }
        )

    return records


def parse_airport_page(airport_url: str, session: requests.Session) -> Dict:
    html = fetch_text(airport_url, session=session)
    soup = BeautifulSoup(html, "html.parser")

    airport_name = ""
    h1 = soup.find("h1")
    if h1:
        airport_name = h1.get_text(" ", strip=True)

    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    iata_from_title = None
    # Examples: "... SFO Lounges ..." / "... JFK Lounges ..."
    m_title = re.search(r"\b([A-Z]{3})\s+Lounges\b", title.upper())
    if m_title:
        iata_from_title = m_title.group(1)

    path_parts = airport_url.replace(BASE_URL, "").strip("/").split("/")
    country_slug = path_parts[1] if len(path_parts) > 2 else ""
    airport_slug = path_parts[2] if len(path_parts) > 2 else ""

    lounge_items: List[Dict] = []
    non_lounge_items: List[Dict] = []

    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href", "").strip()
        link_text = " ".join(anchor.stripped_strings).strip()
        if not href or not link_text:
            continue

        parts = link_text.split(maxsplit=1)
        item_type = parts[0].upper()
        if item_type not in {"LOUNGE", "DINING", "RELAX"}:
            continue
        item_name = parts[1] if len(parts) > 1 else parts[0]

        detail_url = normalize_lounge_detail_url(country_slug=country_slug, href=href)
        if not detail_url:
            continue
        detail_slug = detail_url.rstrip("/").split("/")[-1]

        code_prefix = detail_slug.split("-")[0].upper()
        iata_from_code = None
        m_code = re.match(r"([A-Z]{3})", code_prefix)
        if m_code:
            iata_from_code = m_code.group(1)

        item = {
            "airport_url": airport_url,
            "airport_slug": airport_slug,
            "country_slug": country_slug,
            "airport_name_pp": airport_name,
            "airport_title": title,
            "experience_type": item_type,
            "experience_name": item_name,
            "experience_detail_url": detail_url,
            "experience_detail_slug": detail_slug,
            "iata_from_code": iata_from_code,
            "iata_from_title": iata_from_title,
        }
        if item_type == "LOUNGE":
            lounge_items.append(item)
        else:
            non_lounge_items.append(item)

    # Merge items from embedded payload to include lounges from all terminal/concourse tabs.
    embedded_items = extract_outlet_items_from_embedded_payload(
        html=html,
        country_slug=country_slug,
        airport_slug=airport_slug,
        airport_url=airport_url,
        airport_name=airport_name,
        airport_title=title,
        iata_from_title=iata_from_title,
    )
    for item in embedded_items:
        if item["experience_type"] == "LOUNGE":
            lounge_items.append(item)
        else:
            non_lounge_items.append(item)

    # Deduplicate by type + detail slug to avoid duplicated records from two extraction paths.
    def dedupe(items: List[Dict]) -> List[Dict]:
        seen = set()
        merged: List[Dict] = []
        for it in items:
            key = (it.get("experience_type"), it.get("experience_detail_slug"))
            if key in seen:
                continue
            seen.add(key)
            merged.append(it)
        return merged

    lounge_items = dedupe(lounge_items)
    non_lounge_items = dedupe(non_lounge_items)

    all_iata_candidates = []
    for it in lounge_items:
        if it["iata_from_code"]:
            all_iata_candidates.append(it["iata_from_code"])
    if iata_from_title:
        all_iata_candidates.append(iata_from_title)
    all_iata_candidates.extend(extract_iata_candidates(title))

    airport_iata = None
    for c in all_iata_candidates:
        if re.fullmatch(r"[A-Z]{3}", c):
            airport_iata = c
            break

    return {
        "airport_url": airport_url,
        "airport_slug": airport_slug,
        "country_slug": country_slug,
        "airport_name_pp": airport_name,
        "airport_title": title,
        "airport_iata": airport_iata,
        "lounge_count": len(lounge_items),
        "non_lounge_count": len(non_lounge_items),
        "all_experience_count": len(lounge_items) + len(non_lounge_items),
        "lounge_items": lounge_items,
        "non_lounge_items": non_lounge_items,
    }


def fetch_lounge_image(lounge_url: str, session: requests.Session) -> Dict[str, Optional[str]]:
    try:
        ok, resolved_url = check_url_ok(session=session, url=lounge_url)
        if not ok:
            recovered = try_recover_detail_url_from_redirect(lounge_url, resolved_url)
            if recovered:
                ok_rec, resolved_rec = check_url_ok(session=session, url=recovered)
                if ok_rec:
                    lounge_url = recovered
                    resolved_url = resolved_rec
                    ok = True
            if not ok:
                repaired = repair_duplicated_airport_segment(lounge_url)
                if repaired != lounge_url:
                    ok2, resolved2 = check_url_ok(session=session, url=repaired)
                    if ok2:
                        lounge_url = repaired
                        resolved_url = resolved2
                        ok = True
                    else:
                        recovered2 = try_recover_detail_url_from_redirect(repaired, resolved2)
                        if recovered2:
                            ok3, resolved3 = check_url_ok(session=session, url=recovered2)
                            if ok3:
                                lounge_url = recovered2
                                resolved_url = resolved3
                                ok = True
            if not ok:
                return {"lounge_image_url": None, "lounge_title": None, "resolved_detail_url": resolved_url}

        response = session.get(resolved_url, timeout=35)
        response.raise_for_status()
        html = response.text
    except Exception:
        return {"lounge_image_url": None, "lounge_title": None, "resolved_detail_url": None}

    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text(" ", strip=True) if soup.title else None

    image = None
    meta_og = soup.find("meta", attrs={"property": "og:image"})
    if meta_og and meta_og.get("content"):
        image = meta_og.get("content")

    if not image:
        img = soup.find("img")
        if img and img.get("src"):
            image = urljoin(BASE_URL, img["src"])

    return {
        "lounge_image_url": image,
        "lounge_title": title,
        "resolved_detail_url": to_my_prioritypass_url(response.url),
    }


def build_world_airports_dataframe(session: requests.Session) -> pd.DataFrame:
    response = session.get(OURAIRPORTS_CSV_URL, timeout=90)
    response.raise_for_status()
    csv_bytes = response.content
    data = pd.read_csv(pd.io.common.BytesIO(csv_bytes), low_memory=False)
    # Keep canonical columns we need for mapping and join.
    keep_cols = [
        "id",
        "ident",
        "type",
        "name",
        "latitude_deg",
        "longitude_deg",
        "elevation_ft",
        "continent",
        "iso_country",
        "iso_region",
        "municipality",
        "scheduled_service",
        "gps_code",
        "iata_code",
        "local_code",
        "home_link",
        "wikipedia_link",
        "keywords",
    ]
    existing_cols = [c for c in keep_cols if c in data.columns]
    data = data[existing_cols].copy()
    data["iata_code"] = data["iata_code"].fillna("").astype(str).str.upper().str.strip()
    return data


def create_interactive_map(df_map: pd.DataFrame, output_html: Path) -> None:
    points = []
    for _, row in df_map.iterrows():
        lounge_items = []
        lounges_json = row.get("lounge_items_json", "")
        if isinstance(lounges_json, str) and lounges_json:
            lounge_items = json.loads(lounges_json)
        points.append(
            {
                "lat": float(row["latitude_deg"]),
                "lon": float(row["longitude_deg"]),
                "iata": str(row["iata_code"]),
                "airport_name": str(row["name"]),
                "country": str(row["iso_country"]),
                "lounge_count": int(row["lounge_count"]),
                "lounge_items": lounge_items,
            }
        )

    html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Priority Pass Lounges Map</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css" />
  <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css" />
  <style>
    html, body {{
      width: 100%;
      height: 100%;
      margin: 0;
      padding: 0;
      overflow: hidden;
    }}
    #map {{
      position: fixed;
      inset: 0;
      width: 100vw;
      height: 100vh;
    }}
    .search-panel {{
      position: fixed;
      top: 14px;
      left: 100px;
      z-index: 1200;
      display: flex;
      align-items: center;
      gap: 6px;
      background: rgba(255, 255, 255, 0.95);
      border: 1px solid #d8dde6;
      border-radius: 8px;
      padding: 8px;
      box-shadow: 0 3px 10px rgba(0, 0, 0, 0.12);
      backdrop-filter: blur(2px);
    }}
    .search-panel input {{
      width: 280px;
      max-width: 56vw;
      border: 1px solid #d8dde6;
      border-radius: 6px;
      padding: 8px 10px;
      font-size: 14px;
      outline: none;
    }}
    .search-panel input::placeholder {{
      color: #9aa4b2;
    }}
    .search-panel button {{
      border: 1px solid #2c7be5;
      background: #2c7be5;
      color: #fff;
      border-radius: 6px;
      width: 34px;
      height: 34px;
      cursor: pointer;
      font-size: 16px;
      line-height: 1;
    }}
    .search-panel button:hover {{
      background: #1f6fd6;
    }}
    .search-hint {{
      position: fixed;
      top: 66px;
      left: 100px;
      z-index: 1200;
      background: rgba(255, 255, 255, 0.95);
      border: 1px solid #d8dde6;
      border-radius: 6px;
      padding: 6px 8px;
      font-size: 12px;
      color: #5f6b7a;
      display: none;
    }}
    .pp-tooltip img {{ width: 220px; max-width: 220px; border-radius: 6px; margin-top: 6px; }}
    .pp-popup {{ width: 290px; max-height: 370px; overflow-y: auto; padding-right: 4px; }}
    .pp-popup img {{ width: 240px; max-width: 100%; border-radius: 6px; margin-top: 4px; }}
    .pp-popup .item {{ margin: 8px 0 12px 0; }}
    .map-footer {{
      position: fixed;
      left: 12px;
      bottom: 10px;
      z-index: 1200;
      background: rgba(255, 255, 255, 0.92);
      border: 1px solid #d8dde6;
      border-radius: 6px;
      padding: 6px 8px;
      font-size: 11px;
      color: #445066;
      line-height: 1.35;
      max-width: min(760px, 78vw);
      box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
    }}
  </style>
</head>
<body>
  <div class="search-panel">
    <input id="airportSearchInput" type="text" placeholder="可以输入机场代码 / Enter airport code" />
    <button id="airportSearchBtn" aria-label="Search airport">➤</button>
  </div>
  <div id="searchHint" class="search-hint"></div>
  <div class="map-footer">
    Created by Li Dai, February 2026. Not affiliated with Priority Pass. Data may be incomplete or outdated.
  </div>
  <div id="map"></div>
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"></script>
  <script>
    const points = {json.dumps(points, ensure_ascii=False)};
    const worldBounds = L.latLngBounds([[-85, -180], [85, 180]]);
    const map = L.map('map', {{
      minZoom: 2,
      maxBounds: worldBounds,
      maxBoundsViscosity: 1.0,
      worldCopyJump: false
    }}).setView([20, 0], 2);
    L.tileLayer('https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
      attribution: '&copy; OpenStreetMap contributors &copy; CARTO',
      noWrap: true,
      bounds: worldBounds
    }}).addTo(map);

    const clusters = L.markerClusterGroup();
    const markersByIata = new Map();
    const searchRows = [];

    function escapeHtml(input) {{
      return String(input || '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('\"', '&quot;')
        .replaceAll(\"'\", '&#39;');
    }}

    function norm(input) {{
      return String(input || '')
        .toLowerCase()
        .replaceAll(/\\s+/g, ' ')
        .trim();
    }}

    function showHint(text) {{
      const hint = document.getElementById('searchHint');
      if (!text) {{
        hint.style.display = 'none';
        hint.textContent = '';
        return;
      }}
      hint.textContent = text;
      hint.style.display = 'block';
      window.clearTimeout(window.__ppHintTimer);
      window.__ppHintTimer = window.setTimeout(() => {{
        hint.style.display = 'none';
      }}, 2200);
    }}

    function zoomToMarker(marker) {{
      clusters.zoomToShowLayer(marker, () => {{
        const targetZoom = Math.max(map.getZoom(), 5);
        map.flyTo(marker.getLatLng(), targetZoom, {{ duration: 0.8 }});
        marker.openPopup();
      }});
    }}

    points.forEach((p) => {{
      let firstImage = '';
      for (const item of p.lounge_items) {{
        if (item.lounge_image_url) {{
          firstImage = item.lounge_image_url;
          break;
        }}
      }}

      let tooltip = `<div class="pp-tooltip"><b>${{escapeHtml(p.airport_name)}}</b><br>IATA: ${{escapeHtml(p.iata)}}<br>Country: ${{escapeHtml(p.country)}}<br>Lounges: ${{p.lounge_count}}`;
      if (firstImage) {{
        tooltip += `<br><img src="${{firstImage}}" />`;
      }}
      tooltip += '</div>';

      let popup = `<div class="pp-popup"><h4 style="margin:0 0 8px 0;">${{escapeHtml(p.airport_name)}} (${{escapeHtml(p.iata)}})</h4><div>Country: ${{escapeHtml(p.country)}}</div><div>Lounge count: ${{p.lounge_count}}</div><hr style="margin:8px 0;" />`;
      for (const item of p.lounge_items.slice(0, 8)) {{
        const imageHtml = item.lounge_image_url ? `<div><img src="${{item.lounge_image_url}}" /></div>` : '';
        popup += `<div class="item"><b>${{escapeHtml(item.experience_name)}}</b><br>${{imageHtml}}<a href="${{item.experience_detail_url || '#'}}" target="_blank">Priority Pass page</a></div>`;
      }}
      popup += '</div>';

      const marker = L.circleMarker([p.lat, p.lon], {{
        radius: 5,
        color: '#2c7be5',
        weight: 1,
        fillColor: '#2c7be5',
        fillOpacity: 0.85
      }});
      marker.bindTooltip(tooltip, {{ sticky: true }});
      marker.bindPopup(popup, {{ maxWidth: 320 }});
      clusters.addLayer(marker);

      const iataKey = norm(p.iata);
      if (iataKey && !markersByIata.has(iataKey)) {{
        markersByIata.set(iataKey, marker);
      }}
      searchRows.push({{
        marker: marker,
        iata: iataKey,
        airportName: norm(p.airport_name),
        country: norm(p.country),
        combined: norm(`${{p.iata}} ${{p.airport_name}} ${{p.country}}`),
      }});
    }});

    map.addLayer(clusters);

    function searchAirport() {{
      const inputEl = document.getElementById('airportSearchInput');
      const query = norm(inputEl.value);
      if (!query) {{
        showHint('请输入机场代码或机场名');
        return;
      }}

      if (markersByIata.has(query)) {{
        zoomToMarker(markersByIata.get(query));
        showHint('');
        return;
      }}

      let hit = searchRows.find((r) => r.airportName === query || r.combined === query);
      if (!hit) {{
        hit = searchRows.find((r) => r.airportName.includes(query) || r.combined.includes(query) || r.country.includes(query));
      }}

      if (!hit) {{
        showHint('未找到对应机场，请尝试 IATA 代码');
        return;
      }}
      zoomToMarker(hit.marker);
      showHint('');
    }}

    document.getElementById('airportSearchBtn').addEventListener('click', searchAirport);
    document.getElementById('airportSearchInput').addEventListener('keydown', (evt) => {{
      if (evt.key === 'Enter') {{
        evt.preventDefault();
        searchAirport();
      }}
    }});

    // Keep map rendering synced with browser window size changes.
    window.addEventListener('resize', () => {{
      map.invalidateSize(true);
    }});
  </script>
</body>
</html>
"""
    output_html.write_text(html_template, encoding="utf-8")


def run_pipeline(output_dir: Path, workers: int, max_airports: Optional[int]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    data_dir = output_dir / "data"
    map_dir = output_dir / "map"
    data_dir.mkdir(parents=True, exist_ok=True)
    map_dir.mkdir(parents=True, exist_ok=True)

    session = build_requests_session()

    print("[1/8] Downloading world airports dataset...")
    world_airports_df = build_world_airports_dataframe(session)
    world_airports_csv_path = data_dir / "world_airports.csv"
    world_airports_df.to_csv(world_airports_csv_path, index=False, encoding="utf-8")
    print(f"    Saved {len(world_airports_df):,} airports -> {world_airports_csv_path}")

    print("[2/8] Scraping Priority Pass airport index...")
    index_html = fetch_text(PRIORITY_PASS_AIRPORTS_URL, session=session)
    airport_links = parse_priority_pass_airport_links(index_html)
    if max_airports:
        airport_links = airport_links[:max_airports]
    print(f"    Found {len(airport_links):,} Priority Pass airport pages.")

    print("[3/8] Scraping each Priority Pass airport page and filtering only LOUNGE...")
    airport_summaries: List[Dict] = []
    lounge_items_all: List[Dict] = []

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(parse_airport_page, url, session): url for url in airport_links}
        for i, fut in enumerate(as_completed(futures), 1):
            url = futures[fut]
            try:
                result = fut.result()
                airport_summaries.append(
                    {
                        "airport_url": result["airport_url"],
                        "airport_slug": result["airport_slug"],
                        "country_slug": result["country_slug"],
                        "airport_name_pp": result["airport_name_pp"],
                        "airport_title": result["airport_title"],
                        "airport_iata": result["airport_iata"],
                        "lounge_count": result["lounge_count"],
                        "non_lounge_count": result["non_lounge_count"],
                        "all_experience_count": result["all_experience_count"],
                    }
                )
                lounge_items_all.extend(result["lounge_items"])
            except Exception as err:
                airport_summaries.append(
                    {
                        "airport_url": url,
                        "airport_slug": "",
                        "country_slug": "",
                        "airport_name_pp": "",
                        "airport_title": "",
                        "airport_iata": "",
                        "lounge_count": 0,
                        "non_lounge_count": 0,
                        "all_experience_count": 0,
                        "error": str(err),
                    }
                )
            if i % 25 == 0 or i == len(airport_links):
                print(f"    Processed {i:,}/{len(airport_links):,} airports...")

    pp_airports_df = pd.DataFrame(airport_summaries)
    pp_lounges_df = pd.DataFrame(lounge_items_all)

    if pp_lounges_df.empty:
        raise RuntimeError("No lounge records found. Scraper output is empty.")

    # Official rule: Dining and Relax do NOT count. We only retain LOUNGE.
    pp_lounges_df = pp_lounges_df[pp_lounges_df["experience_type"] == "LOUNGE"].copy()
    pp_lounges_df["iata_code"] = (
        pp_lounges_df["iata_from_code"]
        .fillna(pp_lounges_df["iata_from_title"])
        .fillna("")
        .astype(str)
        .str.upper()
        .str.strip()
    )
    pp_lounges_df = pp_lounges_df[pp_lounges_df["iata_code"].str.fullmatch(r"[A-Z]{3}", na=False)].copy()
    pp_lounges_df = pp_lounges_df[
        pp_lounges_df["experience_detail_url"].fillna("").map(is_lounge_detail_url)
    ].copy()

    print(f"    Lounge records (LOUNGE only): {len(pp_lounges_df):,}")

    print("[4/8] Scraping lounge images from detail pages...")
    unique_lounge_urls = sorted(pp_lounges_df["experience_detail_url"].dropna().unique().tolist())
    image_lookup: Dict[str, Dict[str, Optional[str]]] = {}

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(fetch_lounge_image, u, session): u for u in unique_lounge_urls}
        for i, fut in enumerate(as_completed(futures), 1):
            lounge_url = futures[fut]
            try:
                image_lookup[lounge_url] = fut.result()
            except Exception:
                image_lookup[lounge_url] = {
                    "lounge_image_url": None,
                    "lounge_title": None,
                    "resolved_detail_url": None,
                }
            if i % 50 == 0 or i == len(unique_lounge_urls):
                print(f"    Processed {i:,}/{len(unique_lounge_urls):,} lounge detail pages...")

    pp_lounges_df["lounge_image_url"] = pp_lounges_df["experience_detail_url"].map(
        lambda u: image_lookup.get(u, {}).get("lounge_image_url")
    )
    pp_lounges_df["lounge_title"] = pp_lounges_df["experience_detail_url"].map(
        lambda u: image_lookup.get(u, {}).get("lounge_title")
    )
    pp_lounges_df["resolved_detail_url"] = pp_lounges_df["experience_detail_url"].map(
        lambda u: image_lookup.get(u, {}).get("resolved_detail_url")
    )
    pp_lounges_df["experience_detail_url"] = pp_lounges_df["resolved_detail_url"].fillna(
        pp_lounges_df["experience_detail_url"]
    )
    pp_lounges_df.drop(columns=["resolved_detail_url"], inplace=True)

    print("[5/8] Joining with global airport coordinates...")
    pp_grouped = (
        pp_lounges_df.groupby("iata_code")
        .agg(
            lounge_count=("experience_name", "count"),
            lounge_names=("experience_name", lambda s: " | ".join(sorted(set(s)))),
            lounge_items_json=(
                "experience_name",
                lambda _: "",
            ),
        )
        .reset_index()
    )

    lounge_json_lookup: Dict[str, str] = {}
    for iata, chunk in pp_lounges_df.groupby("iata_code"):
        records = (
            chunk[
                [
                    "experience_name",
                    "experience_detail_url",
                    "lounge_image_url",
                ]
            ]
            .drop_duplicates()
            .to_dict(orient="records")
        )
        lounge_json_lookup[iata] = json.dumps(records, ensure_ascii=False)
    pp_grouped["lounge_items_json"] = pp_grouped["iata_code"].map(lounge_json_lookup)

    world_airports_enriched = world_airports_df.merge(
        pp_grouped,
        how="left",
        on="iata_code",
    )
    world_airports_enriched["has_priority_lounge"] = world_airports_enriched["lounge_count"].fillna(0).astype(int) > 0

    map_df = world_airports_enriched[
        (world_airports_enriched["has_priority_lounge"])
        & world_airports_enriched["latitude_deg"].notna()
        & world_airports_enriched["longitude_deg"].notna()
    ].copy()

    print("[6/8] Rechecking all URLs used by map pins...")
    map_iatas = set(map_df["iata_code"].dropna().astype(str).tolist())
    map_lounges_df = pp_lounges_df[pp_lounges_df["iata_code"].isin(map_iatas)].copy()
    unique_map_urls = sorted(map_lounges_df["experience_detail_url"].dropna().unique().tolist())
    recheck_lookup: Dict[str, str] = {}

    def recheck_and_fix(url: str) -> str:
        try:
            if not is_lounge_detail_url(url):
                return ""
            ok, resolved = check_url_ok(session=session, url=url)
            if ok:
                return to_my_prioritypass_url(resolved)

            recovered = try_recover_detail_url_from_redirect(url, resolved)
            if recovered:
                ok_rec, resolved_rec = check_url_ok(session=session, url=recovered)
                if ok_rec:
                    return to_my_prioritypass_url(resolved_rec)

            repaired = repair_duplicated_airport_segment(url)
            if repaired != url:
                ok2, resolved2 = check_url_ok(session=session, url=repaired)
                if ok2:
                    return to_my_prioritypass_url(resolved2)
                recovered2 = try_recover_detail_url_from_redirect(repaired, resolved2)
                if recovered2:
                    ok3, resolved3 = check_url_ok(session=session, url=recovered2)
                    if ok3:
                        return to_my_prioritypass_url(resolved3)
        except Exception:
            return ""
        return ""

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(recheck_and_fix, url): url for url in unique_map_urls}
        for i, fut in enumerate(as_completed(futures), 1):
            source_url = futures[fut]
            recheck_lookup[source_url] = fut.result()
            if i % 50 == 0 or i == len(unique_map_urls):
                print(f"    Rechecked {i:,}/{len(unique_map_urls):,} URLs...")

    pp_lounges_df["experience_detail_url"] = pp_lounges_df["experience_detail_url"].map(
        lambda u: recheck_lookup.get(u, "")
    )
    pp_lounges_df = pp_lounges_df[pp_lounges_df["experience_detail_url"] != ""].copy()

    # Rebuild map payload after URL recheck to guarantee popup links are valid.
    pp_grouped = (
        pp_lounges_df.groupby("iata_code")
        .agg(
            lounge_count=("experience_name", "count"),
            lounge_names=("experience_name", lambda s: " | ".join(sorted(set(s)))),
            lounge_items_json=(
                "experience_name",
                lambda _: "",
            ),
        )
        .reset_index()
    )

    lounge_json_lookup = {}
    for iata, chunk in pp_lounges_df.groupby("iata_code"):
        records = (
            chunk[
                [
                    "experience_name",
                    "experience_detail_url",
                    "lounge_image_url",
                ]
            ]
            .drop_duplicates()
            .to_dict(orient="records")
        )
        lounge_json_lookup[iata] = json.dumps(records, ensure_ascii=False)
    pp_grouped["lounge_items_json"] = pp_grouped["iata_code"].map(lounge_json_lookup)

    world_airports_enriched = world_airports_df.merge(
        pp_grouped,
        how="left",
        on="iata_code",
    )
    world_airports_enriched["has_priority_lounge"] = world_airports_enriched["lounge_count"].fillna(0).astype(int) > 0
    map_df = world_airports_enriched[
        (world_airports_enriched["has_priority_lounge"])
        & world_airports_enriched["latitude_deg"].notna()
        & world_airports_enriched["longitude_deg"].notna()
    ].copy()

    print(f"    Airports with at least one Priority Pass lounge: {len(map_df):,}")

    print("[7/8] Saving local database and data exports...")
    db_path = data_dir / "priority_pass_lounges.db"
    with sqlite3.connect(db_path) as conn:
        world_airports_df.to_sql("world_airports", conn, if_exists="replace", index=False)
        pp_airports_df.to_sql("priority_pass_airports", conn, if_exists="replace", index=False)
        pp_lounges_df.to_sql("priority_pass_lounges", conn, if_exists="replace", index=False)
        world_airports_enriched.to_sql("world_airports_with_priority_lounges", conn, if_exists="replace", index=False)

    pp_airports_df.to_csv(data_dir / "priority_pass_airports.csv", index=False, encoding="utf-8")
    pp_lounges_df.to_csv(data_dir / "priority_pass_lounges_only.csv", index=False, encoding="utf-8")
    world_airports_enriched.to_csv(
        data_dir / "world_airports_with_priority_lounges.csv",
        index=False,
        encoding="utf-8",
    )
    map_df.to_csv(data_dir / "map_airports_with_lounges.csv", index=False, encoding="utf-8")

    meta = {
        "generated_at_unix": int(time.time()),
        "source_priority_pass": PRIORITY_PASS_AIRPORTS_URL,
        "source_world_airports": OURAIRPORTS_CSV_URL,
        "priority_pass_airports_scraped": int(len(airport_links)),
        "priority_pass_lounges_only_count": int(len(pp_lounges_df)),
        "map_airport_pin_count": int(len(map_df)),
        "map_urls_rechecked_count": int(len(unique_map_urls)),
        "map_urls_valid_after_recheck_count": int(sum(1 for v in recheck_lookup.values() if v)),
        "notes": "Dining and Relax experiences are excluded; only LOUNGE retained.",
    }
    (data_dir / "metadata.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"    SQLite DB: {db_path}")

    print("[8/8] Building interactive static map...")
    map_html_path = map_dir / "priority_pass_lounges_map.html"
    create_interactive_map(map_df, map_html_path)
    print(f"    Map saved: {map_html_path}")
    print("Done.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Scrape global airports + Priority Pass lounge data, "
            "store local DB, and build interactive static map."
        )
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Output directory path (default: output)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Concurrent worker count for scraping (default: 10)",
    )
    parser.add_argument(
        "--max-airports",
        type=int,
        default=None,
        help="Optional limit for airport pages (for quick test)",
    )
    args = parser.parse_args()

    run_pipeline(
        output_dir=Path(args.output_dir),
        workers=max(1, args.workers),
        max_airports=args.max_airports,
    )


if __name__ == "__main__":
    main()
