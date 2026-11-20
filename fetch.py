#!/usr/bin/env python3
"""Pull masajid across the United States, their Jumu'ah/iqamah times and events into data/masajid.json.

Same pipeline as MN Masjid Finder, scaled to 50 states + DC:
  1. OpenStreetMap (Overpass)   -> mosque list, tagged with the state each one sits in
  2. Masjidal directory          -> every US masjid registered on Masjidal (adds ones OSM lacks)
  3. Masjidal / Athan+ API       -> today's adhan + iqamah + Jumu'ah (set by the masjid)
  4. Mohid pages / WordPress Events Calendar -> Jumu'ah, iqamah, events where a site links them
  5. mosques_seed.json           -> hand-maintained fixes, win over everything

Adhan times for masajid with no live source are calculated in the visitor's browser
(ISNA 15/15, in the masjid's own time zone), so they are not stored here.
The browser also refreshes Masjidal times live for the cards a visitor is looking at.

Standard library only.  python3 fetch.py   (first run ~15-30 min; later runs use caches)
"""
from __future__ import annotations

import concurrent.futures as cf
import datetime as dt
import hashlib
import html
import json
import math
import re
import ssl
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
UA = "Mozilla/5.0 (us-masjid-finder; community prayer-times directory)"
GEO_UA = "us-masjid-finder/0.1 (community prayer-times directory)"
OVERPASS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
]
# One pass over every state; a "st" marker element precedes each state's mosques.
OSM_QUERY = """[out:json][timeout:300];
area["ISO3166-1"="US"][admin_level=2]->.us;
rel(area.us)["admin_level"="4"]["ISO3166-2"~"^US-"];
map_to_area->.states;
foreach.states->.s(
  make st code=s.set(t["ISO3166-2"]);
  out;
  nwr["amenity"="place_of_worship"]["religion"="muslim"](area.s);
  out center tags;
);"""
NOMINATIM = "https://nominatim.openstreetmap.org"
MAX_NEW_GEOCODES = int(__import__("os").environ.get("MAX_NEW_GEOCODES", "5000"))
DISCOVER_TTL_DAYS = 7
_last_geo = [0.0]

# ---------------------------------------------------------------- states

STATES = {  # code: (name, default IANA time zone)
    "AL": ("Alabama", "America/Chicago"), "AK": ("Alaska", "America/Anchorage"),
    "AZ": ("Arizona", "America/Phoenix"), "AR": ("Arkansas", "America/Chicago"),
    "CA": ("California", "America/Los_Angeles"), "CO": ("Colorado", "America/Denver"),
    "CT": ("Connecticut", "America/New_York"), "DE": ("Delaware", "America/New_York"),
    "DC": ("District of Columbia", "America/New_York"), "FL": ("Florida", "America/New_York"),
    "GA": ("Georgia", "America/New_York"), "HI": ("Hawaii", "Pacific/Honolulu"),
    "ID": ("Idaho", "America/Boise"), "IL": ("Illinois", "America/Chicago"),
    "IN": ("Indiana", "America/Indiana/Indianapolis"), "IA": ("Iowa", "America/Chicago"),
    "KS": ("Kansas", "America/Chicago"), "KY": ("Kentucky", "America/New_York"),
    "LA": ("Louisiana", "America/Chicago"), "ME": ("Maine", "America/New_York"),
    "MD": ("Maryland", "America/New_York"), "MA": ("Massachusetts", "America/New_York"),
    "MI": ("Michigan", "America/Detroit"), "MN": ("Minnesota", "America/Chicago"),
    "MS": ("Mississippi", "America/Chicago"), "MO": ("Missouri", "America/Chicago"),
    "MT": ("Montana", "America/Denver"), "NE": ("Nebraska", "America/Chicago"),
    "NV": ("Nevada", "America/Los_Angeles"), "NH": ("New Hampshire", "America/New_York"),
    "NJ": ("New Jersey", "America/New_York"), "NM": ("New Mexico", "America/Denver"),
    "NY": ("New York", "America/New_York"), "NC": ("North Carolina", "America/New_York"),
    "ND": ("North Dakota", "America/Chicago"), "OH": ("Ohio", "America/New_York"),
    "OK": ("Oklahoma", "America/Chicago"), "OR": ("Oregon", "America/Los_Angeles"),
    "PA": ("Pennsylvania", "America/New_York"), "RI": ("Rhode Island", "America/New_York"),
    "SC": ("South Carolina", "America/New_York"), "SD": ("South Dakota", "America/Chicago"),
    "TN": ("Tennessee", "America/Chicago"), "TX": ("Texas", "America/Chicago"),
    "UT": ("Utah", "America/Denver"), "VT": ("Vermont", "America/New_York"),
    "VA": ("Virginia", "America/New_York"), "WA": ("Washington", "America/Los_Angeles"),
    "WV": ("West Virginia", "America/New_York"), "WI": ("Wisconsin", "America/Chicago"),
    "WY": ("Wyoming", "America/Denver"),
}
NAME_TO_CODE = {v[0].lower(): k for k, v in STATES.items()}
NAME_TO_CODE.update({"washington dc": "DC", "washington, dc": "DC", "d.c.": "DC", "dc": "DC"})

# first three ZIP digits -> state (fallback when a listing's state field is missing or garbled)
_ZIP3 = [(5, 5, "NY"), (10, 27, "MA"), (28, 29, "RI"), (30, 38, "NH"), (39, 49, "ME"), (50, 54, "VT"),
         (55, 55, "MA"), (56, 59, "VT"), (60, 69, "CT"), (70, 89, "NJ"), (100, 149, "NY"), (150, 196, "PA"),
         (197, 199, "DE"), (200, 200, "DC"), (201, 201, "VA"), (202, 205, "DC"), (206, 219, "MD"),
         (220, 246, "VA"), (247, 268, "WV"), (270, 289, "NC"), (290, 299, "SC"), (300, 319, "GA"),
         (320, 349, "FL"), (350, 369, "AL"), (370, 385, "TN"), (386, 397, "MS"), (398, 399, "GA"),
         (400, 427, "KY"), (430, 459, "OH"), (460, 479, "IN"), (480, 499, "MI"), (500, 528, "IA"),
         (530, 549, "WI"), (550, 567, "MN"), (569, 569, "DC"), (570, 577, "SD"), (580, 588, "ND"),
         (590, 599, "MT"), (600, 629, "IL"), (630, 658, "MO"), (660, 679, "KS"), (680, 693, "NE"),
         (700, 714, "LA"), (716, 729, "AR"), (730, 749, "OK"), (750, 799, "TX"), (800, 816, "CO"),
         (820, 831, "WY"), (832, 838, "ID"), (840, 847, "UT"), (850, 865, "AZ"), (870, 884, "NM"),
         (885, 885, "TX"), (889, 898, "NV"), (900, 961, "CA"), (967, 968, "HI"), (970, 979, "OR"),
         (980, 994, "WA"), (995, 999, "AK")]


def state_from_zip(z):
    m = re.match(r"\s*(\d{3})\d{2}", z or "")
    if not m:
        return None
    n = int(m[1])
    return next((s for a, b, s in _ZIP3 if a <= n <= b), None)


def norm_state(s, zipcode=None):
    v = re.sub(r"[^a-z. ,]", "", (s or "").strip().lower()).strip(" .,")
    if v.upper() in STATES:
        return v.upper()
    return NAME_TO_CODE.get(v) or state_from_zip(zipcode)


def tz_for(st, lat, lon):
    """IANA zone. State default, with the split states handled by rough longitude/latitude lines."""
    tz = STATES[st][1]
    if lat is None or lon is None:
        return tz
    rules = {
        "FL": lon < -85.0 and lat > 29.6 and "America/Chicago",
        "TN": lon > -85.4 and "America/New_York",
        "KY": lon < -86.0 and "America/Chicago",
        "IN": ((lat > 41.0 and lon < -86.6) or (lat < 38.6 and lon < -86.8)) and "America/Chicago",
        "MI": lat > 45.5 and lon < -87.6 and "America/Menominee",
        "ND": lat < 47.0 and lon < -101.0 and "America/Denver",
        "SD": lon < -100.6 and "America/Denver",
        "NE": lon < -101.4 and "America/Denver",
        "KS": lon < -101.5 and "America/Denver",
        "TX": lon < -104.6 and "America/Denver",
        "ID": lat > 45.6 and "America/Los_Angeles",
        "OR": lat < 44.6 and lon > -117.6 and "America/Boise",
    }
    return rules.get(st) or tz


# ---------------------------------------------------------------- HTTP helpers

_CTX = ssl.create_default_context()


def http_get(url, data=None, timeout=25, ua=UA):
    req = urllib.request.Request(url, data=data, headers={"User-Agent": ua, "Accept": "*/*"})
    with urllib.request.urlopen(req, timeout=timeout, context=_CTX) as r:
        return r.read(3_000_000).decode("utf-8", "ignore")


def http_json(url, **kw):
    return json.loads(http_get(url, **kw))


def log(*a):
    print(*a, file=sys.stderr, flush=True)


def km(lat1, lon1, lat2, lon2):
    p = math.pi / 180
    a = (math.sin((lat2 - lat1) * p / 2) ** 2
         + math.cos(lat1 * p) * math.cos(lat2 * p) * math.sin((lon2 - lon1) * p / 2) ** 2)
    return 12742 * math.asin(math.sqrt(a))


# ---------------------------------------------------------------- time parsing

def fmt12(h, m):
    return f"{h % 12 or 12}:{m:02d} {'AM' if h < 12 else 'PM'}"


def norm_time(s):
    """'1:00PM' / '13:00' / '01:00 pm' -> '1:00 PM'. None if unparseable."""
    if not s:
        return None
    s = str(s).strip().upper().replace(".", "")
    m = re.match(r"^(\d{1,2}):(\d{2})\s*(AM|PM)?$", s)
    if not m:
        return None
    h, mi, ap = int(m[1]), int(m[2]), m[3]
    if ap == "PM" and h < 12:
        h += 12
    if ap == "AM" and h == 12:
        h = 0
    if h > 23 or mi > 59:
        return None
    return fmt12(h, mi)


def minutes(s):
    s = norm_time(s)
    if not s:
        return None
    hm, ap = s.split()
    h, m = map(int, hm.split(":"))
    return (h % 12 + (12 if ap == "PM" else 0)) * 60 + m


# ---------------------------------------------------------------- live sources

def src_masjidal(mid, today):
    d = today.isoformat()
    j = http_json(f"https://masjidal.com/api/v1/time/range?masjid_id={urllib.parse.quote(mid)}"
                  f"&from_date={d}&to_date={d}", timeout=20)
    if j.get("status") != "success" or not j.get("data", {}).get("salah"):
        raise RuntimeError(f"masjidal status {j.get('status')}")
    s, q = j["data"]["salah"][0], (j["data"]["iqamah"] or [{}])[0]
    day = {
        "date": s["date"],
        "hijri": f'{s.get("hijri_date", "")} {s.get("hijri_month", "")}'.strip(),
        "adhan": {k2: norm_time(s.get(k1)) for k1, k2 in
                  [("fajr", "fajr"), ("sunrise", "sunrise"), ("zuhr", "dhuhr"),
                   ("asr", "asr"), ("maghrib", "maghrib"), ("isha", "isha")]},
        "iqamah": {k2: norm_time(q.get(k1)) for k1, k2 in
                   [("fajr", "fajr"), ("zuhr", "dhuhr"), ("asr", "asr"),
                    ("maghrib", "maghrib"), ("isha", "isha")]},
    }
    day["iqamah"] = {k: v for k, v in day["iqamah"].items() if v}
    jumuah = []
    for k in sorted(q):
        t = norm_time(q[k]) if k.startswith("jummah") else None
        if t and t not in [x["start"] for x in jumuah]:
            jumuah.append({"start": t})
    if not any(day["adhan"].values()):
        raise RuntimeError("masjidal: empty timetable")
    return {"days": [day], "jumuah": jumuah, "source": "Masjidal (set by masjid)"}


def src_masjidal_jumuah(mid):
    """Every Jumu'ah slot from Masjidal's public widget; the time API only returns the first two."""
    h = http_get(f"https://timing.athanplus.com/masjid/widgets/embed?theme=1&masjid_id={urllib.parse.quote(mid)}",
                 timeout=20)
    blk = re.search(r'class="testing-sec"(.*?)</ul>', h, re.S)
    out = []
    for t, _n in re.findall(r"<b>\s*([^<]*?)\s*</b>\s*<p[^>]*>\s*Jum[a-z']*\s*(\d)", blk.group(1) if blk else "", re.I):
        t = norm_time(t)
        if t and t not in [x["start"] for x in out]:
            out.append({"start": t})
    return out


_HIJRI = ["Muharram", "Safar", "Rabi Al-Awwal", "Rabi Al-Thani", "Jumada Al-Awwal", "Jumada Al-Thani",
          "Rajab", "Shaban", "Ramadan", "Shawwal", "Dhul-Qadah", "Dhul-Hijjah"]


def src_masjiddashboard(cid, today, n=7):
    """MasjidDashboard prayer-time widget (masjiddashboard.com), one request per day."""
    days = []
    for i in range(n):
        d = today + dt.timedelta(days=i)
        j = http_json(f"https://masjiddashboard.com/api/prayer/companyId/{urllib.parse.quote(cid)}"
                      f"/month/{d.month}/day/{d.day}", timeout=20)
        t = j.get("target") if j.get("successful") else None
        if not t:
            if i == 0:
                raise RuntimeError(f"masjiddashboard: {j.get('message')}")
            break
        hy, hm, hd = (t.get("hijriString") or "0/0/0").split("/")
        adhan = {k2: norm_time(t.get(k1)) for k1, k2 in
                 [("fajr", "fajr"), ("sunrise", "sunrise"), ("dhuhr", "dhuhr"),
                  ("asr", "asr"), ("maghrib", "maghrib"), ("isha", "isha")]}
        iq = {}
        for k in ["fajr", "dhuhr", "asr", "maghrib", "isha"]:
            v = (t.get(k + "Iqama") or "").strip()
            off = re.match(r"^\+?(\d+)\s*min", v, re.I)  # "5 Mins" = minutes after the adhan
            if off and adhan.get(k):
                v = fmt12(*divmod(minutes(adhan[k]) + int(off[1]), 60))
            if norm_time(v):
                iq[k] = norm_time(v)
        days.append({"date": d.strftime("%A, %b %d, %Y").replace(" 0", " "),
                     "hijri": f"{int(hd)}, {hy} {_HIJRI[int(hm) - 1]}" if hm.isdigit() and 0 < int(hm) <= 12 else "",
                     "adhan": adhan, "iqamah": iq})
    jumuah = []
    try:
        cfg = http_json(f"https://masjiddashboard.com/api/auth/companies/{urllib.parse.quote(cid)}/configurations",
                        timeout=20)
        txt = next((c.get("value") or "" for c in cfg if c.get("name") == "jumah_prayer"), "")
        for h, mi, ap in re.findall(r"(?<![\d.])(\d{1,2})(?:[:.](\d{2}))?\s*([ap]\.?m\.?)?(?![\d]|st|nd|rd|th)", txt, re.I):
            h, mi = int(h), int(mi or 0)
            ap = (ap or "").lower()[:1] or ("a" if 9 <= h <= 11 else "p")  # bare "2:00" means 2 PM
            t = norm_time(f"{h}:{mi:02d} {ap}m")
            if t and 660 <= minutes(t) <= 960 and t not in [j["start"] for j in jumuah]:
                jumuah.append({"start": t})
    except Exception:
        pass
    return {"days": days, "jumuah": jumuah, "source": "MasjidDashboard (the masjid's website)"}


def _text(h):
    h = re.sub(r"<script.*?</script>|<style.*?</style>", " ", h, flags=re.S | re.I)
    h = re.sub(r"<[^>]+>", " ", h)
    return re.sub(r"\s+", " ", html.unescape(h).replace("\xa0", " "))


def src_mohid(url, today):
    h = http_get(url)
    t = _text(h)
    out = {"source": "Mohid page"}
    kh = dict(re.findall(r"Friday Khutba\s*(\d)\s*(\d{1,2}:\d{2}\s*[AP]M)", t, re.I))
    iq = dict(re.findall(r"Friday Iqama\s*(\d)\s*(\d{1,2}:\d{2}\s*[AP]M)", t, re.I))
    out["jumuah"] = [{k: v for k, v in {"khutbah": norm_time(kh.get(n)), "iqamah": norm_time(iq.get(n)),
                                        "start": norm_time(kh.get(n) or iq.get(n))}.items() if v}
                     for n in sorted(set(kh) | set(iq))]
    names = {"fajar": "fajr", "fajr": "fajr", "duhar": "dhuhr", "dhuhr": "dhuhr", "zuhr": "dhuhr",
             "asr": "asr", "maghrib": "maghrib", "isha": "isha"}
    iqd, adh = {}, {}
    for nm, a, b in re.findall(r"\b(Fajar|Fajr|Duhar|Dhuhr|Zuhr|Asr|Maghrib|Isha)\s+"
                               r"(\d{1,2}:\d{2}\s*[AP]M)\s+(\d{1,2}:\d{2}\s*[AP]M)", t, re.I):
        k = names[nm.lower()]
        iqd.setdefault(k, norm_time(a))
        adh.setdefault(k, norm_time(b))
    if iqd:
        out["days"] = [{"date": today.strftime("%A, %b %-d, %Y"), "adhan": adh, "iqamah": iqd}]
    m = re.search(r"events:\s*(\[.*?\])\s*,\s*timeFormat", h, re.S)
    events = []
    if m:
        try:
            for e in json.loads(m[1]):
                events.append({"title": html.unescape(e.get("title", "")), "start": e.get("start"),
                               "url": e.get("url") or url})
        except json.JSONDecodeError:
            pass
    out["events"] = events
    return out


_JLAB = r"(?<![a-z])(?:jum(?![aā]d)[uʿ'’‘a]*\w*|friday\s+(?:prayers?|salah|khutb\w*|sermon)|khut?b\w*)"
_JTIME = r"(\d{1,2}(?::\d{2})?\s*[ap]\.?\s?m\b\.?)"


def _clock(x):
    x = re.sub(r"[\s.]", "", x).upper()
    return norm_time(x if ":" in x else x[:-2] + ":00" + x[-2:])


def jumuah_on_page(t, label=None):
    """Times written next to a Jumu'ah label in page text. With no label, any midday time near a
    Jumu'ah/Friday/khutbah word (11 AM-3:30 PM), the fingerprint used to notice a change."""
    pat = (rf"(?:{label})\s*(?:at|:|-)?\s*" if label else _JLAB + r"[^.|\n]{0,40}?") + _JTIME
    out = []
    for x in re.findall(pat, t, re.I):
        x = _clock(x)
        if x and x not in out and (label or 660 <= minutes(x) <= 930):
            out.append(x)
    return out if label else sorted(out, key=minutes)


def watch_jumuah(m):
    """Re-read a hand-entered Jumu'ah time from the masjid's own page. Returns an alert string or None.
    With "label", a new time is used straight away. With "seen", the hand entry stays and a change
    only raises an alert, so a misread page never puts a wrong time on the site."""
    w = m["jumuah_watch"]
    hand = [norm_time(j.get("start")) for j in m.get("jumuah") or []]
    site = re.sub(r"^https?://(www[.])?|/$", "", w["url"])
    try:
        t = _text(http_get(w["url"], ua="Mozilla/5.0 (compatible; USAMasjidFinder)"))
    except Exception as e:
        return f"could not open {w['url']} ({str(e)[:80]}); the site still shows {', '.join(hand)}"
    found = jumuah_on_page(t, w.get("label"))
    if not found:
        return f"the Jumu'ah times are gone from {w['url']}; the site still shows {', '.join(hand)}"
    if w.get("label"):
        m["jumuah"] = [{"start": x} for x in found]
        if found != hand:
            m["jumuah_source"] = f"From the masjid's website ({site}), checked daily"
            return (f"Jumu'ah changed on {w['url']}: was {', '.join(hand)}, now {', '.join(found)}. "
                    "The site uses the new time; update mosques_seed.json to match.")
    elif found != w.get("seen"):
        return (f"the Jumu'ah times on {w['url']} changed: the page used to show {', '.join(w.get('seen') or [])}, "
                f"now {', '.join(found)}. The site still shows {', '.join(hand)}; check the page and update "
                "mosques_seed.json (jumuah and jumuah_watch.seen).")
    note = f"; {m['jumuah_note']}" if m.get("jumuah_note") and not m["jumuah_note"].startswith("from ") else ""
    m["jumuah_source"] = f"From the masjid's website ({site}), checked daily{note}"
    return None


def src_wp_events(base, today):
    j = http_json(f"{base.rstrip('/')}/wp-json/tribe/events/v1/events?per_page=10&start_date={today.isoformat()}")
    return [{"title": html.unescape(re.sub(r"<[^>]+>", "", e.get("title", ""))),
             "start": e.get("start_date"), "url": e.get("url")} for e in j.get("events", [])]


class Discover:
    """Scan masjid homepages for Masjidal / Mohid widgets and event feeds. Cached ~1 week per site."""

    def __init__(self):
        self.path = DATA / "discover_cache.json"
        self.cache = json.loads(self.path.read_text()) if self.path.exists() else {}

    def fresh(self, site):
        c = self.cache.get(site)
        if not c:
            return False
        ttl = DISCOVER_TTL_DAYS + int(hashlib.md5(site.encode()).hexdigest(), 16) % 7  # spread re-checks out
        return (dt.date.today() - dt.date.fromisoformat(c["at"])).days < ttl

    def scan(self, site):
        found = {}
        try:
            h = http_get(site, timeout=12)
            ids = re.findall(r"masjid_id=([A-Za-z0-9]{6,12})", h)
            if ids:
                found["masjidal_id"] = ids[0]
            mo = re.search(r"https?://(?:us\.)?mohid\.co/[a-z]{2}/[\w-]+/[\w-]+", h)
            if mo:
                found["mohid_url"] = mo[0]
            if "tribe-events" in h or "tribe_events" in h:
                p = urllib.parse.urlparse(site)
                found["events_wp"] = f"{p.scheme}://{p.netloc}"
        except Exception as e:
            found["site_error"] = str(e)[:100]
        return site, found

    def run(self, sites):
        todo = [s for s in set(sites) if not self.fresh(s)]
        log(f"Website scan: {len(todo)} to check ({len(set(sites)) - len(todo)} cached)")
        with cf.ThreadPoolExecutor(max_workers=32) as ex:
            for n, (site, found) in enumerate(ex.map(self.scan, todo), 1):
                self.cache[site] = {"at": dt.date.today().isoformat(), "found": found}
                if n % 200 == 0:
                    log(f"  …{n}/{len(todo)}")
                    self.save()
        self.save()

    def save(self):
        self.path.write_text(json.dumps(self.cache, indent=0, sort_keys=True))

    def get(self, site):
        return (self.cache.get(site) or {}).get("found", {})


# ---------------------------------------------------------------- mosque list

def load_osm():
    cache = DATA / "osm_cache.json"
    body = urllib.parse.urlencode({"data": OSM_QUERY}).encode()
    for url in OVERPASS:
        try:
            j = http_json(url, data=body, timeout=400, ua=GEO_UA)
            codes = sum(1 for e in j.get("elements", []) if e.get("type") == "st")
            if codes >= 45:  # a partial answer would silently drop whole states
                cache.write_text(json.dumps(j))
                log(f"OSM: {len(j['elements'])} elements, {codes} states, from {url}")
                return j["elements"], "live"
            log(f"OSM {url}: only {codes} states in answer")
        except Exception as e:
            log(f"OSM {url}: {str(e)[:80]}")
    if cache.exists():
        log("OSM unavailable, using cached list")
        return json.loads(cache.read_text())["elements"], "cached"
    raise SystemExit("Could not reach OpenStreetMap and no usable cache exists.")


def osm_to_mosques(elements):
    out, st = [], None
    for e in elements:
        if e.get("type") == "st":
            code = e["tags"].get("code", "")[3:]
            st = code if code in STATES else None
            continue
        t = e.get("tags", {})
        if not st or not t.get("name"):
            continue
        c = e.get("center", e)
        street = " ".join(x for x in [t.get("addr:housenumber"), t.get("addr:street")] if x)
        if t.get("addr:unit"):
            street += f" {t['addr:unit']}"
        out.append({
            "osm_id": f"{e['type']}/{e['id']}",
            "name": t["name"].strip(),
            "street": street or None,
            "zip": t.get("addr:postcode"),
            "city": t.get("addr:city"),
            "state": st,
            "lat": c.get("lat"), "lon": c.get("lon"),
            "phone": t.get("phone") or t.get("contact:phone"),
            "website": t.get("website") or t.get("contact:website"),
        })
    # the same building can sit on a state line; keep the first
    seen, uniq = set(), []
    for m in out:
        if m["osm_id"] not in seen:
            seen.add(m["osm_id"])
            uniq.append(m)
    return uniq


def load_masjidal_directory():
    """Every masjid registered on Masjidal, filtered to the US. ~47 pages; cached for when it's down."""
    cache = DATA / "masjidal_cache.json"
    try:
        out, page, pages = [], 1, 1
        while page <= pages:
            d = json.loads(http_get(f"https://masjidal.com/api/v2/masjids?per-page=50&page={page}",
                                    ua=GEO_UA, timeout=30))
            pages = d["_meta"]["pageCount"]
            out += [i for i in d["items"] if (i.get("country") or "").strip().upper() in ("US", "USA", "UNITED STATES")]
            page += 1
            time.sleep(0.3)
        cache.write_text(json.dumps(out, indent=0))
        log(f"Masjidal directory: {len(out)} US masjids")
        return out
    except Exception as e:
        log(f"Masjidal directory unavailable ({str(e)[:60]}), using cache")
        return json.loads(cache.read_text()) if cache.exists() else []


def _clean(v):
    v = (v or "").strip()
    return None if v in ("", "-") else re.sub(r"\s+", " ", v)


_ACRONYM = re.compile(r"^[A-Z]{2,6}$")


def _nice_name(n, st):
    n = _clean(n) or ""
    n = re.sub(rf"(?<!\bof)(?<!\bOF),?\s+{st}$", "", n)  # "… Rochester MN" -> "… Rochester", keeps "… of DC"
    if n.isupper() and len(n) > 8:  # "MASJID AL-HIKMA ASSOCIATION" -> "Masjid Al-Hikma Association"
        n = re.sub(r"[A-Za-z][A-Za-z']*",
                   lambda w: w[0] if (len(w[0]) <= 4 and not re.search(r"[AEIOU]", w[0][1:]))
                   else w[0][0].upper() + w[0][1:].lower(), n)
    return n


def masjidal_to_mosque(i):
    st = norm_state(i.get("state"), i.get("zipcode"))
    if st not in STATES:
        return None
    city = _clean(i.get("city")) or ""
    city = re.sub(rf",?\s*({st}|{STATES[st][0]})\b.*$", "", city, flags=re.I).strip(" ,") or None
    street = (_clean(i.get("address")) or "").split(",")[0] or None
    try:
        lat, lon = float(i["latitude"]), float(i["longitude"])
        if not (17 < lat < 72 and -180 < lon < -60):
            lat = lon = None
    except (KeyError, TypeError, ValueError):
        lat = lon = None
    raw = _clean(i.get("name")) or ""
    return {"name": _nice_name(raw, st), "caps": raw.isupper(), "street": street, "zip": _clean(i.get("zipcode")),
            "city": city.title() if city and city.islower() else city, "state": st, "lat": lat, "lon": lon,
            "phone": _clean(i.get("phone")), "website": _clean(i.get("website_url")),
            "masjidal_id": i["id"], "from_directory": True}


_DIRS = r"(?:n|s|e|w|ne|nw|se|sw|north|south|east|west|northeast|northwest|southeast|southwest)"


def addr_key(a):
    m = re.match(rf"\s*(\d+)\s+(?:{_DIRS}\.?\s+)?([a-z0-9]+)", (a or "").lower())
    return f"{m[1]} {m[2]}" if m else None


_GENERIC = {"masjid", "masjed", "mosque", "islamic", "center", "centre", "of", "the", "community", "cultural",
            "association", "al", "inc", "society", "foundation", "and"}


def core_name(n):
    words = re.findall(r"[a-z0-9]+", (n or "").lower().replace("-", " "))
    return " ".join(w for w in words if w not in _GENERIC)


def find_same(m, pool, near_km=0.06):
    for o in pool:
        if o is m:
            continue
        if m.get("masjidal_id") and o.get("masjidal_id") == m["masjidal_id"]:
            return o
        if m.get("osm_id") and o.get("osm_id") == m["osm_id"]:
            return o
    if m.get("lat") is None:
        ka, cm = addr_key(m.get("street")), core_name(m.get("name"))
        for o in pool:
            if o is not m and ((ka and ka == addr_key(o.get("street")) and (m.get("city") or "").lower() == (o.get("city") or "").lower())):
                return o
        return None
    for o in pool:
        if o is m or o.get("lat") is None:
            continue
        d = km(m["lat"], m["lon"], o["lat"], o["lon"])
        if d > 12:
            continue
        ka, kb = addr_key(m.get("street")), addr_key(o.get("street"))
        if ka and ka == kb and d < 3:
            return o
        if m.get("from_directory") and o.get("from_directory"):
            continue
        if d < near_km:
            return o
        cm, co = core_name(m.get("name")), core_name(o.get("name"))
        if cm and cm == co and d < 10:
            return o
    return None


class Geo:
    """Nominatim reverse geocoding for city names, cached on disk (1 request/second, per OSM policy)."""

    def __init__(self):
        self.path = DATA / "geocode_cache.json"
        self.cache = json.loads(self.path.read_text()) if self.path.exists() else {}
        self.new = 0

    def _get(self, key, url):
        if key in self.cache:
            return self.cache[key]
        if self.new >= MAX_NEW_GEOCODES:
            return None
        wait = 1.1 - (time.time() - _last_geo[0])
        if wait > 0:
            time.sleep(wait)
        _last_geo[0] = time.time()
        self.new += 1
        try:
            self.cache[key] = http_json(url, ua=GEO_UA, timeout=20)
        except Exception as e:
            log(f"geocode {key}: {str(e)[:80]}")
            return None
        if self.new % 25 == 0:
            self.save()
            log(f"  geocoded {self.new}")
        return self.cache[key]

    def save(self):
        self.path.write_text(json.dumps(self.cache, indent=0))

    def forward(self, address):
        q = urllib.parse.urlencode({"q": address, "format": "jsonv2", "limit": 1, "countrycodes": "us"})
        r = self._get("f:" + address, f"{NOMINATIM}/search?{q}")
        return (float(r[0]["lat"]), float(r[0]["lon"])) if r else None

    def city(self, lat, lon):
        q = urllib.parse.urlencode({"lat": lat, "lon": lon, "format": "jsonv2", "zoom": 14})
        r = self._get(f"r:{lat:.5f},{lon:.5f}", f"{NOMINATIM}/reverse?{q}") or {}
        a = r.get("address", {})
        return a.get("city") or a.get("town") or a.get("village") or a.get("hamlet") or a.get("suburb")


def build_list():
    seed_file = json.loads((ROOT / "mosques_seed.json").read_text())
    exclude = set(seed_file.get("exclude_masjidal", []))
    elements, osm_state = load_osm()
    mosques = osm_to_mosques(elements)
    log(f"OSM: {len(mosques)} named masajid")
    by_state = defaultdict(list)
    for m in mosques:
        by_state[m["state"]].append(m)

    added = merged = 0
    for i in load_masjidal_directory():
        if i["id"] in exclude:
            continue
        d = masjidal_to_mosque(i)
        if not d or not d["name"]:
            continue
        pool = by_state[d["state"]]
        tgt = find_same(d, pool)
        if tgt is None:
            pool.append(d)
            mosques.append(d)
            added += 1
        elif not tgt.get("masjidal_id"):
            merged += 1
            tgt["masjidal_id"] = d["masjidal_id"]
            for k in ("phone", "website", "street", "zip", "city"):
                tgt[k] = tgt.get(k) or d[k]
            if tgt.get("osm_id") and core_name(tgt["name"]) != core_name(d["name"]):
                keep = d["name"] if (not d["caps"] and len(d["name"]) >= len(tgt["name"])) else tgt["name"]
                tgt["aka"] = d["name"] if keep == tgt["name"] else tgt["name"]
                tgt["name"] = keep
    log(f"Masjidal: {merged} matched to OSM, {added} added")

    geo = Geo()
    by_osm = {m["osm_id"]: m for m in mosques if m.get("osm_id")}
    by_mid = {m["masjidal_id"]: m for m in mosques if m.get("masjidal_id")}
    by_name = defaultdict(list)
    for m in mosques:
        by_name[(m["name"].lower(), m["state"])].append(m)
    _site = lambda u: re.sub(r"^https?://(www[.])?", "", u or "").rstrip("/").lower()

    def named(s):
        """The masjid a seed entry names; when two share a name, the one whose website it was read from."""
        c = by_name.get((s.get("name", "").lower(), s.get("state")), [])
        if len(c) > 1:
            web = _site(s.get("website") or (s.get("jumuah_watch") or {}).get("url"))
            c = [m for m in c if web and _site(m.get("website")) == web] or c[-1:]
        return c[0] if c else None
    for s in seed_file["mosques"]:
        s = {k: v for k, v in s.items() if k != "ref"}
        if "address" in s and "street" not in s:
            s["street"] = s["address"].split(",")[0]
        tgt = (by_osm.get(s.get("osm_id")) or by_mid.get(s.get("masjidal_id"))
               or named(s))
        if not tgt and s.get("lat") is None and s.get("address"):
            ll = geo.forward(s["address"])
            if ll:
                s["lat"], s["lon"] = ll
        if not tgt and not s.get("osm_id") and s.get("state") in STATES:
            tgt = find_same(s, by_state[s["state"]], near_km=0.08)
        if tgt:
            if tgt.get("lat") is not None:
                s.pop("lat", None), s.pop("lon", None)
            tgt.update({k: v for k, v in s.items() if v is not None and k != "address"})
        elif s.get("name") and s.get("state") in STATES:
            mosques.append(s)

    missing = [m for m in mosques if not m.get("city") and m.get("lat") is not None]
    log(f"Cities: {len(missing)} to look up (cached where possible)")
    for m in missing:
        m["city"] = geo.city(m["lat"], m["lon"])
    geo.save()

    ids = set()
    for m in mosques:
        c = (m.get("city") or "").strip()
        c = re.sub(r"^(st|saint)\.?\s+", "St. ", c, flags=re.I)
        if c.islower() or c.isupper():
            c = c.title()
        m["city"] = c or None
        line2 = " ".join(x for x in [m["state"], m.get("zip")] if x)
        m["address"] = ", ".join(x for x in [m.get("street"), m.get("city"), line2] if x)
        m["tz"] = tz_for(m["state"], m.get("lat"), m.get("lon"))
        base = re.sub(r"[^a-z0-9]+", "-", f"{m['name']}-{m.get('city') or ''}-{m['state']}".lower()).strip("-")
        mid, n = base, 2
        while mid in ids:
            mid, n = f"{base}-{n}", n + 1
        ids.add(mid)
        m["id"] = mid
    return mosques, osm_state


def enrich(m, disc):
    today = dt.datetime.now(ZoneInfo(m["tz"])).date()
    m["events"], m["errors"] = [], []
    sources = []
    if m.get("website"):
        for k, v in disc.get(m["website"]).items():
            if k != "site_error":
                m.setdefault(k, v)
    if m.get("masjiddashboard_id"):  # what the masjid's own website shows; wins over a stale Masjidal account
        try:
            r = src_masjiddashboard(m["masjiddashboard_id"], today)
            m["days"], m["timing_source"] = r["days"], r["source"]
            if r["jumuah"] and not m.get("jumuah"):  # a hand-entered Jumu'ah wins
                m["jumuah"], m["jumuah_source"] = r["jumuah"], r["source"]
            m.pop("masjidal_id", None)  # stop visitors' browsers overwriting it with Masjidal times
            sources.append("masjiddashboard")
        except Exception as e:
            m["errors"].append(f"masjiddashboard: {str(e)[:100]}")
    if m.get("masjidal_id"):
        try:
            r = src_masjidal(m["masjidal_id"], today)
            m["days"], m["timing_source"] = r["days"], r["source"]
            try:
                w = src_masjidal_jumuah(m["masjidal_id"])
                if len(w) > len(r["jumuah"]):
                    r["jumuah"] = w
            except Exception:
                pass
            if r["jumuah"]:
                m["jumuah"], m["jumuah_source"] = r["jumuah"], r["source"]
            sources.append("masjidal")
        except Exception as e:
            m["errors"].append(f"masjidal: {str(e)[:100]}")
            m["masjidal_dead"] = True
    if m.get("mohid_url"):
        try:
            r = src_mohid(m["mohid_url"], today)
            if r.get("jumuah") and not m.get("jumuah_source"):
                m["jumuah"], m["jumuah_source"] = r["jumuah"], r["source"]
            if r.get("days") and not m.get("days"):
                m["days"], m["timing_source"] = r["days"], r["source"]
            m["events"] += r.get("events", [])
            sources.append("mohid")
        except Exception as e:
            m["errors"].append(f"mohid: {str(e)[:100]}")
    if m.get("events_wp"):
        try:
            m["events"] += src_wp_events(m["events_wp"], today)
        except Exception as e:
            m["errors"].append(f"events: {str(e)[:100]}")
    if m.get("jumuah_watch") and not m.get("jumuah_source"):
        a = watch_jumuah(m)
        if a:
            m["watch_alert"] = a
    if m.get("jumuah") and not m.get("jumuah_source"):  # typed in by hand in the seed
        for j in m["jumuah"]:
            j["start"] = norm_time(j.get("start") or j.get("khutbah") or j.get("iqamah"))
        m["jumuah_source"] = "Entered by hand" + (f" – {m['jumuah_note']}" if m.get("jumuah_note") else "")
    now = today.isoformat()
    m["events"] = sorted([e for e in m["events"] if (e.get("start") or "")[:10] >= now],
                         key=lambda e: e.get("start") or "")[:5]
    seen = set()  # drop empty and repeated slots (some pages list the same one several times)
    m["jumuah"] = [j for j in (m.get("jumuah") or []) if j.get("start") and not (j["start"] in seen or seen.add(j["start"]))]
    for j in m["jumuah"]:
        j["start_min"] = minutes(j["start"])
    m["live"] = bool(sources)
    return m


KEEP = ["id", "name", "aka", "address", "city", "state", "lat", "lon", "tz", "phone", "website",
        "masjidal_id", "mohid_url", "jumuah", "jumuah_source", "days", "timing_source", "events", "live"]


def main():
    DATA.mkdir(exist_ok=True)
    mosques, osm_state = build_list()
    disc = Discover()
    disc.run([m["website"] for m in mosques if m.get("website") and re.match(r"https?://", m["website"])])
    log(f"{len(mosques)} masajid; fetching timings…")
    with cf.ThreadPoolExecutor(max_workers=16) as ex:
        mosques = list(ex.map(lambda m: enrich(m, disc), mosques))
    errors = {m["id"]: m["errors"] for m in mosques if m["errors"]}
    for m in mosques:
        if m.pop("masjidal_dead", False):
            m.pop("masjidal_id", None)  # don't make every visitor's browser retry a dead feed
    mosques.sort(key=lambda m: (m["state"], m["name"].lower()))
    slim = [{k: m[k] for k in KEEP if m.get(k) not in (None, "", [], {})} for m in mosques]
    out = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="minutes"),
        "osm": osm_state,
        "count": len(slim),
        "states": {k: v[0] for k, v in STATES.items()},
        "mosques": slim,
    }
    (DATA / "masajid.json").write_text(json.dumps(out, ensure_ascii=False, separators=(",", ":")))
    (DATA / "masajid.js").write_text("window.MASAJID = " + json.dumps(out, ensure_ascii=False, separators=(",", ":")) + ";\n")
    alerts = {f"{m['name']} ({m['state']})": m["watch_alert"] for m in mosques if m.get("watch_alert")}
    (DATA / "watch_alerts.json").write_text(json.dumps(alerts, indent=1, ensure_ascii=False))
    for k, v in alerts.items():
        log(f"WATCH {k}: {v}")
    (DATA / "errors.json").write_text(json.dumps(errors, indent=1, ensure_ascii=False))
    live = sum(1 for m in mosques if m["live"])
    jm = sum(1 for m in mosques if m["jumuah"])
    ev = sum(len(m["events"]) for m in mosques)
    per = defaultdict(int)
    for m in mosques:
        per[m["state"]] += 1
    log(f"Done: {len(mosques)} masajid in {len(per)} states, {live} with live times, "
        f"{jm} with Jumu'ah times, {ev} upcoming events. {len(errors)} with fetch errors (data/errors.json).")
    log("  " + ", ".join(f"{k} {v}" for k, v in sorted(per.items())))


if __name__ == "__main__":
    main()
